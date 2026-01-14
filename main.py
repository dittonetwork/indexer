import json
import logging
import os
from workers.chain_worker import ChainWorker
from workers.meta_filler import MetaFillerWorker
from workers.wasm_filler import WasmFillerWorker
from core.database import Database
from config import MONGO_URI, DB_NAME, ENV, DEFAULT_LAST_PROCESSED_BLOCK, FRESH_START


def main():
    """
    Main function to initialize the database, load configuration,
    and start all the worker threads.
    """
    # --- Configuration and DB Initialization ---
    db = Database(MONGO_URI, DB_NAME, fresh_start=FRESH_START)

    # --- Load and Process Chains Configuration ---
    config_file = f"chains_config.{ENV}.json"
    try:
        with open(config_file) as f:
            raw_config = json.load(f)
        logging.info(f"Loaded configuration from {config_file}")
    except FileNotFoundError:
        logging.error(f"{config_file} not found. Exiting.")
        return
    except json.JSONDecodeError:
        logging.error(f"{config_file} is not a valid JSON file. Exiting.")
        return

    # The config can be a list of chain objects, or a dict where keys are chain IDs.
    if isinstance(raw_config, dict):
        # Filter out non-dictionary values (like global settings) and only keep chain configurations
        chains_from_file = [value for value in raw_config.values() if isinstance(value, dict)]
    elif isinstance(raw_config, list):
        chains_from_file = raw_config
    else:
        logging.error(
            f"{config_file} must contain a list or a dictionary of chain configurations."
        )
        return

    # Extract global settings from config
    parse_retry_delay = raw_config.get("parse_retry_delay_seconds", 5)
    
    # Process chains config to allow for environment variable overrides for RPCs and last_processed_block
    chains_config = []
    for chain in chains_from_file:
        chain_id_str = str(chain.get("global_chain_id"))
        env_var = f"RPC_URL_{chain_id_str}"
        rpc_url_from_env = os.getenv(env_var)
        if rpc_url_from_env:
            chain["rpc_url"] = rpc_url_from_env
            logging.info(f"Using {env_var} from environment for chain {chain_id_str}.")
        
        # Get last_processed_block from config file (default to DEFAULT_LAST_PROCESSED_BLOCK if not present)
        config_last_block = chain.get("last_processed_block", DEFAULT_LAST_PROCESSED_BLOCK)
        chain["last_processed_block"] = config_last_block
        
        # Override with environment variable if present
        last_block_env_var = f"LAST_PROCESSED_BLOCK_{chain_id_str}"
        last_block_from_env = os.getenv(last_block_env_var)
        if last_block_from_env and last_block_from_env.strip():
            try:
                last_block_value = int(last_block_from_env)
                if last_block_value < 0:
                    raise ValueError(f"Last processed block cannot be negative: {last_block_value}")
                chain["last_processed_block"] = last_block_value
                logging.info(f"Using {last_block_env_var} from environment for chain {chain_id_str} (overriding config value {config_last_block}).")
            except ValueError as e:
                logging.error(f"Invalid value for {last_block_env_var}: {last_block_from_env}. Error: {e}")
                raise ValueError(f"Invalid last_processed_block value for chain {chain_id_str}: {last_block_from_env}")
        else:
            logging.info(f"Using last_processed_block from config ({config_last_block}) for chain {chain_id_str}.")
        
        # Apply global parse retry delay to all chains
        chain["parse_retry_delay_seconds"] = parse_retry_delay
        
        chains_config.append(chain)

    # --- Database Synchronization for Chains ---
    with db.db_session() as session:
        existing_chains_in_db = {
            chain["global_chain_id"] 
            for chain in db.get_all_chains(session=session)
            if chain.get("global_chain_id") is not None
        }

        for chain in chains_config:
            chain_id = chain["global_chain_id"]
            if chain_id not in existing_chains_in_db:
                # Add only essential info to DB, not the full config
                # Initialize WASM last processed block from config, or use regular registry block as fallback
                wasm_last_block = chain.get("wasm_last_processed_block", chain.get("last_processed_block", 0))
                db.insert_chain(
                    {
                        "global_chain_id": chain_id,
                        "last_processed_block": chain.get("last_processed_block", 0),
                        "wasm_last_processed_block": wasm_last_block,
                        "is_synced": False,
                    },
                    session=session,
                )
                logging.info(f"Added new chain {chain_id} to database.")

    # --- Worker Initialization ---
    # Merge DB state with config for workers
    all_chains_from_db = {
        c["global_chain_id"]: c 
        for c in db.get_all_chains() 
        if c.get("global_chain_id") is not None
    }
    worker_configs = []
    for chain in chains_config:
        chain_id = chain["global_chain_id"]
        if chain_id in all_chains_from_db:
            db_doc = all_chains_from_db[chain_id]
            # Config from file is source of truth, except for last_processed_block
            merged_config = chain.copy()
            merged_config["last_processed_block"] = db_doc["last_processed_block"]
            # Load WASM last processed block from DB, or use regular registry block if not set
            merged_config["wasm_last_processed_block"] = db_doc.get(
                "wasm_last_processed_block", 
                db_doc["last_processed_block"]
            )
            worker_configs.append(merged_config)

    threads = []

    # Start a worker for each chain
    for chain_doc in worker_configs:
        worker = ChainWorker(chain_doc, db)
        worker.start()
        threads.append(worker)

    # Start the metadata filler worker
    meta_filler = MetaFillerWorker(db)
    meta_filler.start()
    threads.append(meta_filler)

    # Start the WASM code filler worker
    wasm_filler = WasmFillerWorker(db)
    wasm_filler.start()
    threads.append(wasm_filler)

    # --- Keep Main Thread Alive ---
    logging.info("All workers started. Indexer is running.")
    for t in threads:
        t.join()


if __name__ == "__main__":
    main()
