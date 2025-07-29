import json
import logging
import os
from workers.chain_worker import ChainWorker
from workers.meta_filler import MetaFillerWorker
from core.database import Database
from config import MONGO_URI, DB_NAME


def main():
    """
    Main function to initialize the database, load configuration,
    and start all the worker threads.
    """
    # --- Configuration and DB Initialization ---
    db = Database(MONGO_URI, DB_NAME, fresh_start=False)

    # --- Load and Process Chains Configuration ---
    try:
        with open("chains_config.json") as f:
            raw_config = json.load(f)
    except FileNotFoundError:
        logging.error("chains_config.json not found. Exiting.")
        return
    except json.JSONDecodeError:
        logging.error("chains_config.json is not a valid JSON file. Exiting.")
        return

    # The config can be a list of chain objects, or a dict where keys are chain IDs.
    if isinstance(raw_config, dict):
        chains_from_file = list(raw_config.values())
    elif isinstance(raw_config, list):
        chains_from_file = raw_config
    else:
        logging.error(
            "chains_config.json must contain a list or a dictionary of chain configurations."
        )
        return

    # Process chains config to allow for environment variable overrides for RPCs and last_processed_block
    chains_config = []
    for chain in chains_from_file:
        chain_id_str = str(chain.get("global_chain_id"))
        env_var = f"RPC_{chain_id_str}"
        rpc_url_from_env = os.getenv(env_var)
        if rpc_url_from_env:
            chain["rpc_url"] = rpc_url_from_env
            logging.info(f"Using {env_var} from environment for chain {chain_id_str}.")
        
        last_block_env_var = f"LAST_PROCESSED_BLOCK_{chain_id_str}"
        last_block_from_env = os.getenv(last_block_env_var)
        if not last_block_from_env or not last_block_from_env.strip():
            raise ValueError(f"Environment variable {last_block_env_var} is required for chain {chain_id_str} but is not set or empty")
        
        try:
            last_block_value = int(last_block_from_env)
            if last_block_value < 0:
                raise ValueError(f"Last processed block cannot be negative: {last_block_value}")
            chain["last_processed_block"] = last_block_value
            logging.info(f"Using {last_block_env_var} from environment for chain {chain_id_str}.")
        except ValueError as e:
            logging.error(f"Invalid value for {last_block_env_var}: {last_block_from_env}. Error: {e}")
            raise ValueError(f"Invalid last_processed_block value for chain {chain_id_str}: {last_block_from_env}")
            
        chains_config.append(chain)

    # Validate that all chains have valid last_processed_block values
    for chain in chains_config:
        chain_id = chain["global_chain_id"]
        last_block = chain.get("last_processed_block")
        if last_block is None:
            raise ValueError(f"last_processed_block is required for chain {chain_id} but is None")
        if last_block < 0:
            raise ValueError(f"last_processed_block for chain {chain_id} cannot be negative: {last_block}")

    # --- Database Synchronization for Chains ---
    with db.db_session() as session:
        existing_chains_in_db = {
            chain["global_chain_id"] for chain in db.get_all_chains(session=session)
        }

        for chain in chains_config:
            chain_id = chain["global_chain_id"]
            if chain_id not in existing_chains_in_db:
                # Add only essential info to DB, not the full config
                db.insert_chain(
                    {
                        "global_chain_id": chain_id,
                        "last_processed_block": chain.get("last_processed_block"),
                        "is_synced": False,
                    },
                    session=session,
                )
                logging.info(f"Added new chain {chain_id} to database.")

    # --- Worker Initialization ---
    # Merge DB state with config for workers
    all_chains_from_db = {c["global_chain_id"]: c for c in db.get_all_chains()}
    worker_configs = []
    for chain in chains_config:
        chain_id = chain["global_chain_id"]
        if chain_id in all_chains_from_db:
            db_doc = all_chains_from_db[chain_id]
            # Config from file is source of truth, except for last_processed_block
            merged_config = chain.copy()
            merged_config["last_processed_block"] = db_doc["last_processed_block"]
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

    # --- Keep Main Thread Alive ---
    logging.info("All workers started. Indexer is running.")
    for t in threads:
        t.join()


if __name__ == "__main__":
    main()
