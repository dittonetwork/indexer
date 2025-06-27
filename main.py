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

    # Process chains config to allow for environment variable overrides for RPCs
    chains_config = []
    for chain in chains_from_file:
        chain_id_str = str(chain.get("global_chain_id"))
        env_var = f"RPC_{chain_id_str}"
        rpc_url_from_env = os.getenv(env_var)
        if rpc_url_from_env:
            chain["rpc_url"] = rpc_url_from_env
            logging.info(f"Using {env_var} from environment for chain {chain_id_str}.")
        chains_config.append(chain)

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
                        "last_processed_block": chain.get("last_processed_block", 0),
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
