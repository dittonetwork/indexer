from dotenv import load_dotenv

load_dotenv()

import json
import os
import logging
from db import get_chain, insert_chain
from chain_worker import ChainWorker

logging.basicConfig(
    level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s"
)


def load_config_chains():
    config_path = os.getenv("CHAINS_CONFIG_PATH", "chains_config.json")
    if os.path.exists(config_path):
        with open(config_path) as f:
            chains = json.load(f)
            if isinstance(chains, dict):
                chains = [chains]
            return chains
    else:
        logging.error(f"No chains config found at {config_path}!")
        return []


def ensure_chains_in_db(config_chains):
    for chain in config_chains:
        chain_id = chain["global_chain_id"]
        db_entry = get_chain(chain_id)
        if db_entry is None:
            # Insert with last_processed_block from config (default to 0 if not present)
            last_processed = chain.get("last_processed_block", 0)
            insert_chain(
                {"global_chain_id": chain_id, "last_processed_block": last_processed}
            )
            logging.info(
                f"Inserted chain {chain_id} into DB with last_processed_block={last_processed}"
            )
        else:
            logging.info(f"Chain {chain_id} already present in DB")


def get_worker_chain_docs(config_chains):
    chain_docs = []
    for chain in config_chains:
        chain_id = chain["global_chain_id"]
        db_entry = get_chain(chain_id)
        if db_entry:
            # Merge config and db state: always use config for all fields except last_processed_block
            merged = dict(chain)
            merged["last_processed_block"] = db_entry["last_processed_block"]
            merged["_id"] = db_entry["_id"]
            chain_docs.append(merged)
        else:
            logging.warning(f"Chain {chain_id} missing in DB after ensure step!")
    return chain_docs


def main():
    config_chains = load_config_chains()
    ensure_chains_in_db(config_chains)
    chain_docs = get_worker_chain_docs(config_chains)
    if not chain_docs:
        logging.error("No chains to index. Exiting.")
        return
    workers = []
    for chain_doc in chain_docs:
        logging.info(f"Starting worker for chain {chain_doc['global_chain_id']}")
        worker = ChainWorker(chain_doc)
        worker.start()
        workers.append(worker)
    for worker in workers:
        worker.join()


if __name__ == "__main__":
    main()
