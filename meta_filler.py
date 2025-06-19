import os
import time
import logging
import requests
from db import find_workflow_by_ipfs, update_workflow, db_session
from pymongo import MongoClient
from dotenv import load_dotenv
from threading import Thread
from croniter import croniter
from datetime import datetime
from constants import (
    MONGO_URI,
    DB_NAME,
    META_FILLER_SLEEP,
    IPFS_ENDPOINT,
)

load_dotenv()

# Direct MongoDB access for workflow scan (abstract if needed)
client = MongoClient(MONGO_URI)
db = client[DB_NAME]


def fetch_ipfs_meta(ipfs_hash):
    url = IPFS_ENDPOINT.rstrip("/") + "/" + ipfs_hash
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        return resp.json()

    except Exception as e:
        logging.error(f"Failed to fetch IPFS meta for {ipfs_hash}: {e}")
        return None


def get_next_cron_time(meta):
    # Looks for a cron simulationConfig and returns the next scheduled time if found
    sim_configs = meta.get("simulationConfig")
    if not sim_configs or not isinstance(sim_configs, list):
        logging.warning(
            "No simulationConfig list found in meta, skipping cron scheduling."
        )
        return None

    cron_found = 0
    next_time = None
    for cfg in sim_configs:
        if cfg.get("type") == "cron":
            cron_found += 1
            # Handle both old and new format
            # TODO: Remove this once we have migrated all workflows to the new format
            cron_expr = None
            if "expression" in cfg:  # Old format
                cron_expr = cfg["expression"]
            elif "params" in cfg and "expression" in cfg["params"]:  # New format
                cron_expr = cfg["params"]["expression"]

            if not cron_expr:
                logging.warning(f"Invalid cron config format: {cfg}")
                continue

            now = datetime.utcnow()
            try:
                candidate_time = croniter(cron_expr, now).get_next(datetime)
                if next_time is None:
                    next_time = candidate_time  # Only use the first valid cron
                else:
                    logging.warning(
                        f"Multiple cron configs found; only the first will be used."
                    )
            except Exception as e:
                logging.error(f"Invalid cron expression {cron_expr}: {e}")

    if cron_found == 0:
        logging.info("No valid cron config found in simulationConfig.")
    return next_time


def meta_filler_worker():
    while True:
        # Find one workflow with has_meta: False
        wf = db["workflows"].find_one({"has_meta": False})
        if not wf:
            logging.info("No workflows without meta. Sleeping...")
            time.sleep(META_FILLER_SLEEP)
            continue
        ipfs_hash = wf["ipfs_hash"]
        logging.info(f"Fetching meta for workflow {wf['_id']} (ipfs: {ipfs_hash})")
        meta = fetch_ipfs_meta(ipfs_hash)
        if meta is not None:
            update_fields = {"meta": meta, "has_meta": True}
            next_cron_time = get_next_cron_time(meta)
            if next_cron_time:
                update_fields["next_simulation_time"] = next_cron_time
            with db_session() as session:
                update_workflow(wf["_id"], update_fields, session=session)
                logging.info(f"Meta updated for workflow {wf['_id']}")
        else:
            logging.warning(
                f"Meta fetch failed for workflow {wf['_id']}, will retry later."
            )
        # Short sleep to avoid hammering in case of repeated errors
        time.sleep(2)


class MetaFillerWorker(Thread):
    def run(self):
        meta_filler_worker()


if __name__ == "__main__":
    meta_filler_worker()
