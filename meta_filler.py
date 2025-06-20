import time
import logging
import requests
from threading import Thread
from croniter import croniter
from datetime import datetime
from config import (
    META_FILLER_SLEEP,
    IPFS_ENDPOINT,
    IPFS_CID_V0_PATTERN,
    IPFS_CID_V1_PATTERN,
)
from db import Database


def validate_ipfs_cid(ipfs_hash: str) -> bool:
    """
    Validate IPFS Content Identifier (CID)
    Supports both CIDv0 (Qm...) and CIDv1 (bafy...) formats
    """
    if not ipfs_hash:
        return False

    return bool(
        IPFS_CID_V0_PATTERN.match(ipfs_hash) or IPFS_CID_V1_PATTERN.match(ipfs_hash)
    )


def fetch_ipfs_meta(ipfs_hash: str):
    """
    Fetch metadata from IPFS
    Args:
        ipfs_hash: IPFS Content Identifier (CID)
    Returns:
        dict: Metadata from IPFS or None if fetch fails
    """
    if not validate_ipfs_cid(ipfs_hash):
        logging.error(f"Invalid IPFS CID format: {ipfs_hash}")
        return None

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


def has_invalid_mongo_keys(obj, path=None):
    """
    Recursively check for keys with '.' or starting with '$' in a dict/list structure.
    Returns the first invalid key path found, or None if all keys are valid.
    """
    if path is None:
        path = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            if "." in k or (k and k[0] == "$"):
                return path + [k]
            res = has_invalid_mongo_keys(v, path + [k])
            if res:
                return res
    elif isinstance(obj, list):
        for idx, item in enumerate(obj):
            res = has_invalid_mongo_keys(item, path + [str(idx)])
            if res:
                return res
    return None


def meta_filler_worker(db: Database):
    while True:
        try:
            with db.db_session() as session:
                # Find one workflow with has_meta: False
                wf = db.find_workflow_without_meta(session=session)
                if not wf:
                    logging.info("No workflows without meta. Sleeping...")
                    time.sleep(META_FILLER_SLEEP)
                    continue

                ipfs_hash = wf["ipfs_hash"]
                logging.info(
                    f"Fetching meta for workflow {wf['_id']} (ipfs: {ipfs_hash})"
                )
                meta = fetch_ipfs_meta(ipfs_hash)

                # Check for invalid MongoDB keys
                invalid_path = has_invalid_mongo_keys(meta)
                if invalid_path:
                    logging.error(
                        f"Meta for workflow {wf['_id']} contains invalid MongoDB key at: {'.'.join(invalid_path)}. Skipping update."
                    )
                    continue

                if meta is not None:
                    update_fields = {"meta": meta, "has_meta": True}
                    next_cron_time = get_next_cron_time(meta)
                    if next_cron_time:
                        update_fields["next_simulation_time"] = next_cron_time

                    db.update_workflow(wf["_id"], update_fields, session=session)
                    logging.info(f"Meta updated for workflow {wf['_id']}")
                else:
                    logging.warning(
                        f"Meta fetch failed for workflow {wf['_id']}, will retry later."
                    )
        except Exception as e:
            logging.error(f"Error in meta filler worker: {str(e)}")
            time.sleep(5)  # Sleep longer on errors
            continue

        # Short sleep to avoid hammering in case of repeated errors
        time.sleep(2)


class MetaFillerWorker(Thread):
    def __init__(self, db: Database):
        super().__init__()
        self.db = db

    def run(self):
        meta_filler_worker(self.db)


if __name__ == "__main__":
    from config import MONGO_URI, DB_NAME

    db_instance = Database(MONGO_URI, DB_NAME)
    meta_filler_worker(db_instance)
