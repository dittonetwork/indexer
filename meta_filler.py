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

load_dotenv()

SLEEP_DURATION = int(os.getenv("META_FILLER_SLEEP", 60))  # seconds
default_ipfs_endpoint = "https://ipfs.io/ipfs/"
IPFS_ENDPOINT = os.getenv("IPFS_CONNECTOR_ENDPOINT", default_ipfs_endpoint)

logging.basicConfig(
    level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s"
)

# Direct MongoDB access for workflow scan (abstract if needed)
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME = os.getenv("DB_NAME", "indexer")
client = MongoClient(MONGO_URI)
db = client[DB_NAME]


def fetch_ipfs_meta(ipfs_hash):
    url = IPFS_ENDPOINT.rstrip("/") + "/" + ipfs_hash
    try:
        # resp = requests.get(url, timeout=30)
        # resp.raise_for_status()
        #     return resp.json()
        return {
            "ipfsHash": "QmXyz123...",
            "chainIds": [1, 137],
            "executions": 15,
            "simulationConfig": [
                {
                    "eventTrigger": {
                        "signature": "Transfer(address from, address to, uint256 amount)",
                        "filter": {
                            "address": "0xabc123...deadbeef",
                            "to": "0xdef456...feedface",
                        },
                    }
                },
                {"type": "cron", "expression": "*/5 * * * *"},
            ],
            "account": {"address": "0x2cc1E1Dc713E17A0A295863F4C5F16034D0807dB"},
            "metadata": {
                "text": "Swap 10 DAI to 15 USDC after Transfer event is emitted"
            },
            "executionConfig": {
                "actions": [
                    {
                        "session": {
                            "params": {
                                "permissionParams": {"policies": []},
                                "action": {
                                    "selector": "0xe9ae5c53",
                                    "address": "0x0000000000000000000000000000000000000000",
                                },
                                "validityData": {"validAfter": 0, "validUntil": 0},
                                "accountParams": {
                                    "initCode": "0x",
                                    "accountAddress": "0x2cc1E1Dc713E17A0A295863F4C5F16034D0807dB",
                                },
                                "enableSignature": "0x0b6a5a072eb53688c7724019a79bd529242c5d06ecddbe373e662f56e0d9992b73af25938ca9dda9f9e2d9ac222e8e419198d5c48ed021a73dd55445db32d0d51c",
                            }
                        },
                        "runtime": {
                            "runtimeId": "mvp-calldata",
                            "params": {
                                "to": "0x6B175474E89094C44Da98b954EedeAC495271d0F",
                                "calldata": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
                                "chainId": 1,
                            },
                            "source": "export default async function run(ctx, params) { return { chainId: params.chainId, to: params.to, calldata: params.calldata) }; }",
                        },
                    }
                ]
            },
        }
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
        if cfg.get("type") == "cron" and "expression" in cfg:
            cron_found += 1
            cron_expr = cfg["expression"]
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
            time.sleep(SLEEP_DURATION)
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
