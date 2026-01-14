import time
import logging
import requests
from threading import Thread
from config import (
    META_FILLER_SLEEP,
    META_FILLER_BATCH_SIZE,
    IPFS_ENDPOINT,
    IPFS_CID_V0_PATTERN,
    IPFS_CID_V1_PATTERN,
)
from core.database import Database


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


def fetch_ipfs_wasm(ipfs_hash: str):
    """
    Fetch WASM binary code from IPFS
    Args:
        ipfs_hash: IPFS Content Identifier (CID)
    Returns:
        bytes: WASM binary code from IPFS or None if fetch fails
    """
    if not validate_ipfs_cid(ipfs_hash):
        logging.error(f"Invalid IPFS CID format: {ipfs_hash}")
        return None

    url = IPFS_ENDPOINT.rstrip("/") + "/" + ipfs_hash
    try:
        resp = requests.get(url, timeout=60)  # Longer timeout for binary data
        resp.raise_for_status()
        # Return binary data
        return resp.content

    except Exception as e:
        logging.error(f"Failed to fetch WASM code from IPFS for {ipfs_hash}: {e}")
        return None


def wasm_filler_worker(db: Database):
    while True:
        try:
            with db.db_session() as session:
                # Find multiple WASM modules with has_wasm: False (batch processing)
                wasm_modules = db.find_wasms_without_code_batch(
                    limit=META_FILLER_BATCH_SIZE,
                    skip_recent_failures=True,
                    session=session,
                )

                if not wasm_modules:
                    logging.info(
                        "No WASM modules without code available for processing. Sleeping..."
                    )
                    time.sleep(META_FILLER_SLEEP)
                    continue

                logging.info(
                    f"Processing batch of {len(wasm_modules)} WASM modules for code fetch"
                )

                successful_count = 0
                failed_count = 0

                # Process each WASM module in the batch
                for wasm in wasm_modules:
                    try:
                        ipfs_hash = wasm["ipfs_hash"]
                        wasm_id = wasm["wasm_id"]
                        logging.info(
                            f"Fetching WASM code for module {wasm_id} (ipfs: {ipfs_hash})"
                        )

                        wasm_code = fetch_ipfs_wasm(ipfs_hash)

                        if wasm_code is None:
                            # Mark as failed and continue to next WASM module
                            db.mark_wasm_fetch_failure(wasm_id, session=session)
                            logging.warning(
                                f"WASM code fetch failed for module {wasm_id}, marked for retry later."
                            )
                            failed_count += 1
                            continue

                        # Validate that it's actually WASM binary (starts with WASM magic bytes)
                        if len(wasm_code) < 4 or wasm_code[:4] != b"\x00asm":
                            logging.error(
                                f"Fetched data for WASM module {wasm_id} does not appear to be valid WASM binary (missing magic bytes)."
                            )
                            db.mark_wasm_fetch_failure(wasm_id, session=session)
                            failed_count += 1
                            continue

                        # WASM code is valid, update the module
                        # Store as binary data in MongoDB (BSON Binary type)
                        from bson.binary import Binary
                        update_fields = {
                            "wasm_code": Binary(wasm_code),
                            "wasm_code_size": len(wasm_code),
                            "has_wasm": True,
                        }

                        db.update_wasm(wasm_id, update_fields, session=session)
                        # Clear any previous failure tracking on success
                        db.clear_wasm_fetch_failures(wasm_id, session=session)
                        logging.info(
                            f"WASM code updated for module {wasm_id} (size: {len(wasm_code)} bytes)"
                        )
                        successful_count += 1

                    except Exception as wasm_error:
                        # Handle individual WASM module errors without stopping the batch
                        logging.error(
                            f"Error processing WASM module {wasm.get('wasm_id', 'unknown')}: {wasm_error}"
                        )
                        try:
                            db.mark_wasm_fetch_failure(wasm["wasm_id"], session=session)
                        except Exception:
                            pass  # Don't let failure marking break the batch
                        failed_count += 1
                        continue

                logging.info(
                    f"Batch completed: {successful_count} successful, {failed_count} failed"
                )

        except Exception as e:
            logging.error(f"Error in WASM filler worker batch: {str(e)}")
            time.sleep(5)  # Sleep longer on errors
            continue

        # Short sleep to avoid hammering
        time.sleep(2)


class WasmFillerWorker(Thread):
    def __init__(self, db: Database):
        super().__init__()
        self.db = db

    def run(self):
        wasm_filler_worker(self.db)


if __name__ == "__main__":
    from config import MONGO_URI, DB_NAME

    db_instance = Database(MONGO_URI, DB_NAME)
    wasm_filler_worker(db_instance)
