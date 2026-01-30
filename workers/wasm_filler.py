import time
import logging
import requests
from threading import Thread
from pymongo.errors import OperationFailure
from config import (
    META_FILLER_SLEEP,
    META_FILLER_BATCH_SIZE,
    IPFS_ENDPOINT,
    IPFS_CID_V0_PATTERN,
    IPFS_CID_V1_PATTERN,
)
from core.database import Database

# MongoDB transient error codes that should trigger retry
TRANSIENT_ERROR_CODES = {112, 251}  # WriteConflict, NoSuchTransaction
MAX_RETRY_ATTEMPTS = 3
RETRY_BASE_DELAY = 0.5  # seconds


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


def _is_transient_error(e):
    """Check if an error is a transient MongoDB error that should be retried"""
    if isinstance(e, OperationFailure):
        return e.code in TRANSIENT_ERROR_CODES or "TransientTransactionError" in str(e.details.get("errorLabels", []))
    return False


def _update_wasm_with_retry(db: Database, wasm_id: str, update_fields: dict, clear_failures: bool = False):
    """
    Update a WASM module with retry logic for transient errors.
    Each update runs in its own transaction to minimize conflict window.
    """
    for attempt in range(MAX_RETRY_ATTEMPTS):
        try:
            with db.db_session() as session:
                db.update_wasm(wasm_id, update_fields, session=session)
                if clear_failures:
                    db.clear_wasm_fetch_failures(wasm_id, session=session)
            return True
        except OperationFailure as e:
            if _is_transient_error(e) and attempt < MAX_RETRY_ATTEMPTS - 1:
                delay = RETRY_BASE_DELAY * (2 ** attempt)  # Exponential backoff
                logging.warning(
                    f"Transient error updating WASM {wasm_id}, retrying in {delay}s (attempt {attempt + 1}/{MAX_RETRY_ATTEMPTS}): {e}"
                )
                time.sleep(delay)
            else:
                raise
    return False


def _mark_wasm_failure_with_retry(db: Database, wasm_id: str):
    """Mark WASM fetch failure with retry logic for transient errors"""
    for attempt in range(MAX_RETRY_ATTEMPTS):
        try:
            with db.db_session() as session:
                db.mark_wasm_fetch_failure(wasm_id, session=session)
            return True
        except OperationFailure as e:
            if _is_transient_error(e) and attempt < MAX_RETRY_ATTEMPTS - 1:
                delay = RETRY_BASE_DELAY * (2 ** attempt)
                logging.warning(
                    f"Transient error marking WASM failure {wasm_id}, retrying in {delay}s: {e}"
                )
                time.sleep(delay)
            else:
                logging.error(f"Failed to mark WASM failure for {wasm_id}: {e}")
                return False
    return False


def wasm_filler_worker(db: Database):
    from bson.binary import Binary

    while True:
        try:
            # Step 1: Find WASM modules WITHOUT a transaction (read-only, no lock needed)
            wasm_modules = db.find_wasms_without_code_batch(
                limit=META_FILLER_BATCH_SIZE,
                skip_recent_failures=True,
                session=None,  # No transaction for initial read
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

            # Step 2: Process each WASM module individually with its own transaction
            for wasm in wasm_modules:
                wasm_id = wasm.get("wasm_id", "unknown")
                try:
                    ipfs_hash = wasm["ipfs_hash"]
                    logging.info(
                        f"Fetching WASM code for module {wasm_id} (ipfs: {ipfs_hash})"
                    )

                    # Step 2a: Fetch IPFS data OUTSIDE of any transaction
                    wasm_code = fetch_ipfs_wasm(ipfs_hash)

                    if wasm_code is None:
                        # Mark as failed with retry logic
                        _mark_wasm_failure_with_retry(db, wasm_id)
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
                        _mark_wasm_failure_with_retry(db, wasm_id)
                        failed_count += 1
                        continue

                    # Step 2b: Update DB in its own transaction with retry logic
                    update_fields = {
                        "wasm_code": Binary(wasm_code),
                        "wasm_code_size": len(wasm_code),
                        "has_wasm": True,
                    }

                    if _update_wasm_with_retry(db, wasm_id, update_fields, clear_failures=True):
                        logging.info(
                            f"WASM code updated for module {wasm_id} (size: {len(wasm_code)} bytes)"
                        )
                        successful_count += 1
                    else:
                        failed_count += 1

                except Exception as wasm_error:
                    # Handle individual WASM module errors without stopping the batch
                    logging.error(
                        f"Error processing WASM module {wasm_id}: {wasm_error}"
                    )
                    try:
                        _mark_wasm_failure_with_retry(db, wasm_id)
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
