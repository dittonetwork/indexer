# event_parsers.py
from datetime import datetime
from config import EventType
from core.database import Database
from bson.objectid import ObjectId


def parse_created(event, session, chain_id, timestamp, db: Database):
    """
    - Store event in logs: event name, chain id, blocknumber, tx hash, ipfs hash, timestamp
    - If workflow with ipfs hash does not exist, create it with status 'has_meta': False, reference create event _id
    - If duplicate, only store event
    """
    ipfs_hash = event["args"]["ipfsHash"]
    block_number = event["blockNumber"]
    tx_hash = f"0x{event['transactionHash'].hex()}"
    log_doc = {
        "event": EventType.CREATED,
        "chain_id": chain_id,
        "blocknumber": block_number,
        "transaction_hash": tx_hash,
        "ipfs_hash": ipfs_hash,
        "timestamp": timestamp,
    }
    log_result = db.insert_log(log_doc, session=session)
    # Check for duplicate workflow
    if not db.find_workflow_by_ipfs(ipfs_hash, session=session):
        workflow_doc = {
            "ipfs_hash": ipfs_hash,
            "create_event_id": ObjectId(log_result.inserted_id),
            "has_meta": False,
            "runs": 0,
            "is_cancelled": False,
        }
        db.insert_workflow(workflow_doc, session=session)


def parse_run_with_metadata(event, session, chain_id, timestamp, db, tx_receipt=None):
    """
    - Store event in logs
    - Find workflow by ipfs hash, check if nonce has been seen before
    - Only increment 'runs' if nonce is new (avoid duplicates from different networks)
    - If 'count' exists in meta.workflow and runs >= count, set 'is_cancelled': True
    """
    ipfs_hash = event["args"]["ipfsHash"]
    job_id = event["args"]["jobId"]
    nonce = event["args"]["nonce"]
    block_number = event["blockNumber"]
    tx_hash = f"0x{event['transactionHash'].hex()}"

    log_doc = {
        "event": "Run",
        "chain_id": chain_id,
        "blocknumber": block_number,
        "transaction_hash": tx_hash,
        "ipfs_hash": ipfs_hash,
        "job_id": job_id,
        "nonce": nonce,
        "timestamp": timestamp,
    }
    if tx_receipt:
        log_doc["tx_receipt"] = {
            "gasUsed": tx_receipt.get("gasUsed"),
            "gasPrice": tx_receipt.get("effectiveGasPrice")
            or tx_receipt.get("gasPrice"),
            "from": tx_receipt.get("from"),
        }

    wf = db.find_workflow_by_ipfs(ipfs_hash, session=session)
    if wf:
        # Check if we already have a run event with this nonce BEFORE storing
        nonce_already_exists = db.has_run_event_with_nonce(
            ipfs_hash, nonce, session=session
        )

        if not nonce_already_exists:
            # Nonce is new, increment runs counter
            new_runs = wf.get("runs", 0) + 1
            update = {"runs": new_runs}

            # Check for execution count in the nested meta structure
            meta = wf.get("meta", {})
            workflow_meta = meta.get("workflow", {})
            count = workflow_meta.get("count")

            if count is not None and new_runs >= count:
                update["is_cancelled"] = True

            db.update_workflow(wf["ipfs_hash"], update, session=session)
        # If nonce already exists, we still store the event but don't increment runs

    # Always store the event (regardless of whether nonce was duplicate)
    db.insert_log(log_doc, session=session)


def parse_cancelled(event, session, chain_id, timestamp, db):
    """
    - Store event in logs
    - Mark workflow as 'is_cancelled': True, add cancel event _id
    """
    ipfs_hash = event["args"]["ipfsHash"]
    block_number = event["blockNumber"]
    tx_hash = f"0x{event['transactionHash'].hex()}"
    log_doc = {
        "event": EventType.CANCELLED,
        "chain_id": chain_id,
        "blocknumber": block_number,
        "transaction_hash": tx_hash,
        "ipfs_hash": ipfs_hash,
        "timestamp": timestamp,
    }
    log_result = db.insert_log(log_doc, session=session)
    wf = db.find_workflow_by_ipfs(ipfs_hash, session=session)
    if wf:
        if wf.get("is_cancelled"):
            return
        db.update_workflow(
            wf["ipfs_hash"],
            {"is_cancelled": True, "cancel_event_id": log_result.inserted_id},
            session=session,
        )
