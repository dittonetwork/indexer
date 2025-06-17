# event_parsers.py
from db import insert_log, insert_workflow, find_workflow_by_ipfs, update_workflow
from datetime import datetime


def parse_created(event, session, chain_id):
    """
    - Store event in logs: event name, chain id, blocknumber, tx hash, ipfs hash, timestamp
    - If workflow with ipfs hash does not exist, create it with status 'has_meta': False, reference create event _id
    - If duplicate, only store event
    """
    ipfs_hash = event["args"]["ipfsHash"]
    block_number = event["blockNumber"]
    tx_hash = event["transactionHash"].hex()
    timestamp = get_block_timestamp(event, session)
    log_doc = {
        "event": "Created",
        "chain_id": chain_id,
        "blocknumber": block_number,
        "transaction_hash": tx_hash,
        "ipfs_hash": ipfs_hash,
        "timestamp": timestamp,
    }
    log_result = insert_log(log_doc, session=session)
    # Check for duplicate workflow
    if not find_workflow_by_ipfs(ipfs_hash, session=session):
        workflow_doc = {
            "ipfs_hash": ipfs_hash,
            "create_event_id": log_result.inserted_id,
            "has_meta": False,
            "runs": 0,
            "is_cancelled": False,
        }
        insert_workflow(workflow_doc, session=session)


def parse_run(event, session, chain_id):
    """
    - Store event in logs
    - Find workflow by ipfs hash, increment 'runs'
    - If 'executions' exists and runs >= executions, set 'is_cancelled': True
    """
    ipfs_hash = event["args"]["ipfsHash"]
    block_number = event["blockNumber"]
    tx_hash = event["transactionHash"].hex()
    timestamp = get_block_timestamp(event, session)
    log_doc = {
        "event": "Run",
        "chain_id": chain_id,
        "blocknumber": block_number,
        "transaction_hash": tx_hash,
        "ipfs_hash": ipfs_hash,
        "timestamp": timestamp,
    }
    insert_log(log_doc, session=session)
    wf = find_workflow_by_ipfs(ipfs_hash, session=session)
    if wf:
        new_runs = (wf.get("runs", 0) or 0) + 1
        update = {"runs": new_runs}
        executions = wf.get("executions")
        if executions is not None and new_runs >= executions:
            update["is_cancelled"] = True
        update_workflow(wf["_id"], update, session=session)


def parse_cancelled(event, session, chain_id):
    """
    - Store event in logs
    - Mark workflow as 'is_cancelled': True, add cancel event _id
    """
    ipfs_hash = event["args"]["ipfsHash"]
    block_number = event["blockNumber"]
    tx_hash = event["transactionHash"].hex()
    timestamp = get_block_timestamp(event, session)
    log_doc = {
        "event": "Cancelled",
        "chain_id": chain_id,
        "blocknumber": block_number,
        "transaction_hash": tx_hash,
        "ipfs_hash": ipfs_hash,
        "timestamp": timestamp,
    }
    log_result = insert_log(log_doc, session=session)
    wf = find_workflow_by_ipfs(ipfs_hash, session=session)
    if wf:
        update_workflow(
            wf["_id"],
            {"is_cancelled": True, "cancel_event_id": log_result.inserted_id},
            session=session,
        )


def get_block_timestamp(event, session):
    # Helper to get block timestamp from chain, fallback to now if not available
    try:
        # You may want to pass web3 as a param for efficiency
        # For now, assume only one chain or pass web3 as param
        # This is a stub; in production, pass web3 from the worker
        return datetime.utcnow()
    except Exception:
        return datetime.utcnow()
