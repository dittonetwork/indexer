# event_parsers.py
from db import insert_log, insert_workflow, find_workflow_by_ipfs, update_workflow
from datetime import datetime


def parse_created(event, session, chain_id, timestamp):
    """
    - Store event in logs: event name, chain id, blocknumber, tx hash, ipfs hash, timestamp
    - If workflow with ipfs hash does not exist, create it with status 'has_meta': False, reference create event _id
    - If duplicate, only store event
    """
    ipfs_hash = event["args"]["ipfsHash"]
    block_number = event["blockNumber"]
    tx_hash = event["transactionHash"].hex()
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


def parse_run(event, session, chain_id, timestamp, tx_receipt=None):
    """
    - Store event in logs
    - Find workflow by ipfs hash, increment 'runs'
    - If 'executions' exists and runs >= executions, set 'is_cancelled': True
    """
    ipfs_hash = event["args"]["ipfsHash"]
    block_number = event["blockNumber"]
    tx_hash = event["transactionHash"].hex()
    log_doc = {
        "event": "Run",
        "chain_id": chain_id,
        "blocknumber": block_number,
        "transaction_hash": tx_hash,
        "ipfs_hash": ipfs_hash,
        "timestamp": timestamp,
    }
    if tx_receipt:
        log_doc["tx_receipt"] = {
            "gasUsed": tx_receipt.get("gasUsed"),
            "gasPrice": tx_receipt.get("effectiveGasPrice")
            or tx_receipt.get("gasPrice"),
            "from": tx_receipt.get("from"),
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


def parse_cancelled(event, session, chain_id, timestamp):
    """
    - Store event in logs
    - Mark workflow as 'is_cancelled': True, add cancel event _id
    """
    ipfs_hash = event["args"]["ipfsHash"]
    block_number = event["blockNumber"]
    tx_hash = event["transactionHash"].hex()
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
        if wf.get("is_cancelled"):
            return
        update_workflow(
            wf["_id"],
            {"is_cancelled": True, "cancel_event_id": log_result.inserted_id},
            session=session,
        )
