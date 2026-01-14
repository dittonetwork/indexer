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
            "chains_runs": {},
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
        "event": EventType.RUN,
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
            # Nonce is new, handle runs counting
            current_runs = wf.get("runs", 0)
            chains_runs = wf.get("chains_runs", {})
            
            # Backward compatibility: if workflow has runs > 0 but no chains_runs, use old behavior
            if current_runs > 0 and not chains_runs:
                new_runs = current_runs + 1
                update = {"runs": new_runs}
            else:
                # Use new chains_runs logic
                chain_id_str = str(chain_id)
                if chain_id_str not in chains_runs:
                    chains_runs[chain_id_str] = 0
                chains_runs[chain_id_str] += 1
                
                # Calculate total runs as max across all chains
                new_runs = (max(chains_runs.values()) if chains_runs else 1)
                update = {"runs": new_runs, "chains_runs": chains_runs}

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


def parse_run(event, session, chain_id, timestamp, db, tx_receipt=None):
    """
    - Store event in logs
    - Find workflow by ipfs hash, always increment 'runs' (no nonce deduplication for old Run events)
    - If 'count' exists in meta.workflow and runs >= count, set 'is_cancelled': True
    """
    ipfs_hash = event["args"]["ipfsHash"]
    block_number = event["blockNumber"]
    tx_hash = f"0x{event['transactionHash'].hex()}"

    log_doc = {
        "event": EventType.RUN,
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

    wf = db.find_workflow_by_ipfs(ipfs_hash, session=session)
    if wf:
        # Handle runs counting with backward compatibility
        current_runs = wf.get("runs", 0)
        chains_runs = wf.get("chains_runs", {})
        
        # Backward compatibility: if workflow has runs > 0 but no chains_runs, use old behavior
        if current_runs > 0 and not chains_runs:
            new_runs = current_runs + 1
            update = {"runs": new_runs}
        else:
            # Use new chains_runs logic
            chain_id_str = str(chain_id)
            if chain_id_str not in chains_runs:
                chains_runs[chain_id_str] = 0
            chains_runs[chain_id_str] += 1
            
            # Calculate total runs as max across all chains
            new_runs = max(chains_runs.values()) if chains_runs else 1
            update = {"runs": new_runs, "chains_runs": chains_runs}

        # Check for execution count in the nested meta structure
        meta = wf.get("meta", {})
        workflow_meta = meta.get("workflow", {})
        count = workflow_meta.get("count")

        if count is not None and new_runs >= count:
            update["is_cancelled"] = True

        db.update_workflow(wf["ipfs_hash"], update, session=session)

    # Always store the event (regardless of workflow existence)
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

def parse_wasm_created(event, session, chain_id, timestamp, db):
    """
    - Store event in logs: event name, chain id, blocknumber, tx hash, wasm id, ipfs hash, owner, timestamp
    - If WASM entry with id does not exist, create it with reference to create event _id
    - If duplicate, only store event
    """
    wasm_id = event["args"]["id"].hex() if hasattr(event["args"]["id"], "hex") else event["args"]["id"]
    ipfs_hash = event["args"]["ipfsHash"]
    owner = event["args"]["owner"]
    block_number = event["blockNumber"]
    tx_hash = f"0x{event['transactionHash'].hex()}"
    
    log_doc = {
        "event": EventType.WASM_CREATED,
        "chain_id": chain_id,
        "blocknumber": block_number,
        "transaction_hash": tx_hash,
        "wasm_id": wasm_id,
        "ipfs_hash": ipfs_hash,
        "owner": owner,
        "timestamp": timestamp,
    }
    log_result = db.insert_log(log_doc, session=session)
    
    # Check for duplicate WASM entry
    if not db.find_wasm_by_id(wasm_id, session=session):
        wasm_doc = {
            "wasm_id": wasm_id,
            "ipfs_hash": ipfs_hash,
            "owner": owner,
            "create_event_id": ObjectId(log_result.inserted_id),
            "chain_id": chain_id,
            "has_wasm": False,
        }
        db.insert_wasm(wasm_doc, session=session)


def parse_wasm_updated(event, session, chain_id, timestamp, db):
    """
    - Store event in logs: event name, chain id, blocknumber, tx hash, wasm id, old ipfs hash, new ipfs hash, timestamp
    - Update WASM entry with new IPFS hash and reference to update event _id
    """
    wasm_id = event["args"]["id"].hex() if hasattr(event["args"]["id"], "hex") else event["args"]["id"]
    old_ipfs_hash = event["args"]["oldIpfsHash"]
    new_ipfs_hash = event["args"]["newIpfsHash"]
    block_number = event["blockNumber"]
    tx_hash = f"0x{event['transactionHash'].hex()}"
    
    log_doc = {
        "event": EventType.WASM_UPDATED,
        "chain_id": chain_id,
        "blocknumber": block_number,
        "transaction_hash": tx_hash,
        "wasm_id": wasm_id,
        "old_ipfs_hash": old_ipfs_hash,
        "new_ipfs_hash": new_ipfs_hash,
        "timestamp": timestamp,
    }
    log_result = db.insert_log(log_doc, session=session)
    
    # Update WASM entry if it exists
    wasm_entry = db.find_wasm_by_id(wasm_id, session=session)
    if wasm_entry:
        # Track update history
        update_history = wasm_entry.get("update_history", [])
        update_history.append({
            "old_ipfs_hash": old_ipfs_hash,
            "new_ipfs_hash": new_ipfs_hash,
            "update_event_id": ObjectId(log_result.inserted_id),
            "timestamp": timestamp,
            "chain_id": chain_id,
        })
        
        update_fields = {
            "ipfs_hash": new_ipfs_hash,
            "update_event_id": ObjectId(log_result.inserted_id),
            "update_history": update_history,
            "has_wasm": False,  # Mark as needing new code fetch since IPFS hash changed
            # Remove old WASM code since it's been updated
            "$unset": {"wasm_code": "", "wasm_code_size": ""},
        }
        db.update_wasm(wasm_id, update_fields, session=session)