import logging
import threading
import time
import json
from web3 import Web3
from db import (
    update_chain_last_processed,
    db_session,
)
from event_parsers import parse_created, parse_run, parse_cancelled


class ChainWorker(threading.Thread):
    def __init__(self, chain_doc):
        super().__init__()
        self.chain_doc = chain_doc
        self.chain_id = chain_doc["global_chain_id"]
        self.rpc_url = chain_doc["rpc_url"]
        self.last_processed = chain_doc["last_processed_block"]
        self.batch_size = chain_doc["batch_size"]
        self.block_delay = chain_doc["block_delay"]
        self.sleep_duration = chain_doc["loop_sleep_duration"]
        self.registry_address = chain_doc["registry_contract_address"]
        self.web3 = Web3(Web3.HTTPProvider(self.rpc_url))
        # Load ABI for registry contract
        with open("registry_abi.json") as f:
            self.abi = json.load(f)
        self.contract = self.web3.eth.contract(
            address=self.registry_address, abi=self.abi
        )

    def run(self):
        logging.info(f"Worker started for chain {self.chain_id}")
        while True:
            try:
                self.process_chain()
            except Exception as e:
                logging.error(f"[Chain {self.chain_id}] Error: {e}")
            time.sleep(self.sleep_duration)

    def process_chain(self):
        """
        - Get latest block number, subtract block_delay
        - If new blocks, process in batches
        - For each batch, fetch events, parse, store atomically
        - Update last_processed_block
        """
        latest_block = self.web3.eth.block_number - self.block_delay
        start_block = self.last_processed + 1
        if latest_block < start_block:
            return  # Nothing to do
        end_block = latest_block
        for batch_start in range(start_block, end_block + 1, self.batch_size):
            batch_end = min(batch_start + self.batch_size - 1, end_block)
            # Fetch logs for all three events using get_logs
            event_types = [
                ("Created", self.contract.events.Created),
                ("Run", self.contract.events.Run),
                ("Cancelled", self.contract.events.Cancelled),
            ]
            events = []
            for event_name, event_class in event_types:
                logging.info(
                    f"[Chain {self.chain_id}] Fetching {event_name} logs from {batch_start} to {batch_end}"
                )
                try:
                    logs = event_class.get_logs(
                        from_block=batch_start, to_block=batch_end
                    )
                    for log in logs:
                        events.append((event_name, log))
                except Exception as e:
                    logging.error(
                        f"[Chain {self.chain_id}] Error fetching {event_name} logs: {e}"
                    )
            # Transaction: parse and store events, update last_processed_block
            with db_session() as session:
                for event_name, log in events:
                    if event_name == "Created":
                        parse_created(log, session, self.chain_id)
                    elif event_name == "Run":
                        parse_run(log, session, self.chain_id)
                    elif event_name == "Cancelled":
                        parse_cancelled(log, session, self.chain_id)
                update_chain_last_processed(self.chain_id, batch_end, session=session)
            self.last_processed = batch_end
