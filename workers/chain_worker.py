import logging
import threading
import time
import json
from web3 import Web3
from eth_utils.abi import abi_to_signature, filter_abi_by_type
from events.parsers import parse_created, parse_run, parse_cancelled
from datetime import datetime, timezone
from config import EventType


class ChainWorker(threading.Thread):
    def __init__(self, chain_doc, db):
        super().__init__()
        self.db = db
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
        # Initialize decoder cache
        self._init_decoder_cache()

    def _init_decoder_cache(self):
        """Initialize event decoders cache"""
        self._topic_map = {}
        self._decoder_cache = {}

        # Grab just the *event* ABIs from the full contract ABI
        event_abis = filter_abi_by_type("event", self.abi)

        for ev_abi in event_abis:
            if ev_abi["name"] not in EventType.get_target_names():
                continue

            # canonical signature, e.g. "Created(address,string,uint256)"
            sig_text = abi_to_signature(ev_abi)
            topic_hash = self.web3.keccak(text=sig_text).hex()

            # Create decoder class once (web3.contract.ContractEvent)
            decoder_cls = getattr(self.contract.events, ev_abi["name"])
            decoder = decoder_cls()  # Create instance once

            self._topic_map[topic_hash] = (ev_abi["name"], decoder)
            self._decoder_cache[topic_hash] = decoder

    def process_chain(self):
        """
        • Compute the newest "safe" block (latest – block_delay)
        • Walk through unprocessed blocks in batches
        • ONE eth_getLogs per batch (address-filtered)
        • Decode logs locally, route to the right parser, commit atomically
        """
        latest_block = self.web3.eth.block_number - self.block_delay
        start_block = self.last_processed + 1
        if latest_block < start_block:
            return  # nothing new
        end_block = latest_block

        # Batch loop
        for batch_start in range(start_block, end_block + 1, self.batch_size):
            batch_end = min(batch_start + self.batch_size - 1, end_block)
            logging.info(
                f"[Chain {self.chain_id}] Fetching logs {batch_start} → {batch_end}"
            )

            try:
                raw_logs = self.web3.eth.get_logs(
                    {
                        "address": self.registry_address,
                        "fromBlock": batch_start,
                        "toBlock": batch_end,
                    }
                )
            except Exception as e:
                logging.error(f"[Chain {self.chain_id}] eth_getLogs error: {e}")
                continue  # skip this batch

            # Decode & bucket
            decoded_events = []  # (name, decoded_log, timestamp, tx_receipt)
            block_ts_cache = {}  # block_number -> ISO timestamp

            for raw in raw_logs:
                topic0 = raw["topics"][0].hex()
                mapping = self._topic_map.get(topic0)
                if mapping is None:  # not Created / Run / Cancelled
                    continue

                name, decoder = mapping  # Use cached decoder

                try:
                    block_number = raw["blockNumber"]
                    if block_number not in block_ts_cache:
                        block = self.web3.eth.get_block(block_number)
                        block_ts_cache[block_number] = datetime.fromtimestamp(
                            block["timestamp"], tz=timezone.utc
                        ).isoformat()
                    timestamp = block_ts_cache[block_number]

                    if hasattr(decoder, "process_log"):
                        decoded = decoder.process_log(raw)
                    else:
                        decoded = decoder.processLog(raw)

                    tx_receipt = None
                    if name == EventType.RUN.value:
                        try:
                            tx_receipt = self.web3.eth.get_transaction_receipt(
                                raw["transactionHash"]
                            )
                        except Exception as e:
                            logging.error(
                                f"[Chain {self.chain_id}] Error fetching tx receipt for {raw['transactionHash'].hex()}: {e}"
                            )
                    decoded_events.append((name, decoded, timestamp, tx_receipt))
                except Exception as e:
                    logging.error(f"[Chain {self.chain_id}] {name} decode error: {e}")

            # Persist atomically
            with self.db.db_session() as session:
                for name, evt, timestamp, tx_receipt in decoded_events:
                    try:
                        if name == EventType.CREATED.value:
                            parse_created(
                                evt, session, self.chain_id, timestamp, self.db
                            )
                        elif name == EventType.RUN.value:
                            parse_run(
                                evt,
                                session,
                                self.chain_id,
                                timestamp,
                                self.db,
                                tx_receipt=tx_receipt,
                            )
                        else:  # Cancelled
                            parse_cancelled(
                                evt, session, self.chain_id, timestamp, self.db
                            )
                    except Exception as e:
                        logging.error(
                            f"[Chain {self.chain_id}] {name} parse error: {e}"
                        )

                self.db.update_chain_last_processed(
                    self.chain_id, batch_end, session=session
                )

            # ready for next batch
            self.last_processed = batch_end

    def run(self):
        logging.info(f"Worker started for chain {self.chain_id}")
        while True:
            try:
                self.process_chain()
            except Exception as e:
                logging.error(f"[Chain {self.chain_id}] Error: {e}")
            time.sleep(self.sleep_duration)
