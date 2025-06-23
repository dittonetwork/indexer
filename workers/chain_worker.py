import logging
import threading
import time
import json
from web3 import Web3
from eth_utils.abi import abi_to_signature, filter_abi_by_type
from events.parsers import parse_created, parse_run, parse_cancelled
from datetime import datetime, timezone
from config import EventType
from functools import cache


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
        self.parse_retry_delay = chain_doc.get("parse_retry_delay_seconds", 10)
        self.web3 = Web3(Web3.HTTPProvider(self.rpc_url))
        # Load ABI for registry contract
        with open("registry_abi.json") as f:
            self.abi = json.load(f)
        self.contract = self.web3.eth.contract(
            address=self.registry_address, abi=self.abi
        )
        # Initialize decoder cache
        self._init_decoder_cache()

    @cache
    def get_block_timestamp(self, block_number):
        """Cached block timestamp fetcher"""
        try:
            block = self.web3.eth.get_block(block_number)
            return datetime.fromtimestamp(
                block["timestamp"], tz=timezone.utc
            ).isoformat()
        except Exception as e:
            logging.error(
                f"Error fetching block {block_number} for timestamp on chain {self.chain_id}: {e}"
            )
            return None

    def _init_decoder_cache(self):
        """Initialize event decoders cache"""
        self._topic_map = {}

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

    def _process_batch(self, batch_start, batch_end):
        """
        Processes a single batch of blocks.
        Returns True on success, False on failure.
        Raises an exception if parsing fails, to be caught by the retry logic.
        """
        raw_logs = self.web3.eth.get_logs(
            {
                "address": self.registry_address,
                "fromBlock": batch_start,
                "toBlock": batch_end,
            }
        )

        decoded_events = []
        for raw in raw_logs:
            topic0 = raw["topics"][0].hex()
            mapping = self._topic_map.get(topic0)
            if mapping is None:
                continue

            name, decoder = mapping
            timestamp = self.get_block_timestamp(raw["blockNumber"])
            if not timestamp:
                logging.warning(
                    f"Skipping log in block {raw['blockNumber']} due to timestamp fetch error."
                )
                continue

            if hasattr(decoder, "process_log"):
                decoded = decoder.process_log(raw)
            else:
                decoded = decoder.processLog(raw)

            tx_receipt = None
            if name == EventType.RUN.value:
                tx_receipt = self.web3.eth.get_transaction_receipt(
                    raw["transactionHash"]
                )

            decoded_events.append((name, decoded, timestamp, tx_receipt))

        with self.db.db_session() as session:
            batch_has_errors = False
            for name, evt, timestamp, tx_receipt in decoded_events:
                try:
                    if name == EventType.CREATED.value:
                        parse_created(evt, session, self.chain_id, timestamp, self.db)
                    elif name == EventType.RUN.value:
                        parse_run(
                            evt, session, self.chain_id, timestamp, self.db, tx_receipt
                        )
                    elif name == EventType.CANCELLED.value:
                        parse_cancelled(evt, session, self.chain_id, timestamp, self.db)
                except Exception as e:
                    logging.error(
                        f"[Chain {self.chain_id}] Parse error for tx {evt['transactionHash'].hex()}: {e}"
                    )
                    batch_has_errors = True

            if batch_has_errors:
                raise Exception("Batch has parsing errors.")

            self.db.update_chain_last_processed(
                self.chain_id, batch_end, session=session
            )

        return True

    def process_chain(self):
        """
        Walks through unprocessed blocks in batches, with a retry mechanism.
        """
        latest_block = self.web3.eth.block_number - self.block_delay
        start_block = self.last_processed + 1
        if latest_block < start_block:
            return

        for batch_start in range(start_block, latest_block + 1, self.batch_size):
            batch_end = min(batch_start + self.batch_size - 1, latest_block)

            while True:
                try:
                    logging.info(
                        f"[Chain {self.chain_id}] Processing batch {batch_start}→{batch_end}"
                    )
                    if self._process_batch(batch_start, batch_end):
                        self.last_processed = batch_end
                        break  # Success, exit retry loop
                except Exception as e:
                    logging.error(
                        f"[Chain {self.chain_id}] Batch {batch_start}→{batch_end} failed, retrying in {self.parse_retry_delay}s: {e}"
                    )
                    time.sleep(self.parse_retry_delay)

    def run(self):
        logging.info(f"Worker started for chain {self.chain_id}")
        while True:
            try:
                self.process_chain()
            except Exception as e:
                logging.error(f"[Chain {self.chain_id}] Error: {e}")
            time.sleep(self.sleep_duration)
