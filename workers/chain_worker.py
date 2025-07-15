import logging
import threading
import time
import json
from web3 import Web3
from eth_utils.abi import abi_to_signature, filter_abi_by_type
from events.parsers import parse_created, parse_run_with_metadata, parse_cancelled
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
        self.sync_threshold_blocks = chain_doc.get("sync_threshold_blocks", 5)
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
            if name == EventType.RUN_WITH_METADATA.value:
                tx_receipt = self.web3.eth.get_transaction_receipt(
                    raw["transactionHash"]
                )

            decoded_events.append((name, decoded, timestamp, tx_receipt))

        with self.db.db_session() as session:
            for name, evt, timestamp, tx_receipt in decoded_events:
                try:
                    if name == EventType.CREATED.value:
                        parse_created(evt, session, self.chain_id, timestamp, self.db)
                    elif name == EventType.RUN_WITH_METADATA.value:
                        parse_run_with_metadata(
                            evt, session, self.chain_id, timestamp, self.db, tx_receipt
                        )
                    elif name == EventType.CANCELLED.value:
                        parse_cancelled(evt, session, self.chain_id, timestamp, self.db)
                except Exception as e:
                    logging.error(
                        f"[Chain {self.chain_id}] Parse error for tx {evt['transactionHash'].hex()}: {e}"
                    )
                    raise Exception("Batch has parsing errors.")

            self.db.update_chain_last_processed(
                self.chain_id, batch_end, session=session
            )

        return True

    def process_chain(self):
        """
        Walks through unprocessed blocks in batches, with a retry mechanism.
        """
        current_block = self.web3.eth.block_number
        latest_block = current_block - self.block_delay
        start_block = self.last_processed + 1

        # Check if chain is synced using configurable threshold
        blocks_behind = current_block - self.last_processed
        is_synced = blocks_behind < self.sync_threshold_blocks

        # Update sync status in database
        with self.db.db_session() as session:
            chain = self.db.get_chain(self.chain_id, session=session)
            if chain:
                current_sync_status = chain.get("is_synced", False)
                if is_synced != current_sync_status:
                    self.db.update_chain_sync_status(
                        self.chain_id, is_synced, session=session
                    )
                    if is_synced:
                        logging.info(
                            f"[Chain {self.chain_id}] Marked as synced (behind by {blocks_behind} blocks, threshold: {self.sync_threshold_blocks})"
                        )
                    else:
                        logging.info(
                            f"[Chain {self.chain_id}] Marked as not synced (behind by {blocks_behind} blocks, threshold: {self.sync_threshold_blocks})"
                        )

        # If no new blocks to process, return early
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
