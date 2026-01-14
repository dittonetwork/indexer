import logging
import threading
import time
import json
from web3 import Web3
from eth_utils.abi import abi_to_signature, filter_abi_by_type
from events.parsers import (
    parse_created,
    parse_run,
    parse_run_with_metadata,
    parse_cancelled,
    parse_wasm_created,
    parse_wasm_updated,
)
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
        # Initialize WASM last processed block from chain_doc or database
        self.wasm_last_processed = chain_doc.get("wasm_last_processed_block", chain_doc.get("last_processed_block", 0))
        self.batch_size = chain_doc["batch_size"]
        self.block_delay = chain_doc["block_delay"]
        self.sleep_duration = chain_doc["loop_sleep_duration"]
        self.registry_address = chain_doc["registry_contract_address"]
        self.wasm_registry_address = chain_doc.get("wasm_registry_contract_address")
        self.parse_retry_delay = chain_doc.get("parse_retry_delay_seconds", 10)
        self.sync_threshold_blocks = chain_doc.get("sync_threshold_blocks", 5)
        self.web3 = Web3(Web3.HTTPProvider(self.rpc_url))
        # Load ABI for registry contract
        with open("registry_abi.json") as f:
            self.abi = json.load(f)
        self.contract = self.web3.eth.contract(
            address=self.registry_address, abi=self.abi
        )
        # Initialize WASM registry contract if address is provided and not "--"
        self.wasm_contract = None
        if self.wasm_registry_address and self.wasm_registry_address != "--":
            self.wasm_contract = self.web3.eth.contract(
                address=self.wasm_registry_address, abi=self.abi
            )
        # Initialize decoder cache
        self._init_decoder_cache()
        # Initialize WASM decoder cache if WASM registry is configured
        if self.wasm_contract:
            self._init_wasm_decoder_cache()

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
        """Initialize event decoders cache for regular registry"""
        self._topic_map = {}

        # Grab just the *event* ABIs from the full contract ABI
        event_abis = filter_abi_by_type("event", self.abi)

        for ev_abi in event_abis:
            # Only include non-WASM events in regular registry cache
            if ev_abi["name"] not in EventType.get_target_names():
                continue
            if ev_abi["name"] in [EventType.WASM_CREATED.value, EventType.WASM_UPDATED.value]:
                continue

            # canonical signature, e.g. "Created(address,string,uint256)"
            sig_text = abi_to_signature(ev_abi)
            topic_hash = self.web3.keccak(text=sig_text).hex()

            # Create decoder class once (web3.contract.ContractEvent)
            decoder_cls = getattr(self.contract.events, ev_abi["name"])
            decoder = decoder_cls()  # Create instance once

            self._topic_map[topic_hash] = (ev_abi["name"], decoder)

    def _init_wasm_decoder_cache(self):
        """Initialize event decoders cache for WASM registry"""
        self._wasm_topic_map = {}

        # Grab just the *event* ABIs from the full contract ABI
        event_abis = filter_abi_by_type("event", self.abi)

        for ev_abi in event_abis:
            # Only include WASM events in WASM registry cache
            if ev_abi["name"] not in [EventType.WASM_CREATED.value, EventType.WASM_UPDATED.value]:
                continue

            # canonical signature, e.g. "WasmCreated(bytes32,string,address)"
            sig_text = abi_to_signature(ev_abi)
            topic_hash = self.web3.keccak(text=sig_text).hex()

            # Create decoder class once (web3.contract.ContractEvent)
            decoder_cls = getattr(self.wasm_contract.events, ev_abi["name"])
            decoder = decoder_cls()  # Create instance once

            self._wasm_topic_map[topic_hash] = (ev_abi["name"], decoder)

    def _process_registry_batch(self, batch_start, batch_end):
        """
        Processes a single batch of blocks for regular registry.
        Returns True on success, False on failure.
        Raises an exception if parsing fails, to be caught by the retry logic.
        """
        # Get logs from regular registry
        raw_logs = self.web3.eth.get_logs(
            {
                "address": self.registry_address,
                "fromBlock": batch_start,
                "toBlock": batch_end,
            }
        )

        decoded_events = []
        
        # Process regular registry logs
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
            if name == EventType.RUN.value or name == EventType.RUN_WITH_METADATA.value:
                tx_receipt = self.web3.eth.get_transaction_receipt(
                    raw["transactionHash"]
                )

            decoded_events.append((name, decoded, timestamp, tx_receipt))

        with self.db.db_session() as session:
            for name, evt, timestamp, tx_receipt in decoded_events:
                try:
                    if name == EventType.CREATED.value:
                        parse_created(evt, session, self.chain_id, timestamp, self.db)
                    elif name == EventType.RUN.value:
                        parse_run(
                            evt, session, self.chain_id, timestamp, self.db, tx_receipt
                        )
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

    def _process_wasm_batch(self, batch_start, batch_end):
        """
        Processes a single batch of blocks for WASM registry.
        Returns True on success, False on failure.
        Raises an exception if parsing fails, to be caught by the retry logic.
        """
        if not self.wasm_registry_address or self.wasm_registry_address == "--":
            return True

        # Get logs from WASM registry
        try:
            wasm_raw_logs = self.web3.eth.get_logs(
                {
                    "address": self.wasm_registry_address,
                    "fromBlock": batch_start,
                    "toBlock": batch_end,
                }
            )
        except Exception as e:
            logging.warning(
                f"[Chain {self.chain_id}] Error fetching WASM registry logs: {e}"
            )
            raise Exception("Failed to fetch WASM registry logs.")

        decoded_events = []

        # Process WASM registry logs
        for raw in wasm_raw_logs:
            topic0 = raw["topics"][0].hex()
            mapping = self._wasm_topic_map.get(topic0)
            if mapping is None:
                continue

            name, decoder = mapping
            timestamp = self.get_block_timestamp(raw["blockNumber"])
            if not timestamp:
                logging.warning(
                    f"Skipping WASM log in block {raw['blockNumber']} due to timestamp fetch error."
                )
                continue

            if hasattr(decoder, "process_log"):
                decoded = decoder.process_log(raw)
            else:
                decoded = decoder.processLog(raw)

            decoded_events.append((name, decoded, timestamp, None))

        with self.db.db_session() as session:
            for name, evt, timestamp, tx_receipt in decoded_events:
                try:
                    if name == EventType.WASM_CREATED.value:
                        parse_wasm_created(evt, session, self.chain_id, timestamp, self.db)
                    elif name == EventType.WASM_UPDATED.value:
                        parse_wasm_updated(evt, session, self.chain_id, timestamp, self.db)
                except Exception as e:
                    logging.error(
                        f"[Chain {self.chain_id}] Parse error for WASM tx {evt['transactionHash'].hex()}: {e}"
                    )
                    raise Exception("Batch has parsing errors.")

            self.db.update_chain_wasm_last_processed(
                self.chain_id, batch_end, session=session
            )

        return True

    def process_chain(self):
        """
        Walks through unprocessed blocks in batches for both registries, with a retry mechanism.
        Sync status is only True when both registries are synced.
        """
        # Load latest block numbers from database to ensure we have the most recent state
        with self.db.db_session() as session:
            chain = self.db.get_chain(self.chain_id, session=session)
            if chain:
                self.last_processed = chain.get("last_processed_block", self.last_processed)
                self.wasm_last_processed = chain.get("wasm_last_processed_block", self.wasm_last_processed)
        
        current_block = self.web3.eth.block_number
        latest_block = current_block - self.block_delay
        
        # Process regular registry
        registry_start_block = self.last_processed + 1
        if latest_block >= registry_start_block:
            for batch_start in range(registry_start_block, latest_block + 1, self.batch_size):
                batch_end = min(batch_start + self.batch_size - 1, latest_block)
                while True:
                    try:
                        logging.info(
                            f"[Chain {self.chain_id}] Processing registry batch {batch_start}→{batch_end}"
                        )
                        if self._process_registry_batch(batch_start, batch_end):
                            self.last_processed = batch_end
                            break  # Success, exit retry loop
                    except Exception as e:
                        logging.error(
                            f"[Chain {self.chain_id}] Registry batch {batch_start}→{batch_end} failed, retrying in {self.parse_retry_delay}s: {e}"
                        )
                        time.sleep(self.parse_retry_delay)

        # Process WASM registry if configured
        if self.wasm_registry_address and self.wasm_registry_address != "--":
            wasm_start_block = self.wasm_last_processed + 1
            if latest_block >= wasm_start_block:
                for batch_start in range(wasm_start_block, latest_block + 1, self.batch_size):
                    batch_end = min(batch_start + self.batch_size - 1, latest_block)
                    while True:
                        try:
                            logging.info(
                                f"[Chain {self.chain_id}] Processing WASM registry batch {batch_start}→{batch_end}"
                            )
                            if self._process_wasm_batch(batch_start, batch_end):
                                self.wasm_last_processed = batch_end
                                break  # Success, exit retry loop
                        except Exception as e:
                            logging.error(
                                f"[Chain {self.chain_id}] WASM registry batch {batch_start}→{batch_end} failed, retrying in {self.parse_retry_delay}s: {e}"
                            )
                            time.sleep(self.parse_retry_delay)

        # Check sync status - both registries must be synced
        registry_blocks_behind = current_block - self.last_processed
        registry_is_synced = registry_blocks_behind < self.sync_threshold_blocks
        
        # WASM registry sync check
        wasm_is_synced = True
        if self.wasm_registry_address and self.wasm_registry_address != "--":
            wasm_blocks_behind = current_block - self.wasm_last_processed
            wasm_is_synced = wasm_blocks_behind < self.sync_threshold_blocks
        else:
            # If WASM registry is not configured, consider it synced
            wasm_is_synced = True

        # Chain is synced only when both registries are synced
        is_synced = registry_is_synced and wasm_is_synced

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
                            f"[Chain {self.chain_id}] Marked as synced (registry: {registry_blocks_behind} blocks behind, WASM: {current_block - self.wasm_last_processed if (self.wasm_registry_address and self.wasm_registry_address != '--') else 'N/A'} blocks behind, threshold: {self.sync_threshold_blocks})"
                        )
                    else:
                        logging.info(
                            f"[Chain {self.chain_id}] Marked as not synced (registry: {registry_blocks_behind} blocks behind, WASM: {current_block - self.wasm_last_processed if (self.wasm_registry_address and self.wasm_registry_address != '--') else 'N/A'} blocks behind, threshold: {self.sync_threshold_blocks})"
                        )

    def run(self):
        logging.info(f"Worker started for chain {self.chain_id}")
        while True:
            try:
                self.process_chain()
            except Exception as e:
                logging.error(f"[Chain {self.chain_id}] Error: {e}")
            time.sleep(self.sleep_duration)
