import logging
import threading
import time
import json
from web3 import Web3
from db import (
    update_chain_last_processed,
    db_session,
)
from eth_utils.abi import abi_to_signature, filter_abi_by_type
from event_parsers import parse_created, parse_run, parse_cancelled
from datetime import datetime, timezone


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

    # At top-of-file (with the other imports)

    def process_chain(self):
        """
        • Compute the newest "safe" block (latest – block_delay)
        • Walk through unprocessed blocks in batches
        • ONE eth_getLogs per batch (address-filtered)
        • Decode logs locally, route to the right parser, commit atomically
        """

        # ------------------------------------------------------------------
        # Build & cache: topic-0  ➜  (event_name, decoder_class)
        # ------------------------------------------------------------------
        # We only have to build this once per worker thread.
        if not hasattr(self, "_topic_map"):
            target_names = {"Created", "Run", "Cancelled"}

            # Grab just the *event* ABIs from the full contract ABI
            event_abis = filter_abi_by_type("event", self.abi)

            self._topic_map = {}
            for ev_abi in event_abis:
                if ev_abi["name"] not in target_names:
                    continue

                # canonical signature, e.g. "Created(address,string,uint256)"
                sig_text = abi_to_signature(ev_abi)
                topic_hash = self.web3.keccak(text=sig_text).hex()

                # decoder class (web3.contract.ContractEvent)
                decoder_cls = getattr(self.contract.events, ev_abi["name"])
                self._topic_map[topic_hash] = (ev_abi["name"], decoder_cls)

        # ------------------------------------------------------------------
        # Determine block range to scan
        # ------------------------------------------------------------------
        latest_block = self.web3.eth.block_number - self.block_delay
        start_block = self.last_processed + 1
        if latest_block < start_block:
            return  # nothing new
        end_block = latest_block

        # ------------------------------------------------------------------
        # Batch loop
        # ------------------------------------------------------------------
        for batch_start in range(start_block, end_block + 1, self.batch_size):
            batch_end = min(batch_start + self.batch_size - 1, end_block)
            logging.info(
                f"[Chain {self.chain_id}] Fetching logs {batch_start} → {batch_end}"
            )

            # ---- ONE eth_getLogs for the whole batch ----
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

            # ---- Decode & bucket -----------------------------------------
            decoded_events = []  # (name, decoded_log, timestamp)
            block_ts_cache = {}  # block_number -> ISO timestamp
            for raw in raw_logs:
                topic0 = raw["topics"][0].hex()
                mapping = self._topic_map.get(topic0)
                if mapping is None:  # not Created / Run / Cancelled
                    continue

                name, decoder_cls = mapping
                decoder = decoder_cls()

                try:  # Web3.py 6.x vs 5.x compatibility
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
                    decoded_events.append((name, decoded, timestamp))
                except Exception as e:
                    logging.error(f"[Chain {self.chain_id}] {name} decode error: {e}")

            # ---- Persist atomically --------------------------------------
            with db_session() as session:
                for name, evt, timestamp in decoded_events:
                    try:
                        if name == "Created":
                            parse_created(evt, session, self.chain_id, timestamp)
                        elif name == "Run":
                            parse_run(evt, session, self.chain_id, timestamp)
                        else:  # Cancelled
                            parse_cancelled(evt, session, self.chain_id, timestamp)
                    except Exception as e:
                        logging.error(
                            f"[Chain {self.chain_id}] {name} parse error: {e}"
                        )

                update_chain_last_processed(self.chain_id, batch_end, session=session)

            # ready for next batch
            self.last_processed = batch_end
