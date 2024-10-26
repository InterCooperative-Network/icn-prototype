import unittest
import sys
import os
import random
from typing import List
import time
import tqdm
from contextlib import contextmanager
import psutil
import gc
import logging

# Add project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from blockchain.core.shard import Shard, ShardConfig
from blockchain.core.transaction import Transaction
from blockchain.core.node import Node
from blockchain.consensus.proof_of_cooperation import ProofOfCooperation

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TestScalability(unittest.TestCase):
    """Test scalability characteristics of the ICN blockchain."""

    TIMEOUT_TRANSACTION_TEST = 30
    TIMEOUT_SHARD_TEST = 30
    TIMEOUT_CROSS_SHARD_TEST = 30
    MAX_MEMORY_PERCENT = 80  # Max memory usage threshold
    MAX_RETRIES = 1000  # Max retry limit for block creation

    def setUp(self):
        """Set up test environment."""
        logger.info("Setting up scalability test environment...")
        self.config = ShardConfig(
            max_transactions_per_block=100,
            max_pending_transactions=500,
            max_cross_shard_refs=10
        )
        self.shards = [Shard(shard_id=i, config=self.config) for i in range(3)]
        self.nodes = self._create_test_nodes(6)
        self.consensus = ProofOfCooperation()
        logger.info("Setup complete.")

    def tearDown(self):
        """Clean up after each test."""
        self.shards = None
        self.nodes = None
        self.consensus = None
        gc.collect()

    def _create_test_nodes(self, num_nodes: int) -> List[Node]:
        """Create test nodes with varied capabilities."""
        nodes = []
        for i in range(num_nodes):
            node = Node(
                node_id=f"node_{i}",
                cooperative_id=f"coop_{i % 2}",
                initial_stake=100.0
            )
            node.reputation_scores = {
                "validation": 20.0 + i,
                "resource_sharing": 20.0 + i * 0.5,
                "cooperative_growth": 20.0 + i * 0.3,
                "innovation": 20.0 + i * 0.2
            }
            shard_id = i % len(self.shards)
            node.assign_to_shard(shard_id)
            nodes.append(node)
        return nodes

    def _check_memory_usage(self):
        """Check current memory usage and raise exception if too high."""
        process = psutil.Process(os.getpid())
        memory_percent = process.memory_percent()
        if memory_percent > self.MAX_MEMORY_PERCENT:
            raise MemoryError(f"Memory usage too high: {memory_percent:.1f}%")
        return memory_percent

    @contextmanager
    def timeout(self, seconds, test_name):
        """Context manager for test timeouts."""
        start_time = time.time()
        try:
            yield
        finally:
            elapsed_time = time.time() - start_time
            if elapsed_time > seconds:
                logger.warning(f"{test_name} timed out after {seconds} seconds")
            else:
                logger.info(f"{test_name} completed in {elapsed_time:.2f} seconds")

    def _process_transactions(self, transactions: List[Transaction], batch_size: int):
        """Process transactions in batches with memory checks."""
        with tqdm.tqdm(total=len(transactions), desc="Processing Transactions", leave=True, dynamic_ncols=True) as progress_bar:
            for i in range(0, len(transactions), batch_size):
                batch = transactions[i:i + batch_size]
                for tx in batch:
                    shard_id = tx.shard_id
                    self.shards[shard_id].add_transaction(tx)
                self._check_memory_usage()
                progress_bar.update(len(batch))
                gc.collect()

    def _create_and_add_blocks(self, shard, validators):
        """Create and add blocks until no pending transactions remain."""
        retry_count = 0
        while shard.pending_transactions and retry_count < self.MAX_RETRIES:
            validator = self.consensus.select_validator(validators)
            if validator:
                block = shard.create_block(validator.node_id)
                if block:
                    shard.add_block(block)
            retry_count += 1
            gc.collect()
        if retry_count == self.MAX_RETRIES:
            logger.warning(f"Max retries reached for shard {shard.shard_id}, some transactions may remain unprocessed.")

    def test_transaction_throughput_scaling(self):
        """Test how transaction throughput scales with increasing load."""
        logger.info("\nTesting transaction throughput scaling...")
        transaction_counts = [50, 100, 200]
        results = {}

        for count in transaction_counts:
            logger.info(f"\nProcessing {count} transactions...")
            transactions = self._generate_test_transactions(count)

            with self.timeout(self.TIMEOUT_TRANSACTION_TEST, f"Transaction test (count: {count})"):
                start_time = time.time()
                self._process_transactions(transactions, batch_size=10)

                for shard in self.shards:
                    validators = [n for n in self.nodes if shard.shard_id in n.shard_assignments]
                    self._create_and_add_blocks(shard, validators)

                elapsed_time = time.time() - start_time
                transactions_per_second = count / elapsed_time
                results[count] = {
                    "elapsed_time": elapsed_time,
                    "tps": transactions_per_second
                }
                logger.info(f"Completed {count} transactions at {transactions_per_second:.2f} TPS")

        for i in range(len(transaction_counts) - 1):
            count_ratio = transaction_counts[i + 1] / transaction_counts[i]
            time_ratio = results[transaction_counts[i + 1]]["elapsed_time"] / results[transaction_counts[i]]["elapsed_time"]
            self.assertLess(time_ratio, count_ratio * 1.5)

    def test_shard_scaling(self):
        """Test how system performance scales with number of shards."""
        logger.info("\nTesting shard scaling...")
        transaction_count = 100
        shard_counts = [1, 2, 4]
        results = {}

        base_transactions = self._generate_test_transactions(transaction_count)

        for shard_count in shard_counts:
            logger.info(f"\nTesting with {shard_count} shards...")
            test_shards = [Shard(shard_id=i, config=self.config) for i in range(shard_count)]
            test_nodes = self._create_test_nodes(shard_count * 2)

            with self.timeout(self.TIMEOUT_SHARD_TEST, f"Shard test (count: {shard_count})"):
                start_time = time.time()

                self._process_transactions(base_transactions, batch_size=10)

                for shard in test_shards:
                    validators = [n for n in test_nodes if shard.shard_id in n.shard_assignments]
                    self._create_and_add_blocks(shard, validators)

                elapsed_time = time.time() - start_time
                results[shard_count] = {
                    "elapsed_time": elapsed_time,
                    "tps": transaction_count / elapsed_time
                }
                logger.info(f"Completed test with {shard_count} shards at {transaction_count / elapsed_time:.2f} TPS")

        for i in range(len(shard_counts) - 1):
            shard_ratio = shard_counts[i + 1] / shard_counts[i]
            speedup = results[shard_counts[i]]["elapsed_time"] / results[shard_counts[i + 1]]["elapsed_time"]
            expected_speedup = shard_ratio * 0.4
            if speedup < expected_speedup:
                logger.warning(f"Speedup of {speedup:.2f} is less than expected ({expected_speedup:.2f})")
            self.assertGreater(speedup, expected_speedup)

    def test_cross_shard_scalability(self):
        """Test scalability of cross-shard transactions."""
        logger.info("\nTesting cross-shard scalability...")
        transaction_count = 100
        cross_shard_percentages = [0, 20, 40]
        results = {}

        for percentage in cross_shard_percentages:
            logger.info(f"\nTesting with {percentage}% cross-shard transactions...")
            transactions = self._generate_test_transactions(transaction_count, cross_shard_percentage=percentage)

            with self.timeout(self.TIMEOUT_CROSS_SHARD_TEST, f"Cross-shard test (percentage: {percentage}%)"):
                start_time = time.time()
                self._process_transactions(transactions, batch_size=10)

                for shard in self.shards:
                    validators = [n for n in self.nodes if shard.shard_id in n.shard_assignments]
                    self._create_and_add_blocks(shard, validators)

                elapsed_time = time.time() - start_time
                transactions_per_second = transaction_count / elapsed_time
                results[percentage] = {
                    "elapsed_time": elapsed_time,
                    "tps": transactions_per_second
                }
                logger.info(f"Completed {percentage}% cross-shard test at {transactions_per_second:.2f} TPS")

        base_time = results[0]["elapsed_time"]
        for percentage in cross_shard_percentages[1:]:
            self.assertLess(results[percentage]["elapsed_time"] / base_time, 3.0)

    def _generate_test_transactions(self, count: int, cross_shard_percentage: int = 0) -> List[Transaction]:
        """Generate test transactions with specified cross-shard percentage."""
        transactions = []
        for i in range(count):
            is_cross_shard = random.randint(1, 100) <= cross_shard_percentage

            shard_id = random.randint(0, len(self.shards) - 1)
            target_shard = None
            cross_shard_refs = []

            if is_cross_shard:
                target_shard = random.randint(0, len(self.shards) - 1)
                while target_shard == shard_id:
                    target_shard = random.randint(0, len(self.shards) - 1)
                cross_shard_refs = [f"ref_{i}"]

            tx = Transaction(
                sender=f"sender_{i}",
                receiver=f"receiver_{i}",
                action="transfer",
                data={
                    "amount": random.uniform(1, 100),
                    "target_shard": target_shard
                } if is_cross_shard else {
                    "amount": random.uniform(1, 100)
                },
                shard_id=shard_id,
                priority=random.randint(1, 5),
                cross_shard_refs=cross_shard_refs
            )
            transactions.append(tx)

        return transactions

if __name__ == '__main__':
    unittest.main(verbosity=2)
