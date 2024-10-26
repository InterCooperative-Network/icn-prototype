# tests/performance/test_scalability.py

import unittest
from datetime import datetime, timedelta
import sys
import os
import random
from typing import List, Dict
import time
import tqdm
from contextlib import contextmanager

# Add project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from blockchain.core.shard import Shard, ShardConfig
from blockchain.core.transaction import Transaction
from blockchain.core.block import Block
from blockchain.core.node import Node
from blockchain.consensus.proof_of_cooperation import ProofOfCooperation

class TestScalability(unittest.TestCase):
    """Test scalability characteristics of the ICN blockchain."""
    
    # Maximum time for each test in seconds
    TIMEOUT_TRANSACTION_TEST = 60
    TIMEOUT_SHARD_TEST = 60
    TIMEOUT_CROSS_SHARD_TEST = 60

    def setUp(self):
        """Set up test environment."""
        print("\nSetting up scalability test environment...")
        self.config = ShardConfig(
            max_transactions_per_block=1000,
            max_pending_transactions=5000,
            max_cross_shard_refs=100
        )
        self.shards = [Shard(shard_id=i, config=self.config) for i in range(5)]
        self.nodes = self._create_test_nodes(20)  # 20 nodes across shards
        self.consensus = ProofOfCooperation()
        print("Setup complete.")

    def _create_test_nodes(self, num_nodes: int) -> List[Node]:
        """Create test nodes with varied capabilities."""
        nodes = []
        for i in range(num_nodes):
            node = Node(
                node_id=f"node_{i}",
                cooperative_id=f"coop_{i % 3}",  # Distribute across 3 cooperatives
                initial_stake=100.0
            )
            # Vary node capabilities
            node.reputation_scores = {
                "validation": 20.0 + i,
                "resource_sharing": 20.0 + i * 0.5,
                "cooperative_growth": 20.0 + i * 0.3,
                "innovation": 20.0 + i * 0.2
            }
            # Assign to random shard
            shard_id = random.randint(0, len(self.shards) - 1)
            node.assign_to_shard(shard_id)
            nodes.append(node)
        return nodes

    @contextmanager
    def timeout(self, seconds, test_name):
        """Context manager for test timeouts with progress bar."""
        start_time = time.time()
        progress_bar = tqdm.tqdm(total=seconds, desc=f"Running {test_name}", 
                                unit="s", leave=True)

        def update_progress():
            elapsed = int(time.time() - start_time)
            if elapsed <= seconds:
                progress_bar.n = elapsed
                progress_bar.refresh()

        try:
            while True:
                if time.time() - start_time > seconds:
                    raise TimeoutError(f"Test {test_name} timed out after {seconds} seconds")
                update_progress()
                yield
                break
        finally:
            progress_bar.close()

    def test_transaction_throughput_scaling(self):
        """Test how transaction throughput scales with increasing load."""
        print("\nTesting transaction throughput scaling...")
        transaction_counts = [100, 500, 1000]  # Reduced counts for faster testing
        results = {}

        for count in transaction_counts:
            print(f"\nProcessing {count} transactions...")
            with self.timeout(self.TIMEOUT_TRANSACTION_TEST, f"Transaction test (count: {count})"):
                # Generate test transactions
                transactions = self._generate_test_transactions(count)
                
                # Measure processing time
                start_time = time.time()
                
                # Process transactions with progress bar
                for tx in tqdm.tqdm(transactions, desc="Adding transactions", leave=False):
                    shard_id = tx.shard_id
                    self.shards[shard_id].add_transaction(tx)

                # Process blocks with progress bar
                pending_blocks = True
                while pending_blocks:
                    pending_blocks = False
                    for shard in self.shards:
                        if len(shard.pending_transactions) > 0:
                            pending_blocks = True
                            validator = self.consensus.select_validator(
                                [n for n in self.nodes if shard.shard_id in n.shard_assignments]
                            )
                            if validator:
                                block = shard.create_block(validator.node_id)
                                if block:
                                    shard.add_block(block)

                elapsed_time = time.time() - start_time
                transactions_per_second = count / elapsed_time
                
                results[count] = {
                    "elapsed_time": elapsed_time,
                    "tps": transactions_per_second
                }
                print(f"Completed {count} transactions at {transactions_per_second:.2f} TPS")

        # Verify scaling characteristics
        for i in range(len(transaction_counts) - 1):
            count_ratio = transaction_counts[i + 1] / transaction_counts[i]
            time_ratio = results[transaction_counts[i + 1]]["elapsed_time"] / \
                        results[transaction_counts[i]]["elapsed_time"]
            
            # Time increase should be less than proportional to transaction increase
            self.assertLess(time_ratio, count_ratio * 1.5)

    def test_shard_scaling(self):
        """Test how system performance scales with number of shards."""
        print("\nTesting shard scaling...")
        transaction_count = 500  # Reduced for faster testing
        shard_counts = [1, 2, 4]  # Reduced counts for faster testing
        results = {}

        base_transactions = self._generate_test_transactions(transaction_count)

        for shard_count in shard_counts:
            print(f"\nTesting with {shard_count} shards...")
            with self.timeout(self.TIMEOUT_SHARD_TEST, f"Shard test (count: {shard_count})"):
                test_shards = [Shard(shard_id=i, config=self.config) 
                              for i in range(shard_count)]
                test_nodes = self._create_test_nodes(shard_count * 4)
                
                start_time = time.time()
                
                # Process transactions with progress bar
                for tx in tqdm.tqdm(base_transactions, desc="Processing transactions", leave=False):
                    tx.shard_id = random.randint(0, shard_count - 1)
                    test_shards[tx.shard_id].add_transaction(tx)

                # Process blocks
                pending_blocks = True
                with tqdm.tqdm(desc="Processing blocks", leave=False) as pbar:
                    while pending_blocks:
                        pending_blocks = False
                        for shard in test_shards:
                            if len(shard.pending_transactions) > 0:
                                pending_blocks = True
                                validator = self.consensus.select_validator(
                                    [n for n in test_nodes if shard.shard_id in n.shard_assignments]
                                )
                                if validator:
                                    block = shard.create_block(validator.node_id)
                                    if block:
                                        shard.add_block(block)
                        pbar.update(1)

                elapsed_time = time.time() - start_time
                results[shard_count] = {
                    "elapsed_time": elapsed_time,
                    "tps": transaction_count / elapsed_time
                }
                print(f"Completed test with {shard_count} shards at {transaction_count / elapsed_time:.2f} TPS")

        # Verify scaling efficiency
        for i in range(len(shard_counts) - 1):
            shard_ratio = shard_counts[i + 1] / shard_counts[i]
            speedup = results[shard_counts[i]]["elapsed_time"] / \
                     results[shard_counts[i + 1]]["elapsed_time"]
            
            # Should achieve at least 50% of perfect linear speedup
            self.assertGreater(speedup, shard_ratio * 0.5)

    def test_cross_shard_scalability(self):
        """Test scalability of cross-shard transactions."""
        print("\nTesting cross-shard scalability...")
        transaction_count = 500  # Reduced for faster testing
        cross_shard_percentages = [0, 20, 50]  # Reduced for faster testing
        results = {}

        for percentage in cross_shard_percentages:
            print(f"\nTesting with {percentage}% cross-shard transactions...")
            with self.timeout(self.TIMEOUT_CROSS_SHARD_TEST, 
                            f"Cross-shard test (percentage: {percentage}%)"):
                transactions = self._generate_test_transactions(
                    transaction_count, 
                    cross_shard_percentage=percentage
                )
                
                start_time = time.time()
                
                # Process transactions with progress bar
                for tx in tqdm.tqdm(transactions, desc="Processing transactions", leave=False):
                    shard_id = tx.shard_id
                    self.shards[shard_id].add_transaction(tx)

                # Process blocks
                pending_shards = set(range(len(self.shards)))
                with tqdm.tqdm(desc="Processing blocks", leave=False) as pbar:
                    while pending_shards:
                        for shard_id in list(pending_shards):
                            shard = self.shards[shard_id]
                            if not shard.pending_transactions:
                                pending_shards.remove(shard_id)
                                continue
                                
                            validator = self.consensus.select_validator(
                                [n for n in self.nodes if shard_id in n.shard_assignments]
                            )
                            if validator:
                                block = shard.create_block(validator.node_id)
                                if block:
                                    shard.add_block(block)
                        pbar.update(1)

                elapsed_time = time.time() - start_time
                results[percentage] = {
                    "elapsed_time": elapsed_time,
                    "tps": transaction_count / elapsed_time
                }
                print(f"Completed {percentage}% cross-shard test at {transaction_count / elapsed_time:.2f} TPS")

        # Verify cross-shard scaling characteristics
        base_time = results[0]["elapsed_time"]
        for percentage in cross_shard_percentages[1:]:
            # Even at 50% cross-shard, shouldn't be more than 3x slower
            self.assertLess(
                results[percentage]["elapsed_time"] / base_time,
                3.0
            )

    def _generate_test_transactions(
        self, 
        count: int, 
        cross_shard_percentage: int = 0
    ) -> List[Transaction]:
        """Generate test transactions with specified cross-shard percentage."""
        transactions = []
        for i in range(count):
            # Determine if this should be a cross-shard transaction
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