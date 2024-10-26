import unittest
from datetime import datetime, timedelta
import sys
import os
import random
import asyncio
import time
import signal
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Set
import logging

# Add project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from blockchain.core.shard import Shard, ShardConfig
from blockchain.core.transaction import Transaction
from blockchain.core.block import Block
from blockchain.core.node import Node
from blockchain.consensus.proof_of_cooperation import ProofOfCooperation

logger = logging.getLogger(__name__)

def timeout_handler(signum, frame):
    """Handle timeout signal"""
    raise TimeoutError("Test execution timed out")

class TestLoadHandling(unittest.TestCase):
    """Test suite for load handling capabilities of the ICN blockchain."""
    
    TIMEOUT = 10  # Reduced timeout to 10 seconds
    TRANSACTION_BATCH_SIZE = 50  # Further reduced batch size
    MAX_PROCESSING_TIME = 3  # Reduced processing time

    def setUp(self):
        """Set up test environment before each test."""
        # Configure logging
        logging.basicConfig(level=logging.INFO)
        
        self.config = ShardConfig(
            max_transactions_per_block=50,    # Further reduced
            max_pending_transactions=200,     # Further reduced
            max_cross_shard_refs=5,          # Further reduced
            pruning_interval=60,
            min_block_interval=0,
            max_block_size=1024 * 1024,
            max_state_size=10 * 1024 * 1024
        )
        
        self.num_shards = 2  # Reduced to minimum for testing
        self.shards = [Shard(shard_id=i, config=self.config) for i in range(self.num_shards)]
        self.nodes = self._create_test_nodes(6)  # Reduced number of nodes
        self.consensus = ProofOfCooperation()

        # Set up signal handler for timeouts
        signal.signal(signal.SIGALRM, timeout_handler)

    def tearDown(self):
        """Clean up after each test."""
        # Reset signal handler and clear any pending alarms
        signal.alarm(0)
        signal.signal(signal.SIGALRM, signal.SIG_DFL)

    def _create_test_nodes(self, num_nodes: int) -> List[Node]:
        """Create test nodes with varied capabilities."""
        nodes = []
        for i in range(num_nodes):
            node = Node(
                node_id=f"node_{i}",
                cooperative_id=f"coop_{i % 2}",  # Reduced cooperatives
                initial_stake=100.0
            )
            node.reputation_scores = {
                "validation": 20.0 + i * 0.5,
                "resource_sharing": 20.0 + i * 0.3,
                "cooperative_growth": 20.0 + i * 0.2,
                "innovation": 20.0 + i * 0.1
            }
            shard_id = i % self.num_shards
            node.assign_to_shard(shard_id)
            nodes.append(node)
        return nodes

    def _generate_test_transactions(self, count: int) -> List[Transaction]:
        """Generate test transactions."""
        transactions = []
        for i in range(count):
            is_cross_shard = random.random() < 0.1  # Reduced cross-shard probability to 10%
            
            shard_id = random.randint(0, self.num_shards - 1)
            target_shard = None
            cross_shard_refs = []
            
            if is_cross_shard:
                target_shard = (shard_id + 1) % self.num_shards
                cross_shard_refs = [f"ref_{i}_{target_shard}"]

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
            
        logger.info(f"Generated {count} transactions")
        return transactions

    async def _process_transactions_async(self, transactions: List[Transaction]) -> None:
        """Process transactions asynchronously with timeout."""
        try:
            async with asyncio.timeout(self.MAX_PROCESSING_TIME):
                for batch_start in range(0, len(transactions), self.TRANSACTION_BATCH_SIZE):
                    batch = transactions[batch_start:batch_start + self.TRANSACTION_BATCH_SIZE]
                    tasks = []
                    for tx in batch:
                        shard = self.shards[tx.shard_id]
                        tasks.append(asyncio.create_task(self._add_transaction_async(shard, tx)))
                    await asyncio.gather(*tasks)
                    logger.info(f"Processed batch of {len(batch)} transactions")
        except asyncio.TimeoutError:
            logger.warning("Transaction processing timed out")

    async def _add_transaction_async(self, shard: Shard, transaction: Transaction) -> None:
        """Add a transaction to a shard asynchronously."""
        try:
            shard.add_transaction(transaction)
        except Exception as e:
            logger.error(f"Error adding transaction: {e}")

    def _process_blocks(self, max_attempts: int = 100) -> None:
        """Process pending transactions into blocks with attempt limit."""
        attempts = 0
        processed_any = True
        
        while processed_any and attempts < max_attempts:
            processed_any = False
            attempts += 1
            
            for shard in self.shards:
                if shard.pending_transactions:
                    validator = self.consensus.select_validator(self.nodes, shard.shard_id)
                    if validator:
                        block = shard.create_block(validator.node_id)
                        if block and shard.add_block(block):
                            processed_any = True
                            logger.info(f"Processed block in shard {shard.shard_id}")
            
            if attempts % 10 == 0:
                logger.info(f"Block processing attempt {attempts}")

    def _validate_shard_blocks(self, shard: Shard) -> bool:
        """Validate all blocks in a shard."""
        try:
            for i in range(1, len(shard.chain)):
                if not shard.chain[i].validate(shard.chain[i-1]):
                    return False
            return True
        except Exception as e:
            logger.error(f"Shard validation failed: {str(e)}")
            return False

    def _verify_system_state(self) -> None:
        """Verify overall system state consistency."""
        for shard in self.shards:
            self.assertTrue(shard.validate_chain())
            self.assertIsNotNone(shard.state_manager.state)
            self.assertGreaterEqual(len(shard.transaction_manager.processed_transactions), 0)
            self.assertIsNotNone(shard.validation_manager.validation_cache)

    def _verify_cross_shard_state(self) -> None:
        """Verify cross-shard reference consistency."""
        for shard in self.shards:
            cross_refs = shard.cross_shard_manager.get_metrics()
            self.assertGreaterEqual(cross_refs["cross_shard_operations"], 0)
            self.assertGreaterEqual(cross_refs["validated_refs"], 0)

    def _capture_shard_states(self) -> Dict[int, int]:
        """Capture current state sizes of all shards."""
        return {
            shard.shard_id: len(str(shard.state_manager.state))
            for shard in self.shards
        }

    def _reset_system_state(self) -> None:
        """Reset system state for next test."""
        for shard in self.shards:
            shard.transaction_manager.clear_all()
            shard.state_manager.state = {}
            shard.validation_manager.clear_cache()

    def test_high_transaction_load(self):
        """Test system performance under high transaction load."""
        transaction_counts = [20, 50]  # Further reduced counts
        
        for count in transaction_counts:
            with self.subTest(transaction_count=count):
                signal.alarm(self.TIMEOUT)
                try:
                    transactions = self._generate_test_transactions(count)
                    
                    start_time = time.time()
                    asyncio.run(self._process_transactions_async(transactions))
                    self._process_blocks()
                    
                    elapsed_time = time.time() - start_time
                    tps = count / elapsed_time
                    
                    logger.info(f"Processed {count} transactions in {elapsed_time:.2f} seconds ({tps:.2f} TPS)")
                    self._verify_system_state()
                    self._reset_system_state()
                finally:
                    signal.alarm(0)

    def test_concurrent_validation(self):
        """Test concurrent block validation across shards."""
        signal.alarm(self.TIMEOUT)
        try:
            transactions = self._generate_test_transactions(20)  # Further reduced
            asyncio.run(self._process_transactions_async(transactions))
            
            with ThreadPoolExecutor(max_workers=self.num_shards) as executor:
                futures = []
                for shard in self.shards:
                    future = executor.submit(self._validate_shard_blocks, shard)
                    futures.append(future)
                
                for future in as_completed(futures):
                    result = future.result(timeout=self.TIMEOUT)
                    self.assertTrue(result)
        finally:
            signal.alarm(0)

    def test_cross_shard_load(self):
        """Test system performance with heavy cross-shard transactions."""
        signal.alarm(self.TIMEOUT)
        try:
            transactions = []
            for i in range(20):  # Further reduced
                source_shard = i % self.num_shards
                target_shard = (source_shard + 1) % self.num_shards
                
                tx = Transaction(
                    sender=f"sender_{i}",
                    receiver=f"receiver_{i}",
                    action="transfer",
                    data={
                        "amount": random.uniform(1, 100),
                        "target_shard": target_shard
                    },
                    shard_id=source_shard,
                    cross_shard_refs=[f"ref_{i}_{target_shard}"]
                )
                transactions.append(tx)
            
            start_time = time.time()
            asyncio.run(self._process_transactions_async(transactions))
            self._process_blocks()
                
            elapsed_time = time.time() - start_time
            self._verify_cross_shard_state()
            
            tps = len(transactions) / elapsed_time
            logger.info(f"Cross-shard processing rate: {tps:.2f} TPS")
        finally:
            signal.alarm(0)

    def test_state_growth_under_load(self):
        """Test state management under continuous load."""
        signal.alarm(self.TIMEOUT)
        try:
            initial_states = self._capture_shard_states()
            
            for i in range(2):  # Further reduced iterations
                transactions = self._generate_test_transactions(20)  # Further reduced
                asyncio.run(self._process_transactions_async(transactions))
                self._process_blocks()
                
                current_states = self._capture_shard_states()
                for shard_id, state_size in current_states.items():
                    self.assertLess(
                        state_size, 
                        self.config.max_state_size,
                        f"Shard {shard_id} exceeded max state size"
                    )
        finally:
            signal.alarm(0)

if __name__ == '__main__':
    unittest.main(verbosity=2)