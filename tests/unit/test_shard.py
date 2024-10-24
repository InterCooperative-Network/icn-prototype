# tests/unit/test_shard.py

import unittest
from datetime import datetime, timedelta
import sys
import os
from typing import List

# Add project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from blockchain.core.shard import Shard
from blockchain.core.block import Block
from blockchain.core.transaction import Transaction

class TestShard(unittest.TestCase):
    """Test cases for the Shard class."""

    def setUp(self):
        """Set up test fixtures before each test."""
        self.shard = Shard(shard_id=1, max_transactions_per_block=5)
        self.sample_transactions = self._create_sample_transactions()

    def _create_sample_transactions(self) -> List[Transaction]:
        """Create sample transactions for testing."""
        return [
            Transaction(
                sender=f"user{i}",
                receiver=f"user{i+1}",
                action="transfer",
                data={"amount": 10.0},
                shard_id=1
            ) for i in range(10)
        ]

    def test_initialization(self):
        """Test shard initialization."""
        self.assertEqual(self.shard.shard_id, 1)
        self.assertEqual(self.shard.max_transactions_per_block, 5)
        self.assertEqual(len(self.shard.chain), 1)  # Genesis block
        self.assertEqual(self.shard.height, 1)
        self.assertIsNotNone(self.shard.last_block_time)
        self.assertEqual(self.shard.metrics["blocks_created"], 1)

    def test_genesis_block(self):
        """Test genesis block creation and properties."""
        genesis = self.shard.chain[0]
        self.assertEqual(genesis.index, 0)
        self.assertEqual(genesis.previous_hash, "0" * 64)
        self.assertEqual(len(genesis.transactions), 0)
        self.assertEqual(genesis.validator, "genesis")
        self.assertEqual(genesis.shard_id, 1)

    def test_add_transaction(self):
        """Test adding transactions to the shard."""
        # Test valid transaction
        tx = self.sample_transactions[0]
        self.assertTrue(self.shard.add_transaction(tx))
        self.assertEqual(len(self.shard.pending_transactions), 1)

        # Test duplicate transaction
        self.assertFalse(self.shard.add_transaction(tx))
        self.assertEqual(len(self.shard.pending_transactions), 1)

        # Test transaction with wrong shard_id
        invalid_tx = Transaction(
            sender="user1",
            receiver="user2",
            action="transfer",
            data={"amount": 10.0},
            shard_id=2
        )
        self.assertFalse(self.shard.add_transaction(invalid_tx))

        # Test transaction pool limit
        for tx in self.sample_transactions[1:11]:  # Try to add 10 more
            self.shard.add_transaction(tx)
        self.assertLessEqual(
            len(self.shard.pending_transactions),
            self.shard.max_transactions_per_block * 2
        )

    def test_create_block(self):
        """Test block creation from pending transactions."""
        # Add some transactions
        for tx in self.sample_transactions[:3]:
            self.shard.add_transaction(tx)

        # Create block
        block = self.shard.create_block(validator="test_validator")
        self.assertIsNotNone(block)
        self.assertEqual(block.index, self.shard.height)
        self.assertEqual(block.shard_id, self.shard.shard_id)
        self.assertEqual(len(block.transactions), 3)
        self.assertEqual(block.validator, "test_validator")

        # Test with no pending transactions
        self.shard.pending_transactions = []
        block = self.shard.create_block(validator="test_validator")
        self.assertIsNone(block)

    def test_add_block(self):
        """Test adding blocks to the shard."""
        # Create and add a valid block
        for tx in self.sample_transactions[:3]:
            self.shard.add_transaction(tx)
        
        block = self.shard.create_block(validator="test_validator")
        initial_height = self.shard.height
        self.assertTrue(self.shard.add_block(block))
        self.assertEqual(self.shard.height, initial_height + 1)
        self.assertEqual(len(self.shard.pending_transactions), 0)

        # Test adding block with wrong shard_id
        invalid_block = Block(
            index=self.shard.height,
            previous_hash=self.shard.chain[-1].hash,
            timestamp=datetime.now(),
            transactions=[],
            validator="test_validator",
            shard_id=2
        )
        self.assertFalse(self.shard.add_block(invalid_block))

        # Test adding block with wrong index
        invalid_block = Block(
            index=self.shard.height + 1,
            previous_hash=self.shard.chain[-1].hash,
            timestamp=datetime.now(),
            transactions=[],
            validator="test_validator",
            shard_id=1
        )
        self.assertFalse(self.shard.add_block(invalid_block))

    def test_validate_chain(self):
        """Test chain validation."""
        # Initial chain should be valid
        self.assertTrue(self.shard.validate_chain())

        # Add some valid blocks
        for tx in self.sample_transactions[:3]:
            self.shard.add_transaction(tx)
        block = self.shard.create_block(validator="test_validator")
        self.shard.add_block(block)
        self.assertTrue(self.shard.validate_chain())

        # Tamper with a block
        self.shard.chain[1].transactions = []  # This should invalidate the block's hash
        self.assertFalse(self.shard.validate_chain())

    def test_get_block_by_hash(self):
        """Test retrieving blocks by hash."""
        # Add a block
        for tx in self.sample_transactions[:3]:
            self.shard.add_transaction(tx)
        block = self.shard.create_block(validator="test_validator")
        self.shard.add_block(block)

        # Test retrieval
        retrieved_block = self.shard.get_block_by_hash(block.hash)
        self.assertIsNotNone(retrieved_block)
        self.assertEqual(retrieved_block.hash, block.hash)

        # Test non-existent block
        self.assertIsNone(self.shard.get_block_by_hash("nonexistent"))

    def test_get_transaction_by_id(self):
        """Test retrieving transactions by ID."""
        # Add transactions and create block
        for tx in self.sample_transactions[:3]:
            self.shard.add_transaction(tx)
        
        # Test finding in pending transactions
        tx_id = self.sample_transactions[0].transaction_id
        found_tx = self.shard.get_transaction_by_id(tx_id)
        self.assertIsNotNone(found_tx)
        self.assertEqual(found_tx.transaction_id, tx_id)

        # Create and add block
        block = self.shard.create_block(validator="test_validator")
        self.shard.add_block(block)

        # Test finding in chain
        found_tx = self.shard.get_transaction_by_id(tx_id)
        self.assertIsNotNone(found_tx)
        self.assertEqual(found_tx.transaction_id, tx_id)

        # Test non-existent transaction
        self.assertIsNone(self.shard.get_transaction_by_id("nonexistent"))

    def test_prune_pending_transactions(self):
        """Test pruning old pending transactions."""
        # Add some transactions with old timestamps
        old_tx = Transaction(
            sender="user1",
            receiver="user2",
            action="transfer",
            data={"amount": 10.0},
            shard_id=1
        )
        old_tx.timestamp = datetime.now() - timedelta(hours=2)
        self.shard.add_transaction(old_tx)

        # Add some recent transactions
        self.shard.add_transaction(self.sample_transactions[0])

        # Prune old transactions
        self.shard.prune_pending_transactions(max_age_minutes=60)
        self.assertEqual(len(self.shard.pending_transactions), 1)

    def test_get_metrics(self):
        """Test metrics collection."""
        metrics = self.shard.get_metrics()
        self.assertIn("shard_id", metrics)
        self.assertIn("height", metrics)
        self.assertIn("pending_transactions", metrics)
        self.assertIn("chain_size", metrics)
        self.assertIn("total_transactions_in_chain", metrics)

    def test_serialization(self):
        """Test shard serialization and deserialization."""
        # Add some data to the shard
        for tx in self.sample_transactions[:3]:
            self.shard.add_transaction(tx)
        block = self.shard.create_block(validator="test_validator")
        self.shard.add_block(block)

        # Serialize
        shard_dict = self.shard.to_dict()

        # Deserialize
        new_shard = Shard.from_dict(shard_dict)

        # Verify
        self.assertEqual(new_shard.shard_id, self.shard.shard_id)
        self.assertEqual(new_shard.height, self.shard.height)
        self.assertEqual(len(new_shard.chain), len(self.shard.chain))
        self.assertEqual(
            len(new_shard.pending_transactions),
            len(self.shard.pending_transactions)
        )

    def test_cross_shard_references(self):
        """Test cross-shard transaction handling."""
        # Create a cross-shard transaction
        cross_shard_tx = Transaction(
            sender="user1",
            receiver="user2",
            action="transfer",
            data={"amount": 10.0, "target_shard": 2},
            shard_id=1
        )
        self.shard.add_transaction(cross_shard_tx)

        # Verify reference tracking
        self.assertIn(2, self.shard.cross_shard_references)
        self.assertIn(
            cross_shard_tx.transaction_id,
            self.shard.cross_shard_references[2]
        )

    def test_validation_cache(self):
        """Test transaction validation caching."""
        # Add a transaction
        tx = self.sample_transactions[0]
        self.shard.add_transaction(tx)

        # Verify cache
        self.assertIn(tx.transaction_id, self.shard.validation_cache)
        self.assertTrue(self.shard.validation_cache[tx.transaction_id])

        # Try to add same transaction again
        self.assertFalse(self.shard.add_transaction(tx))

        # Prune transactions and verify cache cleanup
        self.shard.prune_pending_transactions(max_age_minutes=0)
        self.assertNotIn(tx.transaction_id, self.shard.validation_cache)

if __name__ == '__main__':
    unittest.main()