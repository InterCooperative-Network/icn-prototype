# tests/integration/test_shard_management.py

import unittest
from datetime import datetime, timedelta
from typing import List

# Adjust imports based on your project structure
from blockchain.core.shard import Shard
from blockchain.core.transaction import Transaction
from blockchain.core.block import Block

class TestShardManagement(unittest.TestCase):
    """Integration tests for shard management in the ICN blockchain."""

    def setUp(self):
        """Set up initial test conditions before each test."""
        self.shard = Shard(shard_id=1, max_transactions_per_block=5)
        self.sample_transactions = self._create_sample_transactions()

    def _create_sample_transactions(self) -> List[Transaction]:
        """Create a set of sample transactions for testing."""
        return [
            Transaction(
                sender=f"user{i}",
                receiver=f"user{i+1}",
                action="transfer",
                data={"amount": 10.0},
                shard_id=1
            ) for i in range(10)
        ]

    def test_shard_initialization(self):
        """Test proper initialization of a shard."""
        self.assertEqual(self.shard.shard_id, 1, "Shard ID mismatch")
        self.assertEqual(len(self.shard.chain), 1, "Shard should start with a genesis block")
        self.assertEqual(self.shard.height, 1, "Initial shard height should be 1")
        self.assertEqual(self.shard.max_transactions_per_block, 5, "Max transactions per block mismatch")

    def test_transaction_assignment_to_shard(self):
        """Test that transactions are correctly assigned to the shard."""
        for tx in self.sample_transactions[:5]:
            self.shard.add_transaction(tx)
        
        self.assertEqual(len(self.shard.pending_transactions), 5, "Transaction count mismatch in shard")
        for tx in self.shard.pending_transactions:
            self.assertEqual(tx.shard_id, self.shard.shard_id, "Transaction assigned to incorrect shard")

    def test_cross_shard_references(self):
        """Test handling of cross-shard references."""
        cross_shard_tx = Transaction(
            sender="user1",
            receiver="user2",
            action="transfer",
            data={"amount": 10.0, "target_shard": 2},
            shard_id=1
        )
        self.shard.add_transaction(cross_shard_tx)

        self.assertIn(2, self.shard.cross_shard_references, "Cross-shard reference not recorded")
        self.assertIn(
            cross_shard_tx.transaction_id,
            self.shard.cross_shard_references[2],
            "Cross-shard transaction ID not tracked"
        )

    def test_shard_serialization_and_deserialization(self):
        """Test shard serialization and deserialization."""
        # Add transactions and create a block
        for tx in self.sample_transactions[:3]:
            self.shard.add_transaction(tx)
        block = self.shard.create_block(validator="test_validator")
        self.shard.add_block(block)

        # Serialize the shard
        shard_dict = self.shard.to_dict()

        # Deserialize the shard
        new_shard = Shard.from_dict(shard_dict)

        self.assertEqual(new_shard.shard_id, self.shard.shard_id, "Shard ID mismatch after deserialization")
        self.assertEqual(new_shard.height, self.shard.height, "Shard height mismatch after deserialization")
        self.assertEqual(len(new_shard.chain), len(self.shard.chain), "Chain length mismatch after deserialization")

    def test_shard_merging(self):
        """Test merging of two shards, if applicable."""
        # Create a new shard to merge with the existing one
        new_shard = Shard(shard_id=2, max_transactions_per_block=5)
        for tx in self.sample_transactions[:3]:
            new_shard.add_transaction(tx)

        # Simulate merging
        merged_transactions = self.shard.merge_shard(new_shard)
        self.assertEqual(len(merged_transactions), 3, "Merged transaction count mismatch")
        self.assertEqual(new_shard.height, 1, "New shard should be reset after merging")

    def test_shard_splitting(self):
        """Test splitting of a shard, if applicable."""
        # Add transactions to the shard
        for tx in self.sample_transactions:
            self.shard.add_transaction(tx)

        # Simulate shard splitting
        split_shard = self.shard.split_shard(new_shard_id=2)
        self.assertIsNotNone(split_shard, "Shard splitting failed")
        self.assertEqual(split_shard.shard_id, 2, "Split shard ID mismatch")
        self.assertLess(len(self.shard.pending_transactions), 10, "Original shard did not reduce transaction count")

if __name__ == "__main__":
    unittest.main()
