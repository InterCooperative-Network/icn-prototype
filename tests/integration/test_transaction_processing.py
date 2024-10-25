import unittest
from datetime import datetime, timedelta
import sys
import os
from typing import List, Dict, Optional
import json

# Add project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from blockchain.core.transaction import Transaction
from blockchain.core.block import Block
from blockchain.core.node import Node
from blockchain.core.blockchain import Blockchain
from blockchain.consensus.proof_of_cooperation import ProofOfCooperation

class TestTransactionProcessing(unittest.TestCase):
    """Integration tests for transaction processing in the ICN blockchain."""

    def setUp(self):
        """Set up test environment before each test."""
        self.test_transactions = self._create_test_transactions()

    def _create_test_transactions(self, num_transactions: int = 5) -> List[Transaction]:
        """Create test transactions with varied characteristics."""
        transactions = []
        for i in range(num_transactions):
            tx = Transaction(
                sender=f"sender_{i}",
                receiver=f"receiver_{i}",
                action="transfer",
                data={"amount": 10.0 * (i + 1)},
                shard_id=i % 3,  # Use fixed number of shards for testing
                priority=min(i + 1, 5),
                cooperative_tags={f"tag_{i}"},
                cross_shard_refs=[f"ref_{j}" for j in range(i % 3)]
            )
            transactions.append(tx)
        return transactions

    def test_transaction_creation_and_validation(self):
        """Test basic transaction creation and validation."""
        # Test valid transaction
        tx = Transaction(
            sender="user1",
            receiver="user2",
            action="transfer",
            data={"amount": 100.0},
            shard_id=0,
            priority=1
        )
        self.assertTrue(tx.validate())
        self.assertIsNotNone(tx.transaction_id)

        # Test invalid action
        with self.assertRaises(ValueError):
            Transaction(
                sender="user1",
                receiver="user2",
                action="invalid_action",
                data={"amount": 100.0},
                shard_id=0
            )

        # Test invalid priority
        with self.assertRaises(ValueError):
            Transaction(
                sender="user1",
                receiver="user2",
                action="transfer",
                data={"amount": 100.0},
                shard_id=0,
                priority=10  # Invalid priority
            )

    def test_transaction_data_limits(self):
        """Test transaction data size limits."""
        # Test transaction with data at limit
        large_data = {"data": "x" * (Transaction.MAX_DATA_SIZE - 100)}  # Leave room for other fields
        tx = Transaction(
            sender="user1",
            receiver="user2",
            action="store",
            data=large_data,
            shard_id=0
        )
        self.assertTrue(tx.validate())

        # Test transaction exceeding data limit
        too_large_data = {"data": "x" * (Transaction.MAX_DATA_SIZE + 1000)}
        tx = Transaction(
            sender="user1",
            receiver="user2",
            action="store",
            data=too_large_data,
            shard_id=0
        )
        self.assertFalse(tx.validate())

    def test_cross_shard_functionality(self):
        """Test cross-shard transaction handling."""
        # Create cross-shard transaction
        tx = Transaction(
            sender="user1",
            receiver="user2",
            action="transfer",
            data={
                "amount": 100.0,
                "target_shard": 1
            },
            shard_id=0,
            cross_shard_refs=["ref_1", "ref_2"]
        )

        # Test cross-shard detection
        self.assertTrue(tx.is_cross_shard())
        self.assertEqual(tx.get_target_shards(), {0, 1})

        # Test cross-shard reference limit
        tx_many_refs = Transaction(
            sender="user1",
            receiver="user2",
            action="transfer",
            data={"amount": 100.0},
            shard_id=0,
            cross_shard_refs=["ref_" + str(i) for i in range(Transaction.MAX_CROSS_SHARD_REFS + 1)]
        )
        self.assertFalse(tx_many_refs.validate())

    def test_resource_cost_calculation(self):
        """Test resource cost calculation."""
        # Test basic resource cost
        tx = Transaction(
            sender="user1",
            receiver="user2",
            action="transfer",
            data={"amount": 100.0},
            shard_id=0
        )
        self.assertGreater(tx.get_resource_impact(), 0)
        
        # Test increased cost with large data
        large_data = {"data": "x" * 10000}
        tx_large = Transaction(
            sender="user1",
            receiver="user2",
            action="store",
            data=large_data,
            shard_id=0
        )
        self.assertGreater(tx_large.get_resource_impact(), tx.get_resource_impact())

        # Test cross-shard overhead
        tx_cross_shard = Transaction(
            sender="user1",
            receiver="user2",
            action="transfer",
            data={"amount": 100.0},
            shard_id=0,
            cross_shard_refs=["ref_1", "ref_2"]
        )
        self.assertGreater(tx_cross_shard.get_resource_impact(), tx.get_resource_impact())

    def test_cooperative_score(self):
        """Test cooperative score calculation."""
        # Basic transaction
        tx_basic = Transaction(
            sender="user1",
            receiver="user2",
            action="transfer",
            data={"amount": 100.0},
            shard_id=0
        )
        base_score = tx_basic.get_cooperative_score()

        # Transaction with cooperative tags
        tx_cooperative = Transaction(
            sender="user1",
            receiver="user2",
            action="transfer",
            data={"amount": 100.0},
            shard_id=0,
            cooperative_tags={"sharing", "community"}
        )
        self.assertGreater(tx_cooperative.get_cooperative_score(), base_score)

        # Cross-shard transaction
        tx_cross_shard = Transaction(
            sender="user1",
            receiver="user2",
            action="transfer",
            data={"amount": 100.0, "target_shard": 1},
            shard_id=0
        )
        self.assertGreater(tx_cross_shard.get_cooperative_score(), base_score)

    def test_serialization(self):
        """Test transaction serialization and deserialization."""
        original_tx = Transaction(
            sender="user1",
            receiver="user2",
            action="transfer",
            data={"amount": 100.0},
            shard_id=0,
            priority=2,
            cooperative_tags={"tag1", "tag2"},
            cross_shard_refs=["ref_1"]
        )

        # Convert to dictionary
        tx_dict = original_tx.to_dict()

        # Create new transaction from dictionary
        restored_tx = Transaction.from_dict(tx_dict)

        # Verify all attributes match
        self.assertEqual(restored_tx.transaction_id, original_tx.transaction_id)
        self.assertEqual(restored_tx.sender, original_tx.sender)
        self.assertEqual(restored_tx.receiver, original_tx.receiver)
        self.assertEqual(restored_tx.action, original_tx.action)
        self.assertEqual(restored_tx.data, original_tx.data)
        self.assertEqual(restored_tx.shard_id, original_tx.shard_id)
        self.assertEqual(restored_tx.priority, original_tx.priority)
        self.assertEqual(restored_tx.cooperative_tags, original_tx.cooperative_tags)
        self.assertEqual(restored_tx.cross_shard_refs, original_tx.cross_shard_refs)

    def test_metadata_handling(self):
        """Test transaction metadata handling."""
        tx = Transaction(
            sender="user1",
            receiver="user2",
            action="transfer",
            data={"amount": 100.0},
            shard_id=0
        )

        # Verify required metadata fields
        self.assertIn("created_at", tx.metadata)
        self.assertIn("data_size", tx.metadata)
        self.assertIn("version", tx.metadata)

        # Verify metadata persistence through serialization
        tx_dict = tx.to_dict()
        restored_tx = Transaction.from_dict(tx_dict)
        
        # Compare metadata fields individually, ignoring timestamp precision
        self.assertEqual(
            tx.metadata["data_size"], 
            restored_tx.metadata["data_size"]
        )
        self.assertEqual(
            tx.metadata["version"], 
            restored_tx.metadata["version"]
        )
        
        # For timestamp, verify format and rough equality
        original_time = datetime.fromisoformat(tx.metadata["created_at"])
        restored_time = datetime.fromisoformat(restored_tx.metadata["created_at"])
        
        # Should be within 1 second of each other
        self.assertLess(
            abs((original_time - restored_time).total_seconds()),
            1.0
        )
        
    def test_timestamp_validation(self):
        """Test transaction timestamp validation."""
        # Current timestamp should be valid
        tx_current = Transaction(
            sender="user1",
            receiver="user2",
            action="transfer",
            data={"amount": 100.0},
            shard_id=0,
            timestamp=datetime.now()
        )
        self.assertTrue(tx_current.validate())

        # Future timestamp should be invalid
        tx_future = Transaction(
            sender="user1",
            receiver="user2",
            action="transfer",
            data={"amount": 100.0},
            shard_id=0,
            timestamp=datetime.now() + timedelta(minutes=10)
        )
        self.assertFalse(tx_future.validate())

        # Old timestamp should be invalid
        tx_old = Transaction(
            sender="user1",
            receiver="user2",
            action="transfer",
            data={"amount": 100.0},
            shard_id=0,
            timestamp=datetime.now() - timedelta(days=2)
        )
        self.assertFalse(tx_old.validate())

    def test_transaction_ordering(self):
        """Test transaction ordering by priority."""
        # Create transactions with different priorities
        transactions = []
        for i in range(1, 6):
            tx = Transaction(
                sender=f"user{i}",
                receiver=f"user{i+1}",
                action="transfer",
                data={"amount": 100.0},
                shard_id=0,
                priority=i
            )
            transactions.append(tx)

        # Shuffle and sort by priority
        import random
        random.shuffle(transactions)
        sorted_transactions = sorted(transactions, key=lambda x: x.priority, reverse=True)

        # Verify order
        for i in range(len(sorted_transactions) - 1):
            self.assertGreaterEqual(
                sorted_transactions[i].priority,
                sorted_transactions[i + 1].priority
            )

if __name__ == "__main__":
    unittest.main()