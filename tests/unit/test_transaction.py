# tests/unit/test_transaction.py

import unittest
from datetime import datetime, timedelta
import json
import sys
import os
import hashlib
from typing import Dict

# Add project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from blockchain.core.transaction import Transaction

class TestTransaction(unittest.TestCase):
    """Test cases for the Transaction class."""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.transaction_data = {
            "amount": 100.0,
            "fee": 1.0,
            "metadata": {"type": "transfer"}
        }
        
        self.transaction = Transaction(
            sender="sender123",
            receiver="receiver456",
            action="transfer",
            data=self.transaction_data.copy(),
            shard_id=1
        )

    def test_initialization(self):
        """Test transaction initialization and attribute setting."""
        self.assertEqual(self.transaction.sender, "sender123")
        self.assertEqual(self.transaction.receiver, "receiver456")
        self.assertEqual(self.transaction.action, "transfer")
        self.assertEqual(self.transaction.data["amount"], 100.0)
        self.assertEqual(self.transaction.shard_id, 1)
        self.assertIsNotNone(self.transaction.timestamp)
        self.assertIsNotNone(self.transaction.transaction_id)

    def test_invalid_initialization(self):
        """Test transaction initialization with invalid data."""
        # Test empty sender
        with self.assertRaises(ValueError):
            Transaction(
                sender="",
                receiver="receiver456",
                action="transfer",
                data=self.transaction_data,
                shard_id=1
            )

        # Test empty receiver
        with self.assertRaises(ValueError):
            Transaction(
                sender="sender123",
                receiver="",
                action="transfer",
                data=self.transaction_data,
                shard_id=1
            )

        # Test empty action
        with self.assertRaises(ValueError):
            Transaction(
                sender="sender123",
                receiver="receiver456",
                action="",
                data=self.transaction_data,
                shard_id=1
            )

    def test_transaction_id_consistency(self):
        """Test that transaction ID remains consistent after serialization."""
        # Create two identical transactions
        tx1 = Transaction(
            sender="sender123",
            receiver="receiver456",
            action="transfer",
            data=self.transaction_data.copy(),
            shard_id=1
        )
        
        # Convert to dict and back
        tx_dict = tx1.to_dict()
        tx2 = Transaction.from_dict(tx_dict)
        
        # IDs should match
        self.assertEqual(tx1.transaction_id, tx2.transaction_id)
        
        # Create transaction with same data but different timestamp
        tx3 = Transaction(
            sender="sender123",
            receiver="receiver456",
            action="transfer",
            data=self.transaction_data.copy(),
            shard_id=1
        )
        
        # IDs should be different due to timestamp
        self.assertNotEqual(tx1.transaction_id, tx3.transaction_id)

    def test_validation(self):
        """Test transaction validation."""
        # Valid transaction should pass
        self.assertTrue(self.transaction.validate())
        
        # Test invalid timestamp
        tx = Transaction(
            sender="sender123",
            receiver="receiver456",
            action="transfer",
            data=self.transaction_data.copy(),
            shard_id=1
        )
        tx.timestamp = datetime.now() + timedelta(hours=1)
        self.assertFalse(tx.validate())
        
        # Test invalid shard_id
        tx = Transaction(
            sender="sender123",
            receiver="receiver456",
            action="transfer",
            data=self.transaction_data.copy(),
            shard_id=-1
        )
        self.assertFalse(tx.validate())
        
        # Test invalid data type
        tx = Transaction(
            sender="sender123",
            receiver="receiver456",
            action="transfer",
            data="invalid_data",  # Should be dict
            shard_id=1
        )
        self.assertFalse(tx.validate())

    def test_serialization(self):
        """Test transaction serialization and deserialization."""
        # Convert to dictionary
        tx_dict = self.transaction.to_dict()
        
        # Verify dictionary structure
        self.assertIn("transaction_id", tx_dict)
        self.assertIn("sender", tx_dict)
        self.assertIn("receiver", tx_dict)
        self.assertIn("action", tx_dict)
        self.assertIn("data", tx_dict)
        self.assertIn("timestamp", tx_dict)
        self.assertIn("shard_id", tx_dict)
        
        # Create new transaction from dictionary
        new_tx = Transaction.from_dict(tx_dict)
        
        # Verify all attributes match
        self.assertEqual(new_tx.transaction_id, self.transaction.transaction_id)
        self.assertEqual(new_tx.sender, self.transaction.sender)
        self.assertEqual(new_tx.receiver, self.transaction.receiver)
        self.assertEqual(new_tx.action, self.transaction.action)
        self.assertEqual(new_tx.data, self.transaction.data)
        self.assertEqual(new_tx.shard_id, self.transaction.shard_id)
        self.assertEqual(new_tx.timestamp, self.transaction.timestamp)

    def test_hash_consistency(self):
        """Test that transaction hash calculation is consistent."""
        tx1_hash = self.transaction.calculate_hash()
        tx1_dict = self.transaction.to_dict()
        tx2 = Transaction.from_dict(tx1_dict)
        tx2_hash = tx2.calculate_hash()
        
        self.assertEqual(tx1_hash, tx2_hash)
        
        # Modify transaction and verify hash changes
        self.transaction.data["amount"] = 200.0
        self.assertNotEqual(self.transaction.calculate_hash(), tx1_hash)

    def test_deep_copy_data(self):
        """Test that transaction data is properly deep copied."""
        nested_data = {
            "amount": 100.0,
            "metadata": {
                "tags": ["test", "transfer"],
                "extra": {"note": "test transaction"}
            }
        }
        
        tx = Transaction(
            sender="sender123",
            receiver="receiver456",
            action="transfer",
            data=nested_data,
            shard_id=1
        )
        
        # Modify original data
        nested_data["metadata"]["tags"].append("modified")
        
        # Transaction data should be unchanged
        self.assertEqual(len(tx.data["metadata"]["tags"]), 2)
        self.assertNotIn("modified", tx.data["metadata"]["tags"])

    def test_timestamp_serialization(self):
        """Test that timestamp is properly serialized and deserialized."""
        tx_dict = self.transaction.to_dict()
        new_tx = Transaction.from_dict(tx_dict)
        
        self.assertEqual(
            self.transaction.timestamp.isoformat(),
            new_tx.timestamp.isoformat()
        )

if __name__ == '__main__':
    unittest.main()