# tests/unit/test_block.py

import unittest
from datetime import datetime, timedelta
import json
import hashlib
import sys
import os
from typing import List

# Add project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from blockchain.core.block import Block
from blockchain.core.transaction import Transaction

class TestBlock(unittest.TestCase):
    """Test cases for the Block class."""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.sample_transactions = self._create_sample_transactions()
        
        # Create previous block first
        self.previous_block = Block(
            index=0,
            previous_hash="0" * 64,
            timestamp=datetime.now() - timedelta(minutes=5),
            transactions=[],
            validator="genesis",
            shard_id=1
        )

        # Create current block with correct previous hash
        self.block = Block(
            index=1,
            previous_hash=self.previous_block.hash,  # Use actual hash from previous block
            timestamp=datetime.now(),
            transactions=self.sample_transactions[:2],
            validator="node1",
            shard_id=1
        )

    def _create_sample_transactions(self) -> List[Transaction]:
        """Create sample transactions for testing."""
        transactions = []
        for i in range(1, 6):
            tx = Transaction(
                sender=f"user{i}",
                receiver=f"user{i+1}",
                action="transfer",
                data={"amount": 10.0 * i},
                shard_id=1  # All transactions in same shard
            )
            transactions.append(tx)
        return transactions

    def test_initialization(self):
        """Test block initialization and attribute setting."""
        self.assertEqual(self.block.index, 1)
        self.assertEqual(self.block.previous_hash, self.previous_block.hash)
        self.assertEqual(self.block.validator, "node1")
        self.assertEqual(self.block.shard_id, 1)
        self.assertEqual(len(self.block.transactions), 2)
        self.assertIsNotNone(self.block.merkle_root)
        self.assertIsNotNone(self.block.hash)
        self.assertEqual(self.block.nonce, 0)
        self.assertEqual(self.block.version, "1.0")
        self.assertIn("created_at", self.block.metadata)

    def test_merkle_root_calculation(self):
        """Test Merkle root calculation with different scenarios."""
        # Test with existing transactions
        merkle_root = self.block.calculate_merkle_root()
        self.assertEqual(merkle_root, self.block.merkle_root)
        
        # Test with empty transactions
        empty_block = Block(
            index=0,
            previous_hash="0",
            timestamp=datetime.now(),
            transactions=[],
            validator="genesis",
            shard_id=0
        )
        self.assertEqual(
            empty_block.merkle_root,
            hashlib.sha256(b"empty").hexdigest()
        )
        
        # Test with odd number of transactions
        odd_block = Block(
            index=2,
            previous_hash=self.block.hash,
            timestamp=datetime.now(),
            transactions=self.sample_transactions[:3],
            validator="node1",
            shard_id=1
        )
        self.assertIsNotNone(odd_block.merkle_root)
        
        # Verify Merkle root changes with transaction modification
        original_root = self.block.merkle_root
        modified_tx = self.block.transactions[0]
        modified_tx.data["amount"] = 999.9
        new_root = self.block.calculate_merkle_root()
        self.assertNotEqual(original_root, new_root)

    def test_hash_calculation(self):
        """Test block hash calculation and verification."""
        # Test initial hash
        initial_hash = self.block.hash
        calculated_hash = self.block.calculate_hash()
        self.assertEqual(initial_hash, calculated_hash)
        
        # Test hash changes with block modifications
        self.block.nonce += 1
        new_hash = self.block.calculate_hash()
        self.assertNotEqual(initial_hash, new_hash)
        
        # Test hash changes with timestamp modification
        original_timestamp = self.block.timestamp
        self.block.timestamp += timedelta(seconds=1)
        newer_hash = self.block.calculate_hash()
        self.assertNotEqual(new_hash, newer_hash)
        # Restore timestamp for other tests
        self.block.timestamp = original_timestamp
        
        # Verify hash format
        self.assertTrue(all(c in "0123456789abcdef" for c in self.block.hash))
        self.assertEqual(len(self.block.hash), 64)  # SHA-256 produces 64 hex chars

    def test_validation(self):
        """Test block validation logic."""
        # Test valid block
        self.assertTrue(self.block.validate(self.previous_block))
        
        # Test invalid hash
        original_hash = self.block.hash
        self.block.hash = "invalid_hash"
        self.assertFalse(self.block.validate(self.previous_block))
        self.block.hash = original_hash
        
        # Test future timestamp
        original_timestamp = self.block.timestamp
        self.block.timestamp = datetime.now() + timedelta(hours=1)
        self.assertFalse(self.block.validate(self.previous_block))
        self.block.timestamp = original_timestamp
        
        # Test invalid previous hash
        original_prev_hash = self.block.previous_hash
        self.block.previous_hash = "wrong_hash"
        self.assertFalse(self.block.validate(self.previous_block))
        self.block.previous_hash = original_prev_hash

    def test_add_transaction(self):
        """Test adding transactions to the block."""
        initial_tx_count = len(self.block.transactions)
        new_tx = self.sample_transactions[3]  # Unused transaction
        
        # Test adding valid transaction
        self.assertTrue(self.block.add_transaction(new_tx))
        self.assertEqual(len(self.block.transactions), initial_tx_count + 1)
        
        # Verify Merkle root was updated
        self.assertNotEqual(self.block.merkle_root, "")
        
        # Test adding transaction with wrong shard_id
        wrong_shard_tx = Transaction(
            sender="user1",
            receiver="user2",
            action="transfer",
            data={"amount": 50.0},
            shard_id=2  # Different shard
        )
        self.assertFalse(self.block.add_transaction(wrong_shard_tx))
        
        # Test adding duplicate transaction
        self.assertFalse(self.block.add_transaction(new_tx))

    def test_block_size(self):
        """Test block size calculations and limits."""
        # Add maximum transactions
        for tx in self.sample_transactions:
            self.block.add_transaction(tx)
            
        # Verify block can be serialized
        block_dict = self.block.to_dict()
        block_json = json.dumps(block_dict)
        
        # Simulate block size calculation
        block_size = len(block_json.encode('utf-8'))
        self.assertGreater(block_size, 0)

    def test_cross_shard_references(self):
        """Test cross-shard reference handling."""
        # Add cross-shard reference
        ref = "cross_shard_ref_123"
        self.block.cross_shard_refs.append(ref)
        
        # Verify serialization includes references
        block_dict = self.block.to_dict()
        self.assertIn("cross_shard_refs", block_dict)
        self.assertIn(ref, block_dict["cross_shard_refs"])
        
        # Verify deserialization preserves references
        new_block = Block.from_dict(block_dict)
        self.assertIn(ref, new_block.cross_shard_refs)

    def test_metadata(self):
        """Test block metadata handling."""
        # Test default metadata
        self.assertIn("created_at", self.block.metadata)
        
        # Add custom metadata
        self.block.metadata["test_key"] = "test_value"
        
        # Verify serialization includes metadata
        block_dict = self.block.to_dict()
        self.assertIn("metadata", block_dict)
        self.assertEqual(block_dict["metadata"]["test_key"], "test_value")
        
        # Verify deserialization preserves metadata
        new_block = Block.from_dict(block_dict)
        self.assertEqual(new_block.metadata["test_key"], "test_value")

    def test_serialization(self):
        """Test block serialization and deserialization."""
        # Convert block to dictionary
        block_dict = self.block.to_dict()
        
        # Verify dictionary structure
        self.assertIn("index", block_dict)
        self.assertIn("previous_hash", block_dict)
        self.assertIn("timestamp", block_dict)
        self.assertIn("transactions", block_dict)
        self.assertIn("validator", block_dict)
        self.assertIn("hash", block_dict)
        self.assertIn("merkle_root", block_dict)
        self.assertIn("shard_id", block_dict)
        self.assertIn("version", block_dict)
        
        # Create new block from dictionary
        new_block = Block.from_dict(block_dict)
        
        # Verify all attributes match
        self.assertEqual(new_block.index, self.block.index)
        self.assertEqual(new_block.previous_hash, self.block.previous_hash)
        self.assertEqual(new_block.validator, self.block.validator)
        self.assertEqual(new_block.shard_id, self.block.shard_id)
        self.assertEqual(new_block.hash, self.block.hash)
        self.assertEqual(new_block.merkle_root, self.block.merkle_root)
        
        # Verify transactions were properly deserialized
        self.assertEqual(len(new_block.transactions), len(self.block.transactions))
        for orig_tx, new_tx in zip(self.block.transactions, new_block.transactions):
            self.assertEqual(orig_tx.transaction_id, new_tx.transaction_id)
            self.assertEqual(orig_tx.sender, new_tx.sender)
            self.assertEqual(orig_tx.receiver, new_tx.receiver)
            self.assertEqual(orig_tx.data, new_tx.data)

if __name__ == '__main__':
    unittest.main()