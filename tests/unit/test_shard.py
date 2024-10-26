# tests/unit/test_shard.py

import unittest
from datetime import datetime, timedelta
import sys
import os
from typing import List, Dict, Optional, Set
import json

# Add project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from blockchain.core.shard.base import Shard
from blockchain.core.shard.shard_types import ShardConfig, ShardMetrics
from blockchain.core.block import Block
from blockchain.core.transaction import Transaction

class TestShard(unittest.TestCase):
    """Test cases for the modular Shard implementation."""

    def setUp(self):
        """Set up test environment before each test."""
        self.config = ShardConfig(
            max_transactions_per_block=5,
            max_pending_transactions=10,
            max_cross_shard_refs=3,
            pruning_interval=60,
            min_block_interval=1,
            max_block_size=1024 * 1024,
            max_state_size=10 * 1024 * 1024
        )
        self.shard = Shard(shard_id=1, config=self.config)
        self.sample_transactions = self._create_sample_transactions()

        # Initialize some balances in state for testing
        self.shard.state_manager.state["balances"] = {
            "user0": 1000.0,
            "user1": 1000.0,
            "user2": 1000.0,
            "user3": 1000.0,
            "user4": 1000.0
        }

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
        """Test shard initialization with all components."""
        self.assertEqual(self.shard.shard_id, 1)
        self.assertIsInstance(self.shard.transaction_manager.pending_transactions, list)
        self.assertIsInstance(self.shard.state_manager.state, dict)
        self.assertEqual(len(self.shard.chain), 1)  # Genesis block
        self.assertEqual(self.shard.height, 1)
        self.assertEqual(self.shard.chain[0].validator, "genesis")

    def test_genesis_block(self):
        """Test genesis block creation and properties."""
        genesis = self.shard.chain[0]
        self.assertEqual(genesis.index, 0)
        self.assertEqual(genesis.previous_hash, "0" * 64)
        self.assertEqual(len(genesis.transactions), 0)
        self.assertEqual(genesis.validator, "genesis")
        self.assertEqual(genesis.shard_id, 1)
        self.assertTrue(genesis.validate(None))  # Genesis block should validate without previous block

    def test_transaction_management(self):
        """Test transaction management functionality."""
        # Test valid transaction addition
        tx = self.sample_transactions[0]
        self.assertTrue(self.shard.add_transaction(tx))
        self.assertEqual(len(self.shard.pending_transactions), 1)

        # Test duplicate transaction
        self.assertFalse(self.shard.add_transaction(tx))

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
            self.config.max_transactions_per_block * 2
        )

    def test_state_management(self):
        """Test state management and updates."""
        # Add transactions and create block
        for tx in self.sample_transactions[:3]:
            self.shard.add_transaction(tx)
        
        initial_state = self.shard.state.copy()
        initial_balance = initial_state["balances"]["user0"]
        
        block = self.shard.create_block(validator="test_validator")
        self.assertIsNotNone(block)
        
        # Test state update
        self.assertTrue(self.shard.add_block(block))
        
        # Verify balance changes
        new_state = self.shard.state
        self.assertLess(
            new_state["balances"]["user0"],
            initial_balance
        )
        
        # Test state rollback
        self.assertTrue(self.shard.state_manager.rollback_state())
        self.assertEqual(
            self.shard.state["balances"]["user0"],
            initial_balance
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
        self.shard.transaction_manager.clear_all()
        block = self.shard.create_block(validator="test_validator")
        self.assertIsNone(block)

    def test_add_block(self):
        """Test adding blocks to the shard."""
        # Create and add a valid block
        for tx in self.sample_transactions[:3]:
            self.shard.add_transaction(tx)
        
        block = self.shard.create_block(validator="test_validator")
        self.assertIsNotNone(block)
        
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
            # tests/unit/test_shard.py

            validator="test_validator",
            shard_id=2  # Wrong shard_id
        )
        self.assertFalse(self.shard.add_block(invalid_block))

    def test_validate_chain(self):
        """Test chain validation."""
        # Initial chain should be valid
        self.assertTrue(self.shard.validate_chain())

        # Add a valid block
        for tx in self.sample_transactions[:3]:
            self.shard.add_transaction(tx)
        block = self.shard.create_block(validator="test_validator")
        self.assertTrue(self.shard.add_block(block))

        # Verify chain is still valid
        self.assertTrue(self.shard.validate_chain())

        # Tamper with a block
        original_transactions = self.shard.chain[-1].transactions.copy()
        self.shard.chain[-1].transactions = []  # This should invalidate the block's hash
        self.assertFalse(self.shard.validate_chain())

        # Restore chain to valid state
        self.shard.chain[-1].transactions = original_transactions

    def test_cross_shard_operations(self):
        """Test cross-shard operations and references."""
        # Create cross-shard transaction
        cross_tx = Transaction(
            sender="user1",
            receiver="user2",
            action="transfer",
            data={"amount": 10.0, "target_shard": 2},
            shard_id=1,
            cross_shard_refs=["ref_1"]
        )

        # Test cross-shard reference creation
        self.assertTrue(self.shard.add_transaction(cross_tx))
        block = self.shard.create_block(validator="test_validator")
        self.assertIsNotNone(block)
        self.assertTrue(len(block.cross_shard_refs) > 0)

        # Test adding block with cross-shard references
        self.assertTrue(self.shard.add_block(block))
        cross_shard_metrics = self.shard.cross_shard_manager.get_metrics()
        self.assertGreater(cross_shard_metrics["cross_shard_operations"], 0)

    def test_get_metrics(self):
        """Test metrics collection across all managers."""
        # Create some activity to generate metrics
        for tx in self.sample_transactions[:3]:
            self.shard.add_transaction(tx)
        block = self.shard.create_block(validator="test_validator")
        self.shard.add_block(block)

        metrics = self.shard.get_metrics()
        
        # Check core metrics
        self.assertIn("shard_id", metrics)
        self.assertIn("height", metrics)
        self.assertIn("chain_size", metrics)
        
        # Check transaction manager metrics
        self.assertIn("pending_transactions", metrics)
        self.assertIn("state_size", metrics)
        
        # Check cross-shard metrics
        self.assertIn("cross_shard_operations", metrics)

        # Verify metric values
        self.assertEqual(metrics["shard_id"], 1)
        self.assertEqual(metrics["height"], 2)  # Genesis + 1
        self.assertGreater(metrics["state_size"], 0)

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

        # Verify core properties
        self.assertEqual(new_shard.shard_id, self.shard.shard_id)
        self.assertEqual(new_shard.height, self.shard.height)
        self.assertEqual(len(new_shard.chain), len(self.shard.chain))
        
        # Verify manager states
        self.assertEqual(
            len(new_shard.pending_transactions),
            len(self.shard.pending_transactions)
        )
        self.assertEqual(
            new_shard.state["balances"],
            self.shard.state["balances"]
        )

    def test_transaction_validation(self):
        """Test transaction validation in shard context."""
        # Test valid transaction
        tx = self.sample_transactions[0]
        self.assertTrue(self.shard.validation_manager.validate_transaction(tx))

        # Test invalid amount
        invalid_tx = Transaction(
            sender="user1",
            receiver="user2",
            action="transfer",
            data={"amount": -10.0},  # Invalid negative amount
            shard_id=1
        )
        self.assertFalse(self.shard.validation_manager.validate_transaction(invalid_tx))

        # Test insufficient balance
        large_tx = Transaction(
            sender="user1",
            receiver="user2",
            action="transfer",
            data={"amount": 2000.0},  # More than available balance
            shard_id=1
        )
        self.assertFalse(self.shard.validation_manager.validate_transaction(large_tx))

    def test_state_consistency(self):
        """Test state consistency across operations."""
        initial_state = self.shard.state.copy()
        
        # Add and process transactions
        for tx in self.sample_transactions[:3]:
            self.shard.add_transaction(tx)
        
        block = self.shard.create_block(validator="test_validator")
        self.shard.add_block(block)
        
        # Verify state changes are consistent
        new_state = self.shard.state
        for tx in block.transactions:
            sender_balance = new_state["balances"][tx.sender]
            receiver_balance = new_state["balances"][tx.receiver]
            self.assertGreaterEqual(sender_balance, 0)
            self.assertGreater(receiver_balance, initial_state["balances"][tx.receiver])

    def test_manager_coordination(self):
        """Test coordination between different managers."""
        # Add transaction and verify it appears in both transaction and validation managers
        tx = self.sample_transactions[0]
        self.shard.add_transaction(tx)
        
        self.assertIn(tx, self.shard.transaction_manager.pending_transactions)
        self.assertTrue(self.shard.validation_manager.validate_transaction(tx))
        
        # Create block and verify state updates
        block = self.shard.create_block(validator="test_validator")
        initial_state = self.shard.state.copy()
        
        self.assertTrue(self.shard.add_block(block))
        self.assertNotEqual(self.shard.state, initial_state)
        self.assertEqual(len(self.shard.pending_transactions), 0)

if __name__ == '__main__':
    unittest.main()