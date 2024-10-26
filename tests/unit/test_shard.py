import unittest
from datetime import datetime, timedelta
import sys
import os
from typing import List, Dict, Optional, Set
import logging

# Add project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from blockchain.core.shard import Shard, ShardConfig
from blockchain.core.transaction import Transaction
from blockchain.core.block import Block

logger = logging.getLogger(__name__)

class TestShard(unittest.TestCase):
    """Test cases for the modular Shard implementation."""

    def setUp(self):
        """Set up test environment before each test."""
        self.config = ShardConfig(
            max_transactions_per_block=5,
            max_pending_transactions=10,
            max_cross_shard_refs=3,
            pruning_interval=60,
            min_block_interval=0,  # Set to 0 for testing
            max_block_size=1024 * 1024,  # 1MB
            max_state_size=10 * 1024 * 1024  # 10MB
        )
        self.shard = Shard(shard_id=1, config=self.config)
        
        # Initialize some balances in state for testing
        self.shard.state_manager.state = {
            "balances": {
                "user0": 1000.0,
                "user1": 1000.0,
                "user2": 1000.0,
                "user3": 1000.0,
                "user4": 1000.0
            }
        }
        
        self.sample_transactions = self._create_sample_transactions()

    def _create_sample_transactions(self) -> List[Transaction]:
        """Create sample transactions for testing."""
        return [
            Transaction(
                sender=f"user{i}",
                receiver=f"user{i+1}",
                action="transfer",
                data={"amount": 50.0},  # Changed from 10.0 to make changes more noticeable
                shard_id=1
            ) for i in range(4)
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
        self.assertTrue(genesis.validate(None))

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
            data={"amount": 50.0},
            shard_id=2
        )
        self.assertFalse(self.shard.add_transaction(invalid_tx))

    def test_state_management(self):
        """Test state management and updates."""
        initial_state = self.shard.state_manager.state.copy()
        initial_balance = initial_state["balances"]["user0"]

        # Add and process transaction
        tx = self.sample_transactions[0]
        self.shard.add_transaction(tx)
        block = self.shard.create_block("test_validator")
        self.assertTrue(self.shard.add_block(block))

        # Verify balance changes
        new_state = self.shard.state_manager.state
        self.assertEqual(
            new_state["balances"]["user0"],
            initial_balance - 50.0  # Verify sender balance decreased
        )
        self.assertEqual(
            new_state["balances"]["user1"],
            initial_balance + 50.0  # Verify receiver balance increased
        )

    def test_create_block(self):
        """Test block creation from pending transactions."""
        for tx in self.sample_transactions[:3]:
            self.shard.add_transaction(tx)

        block = self.shard.create_block("test_validator")
        self.assertIsNotNone(block)
        self.assertEqual(block.index, self.shard.height)
        self.assertEqual(block.shard_id, self.shard.shard_id)
        self.assertEqual(len(block.transactions), 3)

    def test_add_block(self):
        """Test adding blocks to the shard."""
        for tx in self.sample_transactions[:3]:
            self.shard.add_transaction(tx)
        
        block = self.shard.create_block("test_validator")
        initial_height = self.shard.height
        
        self.assertTrue(self.shard.add_block(block))
        self.assertEqual(self.shard.height, initial_height + 1)
        self.assertEqual(len(self.shard.pending_transactions), 0)

    def test_cross_shard_operations(self):
        """Test cross-shard operations and references."""
        cross_tx = Transaction(
            sender="user1",
            receiver="user2",
            action="transfer",
            data={
                "amount": 50.0,
                "target_shard": 2
            },
            shard_id=1,
            cross_shard_refs=["ref_1"]
        )

        self.assertTrue(self.shard.add_transaction(cross_tx))
        block = self.shard.create_block("test_validator")
        self.assertIsNotNone(block)
        self.assertTrue(len(block.cross_shard_refs) > 0)

        # Verify cross-shard reference tracking
        self.assertTrue(self.shard.add_block(block))
        cross_shard_metrics = self.shard.cross_shard_manager.get_metrics()
        self.assertGreater(cross_shard_metrics["cross_shard_operations"], 0)

    def test_state_consistency(self):
        """Test state consistency across operations."""
        initial_state = self.shard.state_manager.state.copy()
        amount = 50.0  # Transaction amount
        
        # Add and process transactions
        for tx in self.sample_transactions[:3]:
            self.shard.add_transaction(tx)

        block = self.shard.create_block("test_validator")
        self.shard.add_block(block)

        # Verify state changes are consistent
        new_state = self.shard.state_manager.state
        for tx in block.transactions:
            # Get initial balances
            initial_sender_balance = initial_state["balances"][tx.sender]
            initial_receiver_balance = initial_state["balances"][tx.receiver]
            
            # Get new balances
            sender_balance = new_state["balances"][tx.sender]
            receiver_balance = new_state["balances"][tx.receiver]
            
            # Verify balances
            self.assertEqual(sender_balance, initial_sender_balance - amount)
            self.assertEqual(receiver_balance, initial_receiver_balance + amount)
            self.assertGreaterEqual(sender_balance, 0)
            self.assertGreater(receiver_balance, initial_state["balances"][tx.receiver])

    def test_manager_coordination(self):
        """Test coordination between different managers."""
        tx = self.sample_transactions[0]
        
        # Verify transaction gets properly distributed to managers
        self.shard.add_transaction(tx)
        self.assertIn(tx, self.shard.transaction_manager.pending_transactions)
        self.assertTrue(self.shard.validation_manager.validate_transaction(tx))

        # Create and add block
        block = self.shard.create_block("test_validator")
        initial_state = self.shard.state_manager.state.copy()
        
        self.assertTrue(self.shard.add_block(block))
        self.assertNotEqual(self.shard.state_manager.state, initial_state)
        self.assertEqual(len(self.shard.transaction_manager.pending_transactions), 0)

    def test_transaction_validation(self):
        """Test transaction validation in shard context."""
        valid_tx = self.sample_transactions[0]
        self.assertTrue(self.shard.validation_manager.validate_transaction(valid_tx))

        # Test invalid amount
        invalid_tx = Transaction(
            sender="user1",
            receiver="user2",
            action="transfer",
            data={"amount": -50.0},
            shard_id=1
        )
        self.assertFalse(self.shard.validation_manager.validate_transaction(invalid_tx))

        # Test insufficient balance
        large_tx = Transaction(
            sender="user1",
            receiver="user2",
            action="transfer",
            data={"amount": 2000.0},
            shard_id=1
        )
        self.assertFalse(self.shard.validation_manager.validate_transaction(large_tx))

    def test_validate_chain(self):
        """Test chain validation functionality."""
        # Add some blocks
        for tx in self.sample_transactions[:3]:
            self.shard.add_transaction(tx)
            
        block = self.shard.create_block("test_validator")
        self.shard.add_block(block)
        
        self.assertTrue(self.shard.validate_chain())

    def test_get_metrics(self):
        """Test metrics collection across all managers."""
        # Generate some activity
        for tx in self.sample_transactions[:3]:
            self.shard.add_transaction(tx)
            
        block = self.shard.create_block("test_validator")
        self.shard.add_block(block)
        
        metrics = self.shard.get_metrics()
        
        self.assertIn("total_transactions", metrics)
        self.assertIn("blocks_created", metrics)
        self.assertIn("pending_transactions", metrics)
        self.assertIn("state_size", metrics)
        self.assertEqual(metrics["shard_id"], self.shard.shard_id)

    def test_serialization(self):
        """Test shard serialization and deserialization."""
        # Add some data
        for tx in self.sample_transactions[:3]:
            self.shard.add_transaction(tx)
        block = self.shard.create_block("test_validator")
        self.shard.add_block(block)

        # Serialize
        shard_dict = self.shard.to_dict()

        # Deserialize
        new_shard = Shard.from_dict(shard_dict)

        # Verify properties
        self.assertEqual(new_shard.shard_id, self.shard.shard_id)
        self.assertEqual(new_shard.height, self.shard.height)
        self.assertEqual(len(new_shard.chain), len(self.shard.chain))
        self.assertEqual(
            new_shard.state_manager.state["balances"],
            self.shard.state_manager.state["balances"]
        )

if __name__ == '__main__':
    unittest.main()