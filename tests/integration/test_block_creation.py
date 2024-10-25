import unittest
from datetime import datetime, timedelta
import sys
import os
from typing import List, Dict, Optional
import hashlib
import json

# Add project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from blockchain.core.block import Block
from blockchain.core.transaction import Transaction
from blockchain.core.shard import Shard
from blockchain.core.node import Node
from blockchain.consensus.proof_of_cooperation import ProofOfCooperation

class TestBlockCreation(unittest.TestCase):
    """Integration tests for block creation in the ICN blockchain."""

    def setUp(self):
        """Set up test environment before each test."""
        self.shard = Shard(shard_id=1, max_transactions_per_block=5)
        self.consensus = ProofOfCooperation()
        self.validator = self._create_test_validator()
        self.test_transactions = self._create_test_transactions()

    def _create_test_validator(self) -> Node:
        """Create a test validator node."""
        validator = Node(
            node_id="test_validator",
            cooperative_id="test_coop",
            initial_stake=100.0
        )
        validator.reputation_scores = {
            "validation": 25.0,
            "transaction_validation": 25.0,
            "resource_sharing": 25.0,
            "cooperative_growth": 25.0
        }
        validator.performance_metrics = {
            "availability": 98.0,
            "validation_success_rate": 95.0,
            "network_reliability": 97.0
        }
        validator.assign_to_shard(1)
        return validator

    def _create_test_transactions(self) -> List[Transaction]:
        """Create test transactions with varied characteristics."""
        transactions = []
        for i in range(10):
            tx = Transaction(
                sender=f"sender_{i}",
                receiver=f"receiver_{i}",
                action="transfer",
                data={"amount": 10.0 * (i + 1)},
                shard_id=1,
                priority=min(i % 5 + 1, 5),
                cooperative_tags={f"tag_{i}"}
            )
            transactions.append(tx)
        return transactions

    def test_basic_block_creation(self):
        """Test creation of a basic valid block."""
        # Add some transactions to the shard
        for tx in self.test_transactions[:3]:
            self.shard.add_transaction(tx)

        # Create block
        block = self.shard.create_block(validator="test_validator")
        
        # Verify block properties
        self.assertIsNotNone(block)
        self.assertEqual(block.index, self.shard.height)
        self.assertEqual(block.validator, "test_validator")
        self.assertEqual(block.shard_id, self.shard.shard_id)
        self.assertEqual(len(block.transactions), 3)
        self.assertIsNotNone(block.merkle_root)
        self.assertIsNotNone(block.hash)
        
        # Verify block can be added to shard
        self.assertTrue(self.shard.add_block(block))
        self.assertEqual(self.shard.height, block.index + 1)

    def test_transaction_ordering_in_block(self):
        """Test that transactions are properly ordered in block by priority."""
        # Add transactions with different priorities
        for tx in self.test_transactions[:5]:
            self.shard.add_transaction(tx)

        block = self.shard.create_block(validator="test_validator")
        self.assertIsNotNone(block)

        # Verify transactions are ordered by priority
        for i in range(len(block.transactions) - 1):
            self.assertGreaterEqual(
                block.transactions[i].priority,
                block.transactions[i + 1].priority
            )

    def test_block_size_limits(self):
        """Test enforcement of block size limits."""
        # Add maximum number of transactions
        for tx in self.test_transactions[:self.shard.max_transactions_per_block]:
            self.shard.add_transaction(tx)

        # Create block
        block = self.shard.create_block(validator="test_validator")
        self.assertIsNotNone(block)
        self.assertEqual(len(block.transactions), self.shard.max_transactions_per_block)

        # Try to add one more transaction
        extra_tx = self.test_transactions[self.shard.max_transactions_per_block]
        self.shard.add_transaction(extra_tx)
        
        block = self.shard.create_block(validator="test_validator")
        self.assertEqual(len(block.transactions), self.shard.max_transactions_per_block)

    def test_cross_shard_block_creation(self):
        """Test creation of blocks with cross-shard transactions."""
        # Create cross-shard transaction
        cross_shard_tx = Transaction(
            sender="user1",
            receiver="user2",
            action="transfer",
            data={"amount": 100.0, "target_shard": 2},
            shard_id=1,
            cross_shard_refs=["ref_1"]
        )
        
        # Add transactions
        self.shard.add_transaction(cross_shard_tx)
        for tx in self.test_transactions[:2]:
            self.shard.add_transaction(tx)

        # Create block
        block = self.shard.create_block(validator="test_validator")
        self.assertIsNotNone(block)
        
        # Verify cross-shard references
        self.assertIn(cross_shard_tx.transaction_id, 
                     [tx.transaction_id for tx in block.transactions])
        self.assertTrue(block.cross_shard_refs)

    def test_block_validation(self):
        """Test comprehensive block validation."""
        # Add transactions
        for tx in self.test_transactions[:3]:
            self.shard.add_transaction(tx)

        # Create valid block
        valid_block = self.shard.create_block(validator="test_validator")
        self.assertTrue(valid_block.validate(self.shard.chain[-1]))

        # Test with invalid previous hash
        invalid_block = Block(
            index=valid_block.index,
            previous_hash="invalid_hash",
            timestamp=datetime.now(),
            transactions=valid_block.transactions.copy(),
            validator="test_validator",
            shard_id=1
        )
        self.assertFalse(invalid_block.validate(self.shard.chain[-1]))

        # Test with future timestamp
        future_block = Block(
            index=valid_block.index,
            previous_hash=valid_block.previous_hash,
            timestamp=datetime.now() + timedelta(hours=1),
            transactions=valid_block.transactions.copy(),
            validator="test_validator",
            shard_id=1
        )
        self.assertFalse(future_block.validate(self.shard.chain[-1]))

    def test_merkle_root_calculation(self):
        """Test Merkle root calculation with different transaction sets."""
        # Test with no transactions
        empty_block = Block(
            index=0,
            previous_hash="0" * 64,
            timestamp=datetime.now(),
            transactions=[],
            validator="test_validator",
            shard_id=1
        )
        self.assertIsNotNone(empty_block.merkle_root)

        # Test with one transaction
        single_tx_block = Block(
            index=0,
            previous_hash="0" * 64,
            timestamp=datetime.now(),
            transactions=[self.test_transactions[0]],
            validator="test_validator",
            shard_id=1
        )
        self.assertIsNotNone(single_tx_block.merkle_root)
        self.assertNotEqual(single_tx_block.merkle_root, empty_block.merkle_root)

        # Test with multiple transactions
        multi_tx_block = Block(
            index=0,
            previous_hash="0" * 64,
            timestamp=datetime.now(),
            transactions=self.test_transactions[:3],
            validator="test_validator",
            shard_id=1
        )
        self.assertIsNotNone(multi_tx_block.merkle_root)
        self.assertNotEqual(multi_tx_block.merkle_root, single_tx_block.merkle_root)

    def test_block_metadata(self):
        """Test block metadata handling."""
        # Add transactions
        for tx in self.test_transactions[:3]:
            self.shard.add_transaction(tx)

        # Create block with metadata
        block = self.shard.create_block(validator="test_validator")
        block.metadata["test_key"] = "test_value"

        # Verify metadata serialization
        block_dict = block.to_dict()
        restored_block = Block.from_dict(block_dict)
        self.assertEqual(restored_block.metadata["test_key"], "test_value")

    def test_sequential_block_creation(self):
        """Test creation of sequential blocks."""
        created_blocks = []
        
        # Create several blocks sequentially
        for i in range(3):
            # Add new transactions
            for tx in self.test_transactions[i*3:(i+1)*3]:
                self.shard.add_transaction(tx)
                
            # Create and add block
            block = self.shard.create_block(validator="test_validator")
            self.assertTrue(self.shard.add_block(block))
            created_blocks.append(block)

        # Verify block sequence
        for i in range(1, len(created_blocks)):
            self.assertEqual(
                created_blocks[i].previous_hash,
                created_blocks[i-1].hash
            )
            self.assertEqual(
                created_blocks[i].index,
                created_blocks[i-1].index + 1
            )
            self.assertGreater(
                created_blocks[i].timestamp,
                created_blocks[i-1].timestamp
            )

    def test_resource_impact_tracking(self):
        """Test tracking of resource impact in blocks."""
        # Add transactions with varying resource impacts
        for tx in self.test_transactions[:3]:
            self.shard.add_transaction(tx)

        block = self.shard.create_block(validator="test_validator")
        self.assertIsNotNone(block)

        # Calculate total resource impact
        total_computation = sum(tx.resource_cost["computation"] for tx in block.transactions)
        total_storage = sum(tx.resource_cost["storage"] for tx in block.transactions)
        total_bandwidth = sum(tx.resource_cost["bandwidth"] for tx in block.transactions)

        # Verify reasonable resource usage
        self.assertGreater(total_computation, 0)
        self.assertGreater(total_storage, 0)
        self.assertGreater(total_bandwidth, 0)

    def test_cooperative_score_aggregation(self):
        """Test aggregation of cooperative scores in blocks."""
        # Add transactions with varying cooperative scores
        for tx in self.test_transactions[:3]:
            self.shard.add_transaction(tx)

        block = self.shard.create_block(validator="test_validator")
        self.assertIsNotNone(block)

        # Calculate total cooperative impact
        total_score = sum(tx.get_cooperative_score() for tx in block.transactions)
        
        # Verify positive cooperative impact
        self.assertGreater(total_score, 0)
        self.assertGreater(total_score, len(block.transactions))  # Should be higher than just count

if __name__ == "__main__":
    unittest.main()