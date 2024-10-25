import unittest
from datetime import datetime, timedelta
import sys
import os
from typing import List, Dict, Set
import hashlib
import json
import random

# Add project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from blockchain.core.shard import Shard
from blockchain.core.transaction import Transaction
from blockchain.core.block import Block
from blockchain.core.node import Node
from blockchain.consensus.proof_of_cooperation import ProofOfCooperation

class TestShardManagement(unittest.TestCase):
    """Integration tests for shard management in the ICN blockchain."""

    def setUp(self):
        """Set up test environment before each test."""
        self.main_shard = Shard(shard_id=0, max_transactions_per_block=5)
        self.nodes = self._create_test_nodes()
        self.consensus = ProofOfCooperation()
        self.test_transactions = self._create_test_transactions()

    def _create_test_nodes(self, num_nodes: int = 5) -> List[Node]:
        """Create test nodes with varied capabilities."""
        nodes = []
        for i in range(num_nodes):
            node = Node(
                node_id=f"node_{i}",
                cooperative_id="test_coop",
                initial_stake=100.0
            )
            # Vary node capabilities for modular shards
            node.reputation_scores = {
                "validation": 20.0 + i * 2,
                "resource_sharing": 20.0 + i * 2,
                "cooperative_growth": 20.0 + i * 2,
                "innovation": 20.0 + i * 2
            }
            node.performance_metrics = {
                "availability": 95.0 + i,
                "validation_success_rate": 95.0 + i,
                "network_reliability": 95.0 + i
            }
            node.assign_to_shard(i % 3)  # Modular shard assignment
            nodes.append(node)
        return nodes

    def _create_test_transactions(self, num_transactions: int = 10) -> List[Transaction]:
        """Create test transactions with modular shard adaptation."""
        transactions = []
        for i in range(num_transactions):
            tx = Transaction(
                sender=f"sender_{i}",
                receiver=f"receiver_{i}",
                action="transfer",
                data={"amount": 10.0 * (i + 1)},
                shard_id=i % 3,  # Adapted for modular shard
                priority=min(i % 5 + 1, 5),
                cooperative_tags={f"tag_{i}"}
            )
            transactions.append(tx)
        return transactions

    def test_shard_creation_and_initialization(self):
        """Test basic shard creation and initialization."""
        shard = Shard(shard_id=1, max_transactions_per_block=5)
        
        # Verify basic properties
        self.assertEqual(shard.shard_id, 1)
        self.assertEqual(shard.max_transactions_per_block, 5)
        self.assertEqual(shard.height, 1)  # Should start at 1 after genesis
        self.assertTrue(shard.chain)  # Should have genesis block
        
        # Verify genesis block
        genesis = shard.chain[0]
        self.assertEqual(genesis.index, 0)
        self.assertEqual(genesis.previous_hash, "0" * 64)
        self.assertEqual(genesis.validator, "genesis")
        self.assertEqual(genesis.shard_id, 1)

    def test_node_shard_assignment(self):
        """Test assigning nodes to shards."""
        shard_1 = Shard(shard_id=1)
        shard_2 = Shard(shard_id=2)

        # Assign nodes to shards
        for node in self.nodes[:3]:
            self.assertTrue(node.assign_to_shard(1))
            self.assertIn(1, node.shard_assignments)

        for node in self.nodes[2:]:  # Overlapping assignment for node_2
            self.assertTrue(node.assign_to_shard(2))
            self.assertIn(2, node.shard_assignments)

        # Verify node can't be assigned to too many shards
        node = self.nodes[0]
        for i in range(5):  # Try to assign to more shards than allowed
            node.assign_to_shard(i)
        self.assertLessEqual(len(node.shard_assignments), 3)

    def test_transaction_distribution(self):
        """Test transaction distribution across shards."""
        shards = {i: Shard(shard_id=i) for i in range(3)}
        
        # Add transactions to appropriate shards
        for tx in self.test_transactions:
            shard = shards[tx.shard_id]
            self.assertTrue(shard.add_transaction(tx))
            
        # Verify distribution
        for shard_id, shard in shards.items():
            shard_txs = len(shard.pending_transactions)
            self.assertGreaterEqual(shard_txs, 0)
            self.assertLessEqual(shard_txs, shard.max_transactions_per_block * 2)

    def test_cross_shard_transaction_handling(self):
        """Test handling of cross-shard transactions."""
        shard_1 = Shard(shard_id=1)
        shard_2 = Shard(shard_id=2)

        # Create cross-shard transaction
        cross_tx = Transaction(
            sender="user1",
            receiver="user2",
            action="transfer",
            data={
                "amount": 100.0,
                "target_shard": 2
            },
            shard_id=1,
            cross_shard_refs=["ref_1"]
        )

        # Add to source shard
        self.assertTrue(shard_1.add_transaction(cross_tx))
        
        # Create and verify block with cross-shard transaction
        block = shard_1.create_block("test_validator")
        self.assertIsNotNone(block)
        self.assertTrue(block.cross_shard_refs)
        
        # Verify cross-shard reference tracking
        self.assertIn(2, shard_1.cross_shard_references)

    def test_shard_load_balancing(self):
        """Test shard load balancing mechanisms."""
        shards = {i: Shard(shard_id=i) for i in range(3)}
        
        # Add varied load to shards
        for i, tx in enumerate(self.test_transactions):
            # Deliberately overload shard 0
            if i < 5:
                shards[0].add_transaction(tx)
            else:
                shard_id = i % 3
                shards[shard_id].add_transaction(tx)

        # Verify load distribution
        load_stats = {
            shard_id: len(shard.pending_transactions) 
            for shard_id, shard in shards.items()
        }
        
        # Check that no shard is overloaded
        for load in load_stats.values():
            self.assertLessEqual(load, Shard.max_transactions_per_block * 2)

    def test_shard_state_management(self):
        """Test shard state management and persistence."""
        # Add transactions and create blocks
        for tx in self.test_transactions[:3]:
            self.main_shard.add_transaction(tx)
            
        initial_state = self.main_shard.state.copy()
        
        # Create block
        block = self.main_shard.create_block("test_validator")
        self.assertTrue(self.main_shard.add_block(block))
        
        # Verify state updates
        self.assertNotEqual(self.main_shard.state, initial_state)
        self.assertEqual(self.main_shard.height, 2)  # Genesis + 1

        # Test state rollback
        self.main_shard.state = initial_state
        self.assertEqual(self.main_shard.state, initial_state)

    def test_shard_synchronization(self):
        """Test synchronization between shards."""
        shard_1 = Shard(shard_id=1)
        shard_2 = Shard(shard_id=2)

        # Create cross-shard transaction
        cross_tx = Transaction(
            sender="user1",
            receiver="user2",
            action="transfer",
            data={
                "amount": 100.0,
                "target_shard": 2
            },
            shard_id=1
        )

        # Process in source shard
        shard_1.add_transaction(cross_tx)
        block_1 = shard_1.create_block("test_validator")
        shard_1.add_block(block_1)

        # Verify target shard can validate cross-shard references
        self.assertIn(2, shard_1.cross_shard_references)
        reference = list(shard_1.cross_shard_references[2])[0]
        self.assertTrue(shard_2.validate_cross_shard_ref(reference))

    def test_shard_metrics_and_monitoring(self):
        """Test shard performance metrics and monitoring."""
        # Generate some activity
        for tx in self.test_transactions[:5]:
            self.main_shard.add_transaction(tx)
        
        block = self.main_shard.create_block("test_validator")
        self.main_shard.add_block(block)

        # Get metrics
        metrics = self.main_shard.get_metrics()
        
        # Verify metric fields
        self.assertIn("total_transactions", metrics)
        self.assertIn("average_block_time", metrics)
        self.assertIn("pending_count", metrics)
        self.assertIn("chain_size", metrics)
        
        # Verify metric values
        self.assertEqual(metrics["shard_id"], self.main_shard.shard_id)
        self.assertGreater(metrics["total_transactions"], 0)
        self.assertGreaterEqual(metrics["chain_size"], 2)  # Genesis + 1

    def test_shard_recovery(self):
        """Test shard recovery from invalid states."""
        # Create some valid state
        for tx in self.test_transactions[:3]:
            self.main_shard.add_transaction(tx)
        
        valid_block = self.main_shard.create_block("test_validator")
        self.main_shard.add_block(valid_block)
        
        # Save valid state
        valid_state = self.main_shard.to_dict()
        
        # Corrupt shard state
        self.main_shard.chain.append("invalid_block")
        self.assertFalse(self.main_shard.validate_chain())
        
        # Recover from valid state
        recovered_shard = Shard.from_dict(valid_state)
        self.assertTrue(recovered_shard.validate_chain())
        self.assertEqual(recovered_shard.height, 2)  # Genesis + 1

    def test_shard_consensus_integration(self):
        """Test integration between shard management and consensus."""
        # Assign validators to shard
        validators = self.nodes[:3]
        for node in validators:
            node.assign_to_shard(self.main_shard.shard_id)

        # Add transactions
        for tx in self.test_transactions[:3]:
            self.main_shard.add_transaction(tx)

        # Select validator and create block
        validator = self.consensus.select_validator(validators, self.main_shard.shard_id)
        self.assertIsNotNone(validator)
        
        block = self.main_shard.create_block(validator.node_id)
        self.assertIsNotNone(block)
        
        # Validate and add block
        self.assertTrue(self.consensus.validate_block(block, self.main_shard.chain[-1], validator))
        self.assertTrue(self.main_shard.add_block(block))

if __name__ == "__main__":
    unittest.main()
