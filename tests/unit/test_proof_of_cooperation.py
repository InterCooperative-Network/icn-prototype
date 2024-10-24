import unittest
from datetime import datetime, timedelta
import sys
import os
from typing import List
import random

# Add project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from blockchain.consensus.proof_of_cooperation import ProofOfCooperation
from blockchain.core.node import Node
from blockchain.core.block import Block
from blockchain.core.transaction import Transaction

class TestProofOfCooperation(unittest.TestCase):
    """Test cases for the ProofOfCooperation consensus mechanism."""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.poc = ProofOfCooperation(min_reputation=10.0, cooldown_blocks=3)
        self.test_nodes = self._create_test_nodes()
        
        # Create genesis block
        self.genesis_block = Block(
            index=0,
            previous_hash="0" * 64,
            timestamp=datetime.now() - timedelta(minutes=10),
            transactions=[],
            validator="genesis",
            shard_id=1
        )

    def _initialize_test_node(self, node_id: str) -> Node:
        """Initialize a test node with all required attributes."""
        node = Node(
            node_id=node_id,
            cooperative_id="test_coop",
            initial_stake=100.0
        )
        
        # Ensure sufficient reputation
        base_score = 25.0  # Well above min_reputation
        for category in node.reputation_scores:
            node.reputation_scores[category] = base_score
            
        # Add successful validation history
        node.validation_history = [
            {
                "timestamp": datetime.now() - timedelta(minutes=i),
                "category": "validation",
                "score_change": 1.0,
                "evidence": {"success": True}
            }
            for i in range(20)
        ]
        
        # Set high performance metrics
        node.performance_metrics = {
            "availability": 98.0,
            "validation_success_rate": 95.0,
            "network_reliability": 97.0
        }
        
        # Set active state
        node.metadata["status"] = "active"
        node.cooldown = 0
        node.total_validations = 15  # Experienced node
        
        # Add shard assignment
        node.assign_to_shard(1)  # Assign to test shard
        node.active_shards[1] = datetime.now() - timedelta(hours=1)
        
        # Add diverse cooperative interactions
        node.cooperative_interactions = [
            f"coop_{i % 5}" for i in range(30)
        ]
        
        return node

    def _create_test_nodes(self, num_nodes: int = 5) -> List[Node]:
        """Create a list of test nodes."""
        return [
            self._initialize_test_node(f"node_{i}")
            for i in range(num_nodes)
        ]

    def _create_test_block(self, index: int, previous_block: Block, validator_id: str) -> Block:
        """Create a test block properly linked to previous block."""
        return Block(
            index=index,
            previous_hash=previous_block.hash,
            timestamp=datetime.now(),
            transactions=[
                Transaction(
                    sender=f"user_{i}",
                    receiver=f"user_{i+1}",
                    action="transfer",
                    data={"amount": 10.0},
                    shard_id=1
                )
                for i in range(3)
            ],
            validator=validator_id,
            shard_id=1
        )

    def test_initialization(self):
        """Test ProofOfCooperation initialization."""
        self.assertEqual(self.poc.min_reputation, 10.0)
        self.assertEqual(self.poc.cooldown_blocks, 3)
        self.assertGreater(len(self.poc.reputation_weights), 0)
        self.assertGreater(len(self.poc.validation_thresholds), 0)
        self.assertTrue(0 < self.poc.reputation_decay_factor <= 1)

    def test_calculate_cooperation_score(self):
        """Test cooperation score calculation."""
        node = self._initialize_test_node("score_test_node")
        
        # Test basic score calculation
        score = self.poc.calculate_cooperation_score(node)
        self.assertGreater(score, 0)
        
        # Test with shard_id
        shard_score = self.poc.calculate_cooperation_score(node, shard_id=1)
        self.assertGreater(shard_score, 0)
        
        # Test node in cooldown
        node.enter_cooldown(3)
        cooldown_score = self.poc.calculate_cooperation_score(node)
        self.assertEqual(cooldown_score, 0)

    def test_validator_selection(self):
        """Test validator selection process."""
        nodes = [
            self._initialize_test_node(f"select_node_{i}")
            for i in range(5)
        ]
        
        # Test basic selection
        validator = self.poc.select_validator(nodes)
        self.assertIsNotNone(validator)
        self.assertIn(validator, nodes)
        
        # Test selection with shard_id
        shard_validator = self.poc.select_validator(nodes, shard_id=1)
        self.assertIsNotNone(shard_validator)
        
        # Test with all nodes in cooldown
        for node in nodes:
            node.enter_cooldown(3)
        no_validator = self.poc.select_validator(nodes)
        self.assertIsNone(no_validator)

    def test_block_validation(self):
        """Test block validation process."""
        validator = self._initialize_test_node("test_validator")
        
        # Create a valid test block
        test_block = self._create_test_block(1, self.genesis_block, validator.node_id)
        
        # Verify block validation
        is_valid = self.poc.validate_block(test_block, self.genesis_block, validator)
        self.assertTrue(is_valid)
        
        # Test invalid block (future timestamp)
        invalid_block = Block(
            index=1,
            previous_hash=self.genesis_block.hash,
            timestamp=datetime.now() + timedelta(hours=1),
            transactions=[],
            validator=validator.node_id,
            shard_id=1
        )
        is_invalid = self.poc.validate_block(invalid_block, self.genesis_block, validator)
        self.assertFalse(is_invalid)

    def test_collusion_detection(self):
        """Test collusion detection mechanism."""
        node = self._initialize_test_node("collusion_test_node")
        
        # Create block with diverse transactions
        diverse_block = Block(
            index=1,
            previous_hash=self.genesis_block.hash,
            timestamp=datetime.now(),
            transactions=[
                Transaction(
                    sender=f"user_{i}",
                    receiver=f"user_{i+1}",
                    action="transfer",
                    data={"amount": 10.0},
                    shard_id=1
                )
                for i in range(5)
            ],
            validator=node.node_id,
            shard_id=1
        )
        
        # Create block with obvious collusion pattern
        collusion_transactions = [
            Transaction(
                sender="colluding_user",
                receiver=f"receiver_{i}",  # Fixed variable scope issue
                action="transfer",
                data={"amount": 10.0},
                shard_id=1
            )
            for i in range(5)
        ]
        
        collusion_block = Block(
            index=2,
            previous_hash=self.genesis_block.hash,
            timestamp=datetime.now(),
            transactions=collusion_transactions,
            validator=node.node_id,
            shard_id=1
        )
        
        # Test detection
        diverse_collusion = self.poc.detect_collusion(node, diverse_block)
        self.assertFalse(diverse_collusion)
        
        repeated_collusion = self.poc.detect_collusion(node, collusion_block)
        self.assertTrue(repeated_collusion)

    def test_diversity_factor(self):
        """Test diversity factor calculation."""
        diverse_node = self._initialize_test_node("diverse_node")
        diverse_node.cooperative_interactions = [f"coop_{i}" for i in range(10)]
        diverse_factor = self.poc._calculate_diversity_factor(diverse_node)
        
        limited_node = self._initialize_test_node("limited_node")
        limited_node.cooperative_interactions = ["coop_1"] * 10
        limited_factor = self.poc._calculate_diversity_factor(limited_node)
        
        self.assertGreater(diverse_factor, limited_factor)

    def test_consistency_factor(self):
        """Test consistency factor calculation."""
        node = self._initialize_test_node("consistency_node")
        
        # Test with successful validations
        node.validation_history = [
            {"evidence": {"success": True}} for _ in range(10)
        ]
        high_consistency = self.poc._calculate_consistency_factor(node)
        
        # Test with mixed success
        node.validation_history = [
            {"evidence": {"success": i % 2 == 0}} for i in range(10)
        ]
        mixed_consistency = self.poc._calculate_consistency_factor(node)
        
        self.assertGreater(high_consistency, mixed_consistency)

    def test_performance_factor(self):
        """Test performance factor calculation."""
        node = self._initialize_test_node("performance_node")
        
        # Test high performance
        node.performance_metrics = {
            "availability": 98.0,
            "validation_success_rate": 95.0,
            "network_reliability": 97.0
        }
        high_performance = self.poc._calculate_performance_factor(node)
        
        # Test lower performance
        node.performance_metrics = {
            "availability": 85.0,
            "validation_success_rate": 82.0,
            "network_reliability": 88.0
        }
        lower_performance = self.poc._calculate_performance_factor(node)
        
        self.assertGreater(high_performance, lower_performance)

    def test_metrics(self):
        """Test consensus metrics collection."""
        # Record some validation activity
        for i in range(5):
            self.poc.performance_metrics["total_validations"] += 1
            self.poc.performance_metrics["successful_validations"] += (i % 2)
        
        metrics = self.poc.get_metrics()
        
        self.assertIn("total_validations", metrics)
        self.assertIn("successful_validations", metrics)
        self.assertIn("average_block_time", metrics)
        self.assertIn("collusion_detections", metrics)
        self.assertGreaterEqual(metrics["total_validations"], 5)

if __name__ == '__main__':
    unittest.main()