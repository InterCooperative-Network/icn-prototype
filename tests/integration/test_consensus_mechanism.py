import unittest
from datetime import datetime, timedelta
from typing import List

# Adjust imports based on your project structure
from blockchain.consensus.proof_of_cooperation import ProofOfCooperation
from blockchain.core.node import Node
from blockchain.core.block import Block
from blockchain.core.transaction import Transaction

class TestConsensusMechanism(unittest.TestCase):
    """Test suite for the ICN Proof of Cooperation consensus mechanism."""

    def setUp(self):
        """Set up the consensus mechanism and initial test data."""
        self.consensus = ProofOfCooperation(min_reputation=10.0, cooldown_blocks=3)
        self.nodes = self._create_test_nodes()
        
        # Create a genesis block
        self.genesis_block = Block(
            index=0,
            previous_hash="0" * 64,
            timestamp=datetime.now() - timedelta(minutes=10),
            transactions=[],
            validator="genesis",
            shard_id=1
        )

    def _create_test_node(self, node_id: str, reputation_score: float = 25.0) -> Node:
        """Create a test node with predefined characteristics."""
        node = Node(node_id=node_id, cooperative_id="test_coop", initial_stake=100.0)

        # Assign reputation scores
        for category in node.reputation_scores:
            node.reputation_scores[category] = reputation_score

        # Assign validation history
        node.validation_history = [
            {"timestamp": datetime.now() - timedelta(minutes=i),
             "category": "validation", "score_change": 1.0, "evidence": {"success": True}}
            for i in range(20)
        ]

        # Assign performance metrics
        node.performance_metrics = {
            "availability": 98.0,
            "validation_success_rate": 95.0,
            "network_reliability": 97.0
        }

        # Set node state
        node.metadata["status"] = "active"
        node.cooldown = 0

        # Assign shard
        node.assign_to_shard(1)

        # Add cooperative interactions
        node.cooperative_interactions = [f"coop_{i}" for i in range(30)]

        return node

    def _create_test_nodes(self, num_nodes: int = 5) -> List[Node]:
        """Create a list of test nodes with varying characteristics."""
        nodes = []
        for i in range(num_nodes):
            reputation = 25.0 + (i * 5.0)
            node = self._create_test_node(f"node_{i}", reputation)
            nodes.append(node)
        return nodes

    def _create_test_block(self, transactions: List[Transaction]) -> Block:
        """Create a test block with predefined transactions."""
        return Block(
            index=self.genesis_block.index + 1,
            previous_hash=self.genesis_block.hash,
            timestamp=datetime.now(),
            transactions=transactions,
            validator="test_validator",
            shard_id=1
        )

    def test_initialization(self):
        """Test initialization of the consensus mechanism."""
        self.assertEqual(self.consensus.min_reputation, 10.0)
        self.assertEqual(self.consensus.cooldown_blocks, 3)
        self.assertGreater(len(self.consensus.reputation_weights), 0)
        self.assertGreater(len(self.consensus.validation_thresholds), 0)

    def test_validator_selection(self):
        """Test the process of selecting a validator."""
        validator = self.consensus.select_validator(self.nodes)
        self.assertIsNotNone(validator)
        self.assertIn(validator, self.nodes)

        # Test selection with specific shard
        shard_validator = self.consensus.select_validator(self.nodes, shard_id=1)
        self.assertIsNotNone(shard_validator)

        # Test selection when all nodes are in cooldown
        for node in self.nodes:
            node.enter_cooldown(3)
        no_validator = self.consensus.select_validator(self.nodes)
        self.assertIsNone(no_validator)

        # Test selection with varying reputation scores
        for node in self.nodes:
            node.cooldown = 0
        high_rep_node = self._create_test_node("high_rep", 100.0)
        self.nodes.append(high_rep_node)

        selection_counts = {node.node_id: 0 for node in self.nodes}
        for _ in range(100):
            selected = self.consensus.select_validator(self.nodes)
            if selected:
                selection_counts[selected.node_id] += 1

        self.assertGreater(selection_counts["high_rep"], selection_counts["node_0"])

    def test_cooperation_score_calculation(self):
        """Test calculation of cooperation scores."""
        node = self._create_test_node("test_node")

        score = self.consensus.calculate_cooperation_score(node)
        self.assertGreater(score, 0)

        # Test with lower reputation
        for category in node.reputation_scores:
            node.reputation_scores[category] = 5.0
        low_score = self.consensus.calculate_cooperation_score(node)
        self.assertLess(low_score, score)

        # Test with limited cooperative interactions
        node.cooperative_interactions = ["coop_1"] * 30
        limited_score = self.consensus.calculate_cooperation_score(node)
        self.assertLess(limited_score, score)

        # Test with lower performance metrics
        node.performance_metrics["availability"] = 50.0
        poor_score = self.consensus.calculate_cooperation_score(node)
        self.assertLess(poor_score, score)

    def test_block_validation(self):
        """Test the block validation process."""
        validator = self._create_test_node("test_validator")

        transactions = [
            Transaction(
                sender=f"sender_{i}",
                receiver=f"receiver_{i}",
                action="transfer",
                data={"amount": 10.0},
                shard_id=1
            ) for i in range(3)
        ]

        valid_block = self._create_test_block(transactions)
        self.assertTrue(self.consensus.validate_block(valid_block, self.genesis_block, validator))

        # Test with an invalid timestamp
        invalid_block = self._create_test_block(transactions)
        invalid_block.timestamp = datetime.now() + timedelta(hours=1)
        self.assertFalse(self.consensus.validate_block(invalid_block, self.genesis_block, validator))

        # Test with a validator having insufficient reputation
        invalid_validator = self._create_test_node("invalid_validator", reputation_score=5.0)
        self.assertFalse(self.consensus.validate_block(valid_block, self.genesis_block, invalid_validator))

    def test_collusion_detection(self):
        """Test detection of collusion in transactions."""
        validator = self._create_test_node("test_validator")

        normal_transactions = [
            Transaction(
                sender=f"sender_{i}",
                receiver=f"receiver_{i}",
                action="transfer",
                data={"amount": 10.0},
                shard_id=1
            ) for i in range(10)
        ]
        normal_block = self._create_test_block(normal_transactions)

        suspicious_transactions = [
            Transaction(
                sender="suspicious_sender",
                receiver=f"receiver_{i}",
                action="transfer",
                data={"amount": 10.0},
                shard_id=1
            ) for i in range(10)
        ]
        suspicious_block = self._create_test_block(suspicious_transactions)

        self.assertFalse(self.consensus.detect_collusion(validator, normal_block))
        self.assertTrue(self.consensus.detect_collusion(validator, suspicious_block))

    def test_cooldown_mechanism(self):
        """Test the cooldown mechanism for validators."""
        validator = self._create_test_node("test_validator")

        selected = self.consensus.select_validator([validator])
        self.assertIsNotNone(selected)
        self.assertEqual(selected.cooldown, self.consensus.cooldown_blocks)

        # Ensure validator cannot be selected during cooldown
        new_selection = self.consensus.select_validator([validator])
        self.assertIsNone(new_selection)

        # Reset cooldown and test selection again
        validator.cooldown = 0
        final_selection = self.consensus.select_validator([validator])
        self.assertIsNotNone(final_selection)

    def test_validation_metrics(self):
        """Test tracking of validation metrics."""
        validator = self._create_test_node("test_validator")
        transactions = [
            Transaction(
                sender=f"sender_{i}",
                receiver=f"receiver_{i}",
                action="transfer",
                data={"amount": 10.0},
                shard_id=1
            ) for i in range(3)
        ]

        for i in range(5):
            block = self._create_test_block(transactions)
            self.consensus.validate_block(block, self.genesis_block, validator)

        metrics = self.consensus.get_metrics()
        self.assertGreater(metrics["total_validations"], 0)
        self.assertGreater(metrics["successful_validations"], 0)
        self.assertGreaterEqual(metrics["success_rate"], 0)

    def test_shard_specific_validation(self):
        """Test validation in a shard-specific context."""
        validator = self._create_test_node("test_validator")

        self.assertTrue(validator.can_validate(shard_id=1))
        self.assertFalse(validator.can_validate(shard_id=2))

        # Ensure validator selection for a specific shard
        selected = self.consensus.select_validator(self.nodes, shard_id=1)
        if selected:
            self.assertIn(1, selected.shard_assignments)

    def test_progressive_reputation_requirements(self):
        """Test progressive reputation requirements for new nodes."""
        new_node = self._create_test_node("new_node")
        new_node.total_validations = 0

        # Set lower reputation
        for category in new_node.reputation_scores:
            new_node.reputation_scores[category] = self.consensus.min_reputation * 0.6

        self.assertTrue(self.consensus._can_participate(new_node))

        # Increase validations and recheck eligibility
        new_node.total_validations = 20
        self.assertFalse(self.consensus._can_participate(new_node))

    def test_validator_history(self):
        """Test tracking of validator history."""
        for _ in range(5):
            validator = self.consensus.select_validator(self.nodes)
            if validator:
                self.assertIn(
                    validator.node_id,
                    [record[0] for record in self.consensus.validator_history]
                )

        # Ensure history length is capped
        for _ in range(1000):
            self.consensus.validator_history.append(("test_node", datetime.now(), 1))
        self.assertLessEqual(len(self.consensus.validator_history), 1000)

if __name__ == '__main__':
    unittest.main()
