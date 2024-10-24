import unittest
from datetime import datetime, timedelta
from blockchain.core.block import Block
from blockchain.core.transaction import Transaction
from blockchain.core.node import Node
from blockchain.core.shard import Shard
from blockchain.consensus.proof_of_cooperation import ProofOfCooperation

class TestBlock(unittest.TestCase):
    def setUp(self):
        """Set up test environment before each test method."""
        self.transactions = [
            Transaction(
                sender="user1",
                receiver="user2", 
                action="transfer",
                data={"amount": 10.0}
            ),
            Transaction(
                sender="user2",
                receiver="user3",
                action="transfer",
                data={"amount": 5.0}
            )
        ]
        
        self.block = Block(
            index=1,
            previous_hash="abc123",
            timestamp=datetime.now(),
            transactions=self.transactions,
            validator="node1",
            shard_id=1
        )

    def test_initialization(self):
        """Test block initialization with proper values."""
        self.assertEqual(self.block.index, 1)
        self.assertEqual(self.block.previous_hash, "abc123")
        self.assertEqual(self.block.validator, "node1")
        self.assertEqual(self.block.shard_id, 1)
        self.assertEqual(len(self.block.transactions), 2)
        self.assertIsNotNone(self.block.merkle_root)

    def test_calculate_merkle_root(self):
        """Test Merkle root calculation."""
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
        self.assertIsNotNone(empty_block.merkle_root)

    def test_calculate_hash(self):
        """Test block hash calculation."""
        initial_hash = self.block.hash
        new_hash = self.block.calculate_hash()
        self.assertEqual(initial_hash, new_hash)
        
        # Test hash changes with different transactions
        self.block.transactions.append(
            Transaction(
                sender="user3",
                receiver="user4",
                action="transfer",
                data={"amount": 15.0}
            )
        )
        self.assertNotEqual(initial_hash, self.block.calculate_hash())

    def test_validate(self):
        """Test block validation logic."""
        # Create a previous block
        previous_block = Block(
            index=0,
            previous_hash="0" * 64,
            timestamp=datetime.now() - timedelta(minutes=5),
            transactions=[],
            validator="genesis",
            shard_id=1
        )
        
        # Test valid block
        self.assertTrue(self.block.validate(previous_block))
        
        # Test invalid cases
        self.block.hash = "invalid_hash"
        self.assertFalse(self.block.validate(previous_block))
        
        self.block.hash = self.block.calculate_hash()
        self.block.timestamp = datetime.now() + timedelta(hours=1)
        self.assertFalse(self.block.validate(previous_block))

    def test_add_transaction(self):
        """Test adding transactions to the block."""
        new_tx = Transaction(
            sender="user3",
            receiver="user4",
            action="transfer",
            data={"amount": 15.0},
            shard_id=1
        )
        
        initial_merkle_root = self.block.merkle_root
        self.assertTrue(self.block.add_transaction(new_tx))
        self.assertNotEqual(initial_merkle_root, self.block.merkle_root)
        
        # Test adding transaction with wrong shard_id
        invalid_tx = Transaction(
            sender="user4",
            receiver="user5",
            action="transfer",
            data={"amount": 20.0},
            shard_id=2
        )
        self.assertFalse(self.block.add_transaction(invalid_tx))

    def test_to_dict_and_from_dict(self):
        """Test converting block to dict and back."""
        block_dict = self.block.to_dict()
        new_block = Block.from_dict(block_dict)
        
        self.assertEqual(new_block.index, self.block.index)
        self.assertEqual(new_block.previous_hash, self.block.previous_hash)
        self.assertEqual(new_block.validator, self.block.validator)
        self.assertEqual(len(new_block.transactions), len(self.block.transactions))
        self.assertEqual(new_block.hash, self.block.hash)

class TestNode(unittest.TestCase):
    def setUp(self):
        """Set up test environment before each test method."""
        self.node = Node(
            node_id="test_node",
            cooperative_id="test_coop",
            initial_stake=100.0
        )

    def test_initialization(self):
        """Test node initialization with proper values."""
        self.assertEqual(self.node.node_id, "test_node")
        self.assertEqual(self.node.cooperative_id, "test_coop")
        self.assertEqual(self.node.stake, 100.0)
        self.assertEqual(self.node.metadata["status"], "active")
        self.assertGreater(len(self.node.reputation_scores), 0)

    def test_update_reputation(self):
        """Test updating reputation scores."""
        category = "validation"
        initial_score = self.node.reputation_scores[category]
        
        # Test positive update
        success = self.node.update_reputation(
            category=category,
            score=5.0,
            cooperative_id="test_coop",
            evidence={"type": "successful_validation"}
        )
        self.assertTrue(success)
        self.assertEqual(self.node.reputation_scores[category], initial_score + 5.0)
        
        # Test invalid category
        success = self.node.update_reputation(
            category="invalid_category",
            score=3.0
        )
        self.assertFalse(success)

    def test_assign_to_shard(self):
        """Test shard assignment logic."""
        # Test successful assignment
        self.assertTrue(self.node.assign_to_shard(1))
        self.assertIn(1, self.node.shard_assignments)
        
        # Test maximum shard limit
        self.node.assign_to_shard(2)
        self.node.assign_to_shard(3)
        self.assertFalse(self.node.assign_to_shard(4))

    def test_can_validate(self):
        """Test validation eligibility checks."""
        # Test initial state
        self.assertTrue(self.node.can_validate())
        
        # Test cooldown period
        self.node.enter_cooldown(2)
        self.assertFalse(self.node.can_validate())
        
        # Test shard-specific validation
        self.node.assign_to_shard(1)
        self.assertTrue(self.node.can_validate(1))
        self.assertFalse(self.node.can_validate(2))

    def test_to_dict_from_dict(self):
        """Test converting node to dict and back."""
        node_dict = self.node.to_dict()
        restored_node = Node.from_dict(node_dict)
        
        self.assertEqual(restored_node.node_id, self.node.node_id)
        self.assertEqual(restored_node.cooperative_id, self.node.cooperative_id)
        self.assertEqual(restored_node.stake, self.node.stake)
        self.assertEqual(restored_node.reputation_scores, self.node.reputation_scores)

if __name__ == '__main__':
    unittest.main()