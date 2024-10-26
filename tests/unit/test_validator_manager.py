import unittest
from datetime import datetime, timedelta
from blockchain.consensus.proof_of_cooperation.validator_manager import ValidatorManager
from blockchain.core.node import Node

class TestValidatorManager(unittest.TestCase):
    def setUp(self):
        self.manager = ValidatorManager(min_reputation=10.0, cooldown_blocks=3)
        self.node1 = Node(node_id="node1", cooperative_id="coop1", initial_stake=100.0)
        self.node1.reputation = 25.0

    def test_validator_selection(self):
        # Test that node1 can be selected as a validator
        validator = self.manager.select_validator([self.node1])
        self.assertEqual(validator, self.node1)
        self.assertEqual(validator.cooldown, 3)

    def test_validator_ineligibility_due_to_cooldown(self):
        # Put node1 into cooldown and test that it cannot be selected
        self.node1.cooldown = 1
        validator = self.manager.select_validator([self.node1])
        self.assertIsNone(validator)

    def test_priority_calculation(self):
        # Test the priority calculation for validator selection
        score = self.manager._calculate_priority_score(self.node1)
        self.assertGreater(score, 0)

if __name__ == '__main__':
    unittest.main()
