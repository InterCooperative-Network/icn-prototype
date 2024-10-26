import unittest
from datetime import datetime, timedelta
from blockchain.consensus.proof_of_cooperation.cooldown_manager import CooldownManager
from blockchain.core.node import Node

class TestCooldownManagement(unittest.TestCase):
    def setUp(self):
        self.cooldown_manager = CooldownManager(base_cooldown=3, max_cooldown=10)
        self.node1 = Node(node_id="node1", cooperative_id="coop1", initial_stake=100.0)
        self.node1.reputation = 20.0

    def test_dynamic_cooldown_increase(self):
        # Simulate high participation rate to trigger dynamic cooldown increase
        for _ in range(5):
            self.cooldown_manager._track_activity(self.node1)
        
        self.cooldown_manager.apply_cooldown(self.node1)
        self.assertGreater(self.node1.cooldown, 3)  # Cooldown should increase

    def test_cooldown_decay(self):
        # Test that cooldown decreases gradually over time
        self.node1.cooldown = 5
        self.cooldown_manager.reset_cooldown(self.node1)
        self.assertEqual(self.node1.cooldown, 4)

if __name__ == '__main__':
    unittest.main()
