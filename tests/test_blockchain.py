# tests/test_blockchain.py

import unittest
import time
from blockchain.blockchain import Blockchain, Node, ProofOfCooperation, Block
from system.reputation import ReputationSystem
from system.governance import Governance, Proposal
from system.marketplace import Marketplace
from system.storage import DistributedStorage
from did.did import DID, DIDRegistry

class TestICNComponents(unittest.TestCase):
    def setUp(self):
        self.blockchain = Blockchain()
        self.did_registry = DIDRegistry()
        self.reputation_system = ReputationSystem()
        self.governance = Governance(blockchain=self.blockchain)
        self.marketplace = Marketplace(self.blockchain, self.did_registry)
        self.storage = DistributedStorage(self.blockchain)

        # Initialize test DIDs
        self.test_dids = [DID() for _ in range(5)]
        for did in self.test_dids:
            did_id = did.generate_did()
            self.did_registry.register_did(did)
            self.reputation_system.add_user(did_id)

    def test_blockchain_and_poc(self):
        initial_chain_length = len(self.blockchain.get_chain(0))
        self.blockchain.add_new_block("Test Block", 0)
        self.assertEqual(len(self.blockchain.get_chain(0)), initial_chain_length + 1)

        # Test cross-shard transaction
        self.blockchain.cross_shard_transaction(0, 1, "Cross-shard test")
        shard_0_last_block = self.blockchain.get_chain(0)[-1]
        shard_1_last_block = self.blockchain.get_chain(1)[-1]
        self.assertIn("Cross-shard tx start", shard_0_last_block['data'])
        self.assertIn("Cross-shard tx end", shard_1_last_block['data'])

    def test_smart_contracts(self):
        contract_code = """
def execute(input, state):
    if 'counter' not in state:
        state['counter'] = 0
    state['counter'] += 1
    return state
        """
        self.blockchain.deploy_contract("test_contract", contract_code, self.test_dids[0].generate_did())

        # Execute the contract twice and check the counter
        result = self.blockchain.execute_contract("test_contract", {})
        self.assertEqual(result.get('counter', 0), 1)

        result = self.blockchain.execute_contract("test_contract", {})
        self.assertEqual(result.get('counter', 0), 2)

    def test_did_and_reputation(self):
        did = self.test_dids[0]
        did_id = did.generate_did()
        
        # Test DID resolution
        resolved_did = self.did_registry.resolve_did(did_id)
        self.assertEqual(resolved_did.generate_did(), did_id)

        # Test reputation update
        self.reputation_system.update_reputation(did_id, 10, "validation")
        rep = self.reputation_system.get_reputation(did_id)
        self.assertEqual(rep["validation"], 10)

        # Test reputation decay
        self.reputation_system.apply_decay()
        rep = self.reputation_system.get_reputation(did_id)
        self.assertLess(rep["validation"], 10)

class TestEnhancedPoC(unittest.TestCase):
    def setUp(self):
        self.blockchain = Blockchain(num_shards=2)
        self.test_node = self.blockchain.nodes[0]

    def test_reputation_calculation(self):
        # Test initial reputation calculation
        self.test_node.update_reputation("cooperative_growth", 10)
        score = self.blockchain.poc.calculate_reputation_score(self.test_node)
        self.assertGreater(score, 0)

        # Test reputation decay over time
        initial_score = score
        for _ in range(10):
            self.blockchain.poc.update_nodes()
        
        new_score = self.blockchain.poc.calculate_reputation_score(self.test_node)
        self.assertLess(new_score, initial_score)

    def test_diversity_factor(self):
        # Test diversity factor with single cooperative
        self.test_node.update_reputation("cooperative_growth", 10, "coop1")
        single_coop_factor = self.blockchain.poc.calculate_diversity_factor(self.test_node)

        # Test diversity factor with multiple cooperatives
        self.test_node.update_reputation("cooperative_growth", 10, "coop2")
        self.test_node.update_reputation("cooperative_growth", 10, "coop3")
        multi_coop_factor = self.blockchain.poc.calculate_diversity_factor(self.test_node)

        self.assertGreater(multi_coop_factor, single_coop_factor)

    def test_validator_selection(self):
        selections = {}
        for _ in range(100):
            validator = self.blockchain.poc.select_validator()
            if validator:
                selections[validator.node_id] = selections.get(validator.node_id, 0) + 1

        # Check that multiple nodes were selected
        self.assertGreater(len(selections), 1)
        
        # Check that nodes with higher reputation are selected more often
        high_rep_node = max(self.blockchain.nodes, 
                           key=lambda n: sum(n.reputation_scores.values()))
        high_rep_selections = selections.get(high_rep_node.node_id, 0)
        
        for node_id, count in selections.items():
            if node_id != high_rep_node.node_id:
                self.assertGreaterEqual(high_rep_selections, count)

    def test_collusion_detection(self):
        # Simulate repeated validations with same target
        block = Block(1, "prev_hash", time.time(), "test_data", "hash")
        for _ in range(20):
            self.test_node.record_action("validation", "same_target")
        
        # Check collusion detection
        collusion_detected = self.blockchain.poc.detect_collusion(self.test_node, block)
        self.assertTrue(collusion_detected)

        # Test stake slashing
        initial_stake = self.test_node.stake
        self.blockchain.poc.slash_stake(self.test_node, 5)
        self.assertLess(self.test_node.stake, initial_stake)

    def test_network_statistics(self):
        stats = self.blockchain.get_network_statistics()
        
        self.assertIn("total_nodes", stats)
        self.assertIn("active_nodes", stats)
        self.assertIn("average_reputation", stats)
        self.assertIn("cooperation_metrics", stats)

    def test_node_diversity_metrics(self):
        # Add diverse cooperative contributions
        self.test_node.update_reputation("cooperative_growth", 10, "coop1")
        self.test_node.update_reputation("resource_sharing", 5, "coop2")
        
        metrics = self.blockchain.get_node_diversity_metrics()
        node_metric = metrics.get(self.test_node.node_id)
        
        self.assertIn("diversity_factor", node_metric)
        self.assertIn("num_cooperatives", node_metric)
        self.assertIn("total_contribution", node_metric)
        self.assertEqual(node_metric["num_cooperatives"], 2)

    def test_block_validation_reward(self):
        validator = self.blockchain.poc.select_validator()
        if validator:
            initial_reputation = validator.reputation_scores.get("transaction_validation", 0)
            
            block = Block(1, "prev_hash", time.time(), "test_data", "hash")
            self.blockchain.poc.reward_validator(validator, block)
            
            final_reputation = validator.reputation_scores.get("transaction_validation", 0)
            self.assertGreater(final_reputation, initial_reputation)

    def test_mana_management(self):
        initial_mana = self.blockchain.cooperative_mana
        
        # Test mana consumption
        contract_code = "def execute(input, state): return state"
        self.blockchain.deploy_contract("test_contract", contract_code, "test_creator")
        self.blockchain.execute_contract("test_contract", {})
        
        self.assertLess(self.blockchain.cooperative_mana, initial_mana)
        
        # Test mana regeneration
        self.blockchain.regenerate_mana()
        self.assertGreater(self.blockchain.cooperative_mana, 
                          self.blockchain.cooperative_mana - self.blockchain.mana_regen_rate)

    def test_governance(self):
        proposal = Proposal(
            id="proposal_1",
            title="Test Budget Proposal",
            description="Test Description",
            creator=self.test_dids[0].generate_did(),
            proposal_type="budget",
            amount=1000
        )
        proposal_id = self.governance.create_proposal(proposal)
        self.assertIsNotNone(proposal_id)

        vote_started = self.governance.start_voting(proposal_id)
        self.assertTrue(vote_started)

        self.governance.cast_vote(proposal_id, "Yes", self.test_dids[1].generate_did(), 1)
        self.governance.cast_vote(proposal_id, "Yes", self.test_dids[2].generate_did(), 1)

        initial_funds = self.governance.get_cooperative_funds()
        proposal_finalized = self.governance.finalize_proposal(proposal_id)
        self.assertTrue(proposal_finalized)
        self.assertEqual(self.governance.get_cooperative_funds(), initial_funds - 1000)

    def test_marketplace(self):
        seller_did = self.test_dids[0].generate_did()
        buyer_did = self.test_dids[1].generate_did()

        # Test listing creation
        listing_id = self.marketplace.create_listing("test_listing", seller_did, "Test Item", 100)
        self.assertIsNotNone(listing_id)

        # Test order placement and completion
        order_id = "test_order"
        order_placed = self.marketplace.place_order(order_id, buyer_did, listing_id)
        self.assertTrue(order_placed)

        completed_order = self.marketplace.complete_order(order_id)
        self.assertIsNotNone(completed_order)
        self.assertEqual(completed_order.status, "completed")

    def test_storage(self):
        test_data = b"This is a test file content."
        file_hash = self.storage.store_file("test_file.txt", test_data)
        self.assertIsNotNone(file_hash)

        # Test file retrieval and deletion
        retrieved_data = self.storage.retrieve_file(file_hash)
        self.assertEqual(retrieved_data, test_data)

        deleted = self.storage.delete_file(file_hash)
        self.assertTrue(deleted)
        self.assertIsNone(self.storage.retrieve_file(file_hash))

if __name__ == "__main__":
    unittest.main()