# tests/test_all.py

import unittest
from datetime import datetime, timedelta
import time
from typing import List

# Import all components
from blockchain.blockchain import (
    Blockchain, Block, Transaction, Node, 
    ProofOfCooperation, SmartContract
)
from did.did import DID, DIDRegistry, Credential
from system.governance import Governance, Proposal
from system.marketplace import Marketplace
from system.reputation import ReputationSystem
from system.storage import DistributedStorage

class TestICNIntegration(unittest.TestCase):
    """Integration tests for the entire ICN system."""

    def setUp(self):
        """Set up test environment."""
        # Initialize core components
        self.blockchain = Blockchain()
        self.did_registry = DIDRegistry()
        self.reputation_system = ReputationSystem()
        
        # Initialize governance and marketplace
        self.governance = Governance(self.blockchain, self.reputation_system)
        self.marketplace = Marketplace(self.blockchain, self.did_registry)
        
        # Create test nodes
        self.nodes = self._create_test_nodes()
        for node in self.nodes:
            self.blockchain.add_node(node)
        
        # Create test DIDs
        self.dids = self._create_test_dids()
        
        # Initialize storage
        self.storage = DistributedStorage(self.blockchain)

    def _create_test_nodes(self, num_nodes: int = 5) -> List[Node]:
        """Create test nodes with various configurations."""
        nodes = []
        for i in range(num_nodes):
            node = Node(
                node_id=f"node_{i}",
                cooperative_id=f"coop_{i % 2}"  # Distribute among two coops
            )
            # Add some initial reputation
            node.reputation_scores = {
                'validation': 10.0 + i,
                'proposal_creation': 5.0 + i,
                'voting': 7.0 + i,
                'resource_sharing': 6.0 + i,
                'cooperative_growth': 8.0 + i,
                'community_building': 9.0 + i,
                'conflict_resolution': 4.0 + i
            }
            nodes.append(node)
        return nodes

    def _create_test_dids(self, num_dids: int = 5) -> List[DID]:
        """Create test DIDs and register them."""
        dids = []
        for i in range(num_dids):
            did = DID()
            self.did_registry.register_did(did)
            did.update_reputation('validation', 10.0)
            dids.append(did)
        return dids

    def test_1_basic_blockchain_operations(self):
        """Test basic blockchain operations."""
        # Create and add transaction
        transaction = Transaction(
            sender=self.dids[0].generate_did(),
            receiver=self.dids[1].generate_did(),
            action="test_transfer",
            data={"amount": 100}
        )
        
        # Add transaction and create block
        self.assertTrue(self.blockchain.add_transaction(transaction))
        
        # Create new block
        block = self.blockchain.create_block()
        self.assertIsNotNone(block)
        
        # Add block to chain
        self.assertTrue(self.blockchain.add_block(block))
        self.assertEqual(self.blockchain.current_height, 2)  # Including genesis

    def test_2_consensus_mechanism(self):
        """Test Proof of Cooperation consensus."""
        # Test validator selection
        validator = self.blockchain.consensus.select_validator(self.nodes)
        self.assertIsNotNone(validator)
        
        # Verify validator cooldown
        self.assertGreater(validator.cooldown, 0)
        
        # Test cooperation score calculation
        score = self.blockchain.consensus.calculate_cooperation_score(validator)
        self.assertGreater(score, 0)

    def test_3_smart_contracts(self):
        """Test smart contract deployment and execution."""
        # Simple counter contract
        contract_code = """
def execute(input_data, state):
    if 'counter' not in state:
        state['counter'] = 0
    state['counter'] += 1
    return {'counter': state['counter']}
"""
        
        # Deploy contract
        self.assertTrue(self.blockchain.deploy_contract(
            "test_contract",
            contract_code,
            self.dids[0].generate_did()
        ))
        
        # Execute contract
        result = self.blockchain.execute_contract("test_contract", {})
        self.assertIn("result", result)
        self.assertEqual(result["result"]["counter"], 1)

    def test_4_governance_operations(self):
        """Test governance operations."""
        # Create proposal
        proposal = Proposal(
            id="test_proposal",
            title="Test Proposal",
            description="Test Description",
            creator=self.dids[0].generate_did(),
            proposal_type="standard",
            options=["approve", "reject"]
        )
        
        # Submit proposal
        self.assertTrue(self.governance.create_proposal(proposal))
        
        # Start voting
        self.assertTrue(self.governance.start_voting(proposal.id))
        
        # Cast votes
        for i, did in enumerate(self.dids[:3]):
            choice = "approve" if i < 2 else "reject"
            self.assertTrue(self.governance.cast_vote(
                proposal.id,
                did.generate_did(),
                choice
            ))
        
        # Fast forward time
        proposal.end_time = datetime.now() - timedelta(minutes=1)
        
        # Finalize proposal
        self.assertTrue(self.governance.finalize_proposal(proposal.id))
        self.assertEqual(self.governance.proposals[proposal.id].status, "approved")

    def test_5_marketplace_operations(self):
        """Test marketplace operations."""
        # Create listing
        listing_id = self.marketplace.create_listing(
            "test_listing",
            self.dids[0].generate_did(),
            "Test Item",
            100
        )
        self.assertIsNotNone(listing_id)
        
        # Place order
        order_id = "test_order"
        self.assertTrue(self.marketplace.place_order(
            order_id,
            self.dids[1].generate_did(),
            listing_id
        ))
        
        # Complete order
        completed_order = self.marketplace.complete_order(order_id)
        self.assertIsNotNone(completed_order)
        self.assertEqual(completed_order.status, "completed")

    def test_6_reputation_system(self):
        """Test reputation system."""
        did_id = self.dids[0].generate_did()
        
        # Update reputation
        self.assertTrue(self.reputation_system.update_reputation(
            did_id,
            10.0,
            "validation",
            {"block_hash": "test_hash"}
        ))
        
        # Check reputation
        reputation = self.reputation_system.get_reputation(did_id)
        self.assertGreater(reputation["validation"], 0)
        
        # Test decay
        self.reputation_system.apply_decay()
        new_reputation = self.reputation_system.get_reputation(did_id)
        self.assertLess(new_reputation["validation"], reputation["validation"])

    def test_7_storage_operations(self):
        """Test storage operations."""
        test_data = b"Test file content"
        
        # Store file
        file_hash = self.storage.store_file("test.txt", test_data)
        self.assertIsNotNone(file_hash)
        
        # Retrieve file
        retrieved_data = self.storage.retrieve_file(file_hash)
        self.assertEqual(retrieved_data, test_data)
        
        # Delete file
        self.assertTrue(self.storage.delete_file(file_hash))
        self.assertIsNone(self.storage.retrieve_file(file_hash))

    def test_8_system_integration(self):
        """Test full system integration."""
        # Create and process a complex transaction
        transaction = Transaction(
            sender=self.dids[0].generate_did(),
            receiver="system",
            action="cooperative_action",
            data={
                "type": "create_cooperative",
                "name": "Test Cooperative",
                "members": [did.generate_did() for did in self.dids[:3]]
            }
        )
        
        self.assertTrue(self.blockchain.add_transaction(transaction))
        
        # Create and add block
        block = self.blockchain.create_block()
        self.assertTrue(self.blockchain.add_block(block))
        
        # Verify chain state
        metrics = self.blockchain.get_metrics()
        self.assertIn('chain_metrics', metrics)
        self.assertIn('node_metrics', metrics)
        self.assertIn('resource_metrics', metrics)

    def test_9_error_handling(self):
        """Test error handling and edge cases."""
        # Test invalid transaction
        invalid_transaction = Transaction(
            sender="invalid_did",
            receiver="system",
            action="invalid_action",
            data={}
        )
        self.assertFalse(self.blockchain.add_transaction(invalid_transaction))
        
        # Test invalid block
        invalid_block = Block(
            index=999,
            previous_hash="invalid",
            timestamp=datetime.now(),
            transactions=[],
            validator="invalid"
        )
        self.assertFalse(self.blockchain.add_block(invalid_block))
        
        # Test invalid smart contract
        invalid_contract = """
def invalid_code():
    import os  # Should be blocked
    return os.system('ls')
"""
        self.assertFalse(self.blockchain.deploy_contract(
            "invalid_contract",
            invalid_contract,
            self.dids[0].generate_did()
        ))

if __name__ == '__main__':
    unittest.main(verbosity=2)