# ==================== /home/matt/icn-prototype/tests/test_governance.py ====================

import unittest
from blockchain.blockchain import Blockchain
from system.governance import Governance, Proposal

class TestGovernance(unittest.TestCase):

    def setUp(self):
        """
        Set up a blockchain and governance instance for testing.
        """
        self.blockchain = Blockchain(num_shards=1)
        self.governance = Governance(self.blockchain, voting_power_cap=10)

    def test_create_proposal(self):
        """
        Test creating a proposal with the new voting options.
        """
        proposal = Proposal(
            id="prop1",
            title="Test Proposal",
            description="This is a test proposal.",
            creator="creator1",
            proposal_type="amendment"
        )
        proposal_id = self.governance.create_proposal(proposal, creator_reputation=5)
        self.assertEqual(proposal_id, "prop1")
        self.assertIn("prop1", self.governance.proposals)
        self.assertEqual(self.governance.proposals["prop1"].options, ["approve", "reject", "abstain"])

    def test_start_voting(self):
        """
        Test starting the voting process for a proposal.
        """
        proposal = Proposal(
            id="prop2",
            title="Budget Proposal",
            description="A proposal to allocate budget.",
            creator="creator2",
            proposal_type="budget",
            amount=1000
        )
        self.governance.create_proposal(proposal, creator_reputation=10)
        started = self.governance.start_voting("prop2")
        self.assertTrue(started)
        self.assertIn("prop2", self.governance.votes)
        self.assertEqual(self.governance.votes["prop2"], {"approve": 0, "reject": 0, "abstain": 0})

    def test_cast_vote_approve(self):
        """
        Test casting an 'approve' vote for a proposal.
        """
        proposal = Proposal(
            id="prop3",
            title="Amendment Proposal",
            description="Amend bylaws to include a new rule.",
            creator="creator3",
            proposal_type="amendment"
        )
        self.governance.create_proposal(proposal, creator_reputation=7)
        self.governance.start_voting("prop3")
        vote_cast = self.governance.cast_vote("prop3", "approve", "voter1", voting_power=9)
        self.assertTrue(vote_cast)
        self.assertAlmostEqual(self.governance.votes["prop3"]["approve"], 3.0, places=1)  # sqrt(9) = 3

    def test_cast_vote_reject(self):
        """
        Test casting a 'reject' vote for a proposal.
        """
        proposal = Proposal(
            id="prop4",
            title="Reject Proposal Test",
            description="Testing reject votes.",
            creator="creator4",
            proposal_type="amendment"
        )
        self.governance.create_proposal(proposal, creator_reputation=8)
        self.governance.start_voting("prop4")
        vote_cast = self.governance.cast_vote("prop4", "reject", "voter2", voting_power=4)
        self.assertTrue(vote_cast)
        self.assertAlmostEqual(self.governance.votes["prop4"]["reject"], 2.0, places=1)  # sqrt(4) = 2

    def test_cast_vote_abstain(self):
        """
        Test casting an 'abstain' vote for a proposal.
        """
        proposal = Proposal(
            id="prop5",
            title="Abstain Proposal Test",
            description="Testing abstain votes.",
            creator="creator5",
            proposal_type="amendment"
        )
        self.governance.create_proposal(proposal, creator_reputation=9)
        self.governance.start_voting("prop5")
        vote_cast = self.governance.cast_vote("prop5", "abstain", "voter3", voting_power=16)
        self.assertTrue(vote_cast)
        # Expect ceiling-based integer square root calculation
        self.assertEqual(self.governance.votes["prop5"]["abstain"], 4)




    def test_finalize_approve(self):
        """
        Test finalizing a proposal that should be approved.
        """
        proposal = Proposal(
            id="prop6",
            title="Approve Finalization Test",
            description="Testing approval finalization.",
            creator="creator6",
            proposal_type="amendment"
        )
        self.governance.create_proposal(proposal, creator_reputation=10)
        self.governance.start_voting("prop6")
        self.governance.cast_vote("prop6", "approve", "voter1", voting_power=9)
        self.governance.cast_vote("prop6", "reject", "voter2", voting_power=4)
        self.governance.cast_vote("prop6", "abstain", "voter3", voting_power=16)
        finalized = self.governance.finalize_proposal("prop6")
        self.assertTrue(finalized)
        self.assertEqual(self.governance.proposals["prop6"].status, "approved")

    def test_finalize_reject(self):
        """
        Test finalizing a proposal that should be rejected.
        """
        proposal = Proposal(
            id="prop7",
            title="Reject Finalization Test",
            description="Testing rejection finalization.",
            creator="creator7",
            proposal_type="amendment"
        )
        self.governance.create_proposal(proposal, creator_reputation=10)
        self.governance.start_voting("prop7")
        self.governance.cast_vote("prop7", "approve", "voter1", voting_power=4)
        self.governance.cast_vote("prop7", "reject", "voter2", voting_power=9)
        self.governance.cast_vote("prop7", "abstain", "voter3", voting_power=16)
        finalized = self.governance.finalize_proposal("prop7")
        self.assertFalse(finalized)
        self.assertEqual(self.governance.proposals["prop7"].status, "rejected")

    def test_abstain_does_not_affect_outcome(self):
        """
        Test that abstain votes do not affect the approval or rejection of a proposal.
        """
        proposal = Proposal(
            id="prop8",
            title="Abstain Outcome Test",
            description="Testing that abstains do not impact outcome.",
            creator="creator8",
            proposal_type="amendment"
        )
        self.governance.create_proposal(proposal, creator_reputation=10)
        self.governance.start_voting("prop8")
        self.governance.cast_vote("prop8", "approve", "voter1", voting_power=9)
        self.governance.cast_vote("prop8", "abstain", "voter2", voting_power=16)
        finalized = self.governance.finalize_proposal("prop8")
        self.assertTrue(finalized)
        self.assertEqual(self.governance.proposals["prop8"].status, "approved")

if __name__ == "__main__":
    unittest.main()
