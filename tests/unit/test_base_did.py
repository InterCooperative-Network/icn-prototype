import unittest
import sys
import os

# Add the project root directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

# Import BaseDID from the 'did' module
from did.base_did import BaseDID

class TestBaseDID(unittest.TestCase):

    def setUp(self):
        """Set up a BaseDID instance before each test."""
        self.did = BaseDID()

    def test_did_generation(self):
        """Test DID generation logic."""
        did = self.did.generate_did()
        self.assertTrue(did.startswith("did:icn:"))
        self.assertEqual(len(did), 24)  # Expected length: 24

    def test_add_membership(self):
        """Test adding memberships to cooperatives and communities."""
        self.did.add_membership('cooperative', 'coop1')
        self.did.add_membership('community', 'comm1')

        self.assertIn('coop1', self.did.cooperative_memberships)
        self.assertIn('comm1', self.did.community_memberships)

    def test_list_memberships(self):
        """Test listing memberships by type or all."""
        self.did.add_membership('cooperative', 'coop1')
        self.did.add_membership('community', 'comm1')

        self.assertEqual(self.did.list_memberships('cooperative'), ['coop1'])
        self.assertEqual(self.did.list_memberships('community'), ['comm1'])
        self.assertEqual(self.did.list_memberships(), {'cooperative': ['coop1'], 'community': ['comm1']})

    def test_update_reputation(self):
        """Test updating reputation scores."""
        self.did.update_reputation('trustworthiness', 5.0, 'economic')
        self.assertEqual(self.did.reputation_scores['economic']['trustworthiness'], 5.0)

        self.did.update_reputation('participation', 3.0, 'civil')
        self.assertEqual(self.did.reputation_scores['civil']['participation'], 3.0)

    def test_get_total_reputation(self):
        """Test total reputation calculation."""
        self.did.update_reputation('trustworthiness', 5.0, 'economic')
        self.did.update_reputation('participation', 3.0, 'civil')

        self.assertEqual(self.did.get_total_reputation('economic'), 5.0)
        self.assertEqual(self.did.get_total_reputation('civil'), 3.0)

    def test_add_role(self):
        """Test adding roles with permissions."""
        self.did.add_role('admin', ['create', 'delete'], 'cooperative')
        self.assertIn('admin', self.did.roles['cooperative'])
        self.assertEqual(self.did.roles['cooperative']['admin'], ['create', 'delete'])

    def test_has_permission(self):
        """Test permission checking within roles."""
        self.did.add_role('admin', ['create', 'delete'], 'cooperative')

        self.assertTrue(self.did.has_permission('admin', 'create', 'cooperative'))
        self.assertFalse(self.did.has_permission('admin', 'update', 'cooperative'))

    def test_invalid_membership_type(self):
        """Test handling invalid membership types."""
        with self.assertRaises(ValueError) as context:
            self.did.add_membership('invalid_type', 'dao1')
        self.assertEqual(str(context.exception), "Invalid DAO type: invalid_type")

    def test_invalid_role_type(self):
        """Test handling invalid role types."""
        with self.assertRaises(ValueError) as context:
            self.did.add_role('admin', ['create'], 'invalid_type')
        self.assertEqual(str(context.exception), "Invalid DAO type: invalid_type")

if __name__ == '__main__':
    unittest.main()
