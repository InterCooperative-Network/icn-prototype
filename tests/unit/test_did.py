import unittest
from datetime import datetime, timedelta
from typing import Dict

# Import the classes from did.py
from did.did import DID, Credential, IdentityProvider


class TestCredential(unittest.TestCase):

    def setUp(self):
        """Set up a Credential instance for testing."""
        self.credential = Credential(
            issuer="did:icn:issuer",
            subject="did:icn:subject",
            claims={"role": "admin", "access": "full"},
            expires_at=datetime.now() + timedelta(days=365)
        )

    def test_credential_initialization(self):
        """Test credential initialization and attributes."""
        self.assertEqual(self.credential.issuer, "did:icn:issuer")
        self.assertEqual(self.credential.subject, "did:icn:subject")
        self.assertEqual(self.credential.claims["role"], "admin")
        self.assertFalse(self.credential.is_expired())

    def test_verify_without_proof(self):
        """Test verifying a credential without proof."""
        self.assertFalse(self.credential.verify())

    def test_revoke_credential(self):
        """Test revoking a credential."""
        self.credential.revoke()
        self.assertTrue(self.credential.is_expired())

    def test_credential_expiration(self):
        """Test checking if a credential is expired."""
        self.credential.expires_at = datetime.now() - timedelta(days=1)
        self.assertTrue(self.credential.is_expired())

    def test_non_expired_credential(self):
        """Test checking if a credential is not expired."""
        self.credential.expires_at = datetime.now() + timedelta(days=1)
        self.assertFalse(self.credential.is_expired())


class TestDID(unittest.TestCase):

    def setUp(self):
        """Set up a DID instance for testing."""
        self.did = DID()

    def test_generate_did(self):
        """Test DID generation logic."""
        did_str = self.did.generate_did()
        self.assertTrue(did_str.startswith("did:icn:"))
        self.assertEqual(len(did_str), 24)  # Adjusted expected length

    def test_encrypt_and_decrypt_data(self):
        """Test encryption and decryption of data."""
        data = "Sensitive data"
        encrypted_data = self.did.encrypt_data(data)
        decrypted_data = self.did.decrypt_data(encrypted_data)
        self.assertEqual(decrypted_data, data)

    def test_encrypt_data_failure(self):
        """Test failure during encryption with invalid input."""
        with self.assertRaises(TypeError):
            self.did.encrypt_data(None)

    def test_decrypt_data_failure(self):
        """Test failure during decryption with invalid input."""
        with self.assertRaises(ValueError):
            self.did.decrypt_data(b"invalid_data")

    def test_add_cooperative_membership(self):
        """Test adding cooperative membership."""
        self.did.add_cooperative_membership("coop1")
        self.assertIn("coop1", self.did.cooperative_memberships)

    def test_add_duplicate_cooperative_membership(self):
        """Test adding duplicate cooperative membership."""
        self.did.add_cooperative_membership("coop1")
        self.did.add_cooperative_membership("coop1")
        self.assertEqual(len(self.did.cooperative_memberships), 1)

    def test_update_reputation(self):
        """Test updating reputation scores."""
        self.did.update_reputation("trustworthiness", 5.0)
        self.assertEqual(self.did.reputation_scores["trustworthiness"], 5.0)

    def test_update_reputation_with_evidence(self):
        """Test updating reputation with evidence."""
        evidence = {"details": "Participated in community event"}
        self.did.update_reputation("participation", 3.0, evidence=evidence)
        self.assertEqual(self.did.reputation_scores["participation"], 3.0)
        self.assertIn("reputation_evidence", self.did.metadata)

    def test_get_total_reputation(self):
        """Test calculating total reputation."""
        self.did.update_reputation("trustworthiness", 5.0)
        self.did.update_reputation("participation", 3.0)
        total_reputation = self.did.get_total_reputation()
        self.assertEqual(total_reputation, 8.0)

    def test_export_public_credentials(self):
        """Test exporting public credentials."""
        self.did.add_cooperative_membership("coop1")
        public_data = self.did.export_public_credentials()
        self.assertIn("did", public_data)
        self.assertIn("cooperative_memberships", public_data)
        self.assertIn("reputation_scores", public_data)


class MockIdentityProvider(IdentityProvider):
    """Mock implementation of the IdentityProvider abstract class for testing."""

    def issue_credential(self, subject: str, claims: Dict) -> Credential:
        """Issue a new credential."""
        return Credential(
            issuer="did:icn:issuer",
            subject=subject,
            claims=claims,
            proof={"signature": "mock_signature"},  # Added mock proof
            expires_at=datetime.now() + timedelta(days=365)
        )

    def verify_credential(self, credential: Credential) -> bool:
        """Verify a given credential."""
        # Mock verification logic
        return credential.proof is not None and "signature" in credential.proof


class TestIdentityProviderImplementation(unittest.TestCase):

    def setUp(self):
        """Set up an IdentityProvider instance for testing."""
        self.provider = MockIdentityProvider()

    def test_issue_credential(self):
        """Test issuing a credential."""
        credential = self.provider.issue_credential(
            subject="did:icn:subject",
            claims={"role": "admin", "access": "full"}
        )
        self.assertEqual(credential.subject, "did:icn:subject")
        self.assertEqual(credential.claims["role"], "admin")
        self.assertFalse(credential.is_expired())

    def test_verify_credential(self):
        """Test verifying a credential."""
        credential = self.provider.issue_credential(
            subject="did:icn:subject",
            claims={"role": "admin", "access": "full"}
        )
        self.assertTrue(self.provider.verify_credential(credential))


if __name__ == '__main__':
    unittest.main()
