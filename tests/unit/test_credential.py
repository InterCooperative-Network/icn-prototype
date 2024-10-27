import unittest
from datetime import datetime, timedelta
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization

# Import the Credential and CredentialTemplate classes
from did.credential import Credential, CredentialTemplate

class TestCredential(unittest.TestCase):

    def setUp(self):
        """Set up test environment, including RSA key generation."""
        self.private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        self.public_key = self.private_key.public_key()
        
        self.issuer_did = "did:icn:issuer"
        self.subject_did = "did:icn:subject"
        self.claims = {"trustworthiness": 5, "participation": 8}
        self.dao_type = "cooperative"
        
        self.credential = Credential(
            issuer=self.issuer_did,
            subject=self.subject_did,
            claims=self.claims,
            dao_type=self.dao_type,
            expires_at=datetime.now() + timedelta(days=365)
        )

    def test_generate_proof(self):
        """Test generating cryptographic proof for a credential."""
        self.credential.generate_proof(self.private_key)
        self.assertIsNotNone(self.credential.proof)
        self.assertIn('signature', self.credential.proof)
        self.assertIn('data_hash', self.credential.proof)

    def test_verify_valid_credential(self):
        """Test verifying a valid credential."""
        self.credential.generate_proof(self.private_key)
        result = self.credential.verify(self.public_key)
        self.assertTrue(result)

    def test_verify_invalid_signature(self):
        """Test verification with an invalid signature."""
        # Tamper with the proof to simulate an invalid signature
        self.credential.generate_proof(self.private_key)
        self.credential.proof['signature'] = '00' * 128  # Invalid signature
        result = self.credential.verify(self.public_key)
        self.assertFalse(result)

    def test_revoke_credential(self):
        """Test revoking a credential."""
        self.credential.revoke()
        self.assertTrue(self.credential.is_expired())

    def test_expired_credential(self):
        """Test checking for expired credentials."""
        self.credential.expires_at = datetime.now() - timedelta(days=1)
        self.assertTrue(self.credential.is_expired())

    def test_non_expired_credential(self):
        """Test checking for non-expired credentials."""
        self.assertFalse(self.credential.is_expired())

    def test_selective_disclosure(self):
        """Test selective disclosure of credential claims."""
        disclosed_claims = self.credential.selective_disclosure(['trustworthiness'])
        self.assertEqual(disclosed_claims, {'trustworthiness': 5})
        self.assertNotIn('participation', disclosed_claims)

    def test_selective_disclosure_no_fields(self):
        """Test selective disclosure when no fields match."""
        disclosed_claims = self.credential.selective_disclosure(['nonexistent_field'])
        self.assertEqual(disclosed_claims, {})

class TestCredentialTemplate(unittest.TestCase):

    def setUp(self):
        """Set up test environment, including RSA key generation."""
        self.private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        self.issuer_did = "did:icn:issuer"
        self.subject_did = "did:icn:subject"
        self.claims = {"efficiency": 7, "collaboration": 9}
        self.dao_type = "community"
        
        self.template = CredentialTemplate(
            template_name="Community Engagement",
            claims=self.claims,
            dao_type=self.dao_type
        )

    def test_apply_template(self):
        """Test applying a template to create a credential."""
        credential = self.template.apply_template(self.subject_did, self.issuer_did, self.private_key)
        self.assertEqual(credential.issuer, self.issuer_did)
        self.assertEqual(credential.subject, self.subject_did)
        self.assertEqual(credential.claims, self.claims)
        self.assertEqual(credential.dao_type, self.dao_type)
        self.assertIsNotNone(credential.proof)

    def test_apply_template_and_verify(self):
        """Test creating a credential from a template and verifying it."""
        credential = self.template.apply_template(self.subject_did, self.issuer_did, self.private_key)
        result = credential.verify(self.private_key.public_key())
        self.assertTrue(result)

if __name__ == '__main__':
    unittest.main()
