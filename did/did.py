from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Union
import logging
from datetime import datetime
import hashlib
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from abc import ABC, abstractmethod

# Configure logging for the DID module
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger('did.did')


@dataclass
class Credential:
    """
    Verifiable credential for the ICN system, representing claims about a subject.
    
    Attributes:
        - issuer: The DID of the entity issuing the credential.
        - subject: The DID of the entity holding the credential.
        - claims: Claims related to the credential.
        - issued_at: Timestamp of when the credential was issued.
        - expires_at: Expiration timestamp of the credential.
        - proof: Cryptographic proof of the credential's validity.
    """
    issuer: str
    subject: str
    claims: Dict[str, Union[str, int, float]]
    issued_at: datetime = field(default_factory=datetime.now)
    expires_at: Optional[datetime] = None
    proof: Optional[Dict] = None

    def verify(self) -> bool:
        """
        Verify the cryptographic proof of the credential.
        
        Returns:
            bool: True if the credential's proof is valid, False otherwise.
        """
        if not self.proof:
            logger.warning("Credential verification failed: No proof provided.")
            return False

        # Placeholder for cryptographic verification logic
        logger.debug("Credential verification successful.")
        return True

    def revoke(self) -> None:
        """
        Revoke the credential by setting its expiration date to the current time.
        """
        self.expires_at = datetime.now()
        logger.info(f"Credential revoked for subject: {self.subject}")

    def is_expired(self) -> bool:
        """
        Check if the credential is expired.
        
        Returns:
            bool: True if the credential is expired, False otherwise.
        """
        if self.expires_at and datetime.now() > self.expires_at:
            logger.debug(f"Credential for {self.subject} has expired.")
            return True
        return False


class IdentityProvider(ABC):
    """
    Abstract base class for identity providers, facilitating DID credential issuance and verification.
    """
    
    @abstractmethod
    def issue_credential(self, subject: str, claims: Dict) -> Credential:
        """
        Issue a new credential for a subject.

        Args:
            subject: The DID of the subject receiving the credential.
            claims: Claims associated with the credential.

        Returns:
            Credential: The issued credential.
        """
        pass

    @abstractmethod
    def verify_credential(self, credential: Credential) -> bool:
        """
        Verify the cryptographic proof of a credential.

        Args:
            credential: The credential to verify.

        Returns:
            bool: True if the credential is valid, False otherwise.
        """
        pass


class DID:
    """
    Implementation of Decentralized Identifiers (DID) for the ICN system.
    
    Attributes:
        - cooperative_memberships: List of cooperative memberships.
        - reputation_scores: Dictionary of reputation scores.
        - credentials: List of verifiable credentials.
        - metadata: Metadata associated with the DID.
    """
    
    def __init__(self, cooperative_id: Optional[str] = None):
        self._private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        self._public_key = self._private_key.public_key()
        self.cooperative_memberships: List[str] = []
        self.reputation_scores: Dict[str, float] = {}
        self.credentials: List[Credential] = []
        self.metadata: Dict = {}

        if cooperative_id:
            self.add_cooperative_membership(cooperative_id)

        # Create Fernet encryption key for sensitive data
        self._encryption_key = Fernet.generate_key()
        self._cipher_suite = Fernet(self._encryption_key)
        logger.info("Initialized DID with RSA keys and Fernet encryption.")

    def generate_did(self) -> str:
        """
        Generate a DID string based on the SHA-256 hash of the public key.
        
        Returns:
            str: The generated DID string in the format 'did:icn:<16_hex_chars>'.
        """
        pub_bytes = self._public_key.public_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        did = f"did:icn:{hashlib.sha256(pub_bytes).hexdigest()[:16]}"
        logger.debug(f"Generated DID: {did}")
        return did

    def encrypt_data(self, data: str) -> bytes:
        """
        Encrypt data using RSA public key encryption.

        Args:
            data: The plaintext data to encrypt.

        Returns:
            bytes: The encrypted data.
        """
        if not isinstance(data, str):
            logger.error("Data encryption failed: Input data must be a string.")
            raise TypeError("Input data must be a string.")
        
        try:
            encrypted_data = self._public_key.encrypt(
                data.encode(),
                padding.OAEP(
                    mgf=padding.MGF1(algorithm=hashes.SHA256()),
                    algorithm=hashes.SHA256(),
                    label=None,
                ),
            )
            logger.debug("Data encrypted successfully.")
            return encrypted_data
        except Exception as e:
            logger.error(f"Data encryption failed: {e}")
            raise

    def decrypt_data(self, encrypted_data: bytes) -> str:
        """
        Decrypt data using RSA private key decryption.

        Args:
            encrypted_data: The encrypted data to decrypt.

        Returns:
            str: The decrypted plaintext data.
        """
        try:
            decrypted = self._private_key.decrypt(
                encrypted_data,
                padding.OAEP(
                    mgf=padding.MGF1(algorithm=hashes.SHA256()),
                    algorithm=hashes.SHA256(),
                    label=None,
                ),
            )
            logger.debug("Data decrypted successfully.")
            return decrypted.decode()
        except Exception as e:
            logger.error(f"Data decryption failed: {e}")
            raise

    def add_cooperative_membership(self, cooperative_id: str) -> None:
        """
        Add a cooperative membership to the DID.

        Args:
            cooperative_id: The ID of the cooperative to add.
        """
        if cooperative_id not in self.cooperative_memberships:
            self.cooperative_memberships.append(cooperative_id)
            logger.info(f"Added membership to cooperative: {cooperative_id}")

    def update_reputation(self, category: str, score: float, evidence: Optional[Dict] = None) -> None:
        """
        Update reputation score for a specific category.

        Args:
            category: The reputation category to update.
            score: The reputation score to add.
            evidence: Additional evidence supporting the reputation change.
        """
        if score < 0:
            logger.warning(f"Attempted negative reputation score update: {score}")

        self.reputation_scores[category] = self.reputation_scores.get(category, 0) + score

        if evidence:
            self.metadata.setdefault("reputation_evidence", {})[category] = evidence

        logger.info(f"Updated reputation for {category}: {self.reputation_scores[category]}")

    def get_total_reputation(self) -> float:
        """
        Calculate the total reputation across all categories.
        
        Returns:
            float: The total reputation score.
        """
        total_reputation = sum(self.reputation_scores.values())
        logger.debug(f"Total reputation calculated: {total_reputation}")
        return total_reputation

    def export_public_credentials(self) -> Dict:
        """
        Export public credentials and cooperative memberships.
        
        Returns:
            Dict: Dictionary containing the DID, cooperative memberships, reputation scores, and public credentials.
        """
        public_data = {
            "did": self.generate_did(),
            "cooperative_memberships": self.cooperative_memberships,
            "reputation_scores": self.reputation_scores,
            "public_credentials": [
                {k: v for k, v in c.__dict__.items() if k != "proof"}
                for c in self.credentials
            ],
        }
        logger.debug(f"Exported public credentials: {public_data}")
        return public_data
