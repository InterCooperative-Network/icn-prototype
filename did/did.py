# did/did.py
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Optional
import logging
from datetime import datetime
import hashlib
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.primitives import serialization
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)

@dataclass
class Credential:
    """Verifiable credential for the ICN system."""
    issuer: str
    subject: str
    claims: Dict
    issued_at: datetime = field(default_factory=datetime.now)
    expires_at: Optional[datetime] = None
    proof: Optional[Dict] = None

    def verify(self) -> bool:
        """Verify the credential's cryptographic proof."""
        if not self.proof:
            return False
        # Implement cryptographic verification
        return True

    def revoke(self) -> None:
        """Revoke the credential."""
        self.expires_at = datetime.now()

class IdentityProvider(ABC):
    """Abstract base class for identity providers."""
    
    @abstractmethod
    def issue_credential(self, subject: str, claims: Dict) -> Credential:
        pass
    
    @abstractmethod
    def verify_credential(self, credential: Credential) -> bool:
        pass

class DID:
    """Decentralized Identifier implementation for ICN."""
    
    def __init__(self, cooperative_id: Optional[str] = None):
        self._private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048
        )
        self._public_key = self._private_key.public_key()
        self.cooperative_memberships: List[str] = []
        self.reputation_scores: Dict[str, float] = {}
        self.credentials: List[Credential] = []
        self.metadata: Dict = {}
        if cooperative_id:
            self.add_cooperative_membership(cooperative_id)
        
        # Create encryption key for sensitive data
        self._encryption_key = Fernet.generate_key()
        self._cipher_suite = Fernet(self._encryption_key)
        
    def generate_did(self) -> str:
        """Generate the DID string."""
        pub_bytes = self._public_key.public_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        )
        return f"did:icn:{hashlib.sha256(pub_bytes).hexdigest()[:16]}"

    def encrypt_data(self, data: str) -> bytes:
        """Encrypt data using the public key."""
        try:
            return self._public_key.encrypt(
                data.encode(),
                padding.OAEP(
                    mgf=padding.MGF1(algorithm=hashes.SHA256()),
                    algorithm=hashes.SHA256(),
                    label=None
                )
            )
        except Exception as e:
            logger.error(f"Encryption failed: {e}")
            raise

    def decrypt_data(self, encrypted_data: bytes) -> str:
        """Decrypt data using the private key."""
        try:
            decrypted = self._private_key.decrypt(
                encrypted_data,
                padding.OAEP(
                    mgf=padding.MGF1(algorithm=hashes.SHA256()),
                    algorithm=hashes.SHA256(),
                    label=None
                )
            )
            return decrypted.decode()
        except Exception as e:
            logger.error(f"Decryption failed: {e}")
            raise

    def add_cooperative_membership(self, cooperative_id: str) -> None:
        """Add membership to a cooperative."""
        if cooperative_id not in self.cooperative_memberships:
            self.cooperative_memberships.append(cooperative_id)
            logger.info(f"Added membership to cooperative: {cooperative_id}")

    def update_reputation(self, category: str, score: float, 
                         evidence: Optional[Dict] = None) -> None:
        """Update reputation score with optional evidence."""
        if score < 0:
            logger.warning(f"Negative reputation score update: {score}")
        
        old_score = self.reputation_scores.get(category, 0)
        self.reputation_scores[category] = old_score + score
        
        if evidence:
            if 'reputation_evidence' not in self.metadata:
                self.metadata['reputation_evidence'] = {}
            self.metadata['reputation_evidence'][category] = evidence

    def get_total_reputation(self) -> float:
        """Calculate total reputation across all categories."""
        return sum(self.reputation_scores.values())

    def export_public_credentials(self) -> Dict:
        """Export public credentials and memberships."""
        return {
            'did': self.generate_did(),
            'cooperative_memberships': self.cooperative_memberships,
            'reputation_scores': self.reputation_scores,
            'public_credentials': [
                {k: v for k, v in c.__dict__.items() if k != 'proof'}
                for c in self.credentials
            ]
        }

class DIDRegistry:
    """Registry for DIDs in the ICN system."""
    
    def __init__(self):
        self.dids: Dict[str, DID] = {}
        self.revoked_dids: Dict[str, datetime] = {}
        self._identity_providers: Dict[str, IdentityProvider] = {}
        
    def register_did(self, did: DID) -> str:
        """Register a new DID."""
        did_id = did.generate_did()
        if did_id in self.revoked_dids:
            raise ValueError(f"DID {did_id} has been revoked")
        self.dids[did_id] = did
        logger.info(f"Registered new DID: {did_id}")
        return did_id

    def resolve_did(self, did_id: str) -> Optional[DID]:
        """Resolve a DID to its full object."""
        if did_id in self.revoked_dids:
            logger.warning(f"Attempted to resolve revoked DID: {did_id}")
            return None
        return self.dids.get(did_id)

    def verify_did(self, did_id: str) -> bool:
        """Verify a DID's validity."""
        if did_id in self.revoked_dids:
            return False
        return did_id in self.dids

    def revoke_did(self, did_id: str, reason: str) -> None:
        """Revoke a DID."""
        if did_id in self.dids:
            self.revoked_dids[did_id] = datetime.now()
            del self.dids[did_id]
            logger.warning(f"DID revoked: {did_id}, reason: {reason}")

    def register_identity_provider(self, name: str, 
                                 provider: IdentityProvider) -> None:
        """Register a new identity provider."""
        self._identity_providers[name] = provider
        logger.info(f"Registered identity provider: {name}")

    def get_identity_provider(self, name: str) -> Optional[IdentityProvider]:
        """Get an identity provider by name."""
        return self._identity_providers.get(name)