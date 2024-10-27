# credential.py

from dataclasses import dataclass, field
from typing import Dict, Optional
from datetime import datetime, timedelta

@dataclass
class Credential:
    """
    Verifiable credential for the ICN system, with support for cooperative and community DAOs.
    
    Attributes:
    - issuer: The DID that issued the credential.
    - subject: The DID that holds the credential.
    - claims: Claims related to the credential (economic or civil).
    - dao_type: Type of DAO ('cooperative' or 'community').
    - issued_at: Timestamp when the credential was issued.
    - expires_at: Optional expiration timestamp for the credential.
    - proof: Cryptographic proof of the credential's validity.
    """
    issuer: str
    subject: str
    claims: Dict
    dao_type: str
    issued_at: datetime = field(default_factory=datetime.now)
    expires_at: Optional[datetime] = None
    proof: Optional[Dict] = None

    def verify(self) -> bool:
        """
        Verify the cryptographic proof of the credential.
        
        Returns:
        - bool: True if the credential is valid, False otherwise.
        """
        if not self.proof:
            return False
        # Implement cryptographic verification logic here
        return True

    def revoke(self) -> None:
        """
        Revoke the credential by setting its expiration date to the current time.
        """
        self.expires_at = datetime.now()

    def is_expired(self) -> bool:
        """
        Check if the credential is expired.
        
        Returns:
        - bool: True if the credential is expired, False otherwise.
        """
        return self.expires_at and datetime.now() > self.expires_at

    def selective_disclosure(self, fields: List[str]) -> Dict:
        """
        Selectively disclose specific fields of the credential.
        
        Args:
        - fields: List of fields to disclose.

        Returns:
        - Dict: Disclosed fields of the credential.
        """
        return {field: self.claims[field] for field in fields if field in self.claims}

class CredentialTemplate:
    """
    Credential template for creating standard credentials within cooperatives and communities.
    
    Attributes:
    - template_name: Name of the credential template.
    - claims: Predefined claims for the credential.
    - dao_type: Type of DAO for the credential ('cooperative' or 'community').
    """
    def __init__(self, template_name: str, claims: Dict, dao_type: str):
        self.template_name = template_name
        self.claims = claims
        self.dao_type = dao_type

    def apply_template(self, subject_did: str, issuer_did: str) -> Credential:
        """
        Create a credential based on the template.
        
        Args:
        - subject_did: DID of the subject receiving the credential.
        - issuer_did: DID of the issuer.
        
        Returns:
        - Credential: The newly created credential.
        """
        return Credential(
            issuer=issuer_did,
            subject=subject_did,
            claims=self.claims,
            dao_type=self.dao_type,
            expires_at=datetime.now() + timedelta(days=365)
        )
