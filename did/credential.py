from dataclasses import dataclass, field
from typing import Dict, Optional, List, Union
from datetime import datetime, timedelta
import hashlib
import logging
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from cryptography.hazmat.primitives import hashes, serialization

# Configure logging for the credential module
logger = logging.getLogger('did.credential')
logger.setLevel(logging.DEBUG)

@dataclass
class Credential:
    """
    Verifiable credential for the ICN system.

    This credential represents a piece of verifiable data about a subject, issued by a cooperative or community DAO.
    It supports privacy-preserving selective disclosure, cryptographic verification, and expiration management.

    Attributes:
        - issuer (str): The DID of the entity issuing the credential.
        - subject (str): The DID of the entity holding the credential.
        - claims (Dict): Claims related to the credential (e.g., economic or civil claims).
        - dao_type (str): The type of DAO ('cooperative' or 'community').
        - issued_at (datetime): Timestamp of when the credential was issued.
        - expires_at (Optional[datetime]): Expiration timestamp of the credential.
        - proof (Optional[Dict]): Cryptographic proof of the credential's validity.
    """
    issuer: str
    subject: str
    claims: Dict[str, Union[str, int, float]]
    dao_type: str
    issued_at: datetime = field(default_factory=datetime.now)
    expires_at: Optional[datetime] = None
    proof: Optional[Dict] = None

    def verify(self, public_key: rsa.RSAPublicKey) -> bool:
        """
        Verify the cryptographic proof of the credential using the issuer's public key.

        Args:
            public_key (rsa.RSAPublicKey): The RSA public key of the issuer.

        Returns:
            bool: True if the credential's proof is valid, False otherwise.
        """
        if not self.proof or 'signature' not in self.proof or 'data_hash' not in self.proof:
            logger.warning("Credential verification failed: Incomplete proof data.")
            return False

        try:
            # Recalculate the hash of the claims data
            claims_data = self._serialize_claims()
            data_hash = hashlib.sha256(claims_data.encode('utf-8')).hexdigest()

            # Check if the hash matches the provided data hash in the proof
            if data_hash != self.proof['data_hash']:
                logger.warning("Data hash mismatch during credential verification.")
                return False

            # Verify the signature using RSA-PSS
            public_key.verify(
                bytes.fromhex(self.proof['signature']),
                data_hash.encode('utf-8'),
                padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.MAX_LENGTH),
                hashes.SHA256()
            )
            logger.info("Credential verification successful.")
            return True
        except Exception as e:
            logger.error(f"Error during credential verification: {e}")
            return False

    def _serialize_claims(self) -> str:
        """
        Serialize the claims dictionary for hashing and signature.

        Returns:
            str: Serialized string representation of the claims.
        """
        return "|".join(f"{k}:{v}" for k, v in sorted(self.claims.items()))

    def generate_proof(self, private_key: rsa.RSAPrivateKey) -> None:
        """
        Generate a cryptographic proof for the credential using the issuer's private key.

        Args:
            private_key (rsa.RSAPrivateKey): The RSA private key of the issuer.
        """
        claims_data = self._serialize_claims()
        data_hash = hashlib.sha256(claims_data.encode('utf-8')).hexdigest()

        # Sign the data hash using RSA-PSS
        signature = private_key.sign(
            data_hash.encode('utf-8'),
            padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.MAX_LENGTH),
            hashes.SHA256()
        )

        self.proof = {
            'data_hash': data_hash,
            'signature': signature.hex()
        }
        logger.info("Generated cryptographic proof for the credential.")

    def revoke(self) -> None:
        """
        Revoke the credential by setting its expiration to the current time.
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
            logger.debug("Credential is expired.")
            return True
        logger.debug("Credential is not expired.")
        return False

    def selective_disclosure(self, fields: List[str]) -> Dict[str, Union[str, int, float]]:
        """
        Selectively disclose specified fields of the credential.

        Args:
            fields (List[str]): List of fields to disclose.

        Returns:
            Dict[str, Union[str, int, float]]: Disclosed claims of the credential.
        """
        disclosed_fields = {field: self.claims[field] for field in fields if field in self.claims}
        logger.debug(f"Selective disclosure of fields: {disclosed_fields}")
        return disclosed_fields


class CredentialTemplate:
    """
    Template for creating standard credentials within cooperatives and communities.

    Attributes:
        - template_name (str): Name of the credential template.
        - claims (Dict): Predefined claims for the credential.
        - dao_type (str): Type of DAO ('cooperative' or 'community').
    """
    def __init__(self, template_name: str, claims: Dict[str, Union[str, int, float]], dao_type: str):
        self.template_name = template_name
        self.claims = claims
        self.dao_type = dao_type
        logger.info(f"Initialized CredentialTemplate: {template_name}")

    def apply_template(self, subject_did: str, issuer_did: str, private_key: rsa.RSAPrivateKey) -> Credential:
        """
        Create a credential based on the template and sign it with the issuer's private key.

        Args:
            subject_did (str): The DID of the subject receiving the credential.
            issuer_did (str): The DID of the issuer.
            private_key (rsa.RSAPrivateKey): The private key of the issuer for signing.

        Returns:
            Credential: The newly created and signed credential.
        """
        credential = Credential(
            issuer=issuer_did,
            subject=subject_did,
            claims=self.claims,
            dao_type=self.dao_type,
            expires_at=datetime.now() + timedelta(days=365)  # Default expiration: 1 year
        )
        credential.generate_proof(private_key)
        logger.info(f"Credential created using template: {self.template_name}")
        return credential
