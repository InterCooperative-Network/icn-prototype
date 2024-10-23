# blockchain/utils/crypto.py

from typing import Tuple, Optional
import hashlib
import logging
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import rsa, padding, utils
from cryptography.hazmat.primitives import serialization
from cryptography.exceptions import InvalidSignature
from datetime import datetime

logger = logging.getLogger(__name__)


class CryptoUtils:
    """
    Utility class for cryptographic operations.

    Provides methods for:
    - Key generation and management
    - Signing and verification
    - Hashing
    - Encryption and decryption
    """

    @staticmethod
    def generate_key_pair(key_size: int = 2048) -> Tuple[bytes, bytes]:
        """
        Generate a new RSA key pair.

        Args:
            key_size: Size of the key in bits

        Returns:
            Tuple of (private_key, public_key) in PEM format
        """
        try:
            # Generate private key
            private_key = rsa.generate_private_key(
                public_exponent=65537, key_size=key_size
            )

            # Get public key
            public_key = private_key.public_key()

            # Serialize keys
            private_pem = private_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )

            public_pem = public_key.public_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PublicFormat.SubjectPublicKeyInfo,
            )

            return private_pem, public_pem

        except Exception as e:
            logger.error(f"Key generation failed: {str(e)}")
            raise

    @staticmethod
    def sign_message(message: bytes, private_key_pem: bytes) -> bytes:
        """
        Sign a message using a private key.

        Args:
            message: Message to sign
            private_key_pem: Private key in PEM format

        Returns:
            bytes: Signature
        """
        try:
            # Load private key
            private_key = serialization.load_pem_private_key(
                private_key_pem, password=None
            )

            # Create signature
            signature = private_key.sign(
                message,
                padding.PSS(
                    mgf=padding.MGF1(hashes.SHA256()),
                    salt_length=padding.PSS.MAX_LENGTH,
                ),
                hashes.SHA256(),
            )

            return signature

        except Exception as e:
            logger.error(f"Signing failed: {str(e)}")
            raise

    @staticmethod
    def verify_signature(
        message: bytes, signature: bytes, public_key_pem: bytes
    ) -> bool:
        """
        Verify a signature using a public key.

        Args:
            message: Original message
            signature: Signature to verify
            public_key_pem: Public key in PEM format

        Returns:
            bool: True if signature is valid
        """
        try:
            # Load public key
            public_key = serialization.load_pem_public_key(public_key_pem)

            # Verify signature
            public_key.verify(
                signature,
                message,
                padding.PSS(
                    mgf=padding.MGF1(hashes.SHA256()),
                    salt_length=padding.PSS.MAX_LENGTH,
                ),
                hashes.SHA256(),
            )

            return True

        except InvalidSignature:
            return False
        except Exception as e:
            logger.error(f"Verification failed: {str(e)}")
            return False

    @staticmethod
    def hash_data(data: bytes) -> str:
        """
        Create SHA-256 hash of data.

        Args:
            data: Data to hash

        Returns:
            str: Hexadecimal hash string
        """
        try:
            return hashlib.sha256(data).hexdigest()
        except Exception as e:
            logger.error(f"Hashing failed: {str(e)}")
            raise

    @staticmethod
    def double_hash(data: bytes) -> str:
        """
        Create double SHA-256 hash (as used in Bitcoin).

        Args:
            data: Data to hash

        Returns:
            str: Hexadecimal double hash string
        """
        try:
            return hashlib.sha256(hashlib.sha256(data).digest()).hexdigest()
        except Exception as e:
            logger.error(f"Double hashing failed: {str(e)}")
            raise

    @staticmethod
    def merkle_root(hash_list: list) -> Optional[str]:
        """
        Calculate Merkle root from list of hashes.

        Args:
            hash_list: List of hash strings

        Returns:
            str: Merkle root hash or None if list is empty
        """
        try:
            if not hash_list:
                return None

            if len(hash_list) == 1:
                return hash_list[0]

            # Ensure even number of hashes
            if len(hash_list) % 2 == 1:
                hash_list.append(hash_list[-1])

            # Pair hashes and hash together
            new_hash_list = []
            for i in range(0, len(hash_list), 2):
                combined = hash_list[i] + hash_list[i + 1]
                new_hash = hashlib.sha256(combined.encode()).hexdigest()
                new_hash_list.append(new_hash)

            # Recurse until single hash remains
            return CryptoUtils.merkle_root(new_hash_list)

        except Exception as e:
            logger.error(f"Merkle root calculation failed: {str(e)}")
            raise

    @staticmethod
    def create_timestamp() -> str:
        """Create RFC 3339 formatted timestamp."""
        try:
            return datetime.utcnow().isoformat() + "Z"
        except Exception as e:
            logger.error(f"Timestamp creation failed: {str(e)}")
            raise

    @classmethod
    def verify_proof_of_work(cls, block_header: bytes, difficulty: int) -> bool:
        """
        Verify proof of work for a block.

        Args:
            block_header: Block header data
            difficulty: Number of leading zeros required

        Returns:
            bool: True if proof of work is valid
        """
        try:
            block_hash = cls.hash_data(block_header)
            return block_hash.startswith("0" * difficulty)
        except Exception as e:
            logger.error(f"Proof of work verification failed: {str(e)}")
            return False
