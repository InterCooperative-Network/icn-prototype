# ================================================================
# File: blockchain/utils/crypto.py
# Description: Contains cryptographic functions for securing the ICN
# blockchain. Includes hashing, signing, and signature verification to
# ensure the integrity, authenticity, and confidentiality of transactions
# and blocks.
# ================================================================

from typing import Any, Tuple, Optional
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.primitives.asymmetric.utils import Prehashed
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives import hmac
import os
import base64
import logging

logger = logging.getLogger(__name__)

# Constants for encryption/decryption
AES_KEY_SIZE = 32
IV_SIZE = 16

def generate_rsa_key_pair() -> Tuple[rsa.RSAPrivateKey, rsa.RSAPublicKey]:
    """
    Generate an RSA key pair for signing and verification.

    Returns:
        Tuple: A tuple containing the RSA private key and public key.
    """
    try:
        private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048,
            backend=default_backend()
        )
        public_key = private_key.public_key()
        logger.info("Generated RSA key pair")
        return private_key, public_key

    except Exception as e:
        logger.error(f"Failed to generate RSA key pair: {str(e)}")
        raise

def sign_data(private_key: rsa.RSAPrivateKey, data: bytes) -> bytes:
    """
    Sign data using a private RSA key.

    Args:
        private_key (rsa.RSAPrivateKey): The private RSA key.
        data (bytes): The data to be signed.

    Returns:
        bytes: The signature of the data.
    """
    try:
        signature = private_key.sign(
            data,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.MAX_LENGTH
            ),
            hashes.SHA256()
        )
        logger.info("Data signed successfully")
        return signature

    except Exception as e:
        logger.error(f"Failed to sign data: {str(e)}")
        raise

def verify_signature(
    public_key: rsa.RSAPublicKey, signature: bytes, data: bytes
) -> bool:
    """
    Verify the signature of data using a public RSA key.

    Args:
        public_key (rsa.RSAPublicKey): The public RSA key.
        signature (bytes): The signature to verify.
        data (bytes): The data that was signed.

    Returns:
        bool: True if the signature is valid, False otherwise.
    """
    try:
        public_key.verify(
            signature,
            data,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.MAX_LENGTH
            ),
            hashes.SHA256()
        )
        logger.info("Signature verified successfully")
        return True

    except Exception as e:
        logger.error(f"Signature verification failed: {str(e)}")
        return False

def hash_data(data: bytes) -> str:
    """
    Hash data using SHA-256.

    Args:
        data (bytes): The data to be hashed.

    Returns:
        str: The SHA-256 hash of the data in hexadecimal format.
    """
    try:
        digest = hashes.Hash(hashes.SHA256(), backend=default_backend())
        digest.update(data)
        hash_hex = digest.finalize().hex()
        logger.info("Data hashed successfully")
        return hash_hex

    except Exception as e:
        logger.error(f"Failed to hash data: {str(e)}")
        raise

def derive_key(password: bytes, salt: bytes, iterations: int = 100000) -> bytes:
    """
    Derive a cryptographic key from a password using PBKDF2-HMAC-SHA256.

    Args:
        password (bytes): The password to derive the key from.
        salt (bytes): The salt for key derivation.
        iterations (int): Number of iterations for the key derivation.

    Returns:
        bytes: The derived key.
    """
    try:
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=AES_KEY_SIZE,
            salt=salt,
            iterations=iterations,
            backend=default_backend()
        )
        key = kdf.derive(password)
        logger.info("Key derived successfully from password")
        return key

    except Exception as e:
        logger.error(f"Failed to derive key: {str(e)}")
        raise

def aes_encrypt(key: bytes, plaintext: bytes) -> Tuple[bytes, bytes]:
    """
    Encrypt data using AES in CBC mode.

    Args:
        key (bytes): The AES key.
        plaintext (bytes): The data to be encrypted.

    Returns:
        Tuple: The IV and ciphertext.
    """
    try:
        iv = os.urandom(IV_SIZE)
        cipher = Cipher(
            algorithms.AES(key),
            modes.CBC(iv),
            backend=default_backend()
        )
        encryptor = cipher.encryptor()

        # Pad plaintext to block size
        padding_length = AES_KEY_SIZE - (len(plaintext) % AES_KEY_SIZE)
        padded_plaintext = plaintext + bytes([padding_length] * padding_length)

        ciphertext = encryptor.update(padded_plaintext) + encryptor.finalize()
        logger.info("Data encrypted successfully")
        return iv, ciphertext

    except Exception as e:
        logger.error(f"Failed to encrypt data: {str(e)}")
        raise

def aes_decrypt(key: bytes, iv: bytes, ciphertext: bytes) -> bytes:
    """
    Decrypt data using AES in CBC mode.

    Args:
        key (bytes): The AES key.
        iv (bytes): The initialization vector (IV).
        ciphertext (bytes): The data to be decrypted.

    Returns:
        bytes: The decrypted plaintext.
    """
    try:
        cipher = Cipher(
            algorithms.AES(key),
            modes.CBC(iv),
            backend=default_backend()
        )
        decryptor = cipher.decryptor()

        padded_plaintext = decryptor.update(ciphertext) + decryptor.finalize()

        # Remove padding
        padding_length = padded_plaintext[-1]
        plaintext = padded_plaintext[:-padding_length]

        logger.info("Data decrypted successfully")
        return plaintext

    except Exception as e:
        logger.error(f"Failed to decrypt data: {str(e)}")
        raise

def hmac_sign(key: bytes, data: bytes) -> bytes:
    """
    Generate an HMAC signature for data using a symmetric key.

    Args:
        key (bytes): The symmetric key.
        data (bytes): The data to be signed.

    Returns:
        bytes: The HMAC signature.
    """
    try:
        hmac_obj = hmac.HMAC(key, hashes.SHA256(), backend=default_backend())
        hmac_obj.update(data)
        signature = hmac_obj.finalize()
        logger.info("HMAC signature generated successfully")
        return signature

    except Exception as e:
        logger.error(f"Failed to generate HMAC signature: {str(e)}")
        raise

def hmac_verify(key: bytes, signature: bytes, data: bytes) -> bool:
    """
    Verify an HMAC signature for data using a symmetric key.

    Args:
        key (bytes): The symmetric key.
        signature (bytes): The HMAC signature to verify.
        data (bytes): The data that was signed.

    Returns:
        bool: True if the HMAC is valid, False otherwise.
    """
    try:
        hmac_obj = hmac.HMAC(key, hashes.SHA256(), backend=default_backend())
        hmac_obj.update(data)
        hmac_obj.verify(signature)
        logger.info("HMAC signature verified successfully")
        return True

    except Exception as e:
        logger.error(f"HMAC verification failed: {str(e)}")
        return False
