from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Union
import hashlib
import logging
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.primitives import serialization, hashes

# Configure logging for the DID module
logger = logging.getLogger('did.base_did')
logger.setLevel(logging.DEBUG)  # Set to DEBUG for detailed trace logs

@dataclass
class BaseDID:
    """
    Base class for Decentralized Identifiers (DID).

    This class manages core identity functions within the ICN ecosystem, including:
    - RSA key generation for secure identity management
    - Decentralized Identifier (DID) creation
    - Symmetric encryption for sensitive data
    - Membership management (cooperatives and communities)
    - Dual reputation system for economic and civil activities
    - Role-based access control (RBAC) for permission management
    """
    private_key: rsa.RSAPrivateKey = field(init=False)
    public_key: rsa.RSAPublicKey = field(init=False)
    cooperative_memberships: List[str] = field(default_factory=list)
    community_memberships: List[str] = field(default_factory=list)
    reputation_scores: Dict[str, Dict[str, float]] = field(default_factory=lambda: {"economic": {}, "civil": {}})
    roles: Dict[str, Dict[str, List[str]]] = field(default_factory=lambda: {"cooperative": {}, "community": {}})
    metadata: Dict = field(default_factory=dict)

    def __post_init__(self):
        """
        Initialize RSA key pair and symmetric encryption after instance creation.
        """
        self.private_key = self._generate_private_key()
        self.public_key = self.private_key.public_key()
        self._encryption_key = Fernet.generate_key()
        self._cipher_suite = Fernet(self._encryption_key)
        logger.info("Initialized BaseDID with RSA keys and Fernet encryption.")

    def _generate_private_key(self) -> rsa.RSAPrivateKey:
        """
        Generate a new RSA private key for the DID.

        Returns:
            rsa.RSAPrivateKey: The generated RSA private key object.
        """
        return rsa.generate_private_key(public_exponent=65537, key_size=2048)

    def generate_did(self) -> str:
        """
        Generate a Decentralized Identifier (DID) based on the public key's SHA-256 hash.

        Returns:
            str: The generated DID string in the format 'did:icn:<16_hex_chars>'.
        """
        pub_bytes = self.public_key.public_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        did = f"did:icn:{hashlib.sha256(pub_bytes).hexdigest()[:16]}"
        logger.debug(f"Generated DID: {did}")
        return did

    # Membership Management
    def add_membership(self, dao_type: str, dao_id: str) -> None:
        """
        Add membership to a specified DAO (cooperative or community).

        Args:
            dao_type (str): The type of DAO ('cooperative' or 'community').
            dao_id (str): The ID of the DAO.

        Raises:
            ValueError: If the specified dao_type is invalid.
        """
        if dao_type not in ['cooperative', 'community']:
            logger.warning(f"Invalid DAO type for membership addition: {dao_type}")
            raise ValueError(f"Invalid DAO type: {dao_type}")

        membership_list = self.cooperative_memberships if dao_type == 'cooperative' else self.community_memberships
        if dao_id not in membership_list:
            membership_list.append(dao_id)
            logger.info(f"Added {dao_type} membership: {dao_id}")

    def list_memberships(self, dao_type: Optional[str] = None) -> Union[List[str], Dict[str, List[str]]]:
        """
        List memberships for a specific DAO type or both.

        Args:
            dao_type (Optional[str]): The type of DAO ('cooperative' or 'community'). Defaults to None.

        Returns:
            Union[List[str], Dict[str, List[str]]]: List of memberships or dictionary of both types.
        """
        if dao_type == 'cooperative':
            return self.cooperative_memberships
        elif dao_type == 'community':
            return self.community_memberships
        else:
            return {
                "cooperative": self.cooperative_memberships,
                "community": self.community_memberships,
            }

    # Reputation Management
    def update_reputation(self, category: str, score: float, dao_type: str, evidence: Optional[Dict] = None) -> None:
        """
        Update the reputation score for a specific category within a DAO type.

        Args:
            category (str): The category of reputation to update (e.g., 'trustworthiness').
            score (float): The reputation score to add.
            dao_type (str): The type of DAO ('economic' or 'civil').
            evidence (Optional[Dict]): Additional evidence supporting the reputation change.

        Raises:
            ValueError: If the specified dao_type is invalid.
        """
        if dao_type not in self.reputation_scores:
            logger.warning(f"Invalid DAO type for reputation update: {dao_type}")
            raise ValueError(f"Invalid DAO type: {dao_type}")

        old_score = self.reputation_scores[dao_type].get(category, 0)
        new_score = old_score + score
        self.reputation_scores[dao_type][category] = new_score
        logger.info(f"{dao_type.capitalize()} reputation updated for '{category}': {new_score}")

        if evidence:
            self.metadata.setdefault("reputation_evidence", {})[category] = evidence

    def get_total_reputation(self, dao_type: str) -> float:
        """
        Calculate total reputation for a given DAO type.

        Args:
            dao_type (str): The type of DAO ('economic' or 'civil').

        Returns:
            float: Total reputation score for the specified DAO type.

        Raises:
            ValueError: If the specified dao_type is invalid.
        """
        if dao_type not in self.reputation_scores:
            logger.warning(f"Invalid DAO type for reputation calculation: {dao_type}")
            raise ValueError(f"Invalid DAO type: {dao_type}")

        total_reputation = sum(self.reputation_scores.get(dao_type, {}).values())
        logger.info(f"Total {dao_type} reputation calculated: {total_reputation}")
        return total_reputation

    # Role-Based Access Control (RBAC)
    def add_role(self, role: str, permissions: List[str], dao_type: str) -> None:
        """
        Add a role with permissions to a specified DAO type.

        Args:
            role (str): The name of the role to add.
            permissions (List[str]): List of permissions associated with the role.
            dao_type (str): The type of DAO ('cooperative' or 'community').

        Raises:
            ValueError: If the specified dao_type is invalid.
        """
        if dao_type not in self.roles:
            logger.warning(f"Invalid DAO type for role assignment: {dao_type}")
            raise ValueError(f"Invalid DAO type: {dao_type}")

        self.roles[dao_type][role] = permissions
        logger.info(f"Role '{role}' added in {dao_type} with permissions: {permissions}")

    def has_permission(self, role: str, permission: str, dao_type: str) -> bool:
        """
        Check if a role has a specific permission within a DAO type.

        Args:
            role (str): The name of the role to check.
            permission (str): The permission to verify.
            dao_type (str): The type of DAO ('cooperative' or 'community').

        Returns:
            bool: True if the role has the specified permission, False otherwise.
        """
        has_perm = permission in self.roles.get(dao_type, {}).get(role, [])
        logger.debug(f"Permission check for {dao_type} role '{role}' and permission '{permission}': {has_perm}")
        return has_perm
