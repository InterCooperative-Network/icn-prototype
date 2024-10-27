# base_did.py

from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Union
import hashlib
import logging
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.primitives import serialization, hashes
from datetime import datetime

logger = logging.getLogger(__name__)

@dataclass
class BaseDID:
    """
    Base class for Decentralized Identifiers (DID).
    
    This class handles core identity functions, including key generation, DID creation,
    encryption, cooperative and community memberships, dual reputation management, 
    role-based access control (RBAC), and federation with other DAOs.
    
    Attributes:
    - private_key: RSA private key for asymmetric encryption.
    - public_key: RSA public key for DID generation and encryption.
    - cooperative_memberships: List of cooperatives where the DID has membership.
    - community_memberships: List of communities where the DID has membership.
    - reputation_scores: Dictionary of reputation scores (economic and civil).
    - roles: Dictionary of roles and permissions for cooperatives and communities.
    - metadata: Additional metadata related to the DID.
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
        Generate RSA keys and initialize Fernet encryption for sensitive data.
        """
        self.private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        self.public_key = self.private_key.public_key()
        self._encryption_key = Fernet.generate_key()
        self._cipher_suite = Fernet(self._encryption_key)
        logger.info("DID initialized with new RSA keys and Fernet encryption.")

    def generate_did(self) -> str:
        """
        Generate a DID string based on the SHA-256 hash of the public key.

        Returns:
        - str: The generated DID.
        """
        pub_bytes = self.public_key.public_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        did = f"did:icn:{hashlib.sha256(pub_bytes).hexdigest()[:16]}"
        logger.info(f"DID generated: {did}")
        return did

    # Cooperative and Community Membership Management

    def add_membership(self, dao_type: str, dao_id: str) -> None:
        """
        Add membership to a cooperative or community DAO.

        Args:
        - dao_type: 'cooperative' or 'community' indicating the type of DAO.
        - dao_id: The ID of the DAO to add.
        """
        if dao_type == 'cooperative' and dao_id not in self.cooperative_memberships:
            self.cooperative_memberships.append(dao_id)
            logger.info(f"Added cooperative membership: {dao_id}")
        elif dao_type == 'community' and dao_id not in self.community_memberships:
            self.community_memberships.append(dao_id)
            logger.info(f"Added community membership: {dao_id}")
        else:
            logger.warning(f"Invalid DAO type or duplicate membership: {dao_type}, {dao_id}")

    def list_memberships(self, dao_type: Optional[str] = None) -> Union[List[str], Dict[str, List[str]]]:
        """
        List memberships for cooperatives, communities, or both.

        Args:
        - dao_type: Optional, 'cooperative' or 'community'.

        Returns:
        - List[str] | Dict[str, List[str]]: Memberships for the specified type or both.
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

    # Dual Reputation System

    def update_reputation(self, category: str, score: float, dao_type: str, evidence: Optional[Dict] = None) -> None:
        """
        Update reputation score for a specific category within cooperatives or communities.

        Args:
        - category: The category for the reputation score (e.g., 'trustworthiness').
        - score: The score to be added.
        - dao_type: 'economic' or 'civil' indicating the reputation type.
        - evidence: Optional evidence for reputation updates.
        """
        if dao_type not in self.reputation_scores:
            logger.warning(f"Invalid DAO type for reputation update: {dao_type}")
            return

        old_score = self.reputation_scores[dao_type].get(category, 0)
        new_score = old_score + score
        self.reputation_scores[dao_type][category] = new_score
        logger.info(f"{dao_type.capitalize()} reputation updated for '{category}': {new_score}")

        if evidence:
            if "reputation_evidence" not in self.metadata:
                self.metadata["reputation_evidence"] = {}
            self.metadata["reputation_evidence"][category] = evidence

    def get_total_reputation(self, dao_type: str) -> float:
        """
        Calculate total reputation score for cooperatives or communities.

        Args:
        - dao_type: 'economic' or 'civil'.

        Returns:
        - float: The total reputation score.
        """
        total_reputation = sum(self.reputation_scores.get(dao_type, {}).values())
        logger.info(f"Total {dao_type} reputation calculated: {total_reputation}")
        return total_reputation

    # Role-Based Access Control (RBAC)

    def add_role(self, role: str, permissions: List[str], dao_type: str) -> None:
        """
        Add a role with permissions for cooperatives or communities.

        Args:
        - role: The role name (e.g., 'admin', 'member').
        - permissions: List of permissions associated with the role.
        - dao_type: 'cooperative' or 'community'.
        """
        if dao_type not in self.roles:
            logger.warning(f"Invalid DAO type for role assignment: {dao_type}")
            return

        self.roles[dao_type][role] = permissions
        logger.info(f"Role '{role}' added in {dao_type} with permissions: {permissions}")

    def has_permission(self, role: str, permission: str, dao_type: str) -> bool:
        """
        Check if the DID has a specific permission within a cooperative or community.

        Args:
        - role: The role name.
        - permission: The permission to check.
        - dao_type: 'cooperative' or 'community'.

        Returns:
        - bool: True if the permission exists for the role, False otherwise.
        """
        has_perm = permission in self.roles.get(dao_type, {}).get(role, [])
        logger.info(f"Permission check for {dao_type} role '{role}' and permission '{permission}': {has_perm}")
        return has_perm

    # Federation Management

    def federate_with_dao(self, dao_id: str, dao_type: str, terms: Dict) -> None:
        """
        Establish a federation with another cooperative or community.

        Args:
        - dao_id: The ID of the DAO to federate with.
        - dao_type: 'cooperative' or 'community'.
        - terms: Dictionary outlining the terms of federation.
        """
        if "federations" not in self.metadata:
            self.metadata["federations"] = []
        
        federation = {
            "dao_id": dao_id,
            "dao_type": dao_type,
            "terms": terms,
            "established_at": datetime.now().isoformat()
        }
        self.metadata["federations"].append(federation)
        logger.info(f"Federated with {dao_type} '{dao_id}' under terms: {terms}")
