"""
blockchain/core/state/state_transition.py

Handles atomic state transitions and rollbacks for the ICN blockchain.
Ensures consistent state changes across shards while maintaining verifiability.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Optional, Any, Set
import hashlib
import json
import logging
from copy import deepcopy

logger = logging.getLogger(__name__)

@dataclass
class StateTransition:
    """
    Represents an atomic state transition in the blockchain.
    
    Manages the transition between states, including validation,
    verification, and rollback capabilities. Ensures atomic updates
    and maintains transition history for auditing purposes.
    
    Attributes:
        transition_id: Unique identifier for the transition.
        old_state: Previous state before transition.
        new_state: New state after transition.
        shard_id: Optional shard identifier for shard-specific transitions.
        timestamp: Time of transition creation.
        metadata: Additional metadata for the transition.
        verified: Indicates if the transition is verified.
        applied: Indicates if the transition has been applied.
        verification_signatures: Set of validator signatures for verification.
    """
    
    transition_id: Optional[str] = None
    old_state: Dict[str, Any] = field(default_factory=dict)
    new_state: Dict[str, Any] = field(default_factory=dict)
    shard_id: Optional[int] = None
    timestamp: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)
    verified: bool = False
    applied: bool = False
    verification_signatures: Set[str] = field(default_factory=set)

    def __post_init__(self):
        """Initialize transition after creation."""
        if not self.transition_id:
            self.transition_id = self._generate_id()

        # Deep copy states to prevent accidental modification
        self.old_state = deepcopy(self.old_state)
        self.new_state = deepcopy(self.new_state)

        # Initialize metadata with timestamp if not provided
        if 'created_at' not in self.metadata:
            self.metadata['created_at'] = self.timestamp.isoformat()

    def _generate_id(self) -> str:
        """
        Generate a unique transition ID based on state data and timestamp.

        Returns:
            str: A SHA-256 hash as the transition ID.
        """
        data = {
            "old_state": self.old_state,
            "new_state": self.new_state,
            "shard_id": self.shard_id,
            "timestamp": self.timestamp.isoformat()
        }
        return hashlib.sha256(json.dumps(data, sort_keys=True).encode()).hexdigest()

    def validate(self) -> bool:
        """
        Validate the state transition.

        Checks:
        - State structure validity.
        - Consistent keys between old and new states.
        - Transition ID accuracy.
        - Compliance with state invariants.

        Returns:
            bool: True if the transition is valid, False otherwise.
        """
        try:
            # Ensure state structures are dictionaries
            if not isinstance(self.old_state, dict) or not isinstance(self.new_state, dict):
                logger.error("Invalid state structure")
                return False

            # Ensure key consistency between old and new states
            if set(self.old_state.keys()) != set(self.new_state.keys()):
                logger.error("Key mismatch between old and new states")
                return False

            # Validate transition ID
            if self.transition_id != self._generate_id():
                logger.error("Transition ID mismatch")
                return False

            # Verify state invariants
            if not self._verify_state_invariants():
                logger.error("State invariants violated")
                return False

            return True

        except Exception as e:
            logger.error(f"Validation failed: {str(e)}")
            return False

    def _verify_state_invariants(self) -> bool:
        """
        Check that the state transition maintains necessary invariants.

        This function ensures that no critical state properties are violated,
        such as negative balances or broken data integrity.

        Returns:
            bool: True if invariants are maintained, False otherwise.
        """
        for key, value in self.new_state.items():
            if isinstance(value, (int, float)) and value < 0:
                logger.error(f"Negative value for '{key}' in new state")
                return False
        return True

    def apply(self) -> bool:
        """
        Apply the state transition, updating the old state to the new state.

        Returns:
            bool: True if the transition is applied successfully, False otherwise.
        """
        if not self.verified:
            logger.error("Cannot apply unverified transition")
            return False

        try:
            self.old_state = deepcopy(self.new_state)
            self.applied = True
            logger.info(f"Transition {self.transition_id} applied successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to apply transition: {str(e)}")
            return False

    def rollback(self) -> bool:
        """
        Roll back the state transition to the old state.

        Returns:
            bool: True if rollback is successful, False otherwise.
        """
        if not self.applied:
            logger.error("Cannot roll back an unapplied transition")
            return False

        try:
            self.new_state = deepcopy(self.old_state)
            self.applied = False
            logger.info(f"Transition {self.transition_id} rolled back successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to roll back transition: {str(e)}")
            return False

    def add_signature(self, signature: str) -> None:
        """
        Add a verification signature to the transition.

        Args:
            signature (str): The validator's signature.
        """
        self.verification_signatures.add(signature)
        logger.info(f"Signature added to transition {self.transition_id}")

    def is_fully_verified(self, required_signatures: int) -> bool:
        """
        Check if the transition has enough signatures for verification.

        Args:
            required_signatures (int): Number of required signatures.

        Returns:
            bool: True if the transition is fully verified, False otherwise.
        """
        return len(self.verification_signatures) >= required_signatures

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the state transition object to a dictionary.

        Returns:
            Dict[str, Any]: Dictionary representation of the transition.
        """
        return {
            "transition_id": self.transition_id,
            "old_state": self.old_state,
            "new_state": self.new_state,
            "shard_id": self.shard_id,
            "timestamp": self.timestamp.isoformat(),
            "metadata": self.metadata,
            "verified": self.verified,
            "applied": self.applied,
            "verification_signatures": list(self.verification_signatures)
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "StateTransition":
        """
        Create a StateTransition object from a dictionary.

        Args:
            data (Dict[str, Any]): Dictionary containing transition data.

        Returns:
            StateTransition: A new StateTransition instance.
        """
        return cls(
            transition_id=data.get("transition_id"),
            old_state=data.get("old_state", {}),
            new_state=data.get("new_state", {}),
            shard_id=data.get("shard_id"),
            timestamp=datetime.fromisoformat(data.get("timestamp")),
            metadata=data.get("metadata", {}),
            verified=data.get("verified", False),
            applied=data.get("applied", False),
            verification_signatures=set(data.get("verification_signatures", []))
        )
