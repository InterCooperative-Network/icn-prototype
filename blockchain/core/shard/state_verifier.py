"""
blockchain/core/shard/state_verifier.py

Implements state verification for cross-shard operations in the ICN blockchain.
Ensures state consistency and provides rollback mechanisms for failed operations.
Integrates with DID verification and governance rules.
"""

from __future__ import annotations
from typing import Dict, List, Optional, Set, Any, Tuple
from datetime import datetime, timedelta
import hashlib
import json
import logging
from dataclasses import dataclass, field

from ..transaction import Transaction
from ..block import Block
from .cross_shard_transaction import CrossShardTransaction
from did.base_did import BaseDID
from system.governance import GovernanceSystem

logger = logging.getLogger(__name__)

@dataclass
class StateCheckpoint:
    """Represents a state checkpoint with verification metadata."""
    shard_id: int
    state: Dict[str, Any]
    timestamp: datetime
    transaction_id: str
    validator_did: Optional[str]
    verification_signatures: Set[str] = field(default_factory=set)
    verified_by_governance: bool = False
    hash: Optional[str] = None

    def __post_init__(self):
        """Initialize the checkpoint hash if not provided."""
        if not self.hash:
            self.hash = self._calculate_hash()

    def _calculate_hash(self) -> str:
        """Calculate the hash of the checkpoint state."""
        checkpoint_data = {
            "shard_id": self.shard_id,
            "state": self.state,
            "timestamp": self.timestamp.isoformat(),
            "transaction_id": self.transaction_id,
            "validator_did": self.validator_did
        }
        return hashlib.sha256(json.dumps(checkpoint_data, sort_keys=True).encode()).hexdigest()

class StateVerifier:
    """
    Manages state verification and rollback capabilities for cross-shard operations.
    Ensures state consistency across shards during cooperative transactions.
    """

    def __init__(self, governance_system: Optional[GovernanceSystem] = None):
        self.state_checkpoints: Dict[str, Dict[int, StateCheckpoint]] = {}  # transaction_id -> shard_id -> checkpoint
        self.verification_cache: Dict[str, Dict[str, bool]] = {}  # transaction_id -> state_hash -> result
        self.pending_verifications: Set[str] = set()  # Set of pending transaction IDs
        self.governance_system = governance_system
        self.required_verifications = 3  # Minimum number of validator verifications required
        
    async def create_checkpoint(
        self, 
        transaction_id: str, 
        shard_id: int, 
        state: Dict[str, Any],
        validator_did: Optional[BaseDID] = None
    ) -> Tuple[str, bool]:
        """
        Create a checkpoint of the current state before a cross-shard operation.
        
        Args:
            transaction_id: ID of the cross-shard transaction
            shard_id: ID of the shard
            state: Current state to checkpoint
            validator_did: Optional DID of the validator creating checkpoint
            
        Returns:
            Tuple[str, bool]: (Checkpoint hash, success status)
        """
        try:
            checkpoint = StateCheckpoint(
                shard_id=shard_id,
                state=state.copy(),
                timestamp=datetime.now(),
                transaction_id=transaction_id,
                validator_did=validator_did.get_did() if validator_did else None
            )
            
            if transaction_id not in self.state_checkpoints:
                self.state_checkpoints[transaction_id] = {}
            
            self.state_checkpoints[transaction_id][shard_id] = checkpoint
            
            # Record checkpoint in governance system if available
            if self.governance_system and validator_did:
                verified = await self.governance_system.verify_state_checkpoint(
                    transaction_id,
                    shard_id,
                    checkpoint.hash,
                    validator_did.get_did()
                )
                checkpoint.verified_by_governance = verified
            
            logger.info(f"Created checkpoint for transaction {transaction_id} in shard {shard_id}")
            self.pending_verifications.add(transaction_id)
            
            return checkpoint.hash, True

        except Exception as e:
            logger.error(f"Error creating checkpoint: {str(e)}")
            return "", False

    async def verify_state(
        self,
        transaction: CrossShardTransaction,
        current_states: Dict[int, Dict[str, Any]],
        validator_did: Optional[BaseDID] = None
    ) -> bool:
        """
        Verify state consistency across shards after a cross-shard operation.
        
        Args:
            transaction: The cross-shard transaction
            current_states: Current states of involved shards
            validator_did: Optional DID of the verifying validator
            
        Returns:
            bool: True if states are consistent
        """
        try:
            if transaction.state == "aborted":
                return False

            # Get involved shards
            shards = {transaction.source_shard} | transaction.target_shards
            
            # Verify each shard's state
            for shard_id in shards:
                if not await self._verify_shard_state(
                    transaction.transaction_id,
                    shard_id,
                    current_states.get(shard_id, {}),
                    validator_did
                ):
                    return False
                    
            return True

        except Exception as e:
            logger.error(f"Error verifying state: {str(e)}")
            return False

    async def _verify_shard_state(
        self,
        transaction_id: str,
        shard_id: int,
        current_state: Dict[str, Any],
        validator_did: Optional[BaseDID]
    ) -> bool:
        """
        Verify state consistency for a specific shard.
        
        Args:
            transaction_id: ID of the transaction
            shard_id: ID of the shard to verify
            current_state: Current state of the shard
            validator_did: Optional DID of the verifying validator
            
        Returns:
            bool: True if state is consistent
        """
        try:
            # Get checkpoint
            checkpoint = self.state_checkpoints.get(transaction_id, {}).get(shard_id)
            if not checkpoint:
                logger.error(f"No checkpoint found for transaction {transaction_id} in shard {shard_id}")
                return False

            # Validate state transition
            valid_transition = await self._validate_state_transition(
                checkpoint.state,
                current_state,
                transaction_id,
                shard_id
            )
            
            if not valid_transition:
                return False

            # Add validator verification if DID provided
            if validator_did:
                checkpoint.verification_signatures.add(validator_did.get_did())
                
                # Check governance verification if available
                if self.governance_system:
                    checkpoint.verified_by_governance = await self.governance_system.verify_state_transition(
                        transaction_id,
                        shard_id,
                        checkpoint.hash,
                        validator_did.get_did()
                    )

            # Check if we have enough verifications
            if (len(checkpoint.verification_signatures) >= self.required_verifications and
                (not self.governance_system or checkpoint.verified_by_governance)):
                if transaction_id in self.pending_verifications:
                    self.pending_verifications.remove(transaction_id)
                return True

            return False

        except Exception as e:
            logger.error(f"Error verifying shard state: {str(e)}")
            return False

    async def rollback_state(
        self,
        transaction_id: str,
        shard_id: int,
        validator_did: Optional[BaseDID] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Rollback state to the last checkpoint for a failed operation.
        
        Args:
            transaction_id: ID of the failed transaction
            shard_id: ID of the shard to rollback
            validator_did: Optional DID of the validator initiating rollback
            
        Returns:
            Optional[Dict[str, Any]]: The rolled back state if successful
        """
        try:
            checkpoint = self.state_checkpoints.get(transaction_id, {}).get(shard_id)
            if not checkpoint:
                logger.error(f"No checkpoint found for rollback of transaction {transaction_id} in shard {shard_id}")
                return None

            # Record rollback in governance system if available
            if self.governance_system and validator_did:
                await self.governance_system.record_state_rollback(
                    transaction_id,
                    shard_id,
                    checkpoint.hash,
                    validator_did.get_did()
                )

            rolled_back_state = checkpoint.state.copy()
            logger.info(f"Rolled back state for transaction {transaction_id} in shard {shard_id}")
            return rolled_back_state

        except Exception as e:
            logger.error(f"Error rolling back state: {str(e)}")
            return None

    async def _validate_state_transition(
        self,
        previous_state: Dict[str, Any],
        current_state: Dict[str, Any],
        transaction_id: str,
        shard_id: int
    ) -> bool:
        """
        Validate that the state transition is consistent with transaction rules.
        
        Args:
            previous_state: State before the transaction
            current_state: State after the transaction
            transaction_id: ID of the transaction
            shard_id: ID of the shard
            
        Returns:
            bool: True if the transition is valid
        """
        try:
            # Verify all accounts exist in both states
            if set(previous_state.keys()) != set(current_state.keys()):
                return False

            # Verify balances meet conservation rules
            prev_total = sum(float(val.get('balance', 0)) for val in previous_state.values())
            curr_total = sum(float(val.get('balance', 0)) for val in current_state.values())
            
            if not abs(prev_total - curr_total) < 0.0001:  # Allow for minimal floating point differences
                return False

            # Verify other state invariants
            for account_id, prev_account in previous_state.items():
                curr_account = current_state[account_id]
                
                # Verify non-balance fields remain unchanged
                prev_static = {k: v for k, v in prev_account.items() if k != 'balance'}
                curr_static = {k: v for k, v in curr_account.items() if k != 'balance'}
                if prev_static != curr_static:
                    return False
                
                # Verify balance changes are within allowed limits
                prev_balance = float(prev_account.get('balance', 0))
                curr_balance = float(curr_account.get('balance', 0))
                if abs(curr_balance - prev_balance) > prev_balance * 0.5:  # Max 50% change per transaction
                    return False

            return True

        except Exception as e:
            logger.error(f"Error validating state transition: {str(e)}")
            return False

    async def cleanup_old_checkpoints(self, max_age_hours: int = 24) -> None:
        """
        Remove old checkpoints to prevent memory bloat.
        
        Args:
            max_age_hours: Maximum age of checkpoints to keep in hours
        """
        try:
            current_time = datetime.now()
            transactions_to_remove = []

            for transaction_id, shards in self.state_checkpoints.items():
                for checkpoint in shards.values():
                    age = current_time - checkpoint.timestamp
                    
                    if age > timedelta(hours=max_age_hours):
                        transactions_to_remove.append(transaction_id)
                        break

            for transaction_id in transactions_to_remove:
                del self.state_checkpoints[transaction_id]
                if transaction_id in self.verification_cache:
                    del self.verification_cache[transaction_id]
                self.pending_verifications.discard(transaction_id)
            
            logger.info(f"Cleaned up {len(transactions_to_remove)} old checkpoints")

        except Exception as e:
            logger.error(f"Error cleaning up old checkpoints: {str(e)}")

    def get_checkpoint_metrics(self) -> Dict[str, Any]:
        """Get metrics about current checkpoints and verifications."""
        try:
            return {
                "active_checkpoints": len(self.state_checkpoints),
                "pending_verifications": len(self.pending_verifications),
                "transactions": {
                    tx_id: {
                        "shards": list(shards.keys()),
                        "verifications": {
                            shard_id: len(checkpoint.verification_signatures)
                            for shard_id, checkpoint in shards.items()
                        },
                        "governance_verified": all(
                            checkpoint.verified_by_governance
                            for checkpoint in shards.values()
                        )
                    }
                    for tx_id, shards in self.state_checkpoints.items()
                }
            }
        except Exception as e:
            logger.error(f"Error getting checkpoint metrics: {str(e)}")
            return {}

    def to_dict(self) -> Dict[str, Any]:
        """Convert verifier state to dictionary format."""
        try:
            return {
                "checkpoints": {
                    tx_id: {
                        str(shard_id): {
                            "shard_id": checkpoint.shard_id,
                            "state": checkpoint.state,
                            "timestamp": checkpoint.timestamp.isoformat(),
                            "transaction_id": checkpoint.transaction_id,
                            "validator_did": checkpoint.validator_did,
                            "verification_signatures": list(checkpoint.verification_signatures),
                            "verified_by_governance": checkpoint.verified_by_governance,
                            "hash": checkpoint.hash
                        }
                        for shard_id, checkpoint in shards.items()
                    }
                    for tx_id, shards in self.state_checkpoints.items()
                },
                "pending_verifications": list(self.pending_verifications)
            }
        except Exception as e:
            logger.error(f"Error converting verifier to dictionary: {str(e)}")
            return {}

    @classmethod
    def from_dict(cls, data: Dict[str, Any], governance_system: Optional[GovernanceSystem] = None) -> 'StateVerifier':
        """Create verifier from dictionary format."""
        try:
            verifier = cls(governance_system)
            
            # Restore checkpoints
            for tx_id, shards in data.get("checkpoints", {}).items():
                verifier.state_checkpoints[tx_id] = {}
                for shard_id_str, checkpoint_data in shards.items():
                    checkpoint = StateCheckpoint(
                        shard_id=int(shard_id_str),
                        state=checkpoint_data["state"],
                        timestamp=datetime.fromisoformat(checkpoint_data["timestamp"]),
                        transaction_id=checkpoint_data["transaction_id"],
                        validator_did=checkpoint_data["validator_did"],
                        verification_signatures=set(checkpoint_data["verification_signatures"]),
                        verified_by_governance=checkpoint_data["verified_by_governance"],
                        hash=checkpoint_data["hash"]
                    )
                    verifier.state_checkpoints[tx_id][int(shard_id_str)] = checkpoint
            
            # Restore pending verifications
            verifier.pending_verifications = set(data.get("pending_verifications", []))
            
            return verifier
            
        except Exception as e:
            logger.error(f"Error creating verifier from dictionary: {str(e)}")
            raise

    def __str__(self) -> str:
        """Return string representation of the verifier."""
        try:
            metrics = self.get_checkpoint_metrics()
            return (
                f"StateVerifier(checkpoints={metrics['active_checkpoints']}, "
                f"pending={len(self.pending_verifications)})"
            )
        except Exception as e:
            logger.error(f"Error in string representation: {str(e)}")
            return "StateVerifier(error)"