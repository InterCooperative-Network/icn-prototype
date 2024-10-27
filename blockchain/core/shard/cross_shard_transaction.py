"""
blockchain/core/shard/cross_shard_transaction.py

Implements atomic cross-shard transaction handling for the ICN blockchain.
Ensures consistent state across shards during cooperative operations while
integrating with the DID and governance systems.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set
from datetime import datetime
import hashlib
import json
import logging

from ...utils.validation import validate_transaction
from ...consensus.proof_of_cooperation import ProofOfCooperation
from ..transaction import Transaction
from ..block import Block
from did.base_did import BaseDID
from system.governance import GovernanceSystem

logger = logging.getLogger(__name__)

@dataclass
class CrossShardTransactionPhase:
    """Represents a phase in a cross-shard transaction."""
    phase_id: str
    shard_id: int
    status: str = "pending"  # pending, prepared, committed, aborted
    timestamp: datetime = field(default_factory=datetime.now)
    validation_signatures: Set[str] = field(default_factory=set)
    did_attestations: Set[str] = field(default_factory=set)  # DIDs that have attested this phase

class CrossShardTransaction:
    """
    Manages atomic cross-shard transactions using a two-phase commit protocol.
    Ensures state consistency across shards during cooperative operations.
    """

    def __init__(
        self,
        transaction_id: str,
        source_shard: int,
        target_shards: Set[int],
        primary_transaction: Transaction,
        governance_system: Optional[GovernanceSystem] = None
    ):
        self.transaction_id = transaction_id
        self.source_shard = source_shard
        self.target_shards = target_shards
        self.primary_transaction = primary_transaction
        self.phases: Dict[int, CrossShardTransactionPhase] = {}
        self.state = "pending"
        self.created_at = datetime.now()
        self.completed_at: Optional[datetime] = None
        self.required_validations = 3
        self.governance_system = governance_system
        
        # Initialize phases
        self._initialize_phases()

    def _initialize_phases(self) -> None:
        """Initialize transaction phases for all involved shards."""
        all_shards = {self.source_shard} | self.target_shards
        for shard_id in all_shards:
            phase_id = self._generate_phase_id(shard_id)
            self.phases[shard_id] = CrossShardTransactionPhase(
                phase_id=phase_id,
                shard_id=shard_id
            )

    def prepare_phase(self, shard_id: int, validator_id: str, validator_did: BaseDID) -> bool:
        """
        Prepare phase of the two-phase commit protocol with DID attestation.
        
        Args:
            shard_id: ID of the shard being prepared
            validator_id: ID of the validating node
            validator_did: DID of the validator for attestation
            
        Returns:
            bool: True if preparation successful
        """
        try:
            if shard_id not in self.phases:
                logger.error(f"Invalid shard ID {shard_id} for transaction {self.transaction_id}")
                return False

            phase = self.phases[shard_id]
            if phase.status != "pending":
                logger.error(f"Invalid phase status {phase.status} for preparation")
                return False

            # Add validator signature and DID attestation
            phase.validation_signatures.add(validator_id)
            phase.did_attestations.add(validator_did.get_did())
            
            # Check governance rules if system is available
            if self.governance_system and not self.governance_system.validate_cross_shard_action(
                validator_did, 
                self.primary_transaction
            ):
                logger.error(f"Governance validation failed for transaction {self.transaction_id}")
                return False
            
            # Check if we have enough validations
            if len(phase.validation_signatures) >= self.required_validations:
                phase.status = "prepared"
                logger.info(f"Shard {shard_id} prepared for transaction {self.transaction_id}")
                return True
                
            return False

        except Exception as e:
            logger.error(f"Error in prepare phase: {str(e)}")
            return False

    def commit_phase(self, shard_id: int, validator_id: str, validator_did: BaseDID) -> bool:
        """
        Commit phase of the two-phase commit protocol with DID verification.
        
        Args:
            shard_id: ID of the shard being committed
            validator_id: ID of the validating node
            validator_did: DID of the validator for verification
            
        Returns:
            bool: True if commit successful
        """
        try:
            if shard_id not in self.phases:
                return False

            phase = self.phases[shard_id]
            if phase.status != "prepared":
                return False

            # Verify validator DID and add signature
            if not validator_did.verify():
                logger.error(f"Invalid validator DID for commit: {validator_id}")
                return False

            phase.validation_signatures.add(validator_id)
            phase.did_attestations.add(validator_did.get_did())
            
            # Check if we have enough validations
            if len(phase.validation_signatures) >= self.required_validations:
                phase.status = "committed"
                self._check_completion()
                return True
                
            return False

        except Exception as e:
            logger.error(f"Error in commit phase: {str(e)}")
            return False

    def abort_phase(self, shard_id: int, reason: str, validator_did: Optional[BaseDID] = None) -> None:
        """
        Abort the transaction phase for a shard with optional DID attestation.
        
        Args:
            shard_id: ID of the shard to abort
            reason: Reason for abortion
            validator_did: Optional DID of the validator initiating abort
        """
        try:
            if shard_id in self.phases:
                phase = self.phases[shard_id]
                phase.status = "aborted"
                
                if validator_did:
                    phase.did_attestations.add(validator_did.get_did())
                    
                # Notify governance system if available
                if self.governance_system:
                    self.governance_system.record_cross_shard_abort(
                        self.transaction_id,
                        shard_id,
                        reason,
                        validator_did.get_did() if validator_did else None
                    )
                
                logger.warning(f"Aborted phase {phase.phase_id} for shard {shard_id}: {reason}")
                self.state = "aborted"

        except Exception as e:
            logger.error(f"Error aborting phase: {str(e)}")

    def _check_completion(self) -> None:
        """Check if all phases are committed and update transaction state."""
        try:
            if all(phase.status == "committed" for phase in self.phases.values()):
                self.state = "completed"
                self.completed_at = datetime.now()
                
                # Notify governance system of successful completion
                if self.governance_system:
                    self.governance_system.record_cross_shard_completion(
                        self.transaction_id,
                        list(self.phases.keys()),
                        {phase_id: list(phase.did_attestations) 
                         for phase_id, phase in self.phases.items()}
                    )
                
                logger.info(f"Transaction {self.transaction_id} completed successfully")

        except Exception as e:
            logger.error(f"Error checking completion: {str(e)}")

    def to_dict(self) -> Dict:
        """Convert transaction to dictionary format."""
        return {
            "transaction_id": self.transaction_id,
            "source_shard": self.source_shard,
            "target_shards": list(self.target_shards),
            "primary_transaction": self.primary_transaction.to_dict(),
            "state": self.state,
            "created_at": self.created_at.isoformat(),
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "phases": {
                shard_id: {
                    "phase_id": phase.phase_id,
                    "status": phase.status,
                    "timestamp": phase.timestamp.isoformat(),
                    "validation_signatures": list(phase.validation_signatures),
                    "did_attestations": list(phase.did_attestations)
                }
                for shard_id, phase in self.phases.items()
            }
        }

    @classmethod
    def from_dict(cls, data: Dict, governance_system: Optional[GovernanceSystem] = None) -> 'CrossShardTransaction':
        """Create transaction from dictionary format."""
        transaction = cls(
            transaction_id=data["transaction_id"],
            source_shard=data["source_shard"],
            target_shards=set(data["target_shards"]),
            primary_transaction=Transaction.from_dict(data["primary_transaction"]),
            governance_system=governance_system
        )
        
        transaction.state = data["state"]
        transaction.created_at = datetime.fromisoformat(data["created_at"])
        if data["completed_at"]:
            transaction.completed_at = datetime.fromisoformat(data["completed_at"])
            
        # Restore phases
        for shard_id, phase_data in data["phases"].items():
            phase = CrossShardTransactionPhase(
                phase_id=phase_data["phase_id"],
                shard_id=int(shard_id),
                status=phase_data["status"],
                timestamp=datetime.fromisoformat(phase_data["timestamp"])
            )
            phase.validation_signatures = set(phase_data["validation_signatures"])
            phase.did_attestations = set(phase_data["did_attestations"])
            transaction.phases[int(shard_id)] = phase
            
        return transaction

    def _generate_phase_id(self, shard_id: int) -> str:
        """Generate a unique ID for a transaction phase."""
        data = f"{self.transaction_id}:{shard_id}:{datetime.now().isoformat()}"
        return hashlib.sha256(data.encode()).hexdigest()