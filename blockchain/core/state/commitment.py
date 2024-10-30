"""
blockchain/core/state/commitment.py

Manages state commitments and cross-shard state consistency in the ICN blockchain.
Coordinates with consensus mechanism to ensure state finality and validation.
"""

import asyncio
import logging
from typing import Dict, List, Optional, Set, Any, Tuple
from dataclasses import dataclass, field
from datetime import datetime
import hashlib
import json

from .validation import StateValidator, ValidationContext
from ...consensus.proof_of_cooperation import ProofOfCooperation
from ...monitoring.cooperative_metrics import CooperativeMetricsMonitor
from ..block import Block
from ..transaction import Transaction

logger = logging.getLogger(__name__)

@dataclass
class StateCommitment:
    """Represents a commitment to a specific state."""
    commitment_id: str
    block_height: int
    shard_id: Optional[int]
    state_root: str
    validator_signatures: Set[str] = field(default_factory=set)
    cross_shard_refs: Set[str] = field(default_factory=set)
    timestamp: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class CommitmentVerification:
    """Result of a commitment verification."""
    is_valid: bool
    commitment: StateCommitment
    verification_type: str
    validator_id: str
    timestamp: datetime = field(default_factory=datetime.now)
    reason: Optional[str] = None

class StateCommitmentManager:
    """
    Manages state commitments and cross-shard state consistency.
    
    Key responsibilities:
    - Create and verify state commitments
    - Coordinate cross-shard state consistency
    - Integrate with consensus for commitment finalization
    - Support state validation process
    """

    def __init__(
        self,
        state_validator: StateValidator,
        consensus: ProofOfCooperation,
        metrics_monitor: CooperativeMetricsMonitor,
        min_signatures: int = 3
    ):
        """Initialize the commitment manager."""
        self.state_validator = state_validator
        self.consensus = consensus
        self.metrics_monitor = metrics_monitor
        self.min_signatures = min_signatures
        
        # Track commitments and verifications
        self.commitments: Dict[str, StateCommitment] = {}
        self.pending_verifications: Dict[str, Set[str]] = {}  # commitment_id -> validator_ids
        self.finalized_commitments: Dict[int, Dict[int, str]] = {}  # height -> shard_id -> commitment_id
        self.cross_shard_dependencies: Dict[str, Set[str]] = {}  # commitment_id -> dependent_commitment_ids

    async def create_commitment(
        self,
        state: Dict[str, Any],
        block_height: int,
        shard_id: Optional[int],
        validator_id: str
    ) -> Optional[StateCommitment]:
        """
        Create a new state commitment.
        
        Args:
            state: The state to commit
            block_height: Current block height
            shard_id: Optional shard ID
            validator_id: ID of the committing validator
            
        Returns:
            Optional[StateCommitment]: The created commitment if successful
        """
        try:
            # Calculate state root
            state_root = self._calculate_state_root(state)
            
            # Create commitment ID
            commitment_id = self._generate_commitment_id(
                state_root,
                block_height,
                shard_id
            )
            
            # Check for existing commitment
            if commitment_id in self.commitments:
                existing = self.commitments[commitment_id]
                existing.validator_signatures.add(validator_id)
                return existing
            
            # Create new commitment
            commitment = StateCommitment(
                commitment_id=commitment_id,
                block_height=block_height,
                shard_id=shard_id,
                state_root=state_root
            )
            commitment.validator_signatures.add(validator_id)
            
            # Extract cross-shard references
            cross_refs = self._extract_cross_shard_refs(state)
            commitment.cross_shard_refs = cross_refs
            
            # Store commitment
            self.commitments[commitment_id] = commitment
            self.pending_verifications[commitment_id] = set()
            
            # Track cross-shard dependencies
            for ref in cross_refs:
                if ref not in self.cross_shard_dependencies:
                    self.cross_shard_dependencies[ref] = set()
                self.cross_shard_dependencies[ref].add(commitment_id)
            
            # Record metrics
            self.metrics_monitor.record_metric(
                participant_id=validator_id,
                metric_type="commitment_creation",
                value=1.0,
                metadata={
                    "commitment_id": commitment_id,
                    "block_height": block_height,
                    "shard_id": shard_id
                }
            )
            
            return commitment

        except Exception as e:
            logger.error(f"Error creating commitment: {str(e)}")
            return None

    async def verify_commitment(
        self,
        commitment: StateCommitment,
        state: Dict[str, Any],
        validator_id: str
    ) -> CommitmentVerification:
        """
        Verify a state commitment.
        
        Args:
            commitment: The commitment to verify
            state: The state to verify against
            validator_id: ID of the verifying validator
            
        Returns:
            CommitmentVerification: The verification result
        """
        try:
            # Verify state root
            calculated_root = self._calculate_state_root(state)
            if calculated_root != commitment.state_root:
                return CommitmentVerification(
                    is_valid=False,
                    commitment=commitment,
                    verification_type="root_mismatch",
                    validator_id=validator_id,
                    reason="State root mismatch"
                )
            
            # Create validation context
            context = ValidationContext(
                block_height=commitment.block_height,
                shard_id=commitment.shard_id,
                validator_id=validator_id,
                cross_shard_refs=commitment.cross_shard_refs
            )
            
            # Validate state
            validation_result = await self.state_validator.validate_state_transition(
                {},  # No old state for commitment verification
                state,
                context
            )
            
            if not validation_result.is_valid:
                return CommitmentVerification(
                    is_valid=False,
                    commitment=commitment,
                    verification_type="state_invalid",
                    validator_id=validator_id,
                    reason=validation_result.reason
                )
            
            # Add verification signature
            commitment.validator_signatures.add(validator_id)
            self.pending_verifications[commitment.commitment_id].add(validator_id)
            
            # Check for finalization
            await self._check_commitment_finalization(commitment)
            
            # Record metrics
            self.metrics_monitor.record_metric(
                participant_id=validator_id,
                metric_type="commitment_verification",
                value=1.0,
                metadata={
                    "commitment_id": commitment.commitment_id,
                    "verification_type": "success"
                }
            )
            
            return CommitmentVerification(
                is_valid=True,
                commitment=commitment,
                verification_type="success",
                validator_id=validator_id
            )

        except Exception as e:
            logger.error(f"Error verifying commitment: {str(e)}")
            return CommitmentVerification(
                is_valid=False,
                commitment=commitment,
                verification_type="error",
                validator_id=validator_id,
                reason=str(e)
            )

    async def _check_commitment_finalization(self, commitment: StateCommitment) -> None:
        """Check if a commitment can be finalized."""
        try:
            # Check minimum signatures
            if len(commitment.validator_signatures) < self.min_signatures:
                return
            
            # Check cross-shard dependencies
            for ref in commitment.cross_shard_refs:
                if not await self._verify_cross_shard_ref(ref, commitment.block_height):
                    return
            
            # Initialize height tracking if needed
            if commitment.block_height not in self.finalized_commitments:
                self.finalized_commitments[commitment.block_height] = {}
            
            # Record finalized commitment
            if commitment.shard_id is not None:
                self.finalized_commitments[commitment.block_height][commitment.shard_id] = commitment.commitment_id
            
            # Clean up tracking
            if commitment.commitment_id in self.pending_verifications:
                del self.pending_verifications[commitment.commitment_id]
            
            # Record metrics
            self.metrics_monitor.record_metric(
                participant_id="system",
                metric_type="commitment_finalization",
                value=1.0,
                metadata={
                    "commitment_id": commitment.commitment_id,
                    "block_height": commitment.block_height,
                    "shard_id": commitment.shard_id
                }
            )

        except Exception as e:
            logger.error(f"Error checking commitment finalization: {str(e)}")

    async def _verify_cross_shard_ref(
        self,
        ref: str,
        block_height: int
    ) -> bool:
        """Verify a cross-shard reference."""
        try:
            ref_parts = ref.split(':')
            if len(ref_parts) != 2:
                return False
                
            shard_id = int(ref_parts[0])
            
            # Check if referenced shard has finalized commitment
            shard_commitments = self.finalized_commitments.get(block_height, {})
            return shard_id in shard_commitments
            
        except Exception as e:
            logger.error(f"Error verifying cross-shard reference: {str(e)}")
            return False

    def _calculate_state_root(self, state: Dict[str, Any]) -> str:
        """Calculate Merkle root of state."""
        try:
            # Convert state to canonical form
            canonical_state = json.dumps(state, sort_keys=True)
            return hashlib.sha256(canonical_state.encode()).hexdigest()
        except Exception as e:
            logger.error(f"Error calculating state root: {str(e)}")
            return ""

    def _generate_commitment_id(
        self,
        state_root: str,
        block_height: int,
        shard_id: Optional[int]
    ) -> str:
        """Generate unique commitment ID."""
        try:
            commitment_data = f"{state_root}:{block_height}:{shard_id}"
            return hashlib.sha256(commitment_data.encode()).hexdigest()
        except Exception as e:
            logger.error(f"Error generating commitment ID: {str(e)}")
            return ""

    def _extract_cross_shard_refs(self, state: Dict[str, Any]) -> Set[str]:
        """Extract cross-shard references from state."""
        try:
            refs = set()
            if "cross_shard_refs" in state:
                refs.update(state["cross_shard_refs"])
            return refs
        except Exception as e:
            logger.error(f"Error extracting cross-shard refs: {str(e)}")
            return set()

    def get_commitment_status(self, commitment_id: str) -> Dict[str, Any]:
        """Get detailed status of a commitment."""
        try:
            if commitment_id not in self.commitments:
                return {
                    "exists": False,
                    "status": "unknown"
                }
            
            commitment = self.commitments[commitment_id]
            is_finalized = (
                commitment.block_height in self.finalized_commitments and
                commitment.shard_id in self.finalized_commitments[commitment.block_height] and
                self.finalized_commitments[commitment.block_height][commitment.shard_id] == commitment_id
            )
            
            return {
                "exists": True,
                "status": "finalized" if is_finalized else "pending",
                "validator_count": len(commitment.validator_signatures),
                "cross_shard_refs": len(commitment.cross_shard_refs),
                "block_height": commitment.block_height,
                "shard_id": commitment.shard_id,
                "timestamp": commitment.timestamp.isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error getting commitment status: {str(e)}")
            return {
                "exists": False,
                "status": "error",
                "error": str(e)
            }

    def get_finalization_metrics(self) -> Dict[str, Any]:
        """Get commitment finalization metrics."""
        try:
            total_commitments = len(self.commitments)
            finalized_count = sum(
                len(shards)
                for shards in self.finalized_commitments.values()
            )
            
            return {
                "total_commitments": total_commitments,
                "finalized_commitments": finalized_count,
                "pending_verifications": len(self.pending_verifications),
                "cross_shard_dependencies": len(self.cross_shard_dependencies),
                "finalization_rate": (
                    finalized_count / total_commitments
                    if total_commitments > 0 else 0.0
                )
            }
            
        except Exception as e:
            logger.error(f"Error getting finalization metrics: {str(e)}")
            return {}