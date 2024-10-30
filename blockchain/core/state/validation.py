"""
blockchain/core/state/validation.py

Implements enhanced state validation for the ICN blockchain.
Provides comprehensive validation of state transitions with support for
cooperative principles and cross-shard consistency.
"""

import logging
from typing import Dict, List, Optional, Set, Any, Tuple
from dataclasses import dataclass, field
from datetime import datetime
import hashlib
import json

from ..transaction import Transaction
from ..block import Block
from ...monitoring.cooperative_metrics import CooperativeMetricsMonitor

logger = logging.getLogger(__name__)

@dataclass
class ValidationContext:
    """Contextual information for state validation."""
    block_height: int
    shard_id: Optional[int]
    validator_id: str
    timestamp: datetime = field(default_factory=datetime.now)
    metrics: Dict[str, float] = field(default_factory=dict)
    cross_shard_refs: Set[str] = field(default_factory=set)

@dataclass
class StateTransitionValidation:
    """Results of a state transition validation."""
    is_valid: bool
    validation_type: str
    context: ValidationContext
    reason: Optional[str] = None
    affected_states: Dict[str, Any] = field(default_factory=dict)
    resource_impacts: Dict[str, float] = field(default_factory=dict)

class StateValidator:
    """
    Validates state transitions in the ICN blockchain.
    
    Key Features:
    - Comprehensive state transition validation
    - Cooperative principle enforcement
    - Cross-shard consistency checks
    - Resource impact analysis
    - Integration with metrics monitoring
    """

    def __init__(self, metrics_monitor: CooperativeMetricsMonitor):
        """Initialize the state validator."""
        self.metrics_monitor = metrics_monitor
        self.validation_history: List[StateTransitionValidation] = []
        self.recent_validations: Dict[str, List[ValidationContext]] = {}
        self.verification_thresholds = {
            "cooperative_balance": 0.8,  # 80% cooperative balance required
            "resource_variance": 0.2,    # 20% max resource variance allowed
            "participation_rate": 0.7    # 70% minimum participation rate
        }

    async def validate_state_transition(
        self,
        old_state: Dict[str, Any],
        new_state: Dict[str, Any],
        transition_context: ValidationContext
    ) -> StateTransitionValidation:
        """
        Validate a state transition for compliance with ICN principles.
        
        Args:
            old_state: Previous state
            new_state: Proposed new state
            transition_context: Context for the validation
            
        Returns:
            StateTransitionValidation: Validation results
        """
        try:
            # Verify basic state integrity
            if not self._verify_state_integrity(old_state, new_state):
                return StateTransitionValidation(
                    is_valid=False,
                    validation_type="integrity",
                    context=transition_context,
                    reason="State integrity check failed"
                )

            # Verify cooperative principles
            cooperative_validation = await self._verify_cooperative_principles(
                old_state,
                new_state,
                transition_context
            )
            if not cooperative_validation.is_valid:
                return cooperative_validation

            # Verify resource distribution
            resource_validation = await self._verify_resource_distribution(
                old_state,
                new_state,
                transition_context
            )
            if not resource_validation.is_valid:
                return resource_validation

            # Verify cross-shard consistency if applicable
            if transition_context.cross_shard_refs:
                shard_validation = await self._verify_cross_shard_consistency(
                    old_state,
                    new_state,
                    transition_context
                )
                if not shard_validation.is_valid:
                    return shard_validation

            # Calculate resource impacts
            resource_impacts = self._calculate_resource_impacts(
                old_state,
                new_state
            )

            # Record successful validation
            validation = StateTransitionValidation(
                is_valid=True,
                validation_type="complete",
                context=transition_context,
                affected_states=self._get_affected_states(old_state, new_state),
                resource_impacts=resource_impacts
            )

            # Update metrics
            await self._update_validation_metrics(validation)

            return validation

        except Exception as e:
            logger.error(f"Error validating state transition: {str(e)}")
            return StateTransitionValidation(
                is_valid=False,
                validation_type="error",
                context=transition_context,
                reason=str(e)
            )

    def _verify_state_integrity(
        self,
        old_state: Dict[str, Any],
        new_state: Dict[str, Any]
    ) -> bool:
        """Verify the basic integrity of states."""
        try:
            # Check state structure
            if not isinstance(old_state, dict) or not isinstance(new_state, dict):
                return False

            # Verify required fields
            required_fields = {"accounts", "metadata", "version"}
            if not all(field in old_state for field in required_fields):
                return False
            if not all(field in new_state for field in required_fields):
                return False

            # Verify version continuity
            if new_state["version"] < old_state["version"]:
                return False

            return True

        except Exception as e:
            logger.error(f"Error verifying state integrity: {str(e)}")
            return False

    async def _verify_cooperative_principles(
        self,
        old_state: Dict[str, Any],
        new_state: Dict[str, Any],
        context: ValidationContext
    ) -> StateTransitionValidation:
        """
        Verify that state transition adheres to cooperative principles.
        
        Checks:
        - Fair resource distribution
        - Participation balance
        - Cooperative growth metrics
        """
        try:
            # Calculate cooperative metrics
            metrics = {
                "resource_balance": self._calculate_resource_balance(new_state),
                "participation_rate": self._calculate_participation_rate(new_state),
                "cooperative_growth": self._calculate_cooperative_growth(
                    old_state,
                    new_state
                )
            }

            # Check against thresholds
            if metrics["resource_balance"] < self.verification_thresholds["cooperative_balance"]:
                return StateTransitionValidation(
                    is_valid=False,
                    validation_type="cooperative",
                    context=context,
                    reason="Insufficient resource balance"
                )

            if metrics["participation_rate"] < self.verification_thresholds["participation_rate"]:
                return StateTransitionValidation(
                    is_valid=False,
                    validation_type="cooperative",
                    context=context,
                    reason="Insufficient participation rate"
                )

            # Update cooperative metrics
            self.metrics_monitor.record_metric(
                participant_id=context.validator_id,
                metric_type="cooperative_balance",
                value=metrics["resource_balance"]
            )

            return StateTransitionValidation(
                is_valid=True,
                validation_type="cooperative",
                context=context,
                metrics=metrics
            )

        except Exception as e:
            logger.error(f"Error verifying cooperative principles: {str(e)}")
            return StateTransitionValidation(
                is_valid=False,
                validation_type="cooperative",
                context=context,
                reason=str(e)
            )

    async def _verify_resource_distribution(
        self,
        old_state: Dict[str, Any],
        new_state: Dict[str, Any],
        context: ValidationContext
    ) -> StateTransitionValidation:
        """
        Verify fair resource distribution in state transition.
        
        Checks:
        - Resource concentration limits
        - Distribution variance
        - Access equality
        """
        try:
            # Calculate resource metrics
            resource_variance = self._calculate_resource_variance(
                old_state,
                new_state
            )

            if resource_variance > self.verification_thresholds["resource_variance"]:
                return StateTransitionValidation(
                    is_valid=False,
                    validation_type="resource",
                    context=context,
                    reason="Resource variance exceeds threshold"
                )

            # Calculate resource impacts
            impacts = self._calculate_resource_impacts(old_state, new_state)

            # Record resource metrics
            self.metrics_monitor.record_metric(
                participant_id=context.validator_id,
                metric_type="resource_distribution",
                value=1.0 - resource_variance,
                metadata={"impacts": impacts}
            )

            return StateTransitionValidation(
                is_valid=True,
                validation_type="resource",
                context=context,
                resource_impacts=impacts
            )

        except Exception as e:
            logger.error(f"Error verifying resource distribution: {str(e)}")
            return StateTransitionValidation(
                is_valid=False,
                validation_type="resource",
                context=context,
                reason=str(e)
            )

    async def _verify_cross_shard_consistency(
        self,
        old_state: Dict[str, Any],
        new_state: Dict[str, Any],
        context: ValidationContext
    ) -> StateTransitionValidation:
        """Verify consistency across referenced shards."""
        try:
            for ref in context.cross_shard_refs:
                if not await self._verify_shard_reference(ref, new_state):
                    return StateTransitionValidation(
                        is_valid=False,
                        validation_type="cross_shard",
                        context=context,
                        reason=f"Invalid cross-shard reference: {ref}"
                    )

            return StateTransitionValidation(
                is_valid=True,
                validation_type="cross_shard",
                context=context
            )

        except Exception as e:
            logger.error(f"Error verifying cross-shard consistency: {str(e)}")
            return StateTransitionValidation(
                is_valid=False,
                validation_type="cross_shard",
                context=context,
                reason=str(e)
            )

    def _calculate_resource_balance(self, state: Dict[str, Any]) -> float:
        """Calculate the resource balance coefficient."""
        try:
            resources = [
                account.get("resources", 0)
                for account in state.get("accounts", {}).values()
            ]
            
            if not resources:
                return 1.0

            avg_resources = sum(resources) / len(resources)
            max_deviation = max(abs(r - avg_resources) for r in resources)
            
            return 1.0 - (max_deviation / avg_resources if avg_resources > 0 else 0)

        except Exception as e:
            logger.error(f"Error calculating resource balance: {str(e)}")
            return 0.0

    def _calculate_participation_rate(self, state: Dict[str, Any]) -> float:
        """Calculate the participation rate across accounts."""
        try:
            total_accounts = len(state.get("accounts", {}))
            if total_accounts == 0:
                return 0.0

            active_accounts = sum(
                1 for account in state["accounts"].values()
                if account.get("last_active", 0) > 0
            )

            return active_accounts / total_accounts

        except Exception as e:
            logger.error(f"Error calculating participation rate: {str(e)}")
            return 0.0

    def _calculate_cooperative_growth(
        self,
        old_state: Dict[str, Any],
        new_state: Dict[str, Any]
    ) -> float:
        """Calculate cooperative growth metrics between states."""
        try:
            old_metrics = self._extract_cooperative_metrics(old_state)
            new_metrics = self._extract_cooperative_metrics(new_state)

            if old_metrics["total_participants"] == 0:
                return 1.0

            growth_rate = (
                new_metrics["total_participants"] -
                old_metrics["total_participants"]
            ) / old_metrics["total_participants"]

            return max(0.0, min(1.0, growth_rate + 1.0))

        except Exception as e:
            logger.error(f"Error calculating cooperative growth: {str(e)}")
            return 0.0

    def _extract_cooperative_metrics(self, state: Dict[str, Any]) -> Dict[str, float]:
        """Extract cooperative metrics from state."""
        return {
            "total_participants": len(state.get("accounts", {})),
            "total_resources": sum(
                account.get("resources", 0)
                for account in state.get("accounts", {}).values()
            ),
            "active_participants": sum(
                1 for account in state.get("accounts", {}).values()
                if account.get("last_active", 0) > 0
            )
        }

    def _calculate_resource_impacts(
        self,
        old_state: Dict[str, Any],
        new_state: Dict[str, Any]
    ) -> Dict[str, float]:
        """Calculate resource impact metrics for state transition."""
        try:
            impacts = {}
            resource_types = {"computation", "storage", "bandwidth", "energy"}

            for resource in resource_types:
                old_usage = sum(
                    account.get("resources", {}).get(resource, 0)
                    for account in old_state.get("accounts", {}).values()
                )
                new_usage = sum(
                    account.get("resources", {}).get(resource, 0)
                    for account in new_state.get("accounts", {}).values()
                )
                impacts[resource] = new_usage - old_usage

            return impacts

        except Exception as e:
            logger.error(f"Error calculating resource impacts: {str(e)}")
            return {}

    def _get_affected_states(
        self,
        old_state: Dict[str, Any],
        new_state: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Get affected state components."""
        try:
            affected = {}
            for key in old_state:
                if key in new_state and old_state[key] != new_state[key]:
                    affected[key] = {
                        "old": old_state[key],
                        "new": new_state[key]
                    }
            
            return affected

        except Exception as e:
            logger.error(f"Error getting affected states: {str(e)}")
            return {}

    async def _verify_shard_reference(
        self,
        ref: str,
        state: Dict[str, Any]
    ) -> bool:
        """Verify a cross-shard reference."""
        try:
            # Parse shard reference
            ref_parts = ref.split(':')
            if len(ref_parts) != 2:
                return False

            shard_id, ref_id = ref_parts
            shard_id = int(shard_id)

            # Verify reference exists in state
            if 'cross_shard_refs' not in state:
                return False

            if ref not in state['cross_shard_refs']:
                return False

            return True

        except Exception as e:
            logger.error(f"Error verifying shard reference: {str(e)}")
            return False

    async def _update_validation_metrics(self, validation: StateTransitionValidation) -> None:
        """Update metrics based on validation results."""
        try:
            # Record validation result
            self.validation_history.append(validation)

            # Update recent validations
            validator_id = validation.context.validator_id
            if validator_id not in self.recent_validations:
                self.recent_validations[validator_id] = []
            
            self.recent_validations[validator_id].append(validation.context)

            # Trim history if needed
            if len(self.validation_history) > 1000:
                self.validation_history = self.validation_history[-1000:]

            if len(self.recent_validations[validator_id]) > 100:
                self.recent_validations[validator_id] = self.recent_validations[validator_id][-100:]

            # Record metrics
            self.metrics_monitor.record_metric(
                participant_id=validator_id,
                metric_type="state_validation",
                value=1.0 if validation.is_valid else 0.0,
                metadata={
                    "validation_type": validation.validation_type,
                    "block_height": validation.context.block_height,
                    "shard_id": validation.context.shard_id
                }
            )

        except Exception as e:
            logger.error(f"Error updating validation metrics: {str(e)}")

    def get_validation_stats(self) -> Dict[str, Any]:
        """Get validation statistics."""
        try:
            total_validations = len(self.validation_history)
            if total_validations == 0:
                return {
                    "total_validations": 0,
                    "success_rate": 0.0,
                    "validation_types": {},
                    "active_validators": 0
                }

            successful = sum(1 for v in self.validation_history if v.is_valid)
            
            # Count validation types
            validation_types = {}
            for validation in self.validation_history:
                vtype = validation.validation_type
                if vtype not in validation_types:
                    validation_types[vtype] = 0
                validation_types[vtype] += 1

            return {
                "total_validations": total_validations,
                "success_rate": successful / total_validations,
                "validation_types": validation_types,
                "active_validators": len(self.recent_validations),
                "recent_success_rate": self._calculate_recent_success_rate()
            }

        except Exception as e:
            logger.error(f"Error getting validation stats: {str(e)}")
            return {}

    def _calculate_recent_success_rate(self) -> float:
        """Calculate success rate for recent validations."""
        try:
            recent_count = 100
            recent = self.validation_history[-recent_count:] if len(self.validation_history) >= recent_count else self.validation_history
            
            if not recent:
                return 0.0

            successful = sum(1 for v in recent if v.is_valid)
            return successful / len(recent)

        except Exception as e:
            logger.error(f"Error calculating recent success rate: {str(e)}")
            return 0.0

    def export_validation_history(self) -> Dict[str, Any]:
        """Export validation history for analysis."""
        try:
            return {
                "validations": [
                    {
                        "is_valid": v.is_valid,
                        "type": v.validation_type,
                        "context": {
                            "block_height": v.context.block_height,
                            "shard_id": v.context.shard_id,
                            "validator_id": v.context.validator_id,
                            "timestamp": v.context.timestamp.isoformat(),
                            "metrics": v.context.metrics,
                            "cross_shard_refs": list(v.context.cross_shard_refs)
                        },
                        "reason": v.reason,
                        "resource_impacts": v.resource_impacts
                    }
                    for v in self.validation_history
                ],
                "validator_stats": {
                    validator_id: [
                        {
                            "block_height": ctx.block_height,
                            "shard_id": ctx.shard_id,
                            "timestamp": ctx.timestamp.isoformat(),
                            "metrics": ctx.metrics
                        }
                        for ctx in contexts
                    ]
                    for validator_id, contexts in self.recent_validations.items()
                },
                "thresholds": self.verification_thresholds
            }

        except Exception as e:
            logger.error(f"Error exporting validation history: {str(e)}")
            return {}