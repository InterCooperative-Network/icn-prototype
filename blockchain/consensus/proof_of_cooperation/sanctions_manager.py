"""
sanctions_manager.py

This module manages sanctions and recovery for validators in the Proof of Cooperation (PoC) consensus mechanism.
It handles tiered penalties, escalated sanctions, and reputation recovery, and allows for integration 
with governance for handling disputes and appeals.

Classes:
    SanctionsManager
"""

from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
import logging
from .types import Node
from .collusion_detector import CollusionDetector
from .reputation_manager import ReputationManager

logger = logging.getLogger(__name__)

class SanctionsManager:
    """
    Manages sanctions and recovery for validators within the PoC mechanism.
    
    Key Responsibilities:
    - Enforcing tiered sanctions based on collusion risk and frequency of offenses.
    - Allowing validators to recover from penalties through positive behavior.
    - Providing integration points for governance to handle disputes and appeals.
    """

    def __init__(self, collusion_detector: CollusionDetector, reputation_manager: ReputationManager, max_sanction_level: int = 3, recovery_period: int = 7):
        """
        Initialize the SanctionsManager with collusion detection and reputation management integration.

        Args:
            collusion_detector (CollusionDetector): Instance of the collusion detector for integration.
            reputation_manager (ReputationManager): Instance of the reputation manager for integration.
            max_sanction_level (int): Maximum level of sanctions before permanent exclusion (default is 3).
            recovery_period (int): Number of days required for a validator to demonstrate positive behavior for recovery.
        """
        self.collusion_detector = collusion_detector
        self.reputation_manager = reputation_manager
        self.max_sanction_level = max_sanction_level
        self.recovery_period = timedelta(days=recovery_period)
        self.sanctions: Dict[str, int] = {}  # Maps validator IDs to sanction levels
        self.recovery_timers: Dict[str, datetime] = {}  # Maps validator IDs to recovery start times

    def apply_sanction(self, node: Node) -> None:
        """
        Apply a sanction to a validator based on collusion detection outcomes.

        Args:
            node (Node): The validator node to sanction.
        """
        validator_id = node.node_id

        # Increase sanction level for the validator
        current_sanction_level = self.sanctions.get(validator_id, 0) + 1
        self.sanctions[validator_id] = min(current_sanction_level, self.max_sanction_level)

        # Apply reputation slashing based on sanction level
        reputation_penalty = self._calculate_reputation_penalty(current_sanction_level)
        self.reputation_manager.update_validator_reputation(node, -reputation_penalty)

        # Start or reset the recovery timer
        self.recovery_timers[validator_id] = datetime.now()

        # Log the sanction
        logger.info(f"Sanction applied to validator {validator_id}: Level {current_sanction_level}, Reputation penalty: {reputation_penalty}")

        # If maximum sanction level reached, consider permanent exclusion
        if current_sanction_level >= self.max_sanction_level:
            self._handle_permanent_exclusion(node)

    def _calculate_reputation_penalty(self, sanction_level: int) -> float:
        """
        Calculate the reputation penalty based on the current sanction level.

        Args:
            sanction_level (int): The level of the sanction.

        Returns:
            float: The reputation penalty to apply.
        """
        base_penalty = 5.0  # Base penalty for the first level of sanctions
        penalty_multiplier = 1.5  # Multiplier for each additional level
        return base_penalty * (penalty_multiplier ** (sanction_level - 1))

    def _handle_permanent_exclusion(self, node: Node) -> None:
        """
        Handle permanent exclusion for a validator that has reached the maximum sanction level.

        Args:
            node (Node): The validator node to exclude.
        """
        validator_id = node.node_id

        # Mark the validator as permanently excluded
        node.metadata["status"] = "permanently_excluded"
        node.reputation = 0.0  # Set reputation to zero
        node.cooldown = float('inf')  # Indefinite cooldown

        # Log the permanent exclusion
        logger.warning(f"Validator {validator_id} has been permanently excluded from the network.")

    def evaluate_recovery(self, node: Node) -> bool:
        """
        Evaluate if a validator is eligible for recovery based on positive behavior.

        Args:
            node (Node): The validator node to evaluate.

        Returns:
            bool: True if recovery is successful, False otherwise.
        """
        validator_id = node.node_id
        last_recovery_start = self.recovery_timers.get(validator_id)

        # Check if the validator has demonstrated positive behavior over the recovery period
        if last_recovery_start and datetime.now() - last_recovery_start >= self.recovery_period:
            if self._is_behavior_positive(node):
                self._recover_validator(node)
                return True

        return False

    def _is_behavior_positive(self, node: Node) -> bool:
        """
        Check if a validator's recent behavior is positive, warranting recovery.

        Criteria:
        - No new collusion detections.
        - Successful validation rate above the minimum threshold.

        Args:
            node (Node): The validator node to check.

        Returns:
            bool: True if behavior is positive, False otherwise.
        """
        risk_score = self.collusion_detector._calculate_risk_score(node)
        if risk_score > 0.5:
            return False  # High collusion risk disqualifies recovery

        recent_validations = node.validation_history[-50:]
        successful_validations = sum(1 for v in recent_validations if v.get("evidence", {}).get("success", False))
        success_rate = successful_validations / len(recent_validations) if recent_validations else 0

        return success_rate >= self.reputation_manager.config.validation_thresholds["min_success_rate"]

    def _recover_validator(self, node: Node) -> None:
        """
        Recover a validator from sanctions, reducing the sanction level and restoring reputation.

        Args:
            node (Node): The validator node to recover.
        """
        validator_id = node.node_id

        # Decrease the sanction level and restore reputation
        current_sanction_level = self.sanctions.get(validator_id, 0)
        if current_sanction_level > 0:
            self.sanctions[validator_id] = current_sanction_level - 1

        # Restore reputation based on recovery
        recovery_bonus = self._calculate_recovery_bonus(current_sanction_level)
        self.reputation_manager.update_validator_reputation(node, recovery_bonus)

        # Reset recovery timer
        self.recovery_timers[validator_id] = datetime.now()

        # Log the recovery
        logger.info(f"Validator {validator_id} has recovered: Sanction level decreased to {current_sanction_level - 1}, Reputation bonus: {recovery_bonus}")

    def _calculate_recovery_bonus(self, sanction_level: int) -> float:
        """
        Calculate the reputation bonus for recovering from sanctions.

        Args:
            sanction_level (int): The level of the sanction being recovered from.

        Returns:
            float: The reputation bonus to apply.
        """
        base_bonus = 3.0  # Base bonus for the first level of recovery
        bonus_multiplier = 1.2  # Multiplier for each level of recovery
        return base_bonus * (bonus_multiplier ** sanction_level)

    def get_sanction_status(self, node: Node) -> Tuple[int, str]:
        """
        Get the current sanction status of a validator, including level and status.

        Args:
            node (Node): The validator node to check.

        Returns:
            Tuple[int, str]: A tuple containing the sanction level and status.
        """
        validator_id = node.node_id
        sanction_level = self.sanctions.get(validator_id, 0)
        status = node.metadata.get("status", "active")

        return sanction_level, status

    def handle_dispute(self, node: Node) -> None:
        """
        Handle disputes regarding sanctions, allowing validators to appeal their penalties 
        through governance mechanisms.

        Args:
            node (Node): The validator node appealing the sanction.
        """
        validator_id = node.node_id
        # Placeholder for governance-based dispute resolution
        logger.info(f"Validator {validator_id} has initiated a dispute against sanctions.")
        # Future implementation: Integrate with governance for proposals and voting on appeals

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the sanctions manager's state to a dictionary for serialization.

        Returns:
            Dict[str, Any]: Dictionary representation of the sanctions manager's state.
        """
        return {
            "sanctions": self.sanctions,
            "recovery_timers": {k: v.isoformat() for k, v in self.recovery_timers.items()}
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any], collusion_detector: CollusionDetector, reputation_manager: ReputationManager) -> 'SanctionsManager':
        """
        Create a sanctions manager instance from a dictionary of data.

        Args:
            data (Dict[str, Any]): The dictionary data to initialize from.
            collusion_detector (CollusionDetector): Instance of the collusion detector.
            reputation_manager (ReputationManager): Instance of the reputation manager.

        Returns:
            SanctionsManager: A new instance of SanctionsManager.
        """
        manager = cls(collusion_detector, reputation_manager)

        # Restore sanctions and recovery timers
        manager.sanctions = data.get("sanctions", {})
        manager.recovery_timers = {k: datetime.fromisoformat(v) for k, v in data.get("recovery_timers", {}).items()}

        return manager
