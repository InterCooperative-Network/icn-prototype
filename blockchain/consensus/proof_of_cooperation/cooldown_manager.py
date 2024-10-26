"""
cooldown_manager.py

This module manages dynamic cooldown periods for validators in the Proof of Cooperation (PoC) consensus mechanism.
It aims to prevent validator monopolization by adjusting cooldowns based on validator activity and network conditions.

Classes:
    CooldownManager
"""

from typing import List, Dict, Optional
from datetime import datetime, timedelta
from .types import Node

class CooldownManager:
    """
    The CooldownManager is responsible for dynamically managing cooldown periods for validators
    within the PoC mechanism. It adjusts cooldowns based on validator participation frequency,
    network congestion, and other factors to ensure fair and diverse participation.

    Key Features:
    - Dynamic cooldown adjustments to prevent validator monopolization.
    - Cooldown periods vary based on validator activity, performance, and network load.
    - Ensures that validators do not dominate consecutive validation rounds.
    """
    
    def __init__(self, base_cooldown: int = 3, max_cooldown: int = 10):
        """
        Initialize the CooldownManager with base and maximum cooldown settings.

        Args:
            base_cooldown (int): The initial cooldown period after validation.
            max_cooldown (int): The maximum allowable cooldown period.
        """
        self.base_cooldown = base_cooldown
        self.max_cooldown = max_cooldown
        self.validator_activity: Dict[str, List[datetime]] = {}  # Track validator activity timestamps

    def apply_cooldown(self, validator: Node) -> None:
        """
        Apply a dynamic cooldown period to a validator based on its recent activity and performance.

        Cooldown Criteria:
        - If a validator has participated frequently in recent blocks, the cooldown period increases.
        - Validators with lower performance or reputation have longer cooldowns.
        - Cooldowns decrease gradually if the validator has not participated recently.

        Args:
            validator (Node): The validator to apply cooldown to.
        """
        # Track validator activity
        self._track_activity(validator)

        # Calculate dynamic cooldown based on participation frequency and performance
        participation_rate = self._calculate_participation_rate(validator)
        performance_factor = 1 - (validator.performance_metrics.get('validation_success_rate', 0) / 100)

        # Adjust cooldown based on participation rate and performance
        dynamic_cooldown = min(
            int(self.base_cooldown * (1 + participation_rate * 2 + performance_factor)),
            self.max_cooldown
        )
        
        validator.cooldown = dynamic_cooldown

    def _track_activity(self, validator: Node) -> None:
        """
        Track the activity of a validator to determine participation frequency.

        Args:
            validator (Node): The validator to track.
        """
        current_time = datetime.now()
        if validator.node_id not in self.validator_activity:
            self.validator_activity[validator.node_id] = []

        self.validator_activity[validator.node_id].append(current_time)

        # Maintain a limited history of activity timestamps for efficiency
        self.validator_activity[validator.node_id] = [
            timestamp for timestamp in self.validator_activity[validator.node_id]
            if timestamp > current_time - timedelta(hours=1)
        ]

    def _calculate_participation_rate(self, validator: Node) -> float:
        """
        Calculate the participation rate of a validator based on recent activity.

        The participation rate is determined by the number of blocks the validator
        has participated in over a fixed period (e.g., the last hour).

        Args:
            validator (Node): The validator to analyze.

        Returns:
            float: The participation rate as a fraction of the maximum allowable rate.
        """
        max_participation_rate = 10  # Max participations allowed per hour (adjustable)
        recent_participations = len(self.validator_activity.get(validator.node_id, []))

        return min(recent_participations / max_participation_rate, 1)

    def reset_cooldown(self, validator: Node) -> None:
        """
        Reset the cooldown period for a validator if it has not participated recently.

        This helps ensure validators are re-integrated into the validation process
        after extended inactivity.

        Args:
            validator (Node): The validator to reset.
        """
        # If validator has not participated in the last hour, reset cooldown
        if not self.validator_activity.get(validator.node_id):
            validator.cooldown = max(0, validator.cooldown - 1)

    def is_eligible(self, validator: Node) -> bool:
        """
        Check if a validator is eligible for participation based on its cooldown period.

        Args:
            validator (Node): The validator to check.

        Returns:
            bool: True if the validator is eligible, False otherwise.
        """
        return validator.cooldown <= 0

    def decay_cooldown(self) -> None:
        """
        Gradually reduce cooldowns for all validators over time to allow re-participation.

        This is called periodically to ensure validators can rejoin the pool after their cooldowns.
        """
        for validator_id, timestamps in self.validator_activity.items():
            if timestamps and datetime.now() - timestamps[-1] > timedelta(minutes=10):
                node = self._get_validator_by_id(validator_id)
                if node:
                    node.cooldown = max(0, node.cooldown - 1)

    def _get_validator_by_id(self, node_id: str) -> Optional[Node]:
        """
        Retrieve a validator by its node ID.

        Args:
            node_id (str): The ID of the node to retrieve.

        Returns:
            Optional[Node]: The validator node, or None if not found.
        """
        # Placeholder for integration with the broader PoC network
        # This method should interface with a node management system to retrieve nodes
        return None  # Replace with actual retrieval logic

    def clear_inactive_validators(self) -> None:
        """
        Clear validators from the activity tracker if they have not participated for an extended period.

        This helps manage memory and ensures the activity tracker remains efficient.
        """
        current_time = datetime.now()
        for validator_id, timestamps in list(self.validator_activity.items()):
            if timestamps and current_time - timestamps[-1] > timedelta(hours=2):
                del self.validator_activity[validator_id]
