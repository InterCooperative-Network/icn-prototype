"""
validator_manager.py

This module manages validators within the Proof of Cooperation (PoC) consensus mechanism.
It handles validator selection, state management, and integration with the reputation system.

Classes:
    ValidatorManager
"""

from typing import List, Optional
from datetime import datetime, timedelta
from .types import Node, Shard

class ValidatorManager:
    """
    The ValidatorManager is responsible for managing validators within the PoC mechanism.
    
    Key Responsibilities:
    - Selecting eligible validators for block validation.
    - Tracking validator states, including reputation, performance, and cooldown periods.
    - Integrating with the shard management system to ensure validator availability per shard.
    - Enforcing reputation requirements and cooldown periods for fair participation.
    """
    
    def __init__(self, min_reputation: float, cooldown_blocks: int):
        """
        Initialize the ValidatorManager with minimum reputation and cooldown settings.
        
        Args:
            min_reputation (float): Minimum reputation required for validators.
            cooldown_blocks (int): Number of blocks a validator must wait after validation.
        """
        self.min_reputation = min_reputation
        self.cooldown_blocks = cooldown_blocks
        self.validator_history: List[tuple] = []  # Stores tuples of (validator_id, timestamp, shard_id)

    def select_validator(self, nodes: List[Node], shard_id: Optional[int] = None) -> Optional[Node]:
        """
        Select an eligible validator from the provided list of nodes.

        Selection Criteria:
        - Node must have reputation above the minimum threshold.
        - Node must not be in cooldown.
        - Node must be able to validate the specified shard, if shard_id is provided.
        - Nodes with higher reputation, cooperative interactions, and better performance are prioritized.

        Args:
            nodes (List[Node]): List of nodes to select from.
            shard_id (Optional[int]): Shard ID for which a validator is needed.
        
        Returns:
            Optional[Node]: The selected validator node, or None if no eligible validator is found.
        """
        eligible_nodes = [node for node in nodes if self._is_eligible(node, shard_id)]
        if not eligible_nodes:
            return None

        # Sort nodes by priority score
        eligible_nodes.sort(key=self._calculate_priority_score, reverse=True)

        # Select the top candidate
        selected_validator = eligible_nodes[0]
        selected_validator.enter_cooldown(self.cooldown_blocks)
        self._track_validator_history(selected_validator, shard_id)
        return selected_validator

    def _is_eligible(self, node: Node, shard_id: Optional[int]) -> bool:
        """
        Check if a node is eligible to be a validator based on reputation, cooldown, and shard assignment.
        
        Args:
            node (Node): Node to check eligibility for.
            shard_id (Optional[int]): Shard ID to validate eligibility against.

        Returns:
            bool: True if the node is eligible, False otherwise.
        """
        # Check if node's reputation meets the minimum requirement
        if node.reputation < self.min_reputation:
            return False

        # Check if node is in cooldown
        if node.cooldown > 0:
            return False

        # Check if node can validate the specified shard
        if shard_id is not None and not node.can_validate(shard_id):
            return False

        return True

    def _calculate_priority_score(self, node: Node) -> float:
        """
        Calculate a priority score for selecting validators based on multiple factors.

        The score is calculated using:
        - Reputation
        - Number of cooperative interactions
        - Performance metrics (e.g., availability, validation success rate)

        Args:
            node (Node): Node to calculate the priority score for.

        Returns:
            float: Calculated priority score.
        """
        # Weights for different factors in the priority score calculation
        reputation_weight = 0.6
        interaction_weight = 0.2
        performance_weight = 0.2

        score = (
            node.reputation * reputation_weight +
            len(node.cooperative_interactions) * interaction_weight +
            node.performance_metrics.get('validation_success_rate', 0) * performance_weight
        )
        return score

    def _track_validator_history(self, node: Node, shard_id: Optional[int]) -> None:
        """
        Track the history of validators for auditing and performance analysis.

        Args:
            node (Node): The validator node.
            shard_id (Optional[int]): The shard ID for which the node was selected as a validator.
        """
        self.validator_history.append((node.node_id, datetime.now(), shard_id))
        
        # Maintain a capped history size for memory efficiency
        max_history_length = 1000
        if len(self.validator_history) > max_history_length:
            self.validator_history.pop(0)

    def reset_validator(self, node: Node) -> None:
        """
        Reset the cooldown and state of a validator node after it completes its cooldown period.

        Args:
            node (Node): The validator node to reset.
        """
        node.cooldown = 0
        node.reset_performance_metrics()

    def get_validator_history(self, limit: int = 100) -> List[tuple]:
        """
        Retrieve the recent history of validators, useful for auditing and analysis.

        Args:
            limit (int): Maximum number of records to return (default is 100).

        Returns:
            List[tuple]: A list of tuples containing validator history records.
        """
        return self.validator_history[-limit:]

    def update_validator_reputation(self, node: Node, reputation_delta: float) -> None:
        """
        Update the reputation of a validator node by a specified amount.

        Args:
            node (Node): The validator node to update.
            reputation_delta (float): The amount to add or subtract from the node's reputation.
        """
        node.reputation += reputation_delta
        node.reputation = max(0.0, node.reputation)  # Ensure reputation does not fall below zero

    def enforce_cooldown(self, node: Node) -> None:
        """
        Enforce cooldown for a validator node after a validation cycle.

        This method increases the cooldown period for the node to prevent consecutive validations.

        Args:
            node (Node): The validator node to enforce cooldown on.
        """
        node.cooldown = self.cooldown_blocks

    def release_cooldown(self) -> None:
        """
        Release cooldowns for all validators that have completed their cooldown period.

        This method iterates through nodes and decreases their cooldown by one block,
        allowing them to rejoin validation once their cooldown reaches zero.
        """
        for node in self._get_all_nodes():
            if node.cooldown > 0:
                node.cooldown -= 1

    def _get_all_nodes(self) -> List[Node]:
        """
        Placeholder method to retrieve all nodes in the network.

        This method should be replaced with actual logic to fetch nodes from the broader PoC network.

        Returns:
            List[Node]: List of all nodes (currently returns an empty list).
        """
        # Replace this placeholder with actual retrieval logic in the PoC network
        return []

    def get_active_validators(self) -> List[str]:
        """
        Retrieve a list of active validators based on their current state.

        Returns:
            List[str]: A list of node IDs representing active validators.
        """
        return [record[0] for record in self.validator_history if record[1] > datetime.now() - timedelta(hours=1)]
