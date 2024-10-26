# blockchain/consensus/proof_of_cooperation/reputation_manager.py

from typing import Dict, List, Optional, Set, Any
from datetime import datetime, timedelta
import logging
import math
from .types import ConsensusConfig, ValidationResult, ValidationStats
from ..core.node import Node

logger = logging.getLogger(__name__)

class ReputationManager:
    """
    Manages reputation scoring and calculations for the consensus mechanism.
    Handles all aspects of node reputation including score calculation,
    decay, and validation eligibility.
    """

    def __init__(self, config: ConsensusConfig):
        """
        Initialize the reputation manager.

        Args:
            config: The consensus configuration parameters
        """
        self.config = config
        self.node_stats: Dict[str, ValidationStats] = {}
        self.last_score_update: Dict[str, datetime] = {}
        self.score_cache: Dict[str, float] = {}
        self.cache_duration = timedelta(minutes=5)

    def calculate_cooperation_score(self, node: Node, shard_id: Optional[int] = None) -> float:
        """
        Calculate a node's cooperation score with all factors.

        Args:
            node: The node to calculate score for
            shard_id: Optional shard ID for shard-specific scoring

        Returns:
            float: The calculated cooperation score
        """
        try:
            # Check cache first
            cache_key = f"{node.node_id}:{shard_id or 'all'}"
            if cache_key in self.score_cache:
                cache_time = self.last_score_update.get(cache_key)
                if cache_time and datetime.now() - cache_time < self.cache_duration:
                    return self.score_cache[cache_key]

            # Calculate base reputation score
            base_score = sum(
                score * self.config.reputation_weights.get(category, 1.0)
                for category, score in node.reputation_scores.items()
            )

            # Calculate modifying factors
            factors = [
                self._calculate_diversity_factor(node),
                self._calculate_consistency_factor(node),
                self._calculate_performance_factor(node)
            ]

            # Add shard-specific factor if applicable
            if shard_id is not None:
                factors.append(self._calculate_shard_factor(node, shard_id))

            # Apply all factors
            final_score = base_score
            for factor in factors:
                final_score *= factor

            # Apply time decay
            time_factor = self._calculate_time_decay(node)
            final_score *= time_factor

            # Cache result
            self.score_cache[cache_key] = final_score
            self.last_score_update[cache_key] = datetime.now()

            return max(0.0, final_score)

        except Exception as e:
            logger.error(f"Error calculating cooperation score: {str(e)}")
            return 0.0

    def _calculate_diversity_factor(self, node: Node) -> float:
        """
        Calculate diversity factor based on cooperative interactions.

        Args:
            node: The node to calculate factor for

        Returns:
            float: The calculated diversity factor
        """
        try:
            recent_interactions = node.cooperative_interactions[-100:]
            if not recent_interactions:
                return 1.0

            # Calculate unique interaction ratio
            unique_coops = len(set(recent_interactions))
            total_interactions = len(recent_interactions)
            diversity_score = unique_coops / total_interactions

            # Progressive scaling based on experience
            if total_interactions >= 20:
                if unique_coops >= 5:
                    return 1.0 + math.log(1 + diversity_score) * 1.5
                return 1.0 + math.log(1 + diversity_score)

            return max(0.7, diversity_score)  # Minimum baseline for new nodes

        except Exception as e:
            logger.error(f"Error calculating diversity factor: {str(e)}")
            return 0.7

    def _calculate_consistency_factor(self, node: Node) -> float:
        """
        Calculate consistency factor based on validation history.

        Args:
            node: The node to calculate factor for

        Returns:
            float: The calculated consistency factor
        """
        try:
            if not node.validation_history:
                return 1.0

            recent_validations = node.validation_history[-50:]
            successful = sum(
                1 for v in recent_validations 
                if v.get("evidence", {}).get("success", False)
            )
            success_rate = successful / len(recent_validations)

            # Progressive scaling based on experience
            if node.total_validations < 10:
                min_rate = self.config.validation_thresholds["min_success_rate"] * 0.8
            else:
                min_rate = self.config.validation_thresholds["min_success_rate"]

            if success_rate > 0.95:  # Exceptional performance
                return 1.8
            elif success_rate > 0.8:  # Strong performance
                return 1.5
            elif success_rate > min_rate:
                return 1.0 + ((success_rate - min_rate) / (1 - min_rate))

            return max(0.5, success_rate / min_rate)

        except Exception as e:
            logger.error(f"Error calculating consistency factor: {str(e)}")
            return 0.5

    def _calculate_performance_factor(self, node: Node) -> float:
        """
        Calculate performance factor based on node metrics.

        Args:
            node: The node to calculate factor for

        Returns:
            float: The calculated performance factor
        """
        try:
            metrics = node.performance_metrics
            if not metrics:
                return 1.0

            # Weighted performance metrics
            weights = {
                "availability": 0.35,
                "validation_success_rate": 0.35,
                "network_reliability": 0.3
            }

            weighted_sum = sum(
                (metrics.get(metric, 0) / 100) * weight
                for metric, weight in weights.items()
            )

            # Apply bonuses for exceptional performance
            if weighted_sum > 0.95:
                return weighted_sum * 1.2
            elif weighted_sum > 0.9:
                return weighted_sum * 1.1

            return max(
                self.config.validation_thresholds["min_availability"],
                weighted_sum
            )

        except Exception as e:
            logger.error(f"Error calculating performance factor: {str(e)}")
            return self.config.validation_thresholds["min_availability"]

    def _calculate_shard_factor(self, node: Node, shard_id: int) -> float:
        """
        Calculate shard-specific factor.

        Args:
            node: The node to calculate factor for
            shard_id: The shard ID to calculate factor for

        Returns:
            float: The calculated shard factor
        """
        try:
            if shard_id not in node.active_shards:
                return 0.0

            # Calculate time-based experience
            time_in_shard = (
                datetime.now() - node.active_shards[shard_id]
            ).total_seconds()
            experience = min(1.0, time_in_shard / (24 * 3600))

            # Get shard-specific success rate
            stats = self.node_stats.get(node.node_id, ValidationStats())
            shard_stats = stats.shard_validations.get(shard_id, {})
            
            if shard_stats:
                success_rate = (
                    shard_stats.get("successful", 0) /
                    max(1, shard_stats.get("selections", 1))
                )
            else:
                success_rate = 1.0  # New to shard

            # Progressive weighting
            if experience < 0.2:  # New to shard
                return 0.7 + (0.3 * success_rate)
            else:
                return 0.4 + (0.3 * experience) + (0.3 * success_rate)

        except Exception as e:
            logger.error(f"Error calculating shard factor: {str(e)}")
            return 0.5

    def _calculate_time_decay(self, node: Node) -> float:
        """
        Calculate time-based decay factor.

        Args:
            node: The node to calculate decay for

        Returns:
            float: The calculated decay factor
        """
        try:
            stats = self.node_stats.get(node.node_id)
            if not stats or not stats.last_validation:
                return 1.0

            hours_inactive = (
                datetime.now() - stats.last_validation
            ).total_seconds() / 3600

            if hours_inactive > 24:
                return math.exp(-hours_inactive / 24)
            return 1.0

        except Exception as e:
            logger.error(f"Error calculating time decay: {str(e)}")
            return 1.0

    def can_validate(self, node: Node, shard_id: Optional[int] = None) -> bool:
        """
        Check if a node can participate in validation.

        Args:
            node: The node to check
            shard_id: Optional shard ID for shard-specific validation

        Returns:
            bool: True if node can validate
        """
        try:
            # Handle new nodes
            if node.total_validations < 5:
                return (
                    node.can_validate(shard_id) and
                    node.get_total_reputation() >= 
                    self.config.min_reputation * 
                    self.config.validation_thresholds["new_node_reputation_factor"]
                )

            # Standard checks
            if not node.can_validate(shard_id):
                return False

            # Check reputation requirement
            total_reputation = node.get_total_reputation()
            reputation_requirement = self.config.min_reputation

            # Scale requirements with experience
            if node.total_validations > 20:
                reputation_requirement *= 1.2
            elif node.total_validations > 10:
                reputation_requirement *= 1.0
            else:
                reputation_requirement *= 0.7

            if total_reputation < reputation_requirement:
                return False

            # Check recent performance
            stats = self.node_stats.get(node.node_id, ValidationStats())
            if stats.selections > 0:
                recent_success = (
                    stats.successful_validations / stats.selections
                )
                if (recent_success < 
                    self.config.validation_thresholds["min_success_rate"] and
                    node.total_validations > 10):
                    return False

            return True

        except Exception as e:
            logger.error(f"Error checking validation eligibility: {str(e)}")
            return False

    def update_stats(self, node_id: str, result: ValidationResult, shard_id: Optional[int] = None) -> None:
        """
        Update validation statistics for a node.

        Args:
            node_id: ID of the node
            result: The validation result
            shard_id: Optional shard ID where validation occurred
        """
        try:
            if node_id not in self.node_stats:
                self.node_stats[node_id] = ValidationStats()

            stats = self.node_stats[node_id]
            stats.selections += 1
            stats.last_validation = datetime.now()

            if result.success:
                stats.successful_validations += 1
                stats.consecutive_failures = 0
            else:
                stats.consecutive_failures += 1

            if shard_id is not None:
                if shard_id not in stats.shard_validations:
                    stats.shard_validations[shard_id] = {
                        "selections": 0,
                        "successful": 0
                    }
                
                shard_stats = stats.shard_validations[shard_id]
                shard_stats["selections"] += 1
                if result.success:
                    shard_stats["successful"] += 1

        except Exception as e:
            logger.error(f"Error updating validation stats: {str(e)}")

    def get_node_stats(self, node_id: str) -> Optional[ValidationStats]:
        """
        Get validation statistics for a node.

        Args:
            node_id: ID of the node

        Returns:
            Optional[ValidationStats]: The node's validation stats if found
        """
        return self.node_stats.get(node_id)

    def to_dict(self) -> Dict[str, Any]:
        """Convert reputation manager state to dictionary."""
        return {
            "node_stats": {
                node_id: {
                    "selections": stats.selections,
                    "successful_validations": stats.successful_validations,
                    "consecutive_failures": stats.consecutive_failures,
                    "last_validation": stats.last_validation.isoformat() if stats.last_validation else None,
                    "shard_validations": stats.shard_validations
                }
                for node_id, stats in self.node_stats.items()
            },
            "score_cache": self.score_cache,
            "last_score_update": {
                k: v.isoformat() 
                for k, v in self.last_score_update.items()
            }
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any], config: ConsensusConfig) -> 'ReputationManager':
        """Create reputation manager from dictionary data."""
        manager = cls(config)

        # Restore node stats
        for node_id, stats_data in data["node_stats"].items():
            stats = ValidationStats()
            stats.selections = stats_data["selections"]
            stats.successful_validations = stats_data["successful_validations"]
            stats.consecutive_failures = stats_data["consecutive_failures"]
            if stats_data["last_validation"]:
                stats.last_validation = datetime.fromisoformat(
                    stats_data["last_validation"]
                )
            stats.shard_validations = stats_data["shard_validations"]
            manager.node_stats[node_id] = stats

        # Restore cache
        manager.score_cache = data["score_cache"]
        manager.last_score_update = {
            k: datetime.fromisoformat(v)
            for k, v in data["last_score_update"].items()
        }

        return manager