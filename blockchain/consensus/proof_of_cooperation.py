# ================================================================
# File: blockchain/consensus/proof_of_cooperation.py
# Description: Implements the Proof of Cooperation (PoC) consensus
# mechanism for the ICN blockchain. PoC incentivizes cooperative behavior,
# resource contribution, and equitable participation, unlike traditional
# proof-of-work or proof-of-stake models.
# ================================================================

from __future__ import annotations
from typing import Dict, List, Optional, Tuple, Set
from datetime import datetime, timedelta
import math
import random
import logging
from ..core.node import Node
from ..core.block import Block

logger = logging.getLogger(__name__)

class ProofOfCooperation:
    """
    Implements the Proof of Cooperation consensus mechanism for the ICN.

    PoC is designed to prioritize cooperative behavior, community engagement,
    and equitable contribution, placing less emphasis on computational power
    or financial stake. It incorporates factors like reputation, diversity of
    interactions, performance metrics, and collusion detection to select validators.
    """

    def __init__(self, min_reputation: float = 10.0, cooldown_blocks: int = 3):
        """
        Initialize the Proof of Cooperation mechanism.

        Args:
            min_reputation (float): Minimum reputation required to participate as a validator.
            cooldown_blocks (int): Number of blocks a validator must wait before re-validating.
        """
        self.min_reputation = min_reputation
        self.cooldown_blocks = cooldown_blocks
        self.cooperation_scores: Dict[str, float] = {}
        self.reputation_weights: Dict[str, float] = {
            "cooperative_growth": 1.5,
            "proposal_participation": 1.2,
            "transaction_validation": 1.0,
            "resource_sharing": 1.3,
            "conflict_resolution": 1.1,
            "community_building": 1.4,
            "sustainability": 1.2,
            "innovation": 1.3,
            "network_stability": 1.1,
            "data_availability": 1.2,
        }
        self.validation_thresholds: Dict[str, float] = {
            "min_participation": 0.1,
            "min_success_rate": 0.8,
            "min_availability": 0.95,
            "max_consecutive_validations": 3,
        }
        self.reputation_decay_factor = 0.99
        self.collusion_threshold = 0.8
        self.validator_history: List[Tuple[str, datetime, int]] = []
        self.validation_stats: Dict[str, Dict] = {}
        self.performance_metrics: Dict[str, float] = {
            "average_block_time": 0.0,
            "total_validations": 0,
            "successful_validations": 0,
            "collusion_detections": 0,
        }

    def calculate_cooperation_score(self, node: Node, shard_id: Optional[int] = None) -> float:
        """
        Calculate a node's cooperation score based on reputation, diversity,
        performance, and shard-specific factors.

        Args:
            node (Node): Node object representing the validator.
            shard_id (Optional[int]): Shard ID for context, if applicable.

        Returns:
            float: The calculated cooperation score for the node.
        """
        if not node.can_validate(shard_id):
            return 0.0

        try:
            base_score = sum(
                score * self.reputation_weights.get(category, 1.0)
                for category, score in node.reputation_scores.items()
            )

            diversity_factor = self._calculate_diversity_factor(node)
            consistency_factor = self._calculate_consistency_factor(node)
            performance_factor = self._calculate_performance_factor(node)
            shard_factor = self._calculate_shard_factor(node, shard_id) if shard_id else 1.0

            final_score = base_score * diversity_factor * consistency_factor * performance_factor * shard_factor

            logger.info(f"Node {node.node_id} cooperation score: {final_score}")
            return max(0, final_score)

        except Exception as e:
            logger.error(f"Failed to calculate cooperation score: {str(e)}")
            return 0.0

    def _calculate_diversity_factor(self, node: Node) -> float:
        """
        Calculate diversity factor based on the node's cooperative interactions.

        Args:
            node (Node): Node object representing the validator.

        Returns:
            float: The calculated diversity factor for the node.
        """
        try:
            recent_interactions = node.cooperative_interactions[-100:]
            if not recent_interactions:
                return 1.0

            unique_coops = len(set(recent_interactions))
            total_interactions = len(recent_interactions)

            diversity_score = unique_coops / total_interactions
            normalized_score = 1.0 + math.log(diversity_score + 1)

            return max(self.validation_thresholds["min_participation"], normalized_score)

        except Exception as e:
            logger.error(f"Failed to calculate diversity factor: {str(e)}")
            return self.validation_thresholds["min_participation"]

    def _calculate_consistency_factor(self, node: Node) -> float:
        """
        Calculate consistency factor based on the node's validation history.

        Args:
            node (Node): Node object representing the validator.

        Returns:
            float: The calculated consistency factor for the node.
        """
        try:
            if not node.validation_history:
                return 1.0

            recent_validations = node.validation_history[-50:]
            successful = sum(1 for v in recent_validations if v.get("evidence", {}).get("success", False))

            success_rate = successful / len(recent_validations)
            return max(self.validation_thresholds["min_success_rate"], success_rate)

        except Exception as e:
            logger.error(f"Failed to calculate consistency factor: {str(e)}")
            return self.validation_thresholds["min_success_rate"]

    def _calculate_performance_factor(self, node: Node) -> float:
        """
        Calculate performance factor based on node's metrics.

        Args:
            node (Node): Node object representing the validator.

        Returns:
            float: The calculated performance factor for the node.
        """
        try:
            metrics = node.performance_metrics
            if not metrics:
                return 1.0

            factors = [
                metrics.get("availability", 0) / 100,
                metrics.get("validation_success_rate", 0) / 100,
                metrics.get("network_reliability", 0) / 100,
            ]

            avg_performance = sum(factors) / len(factors)
            return max(self.validation_thresholds["min_availability"], avg_performance)

        except Exception as e:
            logger.error(f"Failed to calculate performance factor: {str(e)}")
            return self.validation_thresholds["min_availability"]

    def _calculate_shard_factor(self, node: Node, shard_id: int) -> float:
        """
        Calculate shard-specific factor based on the node's experience in the shard.

        Args:
            node (Node): Node object representing the validator.
            shard_id (int): ID of the shard.

        Returns:
            float: The calculated shard-specific factor.
        """
        try:
            if shard_id not in node.active_shards:
                return 0.0

            time_in_shard = (datetime.now() - node.active_shards[shard_id]).total_seconds()
            shard_experience = min(1.0, time_in_shard / (24 * 3600))

            return 0.5 + (0.5 * shard_experience)

        except Exception as e:
            logger.error(f"Failed to calculate shard factor: {str(e)}")
            return 0.0

    def select_validator(self, nodes: List[Node], shard_id: Optional[int] = None) -> Optional[Node]:
        """
        Select the next validator using a weighted random selection based on cooperation scores.

        Args:
            nodes (List[Node]): List of candidate nodes.
            shard_id (Optional[int]): Shard ID for context, if applicable.

        Returns:
            Optional[Node]: The selected validator node or None if no valid candidate exists.
        """
        try:
            eligible_nodes = [node for node in nodes if self._is_eligible_validator(node, shard_id)]

            if not eligible_nodes:
                logger.warning("No eligible validators available")
                return None

            scores = [self.calculate_cooperation_score(node, shard_id) for node in eligible_nodes]
            total_score = sum(scores)

            if total_score <= 0:
                logger.warning("No nodes with positive cooperation scores")
                selected = random.choice(eligible_nodes)
            else:
                selection_point = random.uniform(0, total_score)
                current_sum = 0

                for node, score in zip(eligible_nodes, scores):
                    current_sum += score
                    if current_sum >= selection_point:
                        selected = node
                        break

            self._record_validator_selection(selected, shard_id)
            selected.enter_cooldown(self.cooldown_blocks)

            logger.info(f"Selected validator: {selected.node_id}")
            return selected

        except Exception as e:
            logger.error(f"Failed to select validator: {str(e)}")
            return None

    def _is_eligible_validator(self, node: Node, shard_id: Optional[int] = None) -> bool:
        """
        Check if a node is eligible to validate blocks based on criteria like reputation,
        performance, and cooldown.

        Args:
            node (Node): Node object representing the validator.
            shard_id (Optional[int]): Shard ID for context, if applicable.

        Returns:
            bool: True if the node is eligible, False otherwise.
        """
        try:
            if not node.can_validate(shard_id):
                return False

            if node.get_total_reputation() < self.min_reputation:
                return False

            performance_factor = self._calculate_performance_factor(node)
            if performance_factor < self.validation_thresholds["min_availability"]:
                return False

            recent_validations = [v[0] for v in self.validator_history[-10:] if v[0] == node.node_id]
            if len(recent_validations) >= self.validation_thresholds["max_consecutive_validations"]:
                return False

            return True

        except Exception as e:
            logger.error(f"Failed to check validator eligibility: {str(e)}")
            return False

    def _record_validator_selection(self, node: Node, shard_id: Optional[int]) -> None:
        """
        Record validator selection and update selection history.

        Args:
            node (Node): The selected validator node.
            shard_id (Optional[int]): The shard ID for the validation, if applicable.
        """
        try:
            self.validator_history.append((node.node_id, datetime.now(), shard_id))
            if len(self.validator_history) > 1000:
                self.validator_history = self.validator_history[-1000:]

            if node.node_id not in self.validation_stats:
                self.validation_stats[node.node_id] = {
                    "selections": 0,
                    "successful_validations": 0,
                    "last_selected": None,
                    "shard_validations": {},
                }

            stats = self.validation_stats[node.node_id]
            stats["selections"] += 1
            stats["last_selected"] = datetime.now()

            if shard_id is not None:
                shard_stats = stats["shard_validations"].setdefault(
                    shard_id, {"selections": 0, "successful": 0}
                )
                shard_stats["selections"] += 1

        except Exception as e:
            logger.error(f"Failed to record validator selection: {str(e)}")

    def validate_block(self, block: Block, previous_block: Optional[Block], validator: Node) -> bool:
        """
        Validate a proposed block based on validator eligibility and block integrity.

        Args:
            block (Block): The block to be validated.
            previous_block (Optional[Block]): The previous block in the chain for context.
            validator (Node): The node proposing the block.

        Returns:
            bool: True if the block is valid, False otherwise.
        """
        try:
            if not self._is_eligible_validator(validator, block.shard_id):
                logger.error("Validator not eligible")
                return False

            if not block.validate(previous_block):
                logger.error("Block validation failed")
                return False

            self._update_validation_stats(validator, block, True)
            return True

        except Exception as e:
            logger.error(f"Block validation failed: {str(e)}")
            self._update_validation_stats(validator, block, False)
            return False

    def detect_collusion(self, validator: Node, block: Block) -> bool:
        """
        Detect potential collusion by analyzing recent transaction patterns.

        Args:
            validator (Node): The node performing the validation.
            block (Block): The block being validated.

        Returns:
            bool: True if potential collusion is detected, False otherwise.
        """
        try:
            recent_validations = validator.validation_history[-20:]
            interaction_counts: Dict[str, int] = {}

            for validation in recent_validations:
                for tx in block.transactions:
                    interaction_counts[tx.sender] = interaction_counts.get(tx.sender, 0) + 1
                    interaction_counts[tx.receiver] = interaction_counts.get(tx.receiver, 0) + 1

            max_interactions = max(interaction_counts.values(), default=0)
            if max_interactions / len(recent_validations) > self.collusion_threshold:
                logger.warning(f"Potential collusion detected for validator {validator.node_id}")
                self.performance_metrics["collusion_detections"] += 1
                return True

            return False

        except Exception as e:
            logger.error(f"Failed to detect collusion: {str(e)}")
            return False

    def get_metrics(self) -> Dict:
        """
        Get metrics for the consensus mechanism, including validation success rates,
        average block time, and collusion detections.

        Returns:
            Dict: Dictionary containing consensus metrics.
        """
        try:
            return {
                "active_validators": len(self.validation_stats),
                "total_validations": self.performance_metrics["total_validations"],
                "successful_validations": self.performance_metrics["successful_validations"],
                "average_block_time": self.performance_metrics["average_block_time"],
                "collusion_detections": self.performance_metrics["collusion_detections"],
            }
        except Exception as e:
            logger.error(f"Failed to get metrics: {str(e)}")
            return {}
