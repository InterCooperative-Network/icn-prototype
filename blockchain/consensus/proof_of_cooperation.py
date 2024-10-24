# ================================================================
# File: blockchain/consensus/proof_of_cooperation.py
# Description: Implements the Proof of Cooperation (PoC) consensus
# mechanism for the ICN blockchain. PoC incentivizes cooperative behavior,
# resource contribution, and equitable participation, unlike traditional
# proof-of-work or proof-of-stake models.
# ================================================================

from __future__ import annotations
from typing import Dict, List, Optional, Tuple, Set, Any
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
    
    The PoC mechanism prioritizes cooperative behavior and equitable participation
    over raw computational power or stake. It incorporates multiple factors to
    evaluate and select validators:
    
    - Reputation scores across different categories
    - Diversity of cooperative interactions
    - Historical consistency and reliability
    - Performance metrics and resource contribution
    - Shard-specific expertise
    """

    def __init__(self, min_reputation: float = 10.0, cooldown_blocks: int = 3):
        """Initialize the Proof of Cooperation mechanism."""
        self.min_reputation = min_reputation
        self.cooldown_blocks = cooldown_blocks
        self.cooperation_scores: Dict[str, float] = {}
        
        # Weights for different reputation categories
        self.reputation_weights = {
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
        
        # Progressive validation thresholds
        self.validation_thresholds = {
            "min_participation": 0.1,    # Base participation requirement
            "min_success_rate": 0.5,     # Minimum success rate (lowered for accessibility)
            "min_availability": 0.7,     # Minimum availability (lowered)
            "max_consecutive_validations": 3,
            "new_node_reputation_factor": 0.5,  # Multiplier for new node requirements
        }
        
        # System parameters
        self.reputation_decay_factor = 0.99
        self.collusion_threshold = 0.7  # Lowered for better detection
        self.min_interactions_for_collusion = 5  # Lowered minimum interactions
        self.validator_history: List[Tuple[str, datetime, int]] = []
        self.validation_stats: Dict[str, Dict[str, Any]] = {}
        
        # Performance metrics
        self.performance_metrics = {
            "average_block_time": 0.0,
            "total_validations": 0,
            "successful_validations": 0,
            "collusion_detections": 0,
            "failed_validations": 0,
            "total_blocks_validated": 0,
        }

    def _can_participate(self, node: Node, shard_id: Optional[int] = None) -> bool:
        """Check if a node can participate in consensus."""
        try:
            # Basic validation check
            if not node.can_validate(shard_id):
                logger.debug(f"Node {node.node_id} failed basic validation check")
                return False

            # Progressive entry for new nodes
            is_new_node = node.total_validations < 10
            reputation_requirement = (
                self.min_reputation * self.validation_thresholds["new_node_reputation_factor"]
                if is_new_node else
                self.min_reputation
            )

            if node.get_total_reputation() < reputation_requirement:
                logger.debug(f"Node {node.node_id} has insufficient reputation")
                return False

            # Check shard assignment if specified
            if shard_id is not None and shard_id not in node.active_shards:
                logger.debug(f"Node {node.node_id} not assigned to shard {shard_id}")
                return False

            return True

        except Exception as e:
            logger.error(f"Error checking participation eligibility: {e}")
            return False

    def calculate_cooperation_score(self, node: Node, shard_id: Optional[int] = None) -> float:
        """Calculate a node's cooperation score."""
        try:
            if not self._can_participate(node, shard_id):
                return 0.0

            # Base score calculation
            base_score = sum(
                score * self.reputation_weights.get(category, 1.0)
                for category, score in node.reputation_scores.items()
            )

            # Calculate modifying factors
            diversity_factor = self._calculate_diversity_factor(node)
            consistency_factor = self._calculate_consistency_factor(node)
            performance_factor = self._calculate_performance_factor(node)
            shard_factor = self._calculate_shard_factor(node, shard_id) if shard_id else 1.0

            # Combined score
            final_score = (base_score * 
                         diversity_factor * 
                         consistency_factor * 
                         performance_factor * 
                         shard_factor)

            # Apply time-based decay
            last_active = self._get_time_since_last_validation(node)
            if last_active > timedelta(hours=24):
                decay = math.exp(-last_active.total_seconds() / (24 * 3600))
                final_score *= decay

            logger.debug(f"Node {node.node_id} cooperation score: {final_score:.2f}")
            return max(0.0, final_score)

        except Exception as e:
            logger.error(f"Error calculating cooperation score: {e}")
            return 0.0

    def _calculate_diversity_factor(self, node: Node) -> float:
        """Calculate diversity factor based on cooperative interactions."""
        try:
            recent_interactions = node.cooperative_interactions[-100:]
            if not recent_interactions:
                return 1.0

            unique_coops = len(set(recent_interactions))
            total_interactions = len(recent_interactions)
            
            # Calculate base diversity score
            diversity_score = unique_coops / total_interactions
            
            # Apply progressive scaling
            if total_interactions >= 20 and unique_coops >= 3:
                normalized_score = 1.0 + math.log(1 + diversity_score) * 1.2
            else:
                normalized_score = 1.0 + math.log(1 + diversity_score)

            return max(self.validation_thresholds["min_participation"], normalized_score)

        except Exception as e:
            logger.error(f"Error calculating diversity factor: {e}")
            return self.validation_thresholds["min_participation"]

    def _calculate_consistency_factor(self, node: Node) -> float:
        """Calculate consistency factor based on validation history."""
        try:
            if not node.validation_history:
                return 1.0

            recent_validations = node.validation_history[-50:]
            successful = sum(1 for v in recent_validations 
                           if v.get("evidence", {}).get("success", False))
            
            success_rate = successful / len(recent_validations)
            min_rate = self.validation_thresholds["min_success_rate"]
            
            # Progressive scaling
            if success_rate > 0.9:  # Exceptional performance
                return 1.5
            elif success_rate > min_rate:
                return 1.0 + ((success_rate - min_rate) / (1 - min_rate))
            else:
                return max(0.5, success_rate / min_rate)

        except Exception as e:
            logger.error(f"Error calculating consistency factor: {e}")
            return self.validation_thresholds["min_success_rate"]

    def _calculate_performance_factor(self, node: Node) -> float:
        """Calculate performance factor based on node metrics."""
        try:
            metrics = node.performance_metrics
            if not metrics:
                return 1.0

            # Weighted average of metrics
            weights = {
                "availability": 0.4,
                "validation_success_rate": 0.4,
                "network_reliability": 0.2
            }
            
            weighted_sum = sum(
                (metrics.get(metric, 0) / 100) * weight
                for metric, weight in weights.items()
            )
            
            # Apply bonus for high performance
            if weighted_sum > 0.95:
                weighted_sum *= 1.1

            return max(self.validation_thresholds["min_availability"], weighted_sum)

        except Exception as e:
            logger.error(f"Error calculating performance factor: {e}")
            return self.validation_thresholds["min_availability"]

    def _calculate_shard_factor(self, node: Node, shard_id: int) -> float:
        """Calculate shard-specific performance factor."""
        try:
            if shard_id not in node.active_shards:
                return 0.0

            # Calculate experience from time in shard
            time_in_shard = (datetime.now() - node.active_shards[shard_id]).total_seconds()
            base_experience = min(1.0, time_in_shard / (24 * 3600))
            
            # Get shard-specific success rate
            shard_stats = (
                self.validation_stats.get(node.node_id, {})
                .get("shard_validations", {})
                .get(shard_id, {})
            )
            success_rate = (
                shard_stats.get("successful", 0) /
                max(1, shard_stats.get("selections", 1))
            )
            
            # Combine factors
            return 0.5 + (0.3 * base_experience) + (0.2 * success_rate)

        except Exception as e:
            logger.error(f"Error calculating shard factor: {e}")
            return 0.0

    def _get_time_since_last_validation(self, node: Node) -> timedelta:
        """Get time since node's last validation."""
        stats = self.validation_stats.get(node.node_id, {})
        last_validation = stats.get("last_validation")
        if not last_validation:
            return timedelta(hours=1)  # Default for new nodes
        return datetime.now() - last_validation

    def select_validator(self, nodes: List[Node], shard_id: Optional[int] = None) -> Optional[Node]:
        """Select the next validator using weighted random selection."""
        try:
            # Get eligible nodes with progressive criteria
            eligible_nodes = [
                node for node in nodes
                if self._can_participate(node, shard_id)
            ]

            if not eligible_nodes:
                logger.warning("No eligible validators available")
                return None

            # Calculate scores
            scores = [
                self.calculate_cooperation_score(node, shard_id)
                for node in eligible_nodes
            ]
            total_score = sum(scores)

            # Select validator
            if total_score <= 0:
                # Fallback selection for new nodes
                new_nodes = [
                    node for node in eligible_nodes
                    if node.total_validations < 10
                ]
                if new_nodes:
                    selected = random.choice(new_nodes)
                else:
                    return None
            else:
                # Weighted random selection
                selection_point = random.uniform(0, total_score)
                current_sum = 0
                selected = eligible_nodes[-1]

                for node, score in zip(eligible_nodes, scores):
                    current_sum += score
                    if current_sum >= selection_point:
                        selected = node
                        break

            # Record selection and apply cooldown
            self._record_validator_selection(selected, shard_id)
            selected.enter_cooldown(self.cooldown_blocks)

            logger.info(f"Selected validator: {selected.node_id}")
            return selected

        except Exception as e:
            logger.error(f"Error selecting validator: {e}")
            return None

    def _record_validator_selection(self, node: Node, shard_id: Optional[int]) -> None:
        """Record validator selection and update history."""
        try:
            self.validator_history.append((node.node_id, datetime.now(), shard_id))
            if len(self.validator_history) > 1000:
                self.validator_history = self.validator_history[-1000:]

            if node.node_id not in self.validation_stats:
                self.validation_stats[node.node_id] = {
                    "selections": 0,
                    "successful_validations": 0,
                    "last_validation": None,
                    "shard_validations": {}
                }

            stats = self.validation_stats[node.node_id]
            stats["selections"] += 1
            stats["last_validation"] = datetime.now()

            if shard_id is not None:
                shard_stats = stats["shard_validations"].setdefault(
                    shard_id, {"selections": 0, "successful": 0}
                )
                shard_stats["selections"] += 1

        except Exception as e:
            logger.error(f"Error recording validator selection: {e}")

    def validate_block(self, block: Block, previous_block: Optional[Block], validator: Node) -> bool:
        """Validate a proposed block."""
        try:
            # Check validator eligibility
            if not self._can_validate_block(validator, block.shard_id):
                logger.error(f"Validator {validator.node_id} not eligible for block validation")
                return False

            # Validate block integrity
            if not block.validate(previous_block):
                logger.error(f"Block {block.index} failed integrity validation")
                self.performance_metrics["failed_validations"] += 1
                return False

            # Check for collusion
            if self.detect_collusion(validator, block):
                logger.warning(f"Potential collusion detected in block {block.index}")
                self.performance_metrics["collusion_detections"] += 1
                return False

            # Update metrics
            self.performance_metrics["total_blocks_validated"] += 1
            self.performance_metrics["successful_validations"] += 1
            
            # Update validation statistics
            self._update_validation_stats(validator, block, True)
            
            logger.info(f"Block {block.index} validated successfully")
            return True

        except Exception as e:
            logger.error(f"Error validating block: {e}")
            self._update_validation_stats(validator, block, False)
            return False

    def _can_validate_block(self, validator: Node, shard_id: Optional[int]) -> bool:
        """Check if a validator can validate a specific block."""
        try:
            # Allow new nodes with reduced requirements
            if validator.total_validations < 5:
                return validator.can_validate(shard_id)

            # Regular validation checks
            if not validator.can_validate(shard_id):
                return False

            reputation = validator.get_total_reputation()
            min_rep = self.min_reputation * 0.7  # 30% tolerance
            return reputation >= min_rep

        except Exception as e:
            logger.error(f"Error checking block validation eligibility: {e}")
            return False

    def detect_collusion(self, validator: Node, block: Block) -> bool:
        """Detect potential collusion in transaction patterns."""
        try:
            if len(block.transactions) < 3:  # Skip small blocks
                return False

            # Build interaction count map
            sender_counts: Dict[str, int] = {}
            receiver_counts: Dict[str, int] = {}
            
            # Count transaction patterns
            total_transactions = len(block.transactions)
            for tx in block.transactions:
                sender_counts[tx.sender] = sender_counts.get(tx.sender, 0) + 1
                receiver_counts[tx.receiver] = receiver_counts.get(tx.receiver, 0) + 1

            # Calculate concentration metrics
            max_sender_concentration = max(sender_counts.values()) / total_transactions
            max_receiver_concentration = max(receiver_counts.values()) / total_transactions
            
            # Dynamic threshold based on block size
            threshold = self.collusion_threshold
            if total_transactions > 10:
                threshold *= 0.9  # Stricter for larger blocks
            
            # Check for concentration patterns
            if max_sender_concentration > threshold or max_receiver_concentration > threshold:
                logger.warning(
                    f"Collusion detected - Sender conc: {max_sender_concentration:.2f}, "
                    f"Receiver conc: {max_receiver_concentration:.2f}"
                )
                return True

            return False

        except Exception as e:
            logger.error(f"Error detecting collusion: {e}")
            return False

    def _update_validation_stats(self, validator: Node, block: Block, success: bool) -> None:
        """Update validation statistics."""
        try:
            if validator.node_id not in self.validation_stats:
                self.validation_stats[validator.node_id] = {
                    "selections": 0,
                    "successful_validations": 0,
                    "last_validation": None,
                    "shard_validations": {}
                }

            stats = self.validation_stats[validator.node_id]
            stats["selections"] += 1
            if success:
                stats["successful_validations"] += 1
            stats["last_validation"] = datetime.now()

            # Update shard-specific stats
            if block.shard_id is not None:
                shard_stats = stats["shard_validations"].setdefault(
                    block.shard_id, {"selections": 0, "successful": 0}
                )
                shard_stats["selections"] += 1
                if success:
                    shard_stats["successful"] += 1

            # Update system metrics
            self.performance_metrics["total_validations"] += 1
            if success:
                self.performance_metrics["successful_validations"] += 1
            else:
                self.performance_metrics["failed_validations"] += 1

        except Exception as e:
            logger.error(f"Error updating validation stats: {e}")

    def get_metrics(self) -> Dict:
        """Get comprehensive consensus metrics."""
        try:
            total_validations = self.performance_metrics["total_validations"]
            success_rate = (
                self.performance_metrics["successful_validations"] / total_validations
                if total_validations > 0 else 0
            )
            
            return {
                "active_validators": len(self.validation_stats),
                "total_validations": total_validations,
                "successful_validations": self.performance_metrics["successful_validations"],
                "success_rate": success_rate,
                "average_block_time": self.performance_metrics["average_block_time"],
                "collusion_detections": self.performance_metrics["collusion_detections"],
                "failed_validations": self.performance_metrics["failed_validations"],
                "total_blocks_validated": self.performance_metrics["total_blocks_validated"]
            }
            
        except Exception as e:
            logger.error(f"Error getting metrics: {e}")
            return {}