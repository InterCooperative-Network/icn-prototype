# ================================================================
# File: blockchain/consensus/proof_of_cooperation.py
# Description: Implements the Proof of Cooperation (PoC) consensus
# mechanism for the ICN blockchain. PoC incentivizes cooperative behavior,
# resource contribution, and equitable participation.
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

    Key improvements:
    - Progressive reputation requirements for new nodes
    - Dynamic scoring adjustments
    - Enhanced collusion detection
    - Improved validation mechanics
    - Better shard-specific handling
    """

    def __init__(self, min_reputation: float = 10.0, cooldown_blocks: int = 3):
        """
        Initialize the Proof of Cooperation mechanism.
        
        Parameters:
        - min_reputation: Minimum reputation required to participate.
        - cooldown_blocks: Number of blocks for the cooldown period after validation.
        """
        # Core parameters
        self.min_reputation = min_reputation
        self.cooldown_blocks = cooldown_blocks
        
        # Reputation category weights
        self.reputation_weights = {
            "cooperative_growth": 1.5,
            "proposal_participation": 1.2,
            "transaction_validation": 1.3,  # Increased importance
            "resource_sharing": 1.3,
            "conflict_resolution": 1.1,
            "community_building": 1.2,
            "sustainability": 1.2,
            "innovation": 1.3,
            "network_stability": 1.4,  # Increased importance
            "data_availability": 1.2,
        }
        
        # Validation thresholds with progressive scaling
        self.validation_thresholds = {
            "min_participation": 0.05,    # Lowered initial barrier
            "min_success_rate": 0.4,      # More forgiving for new nodes
            "min_availability": 0.6,      # Adjusted for better accessibility
            "max_consecutive_validations": 3,
            "new_node_reputation_factor": 0.3,  # More lenient for new nodes
            "min_interactions": 3,         # Minimum required interactions
        }
        
        # System parameters
        self.cooperation_scores: Dict[str, float] = {}
        self.reputation_decay_factor = 0.95  # Slower decay
        self.collusion_threshold = 0.75
        self.validator_history: List[Tuple[str, datetime, int]] = []
        self.validation_stats: Dict[str, Dict[str, Any]] = {}
        
        # Performance tracking
        self.performance_metrics = {
            "average_block_time": 0.0,
            "total_validations": 0,
            "successful_validations": 0,
            "collusion_detections": 0,
            "failed_validations": 0,
            "total_blocks_validated": 0,
            "new_node_participations": 0,
        }

    def _can_participate(self, node: Node, shard_id: Optional[int] = None) -> bool:
        """
        Determine if a node can participate in consensus with progressive requirements.
        
        Parameters:
        - node: Node to be checked for participation eligibility.
        - shard_id: Optional shard ID for shard-specific checks.
        
        Returns:
        - bool: True if node can participate, False otherwise.
        """
        try:
            # Basic validation check
            if not node.can_validate(shard_id):
                return False

            # Get base reputation requirement
            is_new_node = node.total_validations < 10
            base_requirement = (
                self.min_reputation * self.validation_thresholds["new_node_reputation_factor"]
                if is_new_node else
                self.min_reputation
            )
            
            # Apply progressive scaling based on participation history
            if node.total_validations > 0:
                success_rate = self._calculate_success_rate(node)
                if success_rate > 0.8:
                    base_requirement *= 0.8  # Reward consistent good performance
            
            # Check reputation threshold
            total_reputation = node.get_total_reputation()
            if total_reputation < base_requirement:
                return False

            # Verify shard assignment if specified
            if shard_id is not None:
                if shard_id not in node.active_shards:
                    return False
                # Check shard-specific performance
                if not self._check_shard_performance(node, shard_id):
                    return False

            # Additional checks for established nodes
            if not is_new_node:
                if len(node.cooperative_interactions) < self.validation_thresholds["min_interactions"]:
                    return False
                    
            return True

        except Exception as e:
            logger.error(f"Error in participation check: {e}")
            return False

    def _check_shard_performance(self, node: Node, shard_id: int) -> bool:
        """
        Verify node's performance in specific shard.
        
        Parameters:
        - node: Node to be checked for shard performance.
        - shard_id: ID of the shard for performance check.
        
        Returns:
        - bool: True if shard performance is acceptable, False otherwise.
        """
        try:
            stats = self.validation_stats.get(node.node_id, {})
            shard_stats = stats.get("shard_validations", {}).get(shard_id, {})
            
            if not shard_stats:
                return True  # New to this shard
                
            selections = shard_stats.get("selections", 0)
            if selections == 0:
                return True
                
            success_rate = shard_stats.get("successful", 0) / max(1, selections)
            return success_rate >= self.validation_thresholds["min_success_rate"]
            
        except Exception as e:
            logger.error(f"Error checking shard performance: {e}")
            return True  # Fail open for new nodes

    def calculate_cooperation_score(self, node: Node, shard_id: Optional[int] = None) -> float:
        """
        Calculate node's cooperation score with dynamic adjustments.
        
        Parameters:
        - node: Node whose cooperation score is being calculated.
        - shard_id: Optional shard ID for shard-specific adjustments.
        
        Returns:
        - float: Calculated cooperation score.
        """
        try:
            if not self._can_participate(node, shard_id):
                return 0.0

            # Calculate base reputation score
            base_score = sum(
                score * self.reputation_weights.get(category, 1.0)
                for category, score in node.reputation_scores.items()
            )

            # Calculate and apply modifying factors
            factors = [
                self._calculate_diversity_factor(node),
                self._calculate_consistency_factor(node),
                self._calculate_performance_factor(node)
            ]
            
            if shard_id is not None:
                factors.append(self._calculate_shard_factor(node, shard_id))

            # Combine all factors
            final_score = base_score
            for factor in factors:
                final_score *= factor

            # Apply time decay
            time_factor = self._calculate_time_decay(node)
            final_score *= time_factor

            return max(0.0, final_score)

        except Exception as e:
            logger.error(f"Error calculating cooperation score: {e}")
            return 0.0

    def _calculate_diversity_factor(self, node: Node) -> float:
        """
        Calculate diversity factor with improved scaling.
        
        Parameters:
        - node: Node whose diversity factor is being calculated.
        
        Returns:
        - float: Calculated diversity factor.
        """
        try:
            recent_interactions = node.cooperative_interactions[-100:]
            if not recent_interactions:
                return 1.0

            unique_coops = len(set(recent_interactions))
            total_interactions = len(recent_interactions)
            
            # Base diversity score
            diversity_score = unique_coops / total_interactions
            
            # Progressive scaling based on interaction count
            if total_interactions >= 20:
                if unique_coops >= 5:
                    return 1.0 + math.log(1 + diversity_score) * 1.5
                return 1.0 + math.log(1 + diversity_score)
            
            return max(0.7, diversity_score)  # Minimum baseline for new nodes

        except Exception as e:
            logger.error(f"Error calculating diversity factor: {e}")
            return 0.7

    def _calculate_consistency_factor(self, node: Node) -> float:
        """
        Calculate consistency factor with adaptive thresholds.
        
        Parameters:
        - node: Node whose consistency factor is being calculated.
        
        Returns:
        - float: Calculated consistency factor.
        """
        try:
            if not node.validation_history:
                return 1.0

            recent_validations = node.validation_history[-50:]
            successful = sum(1 for v in recent_validations 
                             if v.get("evidence", {}).get("success", False))
            
            success_rate = successful / len(recent_validations)
            
            # Progressive scaling based on experience
            if node.total_validations < 10:
                min_rate = self.validation_thresholds["min_success_rate"] * 0.8
            else:
                min_rate = self.validation_thresholds["min_success_rate"]
            
            if success_rate > 0.95:  # Exceptional performance
                return 1.8
            elif success_rate > 0.8:  # Strong performance
                return 1.5
            elif success_rate > min_rate:
                return 1.0 + ((success_rate - min_rate) / (1 - min_rate))
                
            return max(0.5, success_rate / min_rate)

        except Exception as e:
            logger.error(f"Error calculating consistency factor: {e}")
            return 0.5

    def _calculate_performance_factor(self, node: Node) -> float:
        """
        Calculate performance factor with weighted metrics.
        
        Parameters:
        - node: Node whose performance factor is being calculated.
        
        Returns:
        - float: Calculated performance factor.
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
                
            return max(self.validation_thresholds["min_availability"], weighted_sum)

        except Exception as e:
            logger.error(f"Error calculating performance factor: {e}")
            return self.validation_thresholds["min_availability"]

    def _calculate_shard_factor(self, node: Node, shard_id: int) -> float:
        """
        Calculate shard-specific factor with experience weighting.
        
        Parameters:
        - node: Node whose shard factor is being calculated.
        - shard_id: ID of the shard for factor calculation.
        
        Returns:
        - float: Calculated shard factor.
        """
        try:
            if shard_id not in node.active_shards:
                return 0.0

            # Time-based experience
            time_in_shard = (datetime.now() - node.active_shards[shard_id]).total_seconds()
            experience = min(1.0, time_in_shard / (24 * 3600))
            
            # Success rate in shard
            stats = self.validation_stats.get(node.node_id, {})
            shard_stats = stats.get("shard_validations", {}).get(shard_id, {})
            
            if shard_stats:
                success_rate = (
                    shard_stats.get("successful", 0) /
                    max(1, shard_stats.get("selections", 1))
                )
            else:
                success_rate = 1.0  # New to shard
            
            # Combine factors with progressive weighting
            if experience < 0.2:  # New to shard
                return 0.7 + (0.3 * success_rate)
            else:
                return 0.4 + (0.3 * experience) + (0.3 * success_rate)

        except Exception as e:
            logger.error(f"Error calculating shard factor: {e}")
            return 0.5

    def _calculate_time_decay(self, node: Node) -> float:
        """
        Calculate time-based decay factor.
        
        Parameters:
        - node: Node whose time decay factor is being calculated.
        
        Returns:
        - float: Calculated time decay factor.
        """
        try:
            last_active = self._get_time_since_last_validation(node)
            if last_active > timedelta(hours=24):
                hours_inactive = last_active.total_seconds() / 3600
                return math.exp(-hours_inactive / 24)
            return 1.0
        except Exception as e:
            logger.error(f"Error calculating time decay: {e}")
            return 1.0

    def _get_time_since_last_validation(self, node: Node) -> timedelta:
        """
        Get time since node's last validation.
        
        Parameters:
        - node: Node to check last validation time.
        
        Returns:
        - timedelta: Time since last validation.
        """
        stats = self.validation_stats.get(node.node_id, {})
        last_validation = stats.get("last_validation")
        if not last_validation:
            return timedelta(hours=1)  # Default for new nodes
        return datetime.now() - last_validation

    def select_validator(self, nodes: List[Node], shard_id: Optional[int] = None) -> Optional[Node]:
        """
        Select validator using weighted random selection with safeguards.
        
        Parameters:
        - nodes: List of potential validator nodes.
        - shard_id: Optional shard ID for selection.
        
        Returns:
        - Optional[Node]: Selected validator node, if any.
        """
        try:
            # Get eligible nodes
            eligible_nodes = [
                node for node in nodes
                if self._can_participate(node, shard_id)
            ]

            if not eligible_nodes:
                # Special handling for new network state
                new_nodes = [
                    node for node in nodes
                    if node.total_validations == 0 and node.cooldown == 0
                ]
                if new_nodes:
                    selected = random.choice(new_nodes)
                    self._record_validator_selection(selected, shard_id)
                    return selected
                return None

            # Calculate scores
            scores = [
                self.calculate_cooperation_score(node, shard_id)
                for node in eligible_nodes
            ]
            total_score = sum(scores)

            if total_score <= 0:
                return None

            # Weighted random selection
            selection_point = random.uniform(0, total_score)
            current_sum = 0
            selected = None

            for node, score in zip(eligible_nodes, scores):
                current_sum += score
                if current_sum >= selection_point:
                    selected = node
                    break

            if selected:
                self._record_validator_selection(selected, shard_id)
                selected.enter_cooldown(self.cooldown_blocks)

            return selected

        except Exception as e:
            logger.error(f"Error selecting validator: {e}")
            return None

    def validate_block(self, block: Block, previous_block: Optional[Block], validator: Node) -> bool:
        """
        Validate block with comprehensive checks.
        
        Parameters:
        - block: The block to be validated.
        - previous_block: The previous block in the chain.
        - validator: The node validating the block.
        
        Returns:
        - bool: True if the block is valid, False otherwise.
        """
        try:
            # Eligibility check
            if not self._can_validate_block(validator, block.shard_id):
                logger.error(f"Validator {validator.node_id} not eligible")
                return False

            # Block integrity check
            if not block.validate(previous_block):
                logger.error(f"Block {block.index} failed validation")
                self._update_validation_stats(validator, block, False)
                return False

            # Collusion check
            if self.detect_collusion(validator, block):
                logger.warning(f"Collusion detected in block {block.index}")
                self._update_validation_stats(validator, block, False)
                return False

            # Success case
            self._update_validation_stats(validator, block, True)
            logger.info(f"Block {block.index} validated successfully")
            return True

        except Exception as e:
            logger.error(f"Error validating block: {e}")
            self._update_validation_stats(validator, block, False)
            return False

    def _can_validate_block(self, validator: Node, shard_id: Optional[int]) -> bool:
        """
        Check if validator can validate a specific block.
        
        Parameters:
        - validator: The node attempting to validate the block.
        - shard_id: Optional shard ID for validation.
        
        Returns:
        - bool: True if the validator can validate, False otherwise.
        """
        try:
            # New node allowance
            if validator.total_validations < 5:
                if validator.can_validate(shard_id):
                    return True
                return False

            # Standard validation checks
            if not validator.can_validate(shard_id):
                return False

            # Reputation threshold with experience-based scaling
            total_reputation = validator.get_total_reputation()
            reputation_requirement = self.min_reputation
            
            if validator.total_validations > 20:
                # Increase requirements for experienced validators
                reputation_requirement *= 1.2
            elif validator.total_validations > 10:
                reputation_requirement *= 1.0
            else:
                # Reduce requirements for newer validators
                reputation_requirement *= 0.7

            if total_reputation < reputation_requirement:
                return False

            # Check recent performance
            stats = self.validation_stats.get(validator.node_id, {})
            recent_success = stats.get("successful_validations", 0) / max(1, stats.get("selections", 1))
            
            if recent_success < self.validation_thresholds["min_success_rate"]:
                if validator.total_validations > 10:  # Only apply to experienced validators
                    return False

            return True

        except Exception as e:
            logger.error(f"Error checking block validation eligibility: {e}")
            return False

    def detect_collusion(self, validator: Node, block: Block) -> bool:
        """
        Detect collusion patterns in transactions.
        
        Parameters:
        - validator: The node attempting to validate the block.
        - block: The block being validated.
        
        Returns:
        - bool: True if collusion is detected, False otherwise.
        """
        try:
            if len(block.transactions) < 3:
                return False

            # Track transaction patterns
            sender_counts: Dict[str, int] = {}
            receiver_counts: Dict[str, int] = {}
            address_interactions: Dict[str, Set[str]] = {}
            
            # Analyze transaction patterns
            total_transactions = len(block.transactions)
            for tx in block.transactions:
                # Update counts
                sender_counts[tx.sender] = sender_counts.get(tx.sender, 0) + 1
                receiver_counts[tx.receiver] = receiver_counts.get(tx.receiver, 0) + 1
                
                # Track interactions
                if tx.sender not in address_interactions:
                    address_interactions[tx.sender] = set()
                address_interactions[tx.sender].add(tx.receiver)

            # Calculate concentration metrics
            max_sender_concentration = max(sender_counts.values()) / total_transactions
            max_receiver_concentration = max(receiver_counts.values()) / total_transactions
            
            # Dynamic threshold based on block size
            threshold = self.collusion_threshold
            if total_transactions > 10:
                threshold *= 0.9  # Stricter for larger blocks
            elif total_transactions > 20:
                threshold *= 0.85  # Even stricter for very large blocks
            
            # Check for suspicious patterns
            if max_sender_concentration > threshold or max_receiver_concentration > threshold:
                logger.warning(
                    f"High concentration detected - Sender: {max_sender_concentration:.2f}, "
                    f"Receiver: {max_receiver_concentration:.2f}"
                )
                return True
                
            # Check for circular transaction patterns
            for address, interactions in address_interactions.items():
                if len(interactions) > 2:  # Only check addresses with multiple interactions
                    for receiver in interactions:
                        if receiver in address_interactions and address in address_interactions[receiver]:
                            logger.warning(f"Circular transaction pattern detected involving {address}")
                            return True

            return False

        except Exception as e:
            logger.error(f"Error detecting collusion: {e}")
            return False

    def _update_validation_stats(self, validator: Node, block: Block, success: bool) -> None:
        """
        Update validation statistics comprehensively.
        
        Parameters:
        - validator: The node that validated the block.
        - block: The block that was validated.
        - success: Whether the validation was successful.
        """
        try:
            if validator.node_id not in self.validation_stats:
                self.validation_stats[validator.node_id] = {
                    "selections": 0,
                    "successful_validations": 0,
                    "last_validation": None,
                    "shard_validations": {},
                    "consecutive_failures": 0
                }

            stats = self.validation_stats[validator.node_id]
            
            # Update general stats
            stats["selections"] += 1
            if success:
                stats["successful_validations"] += 1
                stats["consecutive_failures"] = 0
            else:
                stats["consecutive_failures"] += 1
                
            stats["last_validation"] = datetime.now()

            # Update shard-specific stats
            if block.shard_id is not None:
                shard_stats = stats["shard_validations"].setdefault(
                    block.shard_id, {
                        "selections": 0,
                        "successful": 0,
                        "last_validation": None,
                        "failure_count": 0
                    }
                )
                shard_stats["selections"] += 1
                if success:
                    shard_stats["successful"] += 1
                else:
                    shard_stats["failure_count"] += 1
                shard_stats["last_validation"] = datetime.now()

            # Update performance metrics
            self.performance_metrics["total_validations"] += 1
            if success:
                self.performance_metrics["successful_validations"] += 1
            else:
                self.performance_metrics["failed_validations"] += 1

            # Track new node participation
            if validator.total_validations < 10:
                self.performance_metrics["new_node_participations"] += 1

        except Exception as e:
            logger.error(f"Error updating validation stats: {e}")

    def get_metrics(self) -> Dict:
        """
        Get comprehensive consensus metrics.
        
        Returns:
        - Dict: A dictionary of consensus metrics.
        """
        try:
            total_validations = self.performance_metrics["total_validations"]
            if total_validations > 0:
                success_rate = (
                    self.performance_metrics["successful_validations"] / total_validations
                )
                avg_score = sum(self.cooperation_scores.values()) / len(self.cooperation_scores) if self.cooperation_scores else 0
            else:
                success_rate = 0
                avg_score = 0
            
            return {
                "active_validators": len(self.validation_stats),
                "total_validations": total_validations,
                "successful_validations": self.performance_metrics["successful_validations"],
                "success_rate": success_rate,
                "average_block_time": self.performance_metrics["average_block_time"],
                "collusion_detections": self.performance_metrics["collusion_detections"],
                "failed_validations": self.performance_metrics["failed_validations"],
                "total_blocks_validated": self.performance_metrics["total_blocks_validated"],
                "new_node_participations": self.performance_metrics["new_node_participations"],
                "average_cooperation_score": avg_score,
                "total_active_nodes": len([
                    node_id for node_id, stats in self.validation_stats.items()
                    if datetime.now() - stats["last_validation"] < timedelta(hours=24)
                    if stats["last_validation"]
                ])
            }
            
        except Exception as e:
            logger.error(f"Error getting metrics: {e}")
            return {}
