# blockchain/consensus/proof_of_cooperation.py

from __future__ import annotations
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Set
import math
import random
import logging
from ..core.node import Node
from ..core.block import Block

logger = logging.getLogger(__name__)

class ProofOfCooperation:
    """
    Implements the Proof of Cooperation consensus mechanism.
    
    This consensus mechanism prioritizes cooperative behavior and community
    contribution over computational power or stake. It uses reputation scores,
    diversity of cooperation, and historical performance to select validators.
    """
    
    def __init__(self, min_reputation: float = 10.0, cooldown_blocks: int = 3):
        self.min_reputation = min_reputation
        self.cooldown_blocks = cooldown_blocks
        self.cooperation_scores: Dict[str, float] = {}
        self.reputation_weights = {
            'cooperative_growth': 1.5,    # Cooperative development and expansion
            'proposal_participation': 1.2, # Governance participation
            'transaction_validation': 1.0, # Basic validation work
            'resource_sharing': 1.3,      # Contributing resources
            'conflict_resolution': 1.1,   # Helping resolve disputes
            'community_building': 1.4,    # Community engagement
            'sustainability': 1.2,        # Long-term contribution
            'innovation': 1.3,            # New features/improvements
            'network_stability': 1.1,     # Reliable operation
            'data_availability': 1.2      # Data provision
        }
        self.validation_thresholds = {
            'min_participation': 0.1,     # Minimum participation rate
            'min_success_rate': 0.8,      # Minimum validation success rate
            'min_availability': 0.95,     # Minimum node availability
            'max_consecutive_validations': 3  # Max blocks in a row
        }
        self.reputation_decay_factor = 0.99  # Daily reputation decay
        self.collusion_threshold = 0.8    # Threshold for detecting collusion
        self.validator_history: List[Tuple[str, datetime, int]] = []
        self.validation_stats: Dict[str, Dict] = {}
        self.performance_metrics: Dict[str, float] = {
            'average_block_time': 0.0,
            'total_validations': 0,
            'successful_validations': 0,
            'collusion_detections': 0
        }

    def calculate_cooperation_score(self, node: Node, shard_id: Optional[int] = None) -> float:
        """Calculate a node's cooperation score based on multiple factors."""
        if not node.can_validate(shard_id):
            return 0.0

        try:
            # Calculate base score from weighted reputation
            base_score = sum(
                score * self.reputation_weights.get(category, 1.0)
                for category, score in node.reputation_scores.items()
            )
            
            # Apply modifiers
            diversity_factor = self._calculate_diversity_factor(node)
            consistency_factor = self._calculate_consistency_factor(node)
            performance_factor = self._calculate_performance_factor(node)
            shard_factor = self._calculate_shard_factor(node, shard_id) if shard_id else 1.0
            
            # Combine factors
            final_score = (base_score * diversity_factor * consistency_factor * 
                         performance_factor * shard_factor)
                         
            return max(0, final_score)
            
        except Exception as e:
            logger.error(f"Failed to calculate cooperation score: {str(e)}")
            return 0.0

    def _calculate_diversity_factor(self, node: Node) -> float:
        """Calculate diversity factor based on cooperative interactions."""
        try:
            recent_interactions = node.cooperative_interactions[-100:]
            if not recent_interactions:
                return 1.0
                
            unique_coops = len(set(recent_interactions))
            total_interactions = len(recent_interactions)
            
            diversity_score = unique_coops / total_interactions
            normalized_score = 1.0 + math.log(diversity_score + 1)
            
            return max(self.validation_thresholds['min_participation'], normalized_score)
            
        except Exception as e:
            logger.error(f"Failed to calculate diversity factor: {str(e)}")
            return self.validation_thresholds['min_participation']

    def _calculate_consistency_factor(self, node: Node) -> float:
        """Calculate consistency factor based on validation history."""
        try:
            if not node.validation_history:
                return 1.0
                
            recent_validations = node.validation_history[-50:]
            successful = sum(1 for v in recent_validations 
                           if v.get('evidence', {}).get('success', False))
            
            success_rate = successful / len(recent_validations)
            return max(self.validation_thresholds['min_success_rate'], success_rate)
            
        except Exception as e:
            logger.error(f"Failed to calculate consistency factor: {str(e)}")
            return self.validation_thresholds['min_success_rate']

    def _calculate_performance_factor(self, node: Node) -> float:
        """Calculate performance factor based on node metrics."""
        try:
            metrics = node.performance_metrics
            if not metrics:
                return 1.0

            factors = [
                metrics.get('availability', 0) / 100,
                metrics.get('validation_success_rate', 0) / 100,
                metrics.get('network_reliability', 0) / 100
            ]
            
            avg_performance = sum(factors) / len(factors)
            return max(self.validation_thresholds['min_availability'], avg_performance)
            
        except Exception as e:
            logger.error(f"Failed to calculate performance factor: {str(e)}")
            return self.validation_thresholds['min_availability']

    def _calculate_shard_factor(self, node: Node, shard_id: int) -> float:
        """Calculate shard-specific performance factor."""
        try:
            if shard_id not in node.active_shards:
                return 0.0
                
            # Consider time spent in shard
            time_in_shard = (datetime.now() - 
                           node.active_shards[shard_id]).total_seconds()
            shard_experience = min(1.0, time_in_shard / (24 * 3600))
            
            return 0.5 + (0.5 * shard_experience)
            
        except Exception as e:
            logger.error(f"Failed to calculate shard factor: {str(e)}")
            return 0.0

    def select_validator(self, nodes: List[Node], shard_id: Optional[int] = None) -> Optional[Node]:
        """Select the next validator using weighted random selection."""
        try:
            eligible_nodes = [
                node for node in nodes 
                if self._is_eligible_validator(node, shard_id)
            ]
            
            if not eligible_nodes:
                logger.warning("No eligible validators available")
                return None
                
            # Calculate scores for eligible nodes
            scores = [
                self.calculate_cooperation_score(node, shard_id) 
                for node in eligible_nodes
            ]
            total_score = sum(scores)
            
            if total_score <= 0:
                logger.warning("No nodes with positive cooperation scores")
                selected = random.choice(eligible_nodes)
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
            
            return selected
            
        except Exception as e:
            logger.error(f"Failed to select validator: {str(e)}")
            return None

    def _is_eligible_validator(self, node: Node, shard_id: Optional[int] = None) -> bool:
        """Check if a node is eligible to validate blocks."""
        try:
            if not node.can_validate(shard_id):
                return False
                
            # Check minimum reputation requirement
            if node.get_total_reputation() < self.min_reputation:
                return False
                
            # Check performance factors
            performance_factor = self._calculate_performance_factor(node)
            if performance_factor < self.validation_thresholds['min_availability']:
                return False
                
            # Check recent selections to prevent concentration
            recent_validations = [
                v[0] for v in self.validator_history[-10:]
                if v[0] == node.node_id
            ]
            if len(recent_validations) >= self.validation_thresholds['max_consecutive_validations']:
                return False
                
            return True
            
        except Exception as e:
            logger.error(f"Failed to check validator eligibility: {str(e)}")
            return False

    def _record_validator_selection(self, node: Node, shard_id: Optional[int]) -> None:
        """Record validator selection for statistics."""
        try:
            self.validator_history.append((node.node_id, datetime.now(), shard_id))
            if len(self.validator_history) > 1000:
                self.validator_history = self.validator_history[-1000:]
                
            if node.node_id not in self.validation_stats:
                self.validation_stats[node.node_id] = {
                    'selections': 0,
                    'successful_validations': 0,
                    'last_selected': None,
                    'shard_validations': {}
                }
                
            stats = self.validation_stats[node.node_id]
            stats['selections'] += 1
            stats['last_selected'] = datetime.now()
            
            if shard_id is not None:
                shard_stats = stats['shard_validations'].setdefault(shard_id, {
                    'selections': 0,
                    'successful': 0
                })
                shard_stats['selections'] += 1
                
        except Exception as e:
            logger.error(f"Failed to record validator selection: {str(e)}")

    def validate_block(self, block: Block, previous_block: Optional[Block],
                      validator: Node) -> bool:
        """Validate a proposed block."""
        try:
            # Verify validator eligibility
            if not self._is_eligible_validator(validator, block.shard_id):
                logger.error("Validator not eligible")
                return False
                
            # Perform block validation
            if not block.validate(previous_block):
                logger.error("Block validation failed")
                return False
                
            # Update statistics
            self._update_validation_stats(validator, block, True)
            
            return True
            
        except Exception as e:
            logger.error(f"Block validation failed: {str(e)}")
            self._update_validation_stats(validator, block, False)
            return False

    def _update_validation_stats(self, validator: Node, block: Block,
                               success: bool) -> None:
        """Update validation statistics."""
        try:
            stats = self.validation_stats.get(validator.node_id, {
                'selections': 0,
                'successful_validations': 0,
                'shard_validations': {}
            })
            
            if success:
                stats['successful_validations'] += 1
                
            if block.shard_id is not None:
                shard_stats = stats['shard_validations'].setdefault(block.shard_id, {
                    'selections': 0,
                    'successful': 0
                })
                if success:
                    shard_stats['successful'] += 1
                    
            self.validation_stats[validator.node_id] = stats
            
        except Exception as e:
            logger.error(f"Failed to update validation stats: {str(e)}")

    def detect_collusion(self, validator: Node, block: Block) -> bool:
        """Detect potential collusion patterns."""
        try:
            # Check for repeated validations with same actors
            recent_validations = validator.validation_history[-20:]
            interaction_counts: Dict[str, int] = {}
            
            for validation in recent_validations:
                for tx in block.transactions:
                    interaction_counts[tx.sender] = (
                        interaction_counts.get(tx.sender, 0) + 1
                    )
                    interaction_counts[tx.receiver] = (
                        interaction_counts.get(tx.receiver, 0) + 1
                    )
            
            # Check if any actor appears too frequently
            max_interactions = max(interaction_counts.values(), default=0)
            if max_interactions / len(recent_validations) > self.collusion_threshold:
                logger.warning(f"Potential collusion detected for validator {validator.node_id}")
                self.performance_metrics['collusion_detections'] += 1
                return True
                
            return False
            
        except Exception as e:
            logger.error(f"Failed to detect collusion: {str(e)}")
            return False

    def get_metrics(self) -> Dict:
        """Get consensus mechanism metrics."""
        try:
            return {
                'active_validators': len(self.validation_stats),
                'total_validations': self.performance_metrics['total_validations'],
                'successful_validations': self.performance_metrics['successful_validations'],
                'average_block_time': self.performance_metrics['average_block_time'],
                'collusion_detections': self.performance_metrics['collusion_detections']
            }
        except Exception as e:
            logger.error(f"Failed to get metrics: {str(e)}")
            return {}