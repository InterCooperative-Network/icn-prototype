"""
blockchain/consensus/proof_of_cooperation/base.py

Implements the core Proof of Cooperation consensus mechanism.
"""

from typing import List, Optional, Dict, Any
from datetime import datetime
import logging

from .reputation_manager import ReputationManager
from .collusion_detector import CollusionDetector
from .sanctions_manager import SanctionsManager
from .validator_manager import ValidatorManager
from .metrics_manager import MetricsManager
from .cooldown_manager import CooldownManager
from .types import ConsensusConfig, ValidationResult
from ...core.node import Node
from ...core.block import Block

logger = logging.getLogger(__name__)

class ProofOfCooperation:
    """
    Implements the Proof of Cooperation consensus mechanism for the ICN.
    
    Key Features:
    - Modular integration with collusion detection, reputation, and sanctions management.
    - Progressive reputation requirements for new nodes.
    - Dynamic scoring adjustments.
    - Enhanced validator eligibility checks.
    - Improved shard-specific handling.
    """

    def __init__(self, min_reputation: float = 10.0, cooldown_blocks: int = 3):
        """Initialize the PoC mechanism with its component managers."""
        self.config = ConsensusConfig(
            min_reputation=min_reputation,
            cooldown_blocks=cooldown_blocks
        )
        
        # Initialize component managers
        self.reputation_manager = ReputationManager(self.config)
        self.collusion_detector = CollusionDetector()
        self.sanctions_manager = SanctionsManager(self.collusion_detector, self.reputation_manager)
        self.validator_manager = ValidatorManager(
            min_reputation=min_reputation, 
            cooldown_blocks=cooldown_blocks, 
            collusion_detector=self.collusion_detector
        )
        self.metrics_manager = MetricsManager()
        self.cooldown_manager = CooldownManager(cooldown_blocks)

    def select_validator(self, nodes: List[Node], shard_id: Optional[int] = None) -> Optional[Node]:
        """
        Select a validator using modular checks for eligibility and cooperation score.
        
        Args:
            nodes: List of potential validator nodes
            shard_id: Optional shard ID for selection
            
        Returns:
            Optional[Node]: Selected validator node, if any
        """
        try:
            # Get list of eligible nodes
            eligible_nodes = [
                node for node in nodes 
                if self._can_validate(node, shard_id)
            ]
            
            if not eligible_nodes:
                logger.warning("No eligible validators available")
                return None

            # Calculate cooperation scores
            scores = [
                self.reputation_manager.calculate_cooperation_score(node, shard_id)
                for node in eligible_nodes
            ]
            total_score = sum(scores)
            
            if total_score <= 0:
                return None

            # Weighted random selection
            selection_point = random.uniform(0, total_score)
            current_sum = 0
            
            for node, score in zip(eligible_nodes, scores):
                current_sum += score
                if current_sum >= selection_point:
                    # Apply cooldown and record selection
                    self.cooldown_manager.apply_cooldown(node)
                    self.metrics_manager.record_validation(
                        ValidationResult(success=True), node.node_id, shard_id
                    )
                    logger.info(f"Selected validator {node.node_id} for shard {shard_id}")
                    return node

            return None

        except Exception as e:
            logger.error(f"Error selecting validator: {str(e)}")
            return None

    def validate_block(self, block: Block, previous_block: Optional[Block], validator: Node) -> bool:
        """
        Validate a block using reputation, sanctions, and collusion checks.
        
        Args:
            block: The block to validate
            previous_block: Previous block in the chain
            validator: The validating node
            
        Returns:
            bool: True if block is valid
        """
        try:
            # Basic validation checks
            if not self._can_validate_block(validator, block.shard_id):
                logger.error(f"Validator {validator.node_id} not eligible")
                return False

            if not block.validate(previous_block):
                logger.error(f"Block {block.index} failed validation")
                self._update_validation_stats(validator, block, False)
                return False

            # Check for collusion
            if self.collusion_detector.detect_collusion(validator, block):
                logger.warning(f"Collusion detected for block {block.index}")
                self.sanctions_manager.apply_sanction(validator)
                self._update_validation_stats(validator, block, False)
                return False

            # Update metrics and return success
            self._update_validation_stats(validator, block, True)
            logger.info(f"Block {block.index} validated successfully")
            return True

        except Exception as e:
            logger.error(f"Error validating block: {str(e)}")
            self._update_validation_stats(validator, block, False)
            return False

    def _can_validate(self, node: Node, shard_id: Optional[int] = None) -> bool:
        """
        Determine if a node can participate in consensus.
        
        Args:
            node: Node to check
            shard_id: Optional shard ID for specific checks
            
        Returns:
            bool: True if node can participate
        """
        try:
            # Check if node is under sanctions
            if self.sanctions_manager.get_sanction_status(node)[0] > 0:
                return False

            # Check reputation requirements
            if not self.reputation_manager.can_validate(node, shard_id):
                return False

            # Check cooldown status
            if not self.cooldown_manager.is_eligible(node):
                return False

            # Shard-specific checks
            if shard_id is not None and not node.can_validate(shard_id):
                return False

            return True

        except Exception as e:
            logger.error(f"Error checking validation eligibility: {str(e)}")
            return False

    def _can_validate_block(self, validator: Node, shard_id: Optional[int]) -> bool:
        """Check if validator can validate a specific block."""
        return self._can_validate(validator, shard_id)

    def _update_validation_stats(self, validator: Node, block: Block, success: bool) -> None:
        """
        Update validation statistics for a validator.
        
        Args:
            validator: The validating node
            block: The validated block
            success: Whether validation was successful
        """
        try:
            result = ValidationResult(
                success=success,
                block_height=block.index,
                shard_id=block.shard_id
            )
            
            # Update component metrics
            self.metrics_manager.record_validation(result, validator.node_id, block.shard_id)
            self.reputation_manager.update_stats(validator.node_id, result, block.shard_id)
            
            # Update validator reputation
            if success:
                self.reputation_manager.update_validator_reputation(validator, 1.0)
            else:
                self.reputation_manager.update_validator_reputation(validator, -1.0)

        except Exception as e:
            logger.error(f"Error updating validation stats: {str(e)}")

    def get_metrics(self) -> Dict[str, Any]:
        """Get comprehensive consensus metrics."""
        return {
            "reputation_metrics": self.reputation_manager.get_metrics(),
            "validation_metrics": self.metrics_manager.get_metrics(),
            "sanctions_metrics": self.sanctions_manager.get_metrics(),
            "collusion_metrics": self.collusion_detector.get_metrics()
        }

    def to_dict(self) -> Dict[str, Any]:
        """Convert consensus state to dictionary format."""
        return {
            "config": self.config.to_dict(),
            "reputation_manager": self.reputation_manager.to_dict(),
            "sanctions_manager": self.sanctions_manager.to_dict(),
            "metrics_manager": self.metrics_manager.to_dict(),
            "cooldown_manager": self.cooldown_manager.to_dict()
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ProofOfCooperation':
        """Create consensus instance from dictionary."""
        config = ConsensusConfig.from_dict(data["config"])
        instance = cls(
            min_reputation=config.min_reputation,
            cooldown_blocks=config.cooldown_blocks
        )
        
        instance.reputation_manager = ReputationManager.from_dict(data["reputation_manager"])
        instance.sanctions_manager = SanctionsManager.from_dict(data["sanctions_manager"])
        instance.metrics_manager = MetricsManager.from_dict(data["metrics_manager"])
        instance.cooldown_manager = CooldownManager.from_dict(data["cooldown_manager"])
        
        return instance