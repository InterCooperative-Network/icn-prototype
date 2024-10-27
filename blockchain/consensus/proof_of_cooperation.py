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
from .collusion_detector import CollusionDetector
from .reputation_manager import ReputationManager
from .sanctions_manager import SanctionsManager

logger = logging.getLogger(__name__)

class ProofOfCooperation:
    """
    Implements the Proof of Cooperation consensus mechanism for the ICN.

    Key Improvements:
    - Modular integration with collusion detection, reputation, and sanctions management.
    - Progressive reputation requirements for new nodes.
    - Dynamic scoring adjustments.
    - Enhanced validator eligibility checks.
    - Improved shard-specific handling.
    """

    def __init__(self, reputation_manager: ReputationManager, collusion_detector: CollusionDetector, sanctions_manager: SanctionsManager, min_reputation: float = 10.0, cooldown_blocks: int = 3):
        """
        Initialize the Proof of Cooperation mechanism with external modules.

        Args:
            reputation_manager (ReputationManager): Instance for managing reputation.
            collusion_detector (CollusionDetector): Instance for detecting collusion.
            sanctions_manager (SanctionsManager): Instance for managing sanctions.
            min_reputation (float): Minimum reputation required to participate.
            cooldown_blocks (int): Number of blocks for the cooldown period after validation.
        """
        self.reputation_manager = reputation_manager
        self.collusion_detector = collusion_detector
        self.sanctions_manager = sanctions_manager
        self.min_reputation = min_reputation
        self.cooldown_blocks = cooldown_blocks
        
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
        Determine if a node can participate in consensus with modular validation checks.

        Args:
            node (Node): Node to be checked for participation eligibility.
            shard_id (Optional[int]): Optional shard ID for shard-specific checks.

        Returns:
            bool: True if node can participate, False otherwise.
        """
        try:
            if not node.can_validate(shard_id):
                return False

            # Check if the node is under sanctions
            sanction_level, status = self.sanctions_manager.get_sanction_status(node)
            if status == "permanently_excluded":
                return False

            # Reputation checks
            reputation_requirement = self.min_reputation
            if not self.reputation_manager.can_validate(node, shard_id):
                return False

            # Shard-specific checks
            if shard_id is not None and shard_id not in node.active_shards:
                return False

            return True

        except Exception as e:
            logger.error(f"Error in participation check: {e}")
            return False

    def calculate_cooperation_score(self, node: Node, shard_id: Optional[int] = None) -> float:
        """
        Calculate node's cooperation score using reputation manager and collusion detection.

        Args:
            node (Node): Node whose cooperation score is being calculated.
            shard_id (Optional[int]): Optional shard ID for shard-specific adjustments.

        Returns:
            float: Calculated cooperation score.
        """
        try:
            return self.reputation_manager.calculate_cooperation_score(node, shard_id)

        except Exception as e:
            logger.error(f"Error calculating cooperation score: {e}")
            return 0.0

    def select_validator(self, nodes: List[Node], shard_id: Optional[int] = None) -> Optional[Node]:
        """
        Select validator using modular checks for eligibility and cooperation score.

        Args:
            nodes (List[Node]): List of potential validator nodes.
            shard_id (Optional[int]): Optional shard ID for selection.

        Returns:
            Optional[Node]: Selected validator node, if any.
        """
        try:
            eligible_nodes = [node for node in nodes if self._can_participate(node, shard_id)]
            if not eligible_nodes:
                return None

            # Calculate cooperation scores
            scores = [self.calculate_cooperation_score(node, shard_id) for node in eligible_nodes]
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
                selected.enter_cooldown(self.cooldown_blocks)
                logger.info(f"Validator {selected.node_id} selected for shard {shard_id}")

            return selected

        except Exception as e:
            logger.error(f"Error selecting validator: {e}")
            return None

    def validate_block(self, block: Block, previous_block: Optional[Block], validator: Node) -> bool:
        """
        Validate block using reputation, sanctions, and collusion checks.

        Args:
            block (Block): The block to be validated.
            previous_block (Optional[Block]): The previous block in the chain.
            validator (Node): The node validating the block.

        Returns:
            bool: True if the block is valid, False otherwise.
        """
        try:
            if not self._can_validate_block(validator, block.shard_id):
                logger.error(f"Validator {validator.node_id} not eligible")
                return False

            if not block.validate(previous_block):
                logger.error(f"Block {block.index} failed validation")
                self._update_validation_stats(validator, block, False)
                return False

            if self.collusion_detector.detect_collusion(validator, block):
                logger.warning(f"Collusion detected in block {block.index} by validator {validator.node_id}")
                self._update_validation_stats(validator, block, False)
                self.sanctions_manager.apply_sanction(validator)
                return False

            self._update_validation_stats(validator, block, True)
            logger.info(f"Block {block.index} validated successfully by {validator.node_id}")
            return True

        except Exception as e:
            logger.error(f"Error validating block: {e}")
            self._update_validation_stats(validator, block, False)
            return False

    def _can_validate_block(self, validator: Node, shard_id: Optional[int]) -> bool:
        """
        Check if validator can validate a block with modular eligibility checks.

        Args:
            validator (Node): The node attempting to validate the block.
            shard_id (Optional[int]): Optional shard ID for validation.

        Returns:
            bool: True if the validator can validate, False otherwise.
        """
        return self._can_participate(validator, shard_id)

    def _update_validation_stats(self, validator: Node, block: Block, success: bool) -> None:
        """
        Update validation statistics for modular tracking.

        Args:
            validator (Node): The node that validated the block.
            block (Block): The block that was validated.
            success (bool): Whether the validation was successful.
        """
        self.reputation_manager.update_stats(validator.node_id, ValidationResult(success), block.shard_id)

    def get_metrics(self) -> Dict:
        """
        Get comprehensive consensus metrics.

        Returns:
            Dict: A dictionary of consensus metrics.
        """
        return self.reputation_manager.to_dict()
