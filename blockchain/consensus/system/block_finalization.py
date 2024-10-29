"""
blockchain/system/block_finalization.py

This module implements block finalization for the ICN blockchain.
It handles the process of making blocks irreversible once they meet
specific criteria, including cooperative validation thresholds and
cross-shard consistency requirements.
"""

import asyncio
import logging
from typing import Dict, List, Optional, Set, Any
from datetime import datetime, timedelta
from dataclasses import dataclass, field

from ..core.block import Block
from ..core.state.unified_state import UnifiedStateManager
from ..consensus.proof_of_cooperation import ProofOfCooperation
from ..core.shard.state_verifier import StateVerifier
from .chain_reorganization import ChainReorganizationManager

logger = logging.getLogger(__name__)

@dataclass
class FinalizationCriteria:
    """Criteria required for block finalization."""
    min_validator_confirmations: int = 5
    min_cooperative_confirmations: int = 3
    confirmation_depth: int = 100
    cross_shard_validation_threshold: float = 0.8
    time_threshold: timedelta = field(default_factory=lambda: timedelta(hours=1))

@dataclass
class BlockFinalizationStatus:
    """Tracks the finalization status of a block."""
    block: Block
    validator_confirmations: Set[str] = field(default_factory=set)
    cooperative_confirmations: Set[str] = field(default_factory=set)
    cross_shard_validations: Dict[int, Set[str]] = field(default_factory=dict)
    first_confirmation_time: Optional[datetime] = None
    finalization_time: Optional[datetime] = None
    is_finalized: bool = False

class BlockFinalizationManager:
    """
    Manages the process of finalizing blocks in the ICN blockchain.
    
    Responsibilities:
    - Track block confirmations from validators
    - Ensure cross-shard consistency
    - Apply finalization criteria
    - Maintain finalization history
    - Coordinate with reorganization manager
    """

    def __init__(
        self,
        state_manager: UnifiedStateManager,
        consensus: ProofOfCooperation,
        state_verifier: StateVerifier,
        reorg_manager: ChainReorganizationManager,
        criteria: Optional[FinalizationCriteria] = None
    ):
        """Initialize block finalization manager."""
        self.state_manager = state_manager
        self.consensus = consensus
        self.state_verifier = state_verifier
        self.reorg_manager = reorg_manager
        self.criteria = criteria or FinalizationCriteria()
        
        # Track finalization status
        self.pending_finalization: Dict[str, BlockFinalizationStatus] = {}
        self.finalized_blocks: Dict[str, BlockFinalizationStatus] = {}
        self.finalization_height: int = 0
        self.checkpoints: Dict[int, str] = {}  # height -> block_hash
        
        # Background tasks
        self._finalization_task: Optional[asyncio.Task] = None
        self._checkpoint_task: Optional[asyncio.Task] = None
        self.is_running = False

    async def start(self) -> None:
        """Start the finalization manager."""
        if self.is_running:
            return

        self.is_running = True
        self._finalization_task = asyncio.create_task(self._finalization_loop())
        self._checkpoint_task = asyncio.create_task(self._checkpoint_loop())
        logger.info("Block finalization manager started")

    async def stop(self) -> None:
        """Stop the finalization manager."""
        self.is_running = False
        
        if self._finalization_task:
            self._finalization_task.cancel()
            try:
                await self._finalization_task
            except asyncio.CancelledError:
                pass
            
        if self._checkpoint_task:
            self._checkpoint_task.cancel()
            try:
                await self._checkpoint_task
            except asyncio.CancelledError:
                pass
                
        logger.info("Block finalization manager stopped")

    async def add_confirmation(
        self,
        block: Block,
        validator_id: str,
        cooperative_id: Optional[str] = None
    ) -> None:
        """
        Add a validator confirmation for a block.

        Args:
            block: The block being confirmed
            validator_id: ID of the confirming validator
            cooperative_id: Optional cooperative ID of the validator
        """
        try:
            block_hash = block.hash
            if block_hash not in self.pending_finalization:
                self.pending_finalization[block_hash] = BlockFinalizationStatus(block=block)
                
            status = self.pending_finalization[block_hash]
            
            # Record confirmation time if first confirmation
            if not status.first_confirmation_time:
                status.first_confirmation_time = datetime.now()
            
            # Add validator confirmation
            status.validator_confirmations.add(validator_id)
            
            # Add cooperative confirmation if provided
            if cooperative_id:
                status.cooperative_confirmations.add(cooperative_id)
            
            # Check if block can be finalized
            if await self._check_finalization_criteria(status):
                await self._finalize_block(status)
                
        except Exception as e:
            logger.error(f"Error adding confirmation: {str(e)}")

    async def add_cross_shard_validation(
        self,
        block: Block,
        shard_id: int,
        validator_id: str
    ) -> None:
        """
        Add a cross-shard validation for a block.

        Args:
            block: The block being validated
            shard_id: ID of the validating shard
            validator_id: ID of the validating node
        """
        try:
            block_hash = block.hash
            if block_hash not in self.pending_finalization:
                self.pending_finalization[block_hash] = BlockFinalizationStatus(block=block)
                
            status = self.pending_finalization[block_hash]
            
            # Initialize cross-shard validation set if needed
            if shard_id not in status.cross_shard_validations:
                status.cross_shard_validations[shard_id] = set()
                
            # Add validation
            status.cross_shard_validations[shard_id].add(validator_id)
            
            # Check if block can be finalized
            if await self._check_finalization_criteria(status):
                await self._finalize_block(status)
                
        except Exception as e:
            logger.error(f"Error adding cross-shard validation: {str(e)}")

    async def _check_finalization_criteria(self, status: BlockFinalizationStatus) -> bool:
        """
        Check if a block meets all finalization criteria.

        Args:
            status: The block's finalization status

        Returns:
            bool: True if block meets finalization criteria
        """
        try:
            # Check if already finalized
            if status.is_finalized:
                return False

            # Check validator confirmations
            if len(status.validator_confirmations) < self.criteria.min_validator_confirmations:
                return False

            # Check cooperative confirmations
            if len(status.cooperative_confirmations) < self.criteria.min_cooperative_confirmations:
                return False

            # Check confirmation depth
            current_height = self.state_manager.get_height()
            if current_height - status.block.index < self.criteria.confirmation_depth:
                return False

            # Check cross-shard validations
            if status.block.cross_shard_refs:
                validation_ratio = self._calculate_cross_shard_validation_ratio(status)
                if validation_ratio < self.criteria.cross_shard_validation_threshold:
                    return False

            # Check time threshold
            if (datetime.now() - status.first_confirmation_time
                < self.criteria.time_threshold):
                return False

            return True

        except Exception as e:
            logger.error(f"Error checking finalization criteria: {str(e)}")
            return False

    def _calculate_cross_shard_validation_ratio(
        self,
        status: BlockFinalizationStatus
    ) -> float:
        """Calculate the ratio of completed cross-shard validations."""
        try:
            total_shards = len(self.state_manager.get_active_shards())
            validated_shards = len(status.cross_shard_validations)
            
            if total_shards == 0:
                return 0.0
                
            return validated_shards / total_shards
            
        except Exception as e:
            logger.error(f"Error calculating validation ratio: {str(e)}")
            return 0.0

    async def _finalize_block(self, status: BlockFinalizationStatus) -> None:
        """
        Finalize a block that has met all criteria.

        Args:
            status: The block's finalization status
        """
        try:
            block_hash = status.block.hash
            
            # Update finalization status
            status.is_finalized = True
            status.finalization_time = datetime.now()
            
            # Move from pending to finalized
            self.finalized_blocks[block_hash] = status
            if block_hash in self.pending_finalization:
                del self.pending_finalization[block_hash]
            
            # Update finalization height if necessary
            if status.block.index > self.finalization_height:
                self.finalization_height = status.block.index
            
            # Create checkpoint if needed
            if self._should_create_checkpoint(status.block):
                await self._create_checkpoint(status.block)
            
            # Notify components
            await self._notify_finalization(status)
            
            logger.info(f"Finalized block at height {status.block.index}")
            
        except Exception as e:
            logger.error(f"Error finalizing block: {str(e)}")

    async def _finalization_loop(self) -> None:
        """Background task to process pending finalizations."""
        while self.is_running:
            try:
                # Check all pending blocks
                for block_hash, status in list(self.pending_finalization.items()):
                    if await self._check_finalization_criteria(status):
                        await self._finalize_block(status)
                        
                await asyncio.sleep(1)  # Prevent tight loop
                
            except Exception as e:
                logger.error(f"Error in finalization loop: {str(e)}")
                await asyncio.sleep(5)  # Back off on error

    async def _checkpoint_loop(self) -> None:
        """Background task to create periodic checkpoints."""
        while self.is_running:
            try:
                # Find latest finalized block
                latest_height = max(
                    (status.block.index for status in self.finalized_blocks.values()),
                    default=0
                )
                
                # Create checkpoint if needed
                if latest_height > 0:
                    latest_block = next(
                        status.block
                        for status in self.finalized_blocks.values()
                        if status.block.index == latest_height
                    )
                    if self._should_create_checkpoint(latest_block):
                        await self._create_checkpoint(latest_block)
                
                await asyncio.sleep(60)  # Check every minute
                
            except Exception as e:
                logger.error(f"Error in checkpoint loop: {str(e)}")
                await asyncio.sleep(5)

    def _should_create_checkpoint(self, block: Block) -> bool:
        """Determine if a checkpoint should be created for a block."""
        try:
            # Create checkpoint every 1000 blocks
            checkpoint_interval = 1000
            
            # Check if height is at checkpoint interval
            if block.index % checkpoint_interval != 0:
                return False
                
            # Check if checkpoint already exists
            if block.index in self.checkpoints:
                return False
                
            return True
            
        except Exception as e:
            logger.error(f"Error checking checkpoint criteria: {str(e)}")
            return False

    async def _create_checkpoint(self, block: Block) -> None:
        """Create a checkpoint for a finalized block."""
        try:
            # Get state root at block height
            state_root = await self.state_manager.get_state_root(block.index)
            if not state_root:
                return
                
            # Create checkpoint
            self.checkpoints[block.index] = block.hash
            
            # Persist checkpoint data
            checkpoint_data = {
                "height": block.index,
                "block_hash": block.hash,
                "state_root": state_root,
                "timestamp": datetime.now().isoformat()
            }
            
            await self._persist_checkpoint(checkpoint_data)
            logger.info(f"Created checkpoint at height {block.index}")
            
        except Exception as e:
            logger.error(f"Error creating checkpoint: {str(e)}")

    async def _persist_checkpoint(self, checkpoint_data: Dict[str, Any]) -> None:
        """Persist checkpoint data to storage."""
        try:
            # Save checkpoint to state manager
            await self.state_manager.save_checkpoint(checkpoint_data)
            
        except Exception as e:
            logger.error(f"Error persisting checkpoint: {str(e)}")

    async def _notify_finalization(self, status: BlockFinalizationStatus) -> None:
        """Notify relevant components about block finalization."""
        try:
            # Notify consensus mechanism
            await self.consensus.handle_block_finalization(status.block)
            
            # Notify state manager
            await self.state_manager.handle_block_finalization(status.block)
            
            # Notify reorg manager
            await self.reorg_manager.handle_block_finalization(status.block)
            
        except Exception as e:
            logger.error(f"Error notifying finalization: {str(e)}")

    def is_finalized(self, block_hash: str) -> bool:
        """Check if a block is finalized."""
        return block_hash in self.finalized_blocks

    def get_finalization_height(self) -> int:
        """Get the current finalization height."""
        return self.finalization_height

    def get_finalization_stats(self) -> Dict[str, Any]:
        """Get statistics about block finalization."""
        try:
            total_finalized = len(self.finalized_blocks)
            if total_finalized == 0:
                return {
                    "total_finalized": 0,
                    "average_time_to_finalize": 0,
                    "pending_finalizations": len(self.pending_finalization)
                }
            
            # Calculate average time to finalize
            total_time = sum(
                (status.finalization_time - status.first_confirmation_time).total_seconds()
                for status in self.finalized_blocks.values()
                if status.finalization_time and status.first_confirmation_time
            )
            
            return {
                "total_finalized": total_finalized,
                "average_time_to_finalize": total_time / total_finalized,
                "pending_finalizations": len(self.pending_finalization),
                "finalization_height": self.finalization_height,
                "checkpoints": len(self.checkpoints),
                "latest_checkpoint": max(self.checkpoints.keys(), default=0)
            }
            
        except Exception as e:
            logger.error(f"Error getting finalization stats: {str(e)}")
            return {}