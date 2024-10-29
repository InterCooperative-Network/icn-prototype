"""
blockchain/system/block_production.py

Manages the scheduling and production of blocks in the ICN blockchain.
Coordinates block creation across shards while ensuring fair validator
participation and maintaining cooperative principles.
"""

import asyncio
import logging
from typing import Dict, List, Optional, Set, Any
from datetime import datetime, timedelta
from dataclasses import dataclass, field

from ..core.block import Block
from ..core.node import Node
from ..core.transaction import Transaction
from ..consensus.proof_of_cooperation import ProofOfCooperation
from ..core.state.unified_state import UnifiedStateManager

logger = logging.getLogger(__name__)

@dataclass
class ProductionSlot:
    """Represents a scheduled block production slot."""
    timestamp: datetime
    shard_id: int
    validator_id: Optional[str] = None
    is_filled: bool = False
    block_hash: Optional[str] = None
    attempts: int = 0
    max_attempts: int = 3
    
class BlockProductionScheduler:
    """
    Manages the scheduling and coordination of block production.
    
    Responsibilities:
    - Schedule block production slots
    - Select validators for block production
    - Coordinate cross-shard block production
    - Ensure fair validator participation
    - Handle production failures and timeouts
    """

    def __init__(
        self,
        consensus: ProofOfCooperation,
        state_manager: UnifiedStateManager,
        block_time: int = 15,  # seconds
        max_missed_slots: int = 3
    ):
        """Initialize the block production scheduler."""
        self.consensus = consensus
        self.state_manager = state_manager
        self.block_time = block_time
        self.max_missed_slots = max_missed_slots
        
        # Production scheduling
        self.production_slots: Dict[int, Dict[int, ProductionSlot]] = {}  # height -> shard_id -> slot
        self.current_height = 0
        self.last_production_time: Dict[int, datetime] = {}  # shard_id -> timestamp
        
        # Validator tracking
        self.active_producers: Dict[str, int] = {}  # validator_id -> assigned_height
        self.missed_slots: Dict[str, int] = {}  # validator_id -> count
        
        # Background tasks
        self.scheduler_task: Optional[asyncio.Task] = None
        self.cleanup_task: Optional[asyncio.Task] = None
        self.is_running = False
        
        # Metrics
        self.metrics = {
            "slots_scheduled": 0,
            "blocks_produced": 0,
            "missed_slots": 0,
            "validator_participation": {},
            "average_block_time": 0.0,
            "cross_shard_blocks": 0
        }

    async def start(self) -> None:
        """Start the block production scheduler."""
        if self.is_running:
            return

        self.is_running = True
        self.scheduler_task = asyncio.create_task(self._scheduling_loop())
        self.cleanup_task = asyncio.create_task(self._cleanup_loop())
        logger.info("Block production scheduler started")

    async def stop(self) -> None:
        """Stop the block production scheduler."""
        self.is_running = False
        
        if self.scheduler_task:
            self.scheduler_task.cancel()
            try:
                await self.scheduler_task
            except asyncio.CancelledError:
                pass
            
        if self.cleanup_task:
            self.cleanup_task.cancel()
            try:
                await self.cleanup_task
            except asyncio.CancelledError:
                pass

        logger.info("Block production scheduler stopped")

    async def schedule_block_production(self, height: int) -> None:
        """
        Schedule block production for a given height.

        Args:
            height: The block height to schedule
        """
        try:
            # Initialize slots for height
            self.production_slots[height] = {}
            
            # Schedule for each active shard
            active_shards = self.state_manager.get_active_shards()
            for shard_id in active_shards:
                # Calculate slot time
                slot_time = self._calculate_next_slot_time(shard_id)
                
                # Create production slot
                slot = ProductionSlot(
                    timestamp=slot_time,
                    shard_id=shard_id
                )
                
                # Assign validator
                validator = await self._select_validator(height, shard_id)
                if validator:
                    slot.validator_id = validator.node_id
                    self.active_producers[validator.node_id] = height
                
                self.production_slots[height][shard_id] = slot
                self.metrics["slots_scheduled"] += 1
                
            logger.info(f"Scheduled block production for height {height}")
            
        except Exception as e:
            logger.error(f"Error scheduling block production: {str(e)}")

    async def record_block_production(
        self,
        height: int,
        shard_id: int,
        block: Block,
        validator_id: str
    ) -> None:
        """
        Record successful block production.

        Args:
            height: Block height
            shard_id: Shard ID
            block: The produced block
            validator_id: ID of the producing validator
        """
        try:
            if height not in self.production_slots or shard_id not in self.production_slots[height]:
                logger.error(f"No production slot found for height {height}, shard {shard_id}")
                return

            slot = self.production_slots[height][shard_id]
            
            # Update slot status
            slot.is_filled = True
            slot.block_hash = block.hash
            
            # Update metrics
            self.metrics["blocks_produced"] += 1
            self.last_production_time[shard_id] = datetime.now()
            
            if block.cross_shard_refs:
                self.metrics["cross_shard_blocks"] += 1
                
            # Update validator participation
            if validator_id not in self.metrics["validator_participation"]:
                self.metrics["validator_participation"][validator_id] = 0
            self.metrics["validator_participation"][validator_id] += 1
            
            # Clear from active producers
            if validator_id in self.active_producers:
                del self.active_producers[validator_id]
                
            # Reset missed slots for validator
            if validator_id in self.missed_slots:
                del self.missed_slots[validator_id]
                
            # Update average block time
            self._update_average_block_time(slot)
            
            logger.info(f"Recorded block production for height {height}, shard {shard_id}")
            
        except Exception as e:
            logger.error(f"Error recording block production: {str(e)}")

    async def record_missed_slot(self, height: int, shard_id: int) -> None:
        """
        Record a missed production slot.

        Args:
            height: Block height
            shard_id: Shard ID
        """
        try:
            if height not in self.production_slots or shard_id not in self.production_slots[height]:
                return

            slot = self.production_slots[height][shard_id]
            validator_id = slot.validator_id
            
            if validator_id:
                # Update missed slots count
                if validator_id not in self.missed_slots:
                    self.missed_slots[validator_id] = 0
                self.missed_slots[validator_id] += 1
                
                # Handle excessive misses
                if self.missed_slots[validator_id] >= self.max_missed_slots:
                    await self._handle_excessive_misses(validator_id)
                    
            # Update metrics
            self.metrics["missed_slots"] += 1
            
            # Try to reassign slot if attempts remain
            if slot.attempts < slot.max_attempts:
                slot.attempts += 1
                await self._reassign_slot(height, shard_id, slot)
                
            logger.warning(f"Recorded missed slot for height {height}, shard {shard_id}")
            
        except Exception as e:
            logger.error(f"Error recording missed slot: {str(e)}")

    async def _scheduling_loop(self) -> None:
        """Background task for scheduling block production."""
        while self.is_running:
            try:
                # Schedule next height if needed
                current_height = self.state_manager.get_height()
                scheduling_horizon = 10  # Schedule 10 blocks ahead
                
                for height in range(current_height + 1, current_height + scheduling_horizon + 1):
                    if height not in self.production_slots:
                        await self.schedule_block_production(height)
                        
                await asyncio.sleep(1)  # Check every second
                
            except Exception as e:
                logger.error(f"Error in scheduling loop: {str(e)}")
                await asyncio.sleep(5)

    async def _cleanup_loop(self) -> None:
        """Background task for cleaning up old production slots."""
        while self.is_running:
            try:
                current_height = self.state_manager.get_height()
                cleanup_threshold = 100  # Keep 100 blocks of history
                
                # Remove old slots
                for height in list(self.production_slots.keys()):
                    if height < current_height - cleanup_threshold:
                        del self.production_slots[height]
                        
                await asyncio.sleep(60)  # Clean up every minute
                
            except Exception as e:
                logger.error(f"Error in cleanup loop: {str(e)}")
                await asyncio.sleep(5)

    async def _select_validator(self, height: int, shard_id: int) -> Optional[Node]:
        """Select a validator for block production."""
        try:
            # Get eligible validators from consensus
            validators = await self.consensus.get_eligible_validators(shard_id)
            if not validators:
                return None
                
            # Filter out currently assigned validators
            available_validators = [
                v for v in validators
                if v.node_id not in self.active_producers
            ]
            
            if not available_validators:
                return None
                
            # Select validator using consensus mechanism
            selected = await self.consensus.select_validator(available_validators, shard_id)
            return selected
            
        except Exception as e:
            logger.error(f"Error selecting validator: {str(e)}")
            return None

    def _calculate_next_slot_time(self, shard_id: int) -> datetime:
        """Calculate the next block production slot time for a shard."""
        try:
            last_time = self.last_production_time.get(shard_id)
            if not last_time:
                return datetime.now() + timedelta(seconds=self.block_time)
                
            next_time = last_time + timedelta(seconds=self.block_time)
            if next_time < datetime.now():
                next_time = datetime.now() + timedelta(seconds=self.block_time)
                
            return next_time
            
        except Exception as e:
            logger.error(f"Error calculating slot time: {str(e)}")
            return datetime.now() + timedelta(seconds=self.block_time)

    async def _reassign_slot(
        self,
        height: int,
        shard_id: int,
        slot: ProductionSlot
    ) -> None:
        """Reassign a production slot to a new validator."""
        try:
            # Select new validator
            new_validator = await self._select_validator(height, shard_id)
            if new_validator:
                # Update slot
                slot.validator_id = new_validator.node_id
                slot.timestamp = self._calculate_next_slot_time(shard_id)
                self.active_producers[new_validator.node_id] = height
                
                logger.info(
                    f"Reassigned slot for height {height}, shard {shard_id} "
                    f"to validator {new_validator.node_id}"
                )
                
        except Exception as e:
            logger.error(f"Error reassigning slot: {str(e)}")

    async def _handle_excessive_misses(self, validator_id: str) -> None:
        """Handle a validator that has missed too many slots."""
        try:
            # Remove from active producers
            if validator_id in self.active_producers:
                del self.active_producers[validator_id]
                
            # Remove from missed slots tracking
            if validator_id in self.missed_slots:
                del self.missed_slots[validator_id]
                
            # Notify consensus mechanism
            await self.consensus.handle_validator_timeout(validator_id)
            
            logger.warning(f"Handled excessive misses for validator {validator_id}")
            
        except Exception as e:
            logger.error(f"Error handling excessive misses: {str(e)}")

    def _update_average_block_time(self, slot: ProductionSlot) -> None:
        """Update average block production time metrics."""
        try:
            current_avg = self.metrics["average_block_time"]
            total_blocks = self.metrics["blocks_produced"]
            
            if total_blocks == 0:
                self.metrics["average_block_time"] = self.block_time
                return
                
            # Calculate new average
            actual_time = (datetime.now() - slot.timestamp).total_seconds()
            new_avg = ((current_avg * total_blocks) + actual_time) / (total_blocks + 1)
            self.metrics["average_block_time"] = new_avg
            
        except Exception as e:
            logger.error(f"Error updating average block time: {str(e)}")

    def get_scheduled_validators(self, height: int) -> Dict[int, str]:
        """Get scheduled validators for a height."""
        try:
            if height not in self.production_slots:
                return {}
                
            return {
                shard_id: slot.validator_id
                for shard_id, slot in self.production_slots[height].items()
                if slot.validator_id is not None
            }
            
        except Exception as e:
            logger.error(f"Error getting scheduled validators: {str(e)}")
            return {}

    def get_production_metrics(self) -> Dict[str, Any]:
        """Get comprehensive block production metrics."""
        return {
            **self.metrics,
            "active_producers": len(self.active_producers),
            "current_slots": sum(
                len(slots) for slots in self.production_slots.values()
            ),
            "filled_slots": sum(
                len([s for s in slots.values() if s.is_filled])
                for slots in self.production_slots.values()
            ),
            "scheduled_heights": sorted(self.production_slots.keys()),
            "validators_with_misses": len(self.missed_slots)
        }