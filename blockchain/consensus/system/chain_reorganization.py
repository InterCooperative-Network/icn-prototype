"""
blockchain/system/chain_reorganization.py

Handles chain reorganization events in the ICN blockchain system.
Manages fork detection, chain switching, and state rollbacks while 
maintaining cross-shard consistency and cooperative principles.
"""

import asyncio
import logging
from typing import Dict, List, Optional, Tuple, Set
from datetime import datetime
from dataclasses import dataclass, field

from ..core.block import Block
from ..core.transaction import Transaction
from ..core.state.unified_state import UnifiedStateManager
from ..consensus.proof_of_cooperation import ProofOfCooperation
from ..core.shard.state_verifier import StateVerifier

logger = logging.getLogger(__name__)

@dataclass
class ReorgEvent:
    """Represents a chain reorganization event."""
    old_chain: List[Block]
    new_chain: List[Block]
    fork_point: Block
    timestamp: datetime = field(default_factory=datetime.now)
    affected_shards: Set[int] = field(default_factory=set)
    cross_shard_impacts: Dict[int, List[str]] = field(default_factory=dict)
    reverted_transactions: List[Transaction] = field(default_factory=list)
    reapplied_transactions: List[Transaction] = field(default_factory=list)

class ChainReorganizationManager:
    """
    Manages chain reorganization events in the ICN blockchain.
    
    Responsibilities:
    - Detect and validate alternative chains
    - Manage safe chain switching
    - Handle state rollbacks and reapplication
    - Maintain cross-shard consistency
    - Preserve cooperative principles during reorgs
    """

    def __init__(
        self,
        state_manager: UnifiedStateManager,
        consensus: ProofOfCooperation,
        state_verifier: StateVerifier,
        max_reorg_depth: int = 100,
        min_fork_length: int = 3
    ):
        """Initialize the reorganization manager."""
        self.state_manager = state_manager
        self.consensus = consensus
        self.state_verifier = state_verifier
        self.max_reorg_depth = max_reorg_depth
        self.min_fork_length = min_fork_length
        
        # Track reorg history
        self.reorg_events: List[ReorgEvent] = []
        self.processing_reorg = False
        self.last_verification: Dict[int, datetime] = {}

    async def handle_potential_reorg(
        self,
        current_chain: List[Block],
        alternative_chain: List[Block],
        shard_id: Optional[int] = None
    ) -> bool:
        """
        Handle a potential chain reorganization.

        Args:
            current_chain: Current chain of blocks
            alternative_chain: Competing chain of blocks
            shard_id: Optional shard ID if shard-specific

        Returns:
            bool: True if reorganization was successful
        """
        try:
            if self.processing_reorg:
                logger.warning("Already processing a reorganization")
                return False

            self.processing_reorg = True

            try:
                # Validate reorganization possibility
                if not self._validate_reorg_possibility(current_chain, alternative_chain):
                    return False

                # Find fork point
                fork_point = await self._find_fork_point(current_chain, alternative_chain)
                if not fork_point:
                    logger.error("Could not find valid fork point")
                    return False

                # Calculate chains to revert and apply
                to_revert, to_apply = self._calculate_chain_differences(
                    current_chain, alternative_chain, fork_point
                )

                # Verify state transitions
                if not await self._verify_state_transitions(to_apply, fork_point):
                    logger.error("State transition verification failed")
                    return False

                # Create reorg event
                reorg_event = ReorgEvent(
                    old_chain=current_chain,
                    new_chain=alternative_chain,
                    fork_point=fork_point
                )

                # Execute the reorganization
                success = await self._execute_reorganization(reorg_event)
                if success:
                    self.reorg_events.append(reorg_event)
                    await self._notify_reorg_completion(reorg_event)
                    return True

                return False

            finally:
                self.processing_reorg = False

        except Exception as e:
            logger.error(f"Error handling chain reorganization: {str(e)}")
            self.processing_reorg = False
            return False

    async def _verify_state_transitions(
        self,
        blocks: List[Block],
        fork_point: Block
    ) -> bool:
        """
        Verify that the state transitions in the new chain are valid.
        
        Args:
            blocks: List of blocks to verify
            fork_point: The fork point block
            
        Returns:
            bool: True if state transitions are valid
        """
        try:
            # Create temporary state for verification
            temp_state = self.state_manager.create_snapshot()
            
            # Apply each block's state changes
            current_block = fork_point
            for block in blocks:
                # Verify block links to previous
                if block.previous_hash != current_block.hash:
                    return False
                    
                # Verify transactions and state changes
                if not await self.state_verifier.verify_block_state(block, temp_state):
                    return False
                    
                # Update for next iteration    
                current_block = block
                
            return True
            
        except Exception as e:
            logger.error(f"Error verifying state transitions: {str(e)}")
            return False

    async def _execute_reorganization(self, reorg_event: ReorgEvent) -> bool:
        """
        Execute the chain reorganization.

        Args:
            reorg_event: The reorganization event to execute

        Returns:
            bool: True if reorganization was successful
        """
        try:
            # Begin state transition
            async with self.state_manager.state_transition():
                # Revert blocks from current chain
                for block in reversed(reorg_event.old_chain[reorg_event.fork_point.index + 1:]):
                    if not await self._revert_block(block):
                        await self.state_manager.rollback_transition()
                        return False
                    reorg_event.reverted_transactions.extend(block.transactions)

                # Apply blocks from new chain
                for block in reorg_event.new_chain[reorg_event.fork_point.index + 1:]:
                    if not await self._apply_block(block):
                        await self.state_manager.rollback_transition()
                        return False
                    reorg_event.reapplied_transactions.extend(block.transactions)

                # Verify final state
                if not await self._verify_final_state(reorg_event):
                    await self.state_manager.rollback_transition()
                    return False

                # Update affected shards tracking
                self._track_affected_shards(reorg_event)

                # Commit the reorganization
                await self.state_manager.commit_transition()
                return True

        except Exception as e:
            logger.error(f"Error executing reorganization: {str(e)}")
            return False

    async def _revert_block(self, block: Block) -> bool:
        """
        Revert a block during reorganization.

        Args:
            block: Block to revert

        Returns:
            bool: True if block was reverted successfully
        """
        try:
            # Revert transactions in reverse order
            for tx in reversed(block.transactions):
                if not await self.state_manager.revert_transaction(tx):
                    return False

            # Handle cross-shard references
            if block.cross_shard_refs:
                for ref in block.cross_shard_refs:
                    if not await self._revert_cross_shard_ref(ref, block.shard_id):
                        return False

            # Update state
            return await self.state_manager.revert_block_state(block)

        except Exception as e:
            logger.error(f"Error reverting block: {str(e)}")
            return False

    async def _apply_block(self, block: Block) -> bool:
        """
        Apply a block during reorganization.

        Args:
            block: Block to apply

        Returns:
            bool: True if block was applied successfully
        """
        try:
            # Verify block validity
            if not await self.consensus.validate_block(block, None, None):
                return False

            # Apply transactions
            for tx in block.transactions:
                if not await self.state_manager.apply_transaction(tx):
                    return False

            # Handle cross-shard references
            if block.cross_shard_refs:
                for ref in block.cross_shard_refs:
                    if not await self._apply_cross_shard_ref(ref, block.shard_id):
                        return False

            # Update state
            return await self.state_manager.apply_block_state(block)

        except Exception as e:
            logger.error(f"Error applying block: {str(e)}")
            return False

    def _track_affected_shards(self, reorg_event: ReorgEvent) -> None:
        """
        Track which shards are affected by the reorganization.

        Args:
            reorg_event: The reorganization event
        """
        # Track shards from old chain
        for block in reorg_event.old_chain:
            reorg_event.affected_shards.add(block.shard_id)
            if block.cross_shard_refs:
                for ref in block.cross_shard_refs:
                    target_shard = self._get_ref_target_shard(ref)
                    if target_shard is not None:
                        reorg_event.affected_shards.add(target_shard)
                        if target_shard not in reorg_event.cross_shard_impacts:
                            reorg_event.cross_shard_impacts[target_shard] = []
                        reorg_event.cross_shard_impacts[target_shard].append(ref)

        # Track shards from new chain
        for block in reorg_event.new_chain:
            reorg_event.affected_shards.add(block.shard_id)
            if block.cross_shard_refs:
                for ref in block.cross_shard_refs:
                    target_shard = self._get_ref_target_shard(ref)
                    if target_shard is not None:
                        reorg_event.affected_shards.add(target_shard)
                        if target_shard not in reorg_event.cross_shard_impacts:
                            reorg_event.cross_shard_impacts[target_shard] = []
                        reorg_event.cross_shard_impacts[target_shard].append(ref)

    def get_reorg_metrics(self) -> Dict:
        """Get metrics about chain reorganizations."""
        return {
            "total_reorgs": len(self.reorg_events),
            "average_reorg_depth": sum(
                len(event.old_chain) - event.fork_point.index 
                for event in self.reorg_events
            ) / max(1, len(self.reorg_events)),
            "max_reorg_depth": max(
                (len(event.old_chain) - event.fork_point.index 
                 for event in self.reorg_events),
                default=0
            ),
            "affected_shards": list(set().union(
                *(event.affected_shards for event in self.reorg_events)
            )),
            "last_reorg_time": self.reorg_events[-1].timestamp.isoformat() 
                if self.reorg_events else None,
            "cross_shard_impacts": sum(
                len(event.cross_shard_impacts) for event in self.reorg_events
            )
        }

    def _get_ref_target_shard(self, ref: str) -> Optional[int]:
        """Extract target shard from cross-shard reference."""
        try:
            # Parse reference format to extract target shard
            # Actual implementation depends on reference format
            return int(ref.split(':')[1])
        except Exception:
            return None