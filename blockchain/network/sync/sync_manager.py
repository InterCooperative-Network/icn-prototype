# ================================================================
# File: blockchain/network/sync/sync_manager.py
# Description: State synchronization manager for ICN network.
# Handles blockchain state synchronization between nodes, including
# block synchronization, state verification, and chain reorganization.
# ================================================================

import asyncio
import logging
from typing import Dict, Set, List, Optional, Tuple, Any
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import json
import hashlib
from enum import Enum, auto

from ..config import NetworkConfig
from ..protocol.dispatcher import MessageDispatcher, MessagePriority, MessageRoute
from ...core.block import Block
from ...core.transaction import Transaction

logger = logging.getLogger(__name__)

class SyncState(Enum):
    """State of node synchronization."""
    IDLE = auto()
    SYNCING_HEADERS = auto()
    SYNCING_BLOCKS = auto()
    SYNCING_STATE = auto()
    VERIFYING = auto()

@dataclass
class SyncProgress:
    """Track synchronization progress."""
    start_height: int
    current_height: int
    target_height: int
    start_time: datetime = field(default_factory=datetime.now)
    blocks_processed: int = 0
    blocks_verified: int = 0
    failed_blocks: int = 0
    state_size: int = 0
    
    @property
    def progress_percentage(self) -> float:
        """Calculate sync progress percentage."""
        if self.target_height == self.start_height:
            return 100.0
        return (self.current_height - self.start_height) / (self.target_height - self.start_height) * 100

    @property
    def blocks_per_second(self) -> float:
        """Calculate block processing rate."""
        elapsed = (datetime.now() - self.start_time).total_seconds()
        if elapsed == 0:
            return 0.0
        return self.blocks_processed / elapsed

    @property
    def estimated_time_remaining(self) -> timedelta:
        """Estimate remaining sync time."""
        if self.blocks_per_second == 0:
            return timedelta(hours=999)  # Large value to indicate unknown
        blocks_remaining = self.target_height - self.current_height
        seconds_remaining = blocks_remaining / self.blocks_per_second
        return timedelta(seconds=seconds_remaining)

class SyncManager:
    """
    Manages blockchain state synchronization.
    
    Features:
    - Block header synchronization
    - Block data synchronization
    - State verification
    - Chain reorganization
    - Checkpoint validation
    - Parallel block downloading
    - State snapshot handling
    """

    def __init__(
        self,
        config: NetworkConfig,
        dispatcher: MessageDispatcher,
        chain,  # Actual blockchain instance
        max_parallel_downloads: int = 10,
        verify_interval: int = 100  # Verify every N blocks
    ):
        """Initialize the sync manager."""
        self.config = config
        self.dispatcher = dispatcher
        self.chain = chain
        self.max_parallel_downloads = max_parallel_downloads
        self.verify_interval = verify_interval
        
        # Sync state
        self.sync_state = SyncState.IDLE
        self.current_sync: Optional[SyncProgress] = None
        self.active_downloads: Set[int] = set()
        self.downloaded_blocks: Dict[int, Block] = {}
        self.verified_blocks: Set[int] = set()
        self.failed_blocks: Set[int] = set()
        
        # Peer tracking
        self.peer_heights: Dict[str, int] = {}
        self.sync_peers: Set[str] = set()
        self.banned_peers: Set[str] = set()
        
        # State verification
        self.checkpoints: Dict[int, str] = {}  # height -> state root
        self.state_roots: Dict[int, str] = {}  # height -> state root
        
        # Background tasks
        self.sync_task: Optional[asyncio.Task] = None
        self.verification_task: Optional[asyncio.Task] = None
        
        # Metrics
        self.metrics = {
            "total_syncs": 0,
            "successful_syncs": 0,
            "failed_syncs": 0,
            "blocks_downloaded": 0,
            "blocks_verified": 0,
            "reorgs_processed": 0,
            "average_sync_time": 0.0,
        }

    async def start(self) -> None:
        """Start the sync manager."""
        # Register message handlers
        self.dispatcher.register_handler(
            "block_headers",
            self._handle_block_headers,
            priority=MessagePriority.HIGH
        )
        self.dispatcher.register_handler(
            "block_data",
            self._handle_block_data,
            priority=MessagePriority.HIGH
        )
        self.dispatcher.register_handler(
            "state_root",
            self._handle_state_root,
            priority=MessagePriority.MEDIUM
        )
        self.dispatcher.register_handler(
            "checkpoint",
            self._handle_checkpoint,
            priority=MessagePriority.MEDIUM
        )
        
        # Start verification task
        self.verification_task = asyncio.create_task(self._verification_loop())
        
        logger.info("Sync manager started")

    async def stop(self) -> None:
        """Stop the sync manager."""
        if self.sync_task:
            self.sync_task.cancel()
            
        if self.verification_task:
            self.verification_task.cancel()
            
        logger.info("Sync manager stopped")

    async def start_sync(self, target_height: Optional[int] = None) -> bool:
        """
        Start blockchain synchronization.
        
        Args:
            target_height: Optional target height to sync to
            
        Returns:
            bool: True if sync started successfully
        """
        if self.sync_state != SyncState.IDLE:
            logger.warning("Sync already in progress")
            return False
            
        try:
            # Get current chain height
            current_height = self.chain.height
            
            # Find best peer height if no target specified
            if target_height is None:
                peer_heights = list(self.peer_heights.values())
                if not peer_heights:
                    logger.warning("No peers available for sync")
                    return False
                target_height = max(peer_heights)
            
            if target_height <= current_height:
                logger.info("Chain already synced")
                return False
            
            # Initialize sync progress
            self.current_sync = SyncProgress(
                start_height=current_height,
                current_height=current_height,
                target_height=target_height
            )
            
            # Start sync task
            self.sync_state = SyncState.SYNCING_HEADERS
            self.sync_task = asyncio.create_task(self._sync_loop())
            
            logger.info(
                f"Starting sync from height {current_height} to {target_height}"
            )
            return True
            
        except Exception as e:
            logger.error(f"Error starting sync: {str(e)}")
            return False

    def get_sync_progress(self) -> Optional[SyncProgress]:
        """Get current sync progress."""
        return self.current_sync

    def add_checkpoint(self, height: int, state_root: str) -> None:
        """Add a trusted checkpoint."""
        self.checkpoints[height] = state_root
        logger.info(f"Added checkpoint at height {height}")

    async def verify_chain_state(self, height: Optional[int] = None) -> bool:
        """
        Verify chain state at specified height.
        
        Args:
            height: Height to verify, defaults to current height
            
        Returns:
            bool: True if state is valid
        """
        try:
            if height is None:
                height = self.chain.height
                
            # Get state root at height
            state_root = await self._calculate_state_root(height)
            if not state_root:
                return False
                
            # Check against checkpoint if available
            checkpoint_root = self.checkpoints.get(height)
            if checkpoint_root and checkpoint_root != state_root:
                logger.error(f"State root mismatch at checkpoint {height}")
                return False
                
            # Verify against peer state roots
            peer_roots = await self._get_peer_state_roots(height)
            if not peer_roots:
                return True  # No peers to verify against
                
            # State is valid if it matches majority of peers
            matching_peers = sum(1 for root in peer_roots if root == state_root)
            return matching_peers > len(peer_roots) / 2
            
        except Exception as e:
            logger.error(f"Error verifying chain state: {str(e)}")
            return False

    async def _sync_loop(self) -> None:
        """Main synchronization loop."""
        try:
            while self.sync_state != SyncState.IDLE:
                if self.sync_state == SyncState.SYNCING_HEADERS:
                    await self._sync_headers()
                elif self.sync_state == SyncState.SYNCING_BLOCKS:
                    await self._sync_blocks()
                elif self.sync_state == SyncState.SYNCING_STATE:
                    await self._sync_state()
                elif self.sync_state == SyncState.VERIFYING:
                    await self._verify_sync()
                    
                await asyncio.sleep(0.1)
                
        except asyncio.CancelledError:
            logger.info("Sync loop cancelled")
        except Exception as e:
            logger.error(f"Error in sync loop: {str(e)}")
            self.metrics["failed_syncs"] += 1
            self.sync_state = SyncState.IDLE

    async def _sync_headers(self) -> None:
        """Synchronize block headers."""
        try:
            if not self.current_sync:
                return
                
            start_height = self.current_sync.current_height
            target_height = self.current_sync.target_height
            
            # Request headers in batches
            batch_size = 2000
            current_height = start_height
            
            while current_height < target_height:
                end_height = min(current_height + batch_size, target_height)
                
                # Request headers from peers
                headers = await self._request_headers(
                    current_height,
                    end_height
                )
                
                if not headers:
                    logger.error("Failed to get headers")
                    self.sync_state = SyncState.IDLE
                    return
                    
                # Verify and store headers
                if not await self._verify_headers(headers):
                    logger.error("Header verification failed")
                    self.sync_state = SyncState.IDLE
                    return
                    
                current_height = end_height
                self.current_sync.current_height = current_height
                
            # Move to block sync
            self.sync_state = SyncState.SYNCING_BLOCKS
            
        except Exception as e:
            logger.error(f"Error syncing headers: {str(e)}")
            self.sync_state = SyncState.IDLE

    async def _sync_blocks(self) -> None:
        """Synchronize full blocks."""
        try:
            if not self.current_sync:
                return
                
            while len(self.active_downloads) < self.max_parallel_downloads:
                next_height = self._get_next_block_height()
                if next_height is None:
                    if not self.active_downloads:
                        # All blocks downloaded
                        self.sync_state = SyncState.SYNCING_STATE
                    break
                    
                # Start block download
                self.active_downloads.add(next_height)
                asyncio.create_task(self._download_block(next_height))
                
        except Exception as e:
            logger.error(f"Error syncing blocks: {str(e)}")
            self.sync_state = SyncState.IDLE

    async def _sync_state(self) -> None:
        """Synchronize chain state."""
        try:
            if not self.current_sync:
                return
                
            # Request state snapshot
            state_data = await self._request_state_snapshot(
                self.current_sync.current_height
            )
            
            if not state_data:
                logger.error("Failed to get state snapshot")
                self.sync_state = SyncState.IDLE
                return
                
            # Verify state
            if not await self._verify_state_snapshot(
                state_data,
                self.current_sync.current_height
            ):
                logger.error("State verification failed")
                self.sync_state = SyncState.IDLE
                return
                
            # Apply state
            if not await self._apply_state_snapshot(state_data):
                logger.error("Failed to apply state snapshot")
                self.sync_state = SyncState.IDLE
                return
                
            # Move to verification
            self.sync_state = SyncState.VERIFYING
            
        except Exception as e:
            logger.error(f"Error syncing state: {str(e)}")
            self.sync_state = SyncState.IDLE

    async def _verification_loop(self) -> None:
        """Periodic chain verification loop."""
        while True:
            try:
                if self.chain.height % self.verify_interval == 0:
                    await self.verify_chain_state()
                    
                await asyncio.sleep(10)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in verification loop: {str(e)}")
                await asyncio.sleep(10)

    async def _request_headers(
        self,
        start_height: int,
        end_height: int
    ) -> Optional[List[Dict[str, Any]]]:
        """Request block headers from peers."""
        try:
            response = await self.dispatcher.dispatch_message(
                "get_headers",
                {
                    "start_height": start_height,
                    "end_height": end_height
                },
                routing=MessageRoute(
                    message_type="get_headers",
                    target_shards=set(),
                    target_cooperatives=set(),
                    exclude_peers=self.banned_peers,
                    broadcast=True
                ),
                wait_response=True
            )
            
            if not response or "headers" not in response:
                return None
                
            return response["headers"]
            
        except Exception as e:
            logger.error(f"Error requesting headers: {str(e)}")
            return None

    async def _verify_headers(self, headers: List[Dict[str, Any]]) -> bool:
        """Verify block headers."""
        try:
            previous_hash = None
            for header in headers:
                # Create block from header
                block = Block.from_dict(header)
                
                # Verify hash chain
                if previous_hash and block.previous_hash != previous_hash:
                    return False
                    
                previous_hash = block.hash
                
                # Store header
                self.chain.add_block_header(block)
                
            return True
            
        except Exception as e:
            logger.error(f"Error verifying headers: {str(e)}")
            return False

    def _get_next_block_height(self) -> Optional[int]:
        """Get next block height to download."""
        if not self.current_sync:
            return None
            
        current_height = self.current_sync.current_height
        target_height = self.current_sync.target_height
        
        for height in range(current_height, target_height + 1):
            if (height not in self.active_downloads and
                height not in self.downloaded_blocks and
                height not in self.failed_blocks):
                return height
                
        return None

    async def _download_block(self, height: int) -> None:
        """Download a specific block."""
        try:
            # Request block from peers
            response = await self.dispatcher.dispatch_message(
                "get_block",
                {"height": height},
                routing=MessageRoute(
                    message_type="get_block",
                    target_shards=set(),
                    target_cooperatives=set(),
                    exclude_peers=self.banned_peers,
                    broadcast=True
                ),
                wait_response=True
            )
            
            if not response or "block" not in response:
                logger.error(f"Failed to download block at height {height}")
                self.failed_blocks.add(height)
                return
                
            # Create and verify block
            block = Block.from_dict(response["block"])
            if not await self._verify_block(block):
                logger.error(f"Block verification failed at height {height}")
                self.failed_blocks.add(height)
                return
                
            # Store block
            self.downloaded_blocks[height] = block
            self.current_sync.blocks_processed += 1
            self.metrics["blocks_downloaded"] += 1
            
        except Exception as e:
            logger.error(f"Error downloading block {height}: {str(e)}")
            self.failed_blocks.add(height)
            
        finally:
            self.active_downloads.discard(height)

    async def _verify_block(self, block: Block) -> bool:
        """Verify a downloaded block."""
        try:
            # Verify basic structure
            if not block.validate(None):  # Pass None as we don't have previous block here
                return False
                
            # Verify transactions
            for tx in block.transactions:
                if not await self._verify_transaction(tx):
                    return False
                    
            # Verify state transitions
            if not await self._verify_state_transitions(block):
                return False
                
            # Verify cooperative signatures if present
            if not await self._verify_signatures(block):
                return False
                
            return True
            
        except Exception as e:
            logger.error(f"Error verifying block: {str(e)}")
            return False

    async def _verify_transaction(self, transaction: Transaction) -> bool:
        """Verify a transaction within a block."""
        try:
            # Basic validation
            if not transaction.validate():
                return False
                
            # Verify signatures
            if not transaction.verify_signatures():
                return False
                
            # Verify state transitions
            if not await self._verify_transaction_state(transaction):
                return False
                
            return True
            
        except Exception as e:
            logger.error(f"Error verifying transaction: {str(e)}")
            return False

    async def _verify_transaction_state(self, transaction: Transaction) -> bool:
        """Verify transaction state transitions."""
        try:
            # Get pre-state
            pre_state = await self._get_account_state(transaction.sender)
            
            # Verify sender has sufficient resources
            if not await self._verify_resource_availability(transaction, pre_state):
                return False
                
            # Verify state transition rules
            if not await self._verify_transition_rules(transaction, pre_state):
                return False
                
            return True
            
        except Exception as e:
            logger.error(f"Error verifying transaction state: {str(e)}")
            return False

    async def _verify_state_transitions(self, block: Block) -> bool:
        """Verify state transitions in a block."""
        try:
            # Get pre-block state
            pre_state = await self._get_block_state(block.previous_hash)
            
            # Apply all transactions
            current_state = pre_state.copy()
            for tx in block.transactions:
                if not await self._apply_transaction(tx, current_state):
                    return False
                    
            # Verify final state matches block's state root
            state_root = self._calculate_state_root_from_state(current_state)
            return state_root == block.state_root
            
        except Exception as e:
            logger.error(f"Error verifying state transitions: {str(e)}")
            return False

    async def _verify_signatures(self, block: Block) -> bool:
        """Verify cooperative signatures on block."""
        try:
            # Get required signers
            required_signers = await self._get_required_signers(block)
            
            # Verify all required signatures are present
            for signer in required_signers:
                if not await self._verify_signer_signature(block, signer):
                    return False
                    
            return True
            
        except Exception as e:
            logger.error(f"Error verifying signatures: {str(e)}")
            return False

    async def _apply_blocks(self) -> bool:
        """Apply downloaded blocks to chain."""
        try:
            if not self.current_sync:
                return False
                
            current_height = self.current_sync.start_height
            target_height = self.current_sync.target_height
            
            while current_height <= target_height:
                # Get block
                block = self.downloaded_blocks.get(current_height)
                if not block:
                    return False
                    
                # Apply block
                if not await self.chain.add_block(block):
                    return False
                    
                # Update state roots
                self.state_roots[current_height] = block.state_root
                
                current_height += 1
                self.current_sync.blocks_verified += 1
                self.metrics["blocks_verified"] += 1
                
            return True
            
        except Exception as e:
            logger.error(f"Error applying blocks: {str(e)}")
            return False

    async def _request_state_snapshot(self, height: int) -> Optional[Dict[str, Any]]:
        """Request state snapshot from peers."""
        try:
            response = await self.dispatcher.dispatch_message(
                "get_state_snapshot",
                {"height": height},
                routing=MessageRoute(
                    message_type="get_state_snapshot",
                    target_shards=set(),
                    target_cooperatives=set(),
                    exclude_peers=self.banned_peers,
                    broadcast=True
                ),
                wait_response=True
            )
            
            if not response or "state" not in response:
                return None
                
            return response["state"]
            
        except Exception as e:
            logger.error(f"Error requesting state snapshot: {str(e)}")
            return None

    async def _verify_state_snapshot(self, state_data: Dict[str, Any], height: int) -> bool:
        """Verify state snapshot integrity."""
        try:
            # Calculate state root
            state_root = self._calculate_state_root_from_state(state_data)
            
            # Verify against block state root
            block_state_root = self.state_roots.get(height)
            if block_state_root and state_root != block_state_root:
                return False
                
            # Verify against checkpoint if available
            checkpoint_root = self.checkpoints.get(height)
            if checkpoint_root and state_root != checkpoint_root:
                return False
                
            return True
            
        except Exception as e:
            logger.error(f"Error verifying state snapshot: {str(e)}")
            return False

    async def _apply_state_snapshot(self, state_data: Dict[str, Any]) -> bool:
        """Apply verified state snapshot."""
        try:
            # Backup current state
            await self._backup_current_state()
            
            # Apply new state
            return await self.chain.apply_state(state_data)
            
        except Exception as e:
            logger.error(f"Error applying state snapshot: {str(e)}")
            return False

    async def _verify_sync(self) -> None:
        """Verify completed synchronization."""
        try:
            if not self.current_sync:
                return
                
            # Verify chain continuity
            if not await self._verify_chain_continuity():
                logger.error("Chain continuity verification failed")
                self.sync_state = SyncState.IDLE
                return
                
            # Verify final state
            if not await self.verify_chain_state():
                logger.error("Final state verification failed")
                self.sync_state = SyncState.IDLE
                return
                
            # Sync completed successfully
            logger.info(
                f"Sync completed successfully at height {self.current_sync.target_height}"
            )
            
            self.metrics["successful_syncs"] += 1
            self.sync_state = SyncState.IDLE
            self.current_sync = None
            
        except Exception as e:
            logger.error(f"Error verifying sync: {str(e)}")
            self.sync_state = SyncState.IDLE

    async def _backup_current_state(self) -> None:
        """Backup current chain state."""
        try:
            # TODO: Implement state backup
            pass
        except Exception as e:
            logger.error(f"Error backing up state: {str(e)}")
            raise

    async def _verify_chain_continuity(self) -> bool:
        """Verify continuity of synchronized chain."""
        try:
            if not self.current_sync:
                return False
                
            current_height = self.current_sync.start_height
            target_height = self.current_sync.target_height
            
            previous_hash = None
            while current_height <= target_height:
                block = await self.chain.get_block(current_height)
                if not block:
                    return False
                    
                if previous_hash and block.previous_hash != previous_hash:
                    return False
                    
                previous_hash = block.hash
                current_height += 1
                
            return True
            
        except Exception as e:
            logger.error(f"Error verifying chain continuity: {str(e)}")
            return False

    def get_metrics(self) -> Dict[str, Any]:
        """Get sync manager metrics."""
        metrics = self.metrics.copy()
        
        if self.current_sync:
            metrics.update({
                "sync_progress": self.current_sync.progress_percentage,
                "blocks_per_second": self.current_sync.blocks_per_second,
                "estimated_time_remaining": str(self.current_sync.estimated_time_remaining),
                "current_height": self.current_sync.current_height,
                "target_height": self.current_sync.target_height
            })
            
        return metrics

# Example usage
async def example_usage():
    config = NetworkConfig(node_id="test_node")
    dispatcher = MessageDispatcher(config, None)  # Pass proper transport
    chain = None  # Pass actual blockchain instance
    
    sync_manager = SyncManager(config, dispatcher, chain)
    await sync_manager.start()
    
    # Start sync
    await sync_manager.start_sync(1000)  # Sync to height 1000
    
    # Wait for sync to complete
    while sync_manager.get_sync_progress():
        progress = sync_manager.get_sync_progress()
        print(f"Sync progress: {progress.progress_percentage:.2f}%")
        await asyncio.sleep(1)
    
    await sync_manager.stop()

if __name__ == "__main__":
    asyncio.run(example_usage())