# blockchain/system/coordinator.py
"""
System coordinator for managing component interactions and message flow.
"""

from typing import Optional, Dict, Any
import logging
import asyncio
from dataclasses import dataclass
from datetime import datetime

from blockchain.core.blockchain import Blockchain
from blockchain.core.transaction import Transaction
from blockchain.core.block import Block
from blockchain.consensus.proof_of_cooperation import ProofOfCooperation
from blockchain.network.manager import NetworkManager
from blockchain.core.state.unified_state import UnifiedStateManager

logger = logging.getLogger(__name__)

@dataclass
class SystemState:
    """Tracks current system state."""
    is_running: bool = False
    last_block_time: Optional[datetime] = None
    pending_transactions: int = 0
    active_nodes: int = 0
    network_peers: int = 0

class SystemCoordinator:
    """
    Coordinates interactions between system components and manages message flow.
    
    Responsibilities:
    - Transaction and block propagation
    - Component state coordination
    - System health monitoring
    - Error recovery
    """
    
    def __init__(
        self,
        blockchain: Blockchain,
        consensus: ProofOfCooperation,
        network: NetworkManager,
        state_manager: UnifiedStateManager
    ):
        self.blockchain = blockchain
        self.consensus = consensus
        self.network = network
        self.state_manager = state_manager
        
        self.system_state = SystemState()
        self.message_queue = asyncio.Queue()
        self.background_tasks: Set[asyncio.Task] = set()

    async def start(self) -> bool:
        """Start the system coordinator."""
        try:
            self.system_state.is_running = True
            
            # Start background tasks
            self.background_tasks.add(
                asyncio.create_task(self._process_message_queue())
            )
            self.background_tasks.add(
                asyncio.create_task(self._monitor_system_health())
            )
            
            logger.info("System coordinator started")
            return True
            
        except Exception as e:
            logger.error(f"Failed to start coordinator: {e}")
            return False

    async def stop(self) -> None:
        """Stop the system coordinator."""
        self.system_state.is_running = False
        
        # Cancel background tasks
        for task in self.background_tasks:
            task.cancel()
            
        await asyncio.gather(*self.background_tasks, return_exceptions=True)
        self.background_tasks.clear()

    async def handle_new_transaction(self, transaction: Transaction) -> bool:
        """
        Process a new transaction through the system.
        
        Flow:
        1. Validate transaction
        2. Add to transaction pool
        3. Broadcast to network
        4. Update system state
        """
        try:
            # Validate transaction
            if not transaction.validate():
                logger.error(f"Invalid transaction: {transaction.transaction_id}")
                return False
                
            # Add to blockchain
            if not await self.blockchain.add_transaction(transaction.to_dict()):
                return False
                
            # Broadcast to network
            await self.network.broadcast_message(
                "new_transaction",
                transaction.to_dict()
            )
            
            # Update state
            self.system_state.pending_transactions += 1
            
            return True
            
        except Exception as e:
            logger.error(f"Error handling transaction: {e}")
            return False

    async def handle_new_block(self, block: Block) -> bool:
        """
        Process a new block through the system.
        
        Flow:
        1. Validate block
        2. Update state
        3. Broadcast to network
        4. Update metrics
        """
        try:
            # Validate block
            validator = self.blockchain.nodes.get(block.validator)
            if not validator:
                logger.error(f"Unknown validator: {block.validator}")
                return False
                
            if not await self.consensus.validate_block(
                block,
                self.blockchain.chain[-1] if self.blockchain.chain else None,
                validator
            ):
                return False
                
            # Add to blockchain
            if not await self.blockchain.add_block(block):
                return False
                
            # Update state
            if not await self.state_manager.update_state(block):
                return False
                
            # Broadcast block
            await self.network.broadcast_message(
                "new_block",
                block.to_dict()
            )
            
            # Update system state
            self.system_state.last_block_time = datetime.now()
            self.system_state.pending_transactions -= len(block.transactions)
            
            return True
            
        except Exception as e:
            logger.error(f"Error handling block: {e}")
            return False

    async def _process_message_queue(self) -> None:
        """Process messages in the queue."""
        while self.system_state.is_running:
            try:
                message = await self.message_queue.get()
                
                # Process based on message type
                if message["type"] == "transaction":
                    await self.handle_new_transaction(
                        Transaction.from_dict(message["data"])
                    )
                elif message["type"] == "block":
                    await self.handle_new_block(
                        Block.from_dict(message["data"])
                    )
                    
                self.message_queue.task_done()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error processing message: {e}")

    async def _monitor_system_health(self) -> None:
        """Monitor system health and component status."""
        while self.system_state.is_running:
            try:
                # Update system metrics
                self.system_state.active_nodes = len(self.blockchain.nodes)
                self.system_state.network_peers = len(self.network.peers)
                
                # Check component health
                await self._check_component_health()
                
                await asyncio.sleep(30)  # Check every 30 seconds
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error monitoring health: {e}")
                await asyncio.sleep(5)

    async def _check_component_health(self) -> None:
        """Check health of individual components."""
        try:
            # Check blockchain
            chain_metrics = self.blockchain.get_chain_metrics()
            if chain_metrics["chain_length"] < 1:
                logger.warning("Blockchain health check failed")
                
            # Check consensus
            consensus_metrics = self.consensus.get_metrics()
            if not consensus_metrics:
                logger.warning("Consensus health check failed")
                
            # Check network
            if not self.network.is_running:
                logger.warning("Network health check failed")
                
        except Exception as e:
            logger.error(f"Error checking component health: {e}")