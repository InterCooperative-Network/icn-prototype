"""
blockchain/integration.py

Provides system-wide initialization and integration management for the ICN blockchain.
"""

import asyncio
import logging
from typing import Dict, Optional, Any
from dataclasses import dataclass
from datetime import datetime

from .core.blockchain import Blockchain
from .core.node import Node
from .consensus.proof_of_cooperation import ProofOfCooperation
from .network.manager import NetworkManager
from .network.config import NetworkConfig
from .contracts.contract_executor import ContractExecutor

logger = logging.getLogger(__name__)

@dataclass
class SystemConfig:
    """System-wide configuration parameters."""
    network_config: NetworkConfig
    initial_mana: int = 1000
    mana_regen_rate: int = 10
    num_shards: int = 4
    min_nodes: int = 3
    max_nodes: int = 100

class BlockchainSystem:
    """
    Manages system-wide initialization and coordination of blockchain components.
    Ensures proper interaction between core blockchain, consensus, and networking layers.
    """

    def __init__(self, config: SystemConfig):
        """Initialize the blockchain system with configuration."""
        self.config = config
        self.blockchain = None
        self.consensus = None
        self.network = None
        self.contract_executor = None
        self.started = False
        self.start_time = None

    async def initialize(self) -> bool:
        """
        Initialize all blockchain system components in the correct order.
        
        Returns:
            bool: True if initialization successful
        """
        try:
            # Initialize consensus mechanism
            self.consensus = ProofOfCooperation(
                min_reputation=10.0,
                cooldown_blocks=3
            )

            # Initialize blockchain
            self.blockchain = Blockchain(
                num_shards=self.config.num_shards,
                initial_mana=self.config.initial_mana,
                mana_regen_rate=self.config.mana_regen_rate
            )

            # Initialize network manager
            self.network = NetworkManager(
                config=self.config.network_config
            )

            # Initialize contract executor
            self.contract_executor = ContractExecutor(
                initial_mana=self.config.initial_mana,
                mana_regen_rate=self.config.mana_regen_rate
            )

            # Start network services
            await self.network.start()

            self.started = True
            self.start_time = datetime.now()
            
            logger.info("Blockchain system initialized successfully")
            return True

        except Exception as e:
            logger.error(f"Failed to initialize blockchain system: {str(e)}")
            await self.shutdown()
            return False

    async def shutdown(self) -> None:
        """Gracefully shutdown all system components."""
        try:
            if self.network:
                await self.network.stop()

            self.started = False
            logger.info("Blockchain system shutdown completed")

        except Exception as e:
            logger.error(f"Error during system shutdown: {str(e)}")

    async def add_node(self, node: Node) -> bool:
        """
        Add a new node to the blockchain network.
        
        Args:
            node: The node to add
            
        Returns:
            bool: True if node added successfully
        """
        try:
            if len(self.blockchain.nodes) >= self.config.max_nodes:
                logger.error("Maximum number of nodes reached")
                return False

            # Register with blockchain
            if not self.blockchain.register_node(node):
                return False

            # Initialize node network services
            await self.network.connect_to_peer(
                node.node_id,
                node.metadata.get("address", ""),
                node.metadata.get("port", 0)
            )

            logger.info(f"Node {node.node_id} added successfully")
            return True

        except Exception as e:
            logger.error(f"Failed to add node: {str(e)}")
            return False

    async def process_transaction(self, transaction: Dict) -> bool:
        """
        Process a new transaction through the system.
        
        Args:
            transaction: Transaction data
            
        Returns:
            bool: True if transaction processed successfully
        """
        try:
            # Add transaction to blockchain
            if not self.blockchain.add_transaction(transaction):
                return False

            # Broadcast transaction to network
            await self.network.broadcast_message(
                "transaction",
                {"transaction": transaction}
            )

            return True

        except Exception as e:
            logger.error(f"Failed to process transaction: {str(e)}")
            return False

    async def create_block(self, validator_id: str, shard_id: Optional[int] = None) -> bool:
        """
        Create a new block using the specified validator.
        
        Args:
            validator_id: ID of the validating node
            shard_id: Optional shard ID
            
        Returns:
            bool: True if block created successfully
        """
        try:
            # Get validator node
            validator = self.blockchain.nodes.get(validator_id)
            if not validator:
                return False

            # Create block
            block = self.blockchain.create_block(shard_id)
            if not block:
                return False

            # Validate block
            if not self.consensus.validate_block(block, self.blockchain.chain[-1], validator):
                return False

            # Add block to chain
            if not self.blockchain.add_block(block):
                return False

            # Broadcast block to network
            await self.network.broadcast_message(
                "block",
                {"block": block.to_dict()}
            )

            return True

        except Exception as e:
            logger.error(f"Failed to create block: {str(e)}")
            return False

    def get_system_status(self) -> Dict[str, Any]:
        """Get comprehensive system status."""
        return {
            "started": self.started,
            "uptime": (datetime.now() - self.start_time).total_seconds() if self.start_time else 0,
            "blockchain_metrics": self.blockchain.get_chain_metrics(),
            "consensus_metrics": self.consensus.get_metrics(),
            "network_metrics": self.network.get_metrics() if self.network else {},
            "node_count": len(self.blockchain.nodes),
            "shard_count": len(self.blockchain.shards)
        }

    def __str__(self) -> str:
        """String representation of system status."""
        return (
            f"BlockchainSystem(started={self.started}, "
            f"nodes={len(self.blockchain.nodes) if self.blockchain else 0}, "
            f"shards={self.config.num_shards})"
        )