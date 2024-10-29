"""
blockchain/system/initialization_manager.py

Manages the initialization and startup sequence for the ICN blockchain system.
Handles component initialization order, state recovery, and system health checks.
"""

import asyncio
import logging
from typing import Dict, Optional, List, Any
from dataclasses import dataclass
from datetime import datetime
import json
from pathlib import Path

from ..core.blockchain import Blockchain
from ..core.node import Node
from ..consensus.proof_of_cooperation import ProofOfCooperation
from ..network.manager import NetworkManager
from ..network.config import NetworkConfig
from ..contracts.contract_executor import ContractExecutor
from ..core.state.unified_state import UnifiedStateManager

logger = logging.getLogger(__name__)

@dataclass
class SystemState:
    """Tracks the initialization and health state of blockchain components."""
    blockchain_ready: bool = False
    network_ready: bool = False
    consensus_ready: bool = False
    contracts_ready: bool = False
    state_ready: bool = False
    components_initialized: Dict[str, bool] = field(default_factory=dict)
    initialization_time: Optional[datetime] = None
    last_health_check: Optional[datetime] = None
    errors: List[str] = field(default_factory=list)

class InitializationManager:
    """
    Manages the initialization sequence for the ICN blockchain system.
    
    Responsibilities:
    - Coordinated startup of all blockchain components
    - State recovery and verification
    - System health monitoring
    - Graceful shutdown handling
    """

    def __init__(self, config_path: str = "config/blockchain_config.json"):
        """Initialize the manager with configuration."""
        self.config_path = Path(config_path)
        self.state = SystemState()
        self.blockchain: Optional[Blockchain] = None
        self.network: Optional[NetworkManager] = None
        self.consensus: Optional[ProofOfCooperation] = None
        self.contract_executor: Optional[ContractExecutor] = None
        self.state_manager: Optional[UnifiedStateManager] = None
        
        # Track initialization order
        self.initialization_order = [
            "state_manager",
            "blockchain",
            "consensus",
            "network",
            "contracts"
        ]
        
        # Component status tracking
        self.component_status: Dict[str, bool] = {
            component: False for component in self.initialization_order
        }

    async def initialize_system(self) -> bool:
        """
        Initialize all blockchain system components in the correct order.
        
        Returns:
            bool: True if initialization successful, False otherwise
        """
        try:
            logger.info("Beginning blockchain system initialization...")
            
            # Load configuration
            config = await self._load_configuration()
            if not config:
                return False

            # Initialize components in order
            for component in self.initialization_order:
                success = await self._initialize_component(component, config)
                if not success:
                    logger.error(f"Failed to initialize {component}")
                    return False

            # Verify system state
            if not await self._verify_system_state():
                logger.error("System state verification failed")
                return False

            # Start health monitoring
            asyncio.create_task(self._monitor_system_health())
            
            self.state.initialization_time = datetime.now()
            logger.info("Blockchain system initialization completed successfully")
            return True

        except Exception as e:
            logger.error(f"System initialization failed: {str(e)}")
            self.state.errors.append(f"Initialization error: {str(e)}")
            return False

    async def _load_configuration(self) -> Optional[Dict[str, Any]]:
        """Load system configuration from file."""
        try:
            if not self.config_path.exists():
                logger.error(f"Configuration file not found: {self.config_path}")
                return None

            async with aiofiles.open(self.config_path, 'r') as f:
                config_data = json.loads(await f.read())
                return config_data

        except Exception as e:
            logger.error(f"Error loading configuration: {str(e)}")
            return None

    async def _initialize_component(self, component: str, config: Dict[str, Any]) -> bool:
        """Initialize a specific system component."""
        try:
            logger.info(f"Initializing component: {component}")

            if component == "state_manager":
                self.state_manager = UnifiedStateManager(
                    shard_count=config.get("shard_count", 4)
                )
                success = await self._verify_state_manager()

            elif component == "blockchain":
                self.blockchain = Blockchain(
                    num_shards=config.get("shard_count", 4),
                    initial_mana=config.get("initial_mana", 1000),
                    mana_regen_rate=config.get("mana_regen_rate", 10)
                )
                success = self.blockchain.genesis_block_created

            elif component == "consensus":
                self.consensus = ProofOfCooperation(
                    min_reputation=config.get("min_reputation", 10.0),
                    cooldown_blocks=config.get("cooldown_blocks", 3)
                )
                success = True

            elif component == "network":
                network_config = NetworkConfig(
                    node_id=config.get("node_id", ""),
                    host=config.get("host", "0.0.0.0"),
                    port=config.get("port", 30303)
                )
                self.network = NetworkManager(network_config)
                success = await self.network.start()

            elif component == "contracts":
                self.contract_executor = ContractExecutor(
                    initial_mana=config.get("initial_mana", 1000),
                    mana_regen_rate=config.get("mana_regen_rate", 10)
                )
                success = True

            else:
                logger.error(f"Unknown component: {component}")
                return False

            self.component_status[component] = success
            return success

        except Exception as e:
            logger.error(f"Error initializing {component}: {str(e)}")
            self.state.errors.append(f"{component} initialization error: {str(e)}")
            return False

    async def _verify_state_manager(self) -> bool:
        """Verify state manager initialization and recovery."""
        try:
            if not self.state_manager:
                return False

            # Verify state access
            test_key = "initialization_test"
            test_value = "test_data"
            
            # Test state operations
            self.state_manager.state["test"] = {test_key: test_value}
            retrieved_value = self.state_manager.state.get("test", {}).get(test_key)
            
            if retrieved_value != test_value:
                logger.error("State manager verification failed")
                return False

            # Clean up test data
            del self.state_manager.state["test"]
            return True

        except Exception as e:
            logger.error(f"State manager verification failed: {str(e)}")
            return False

    async def _verify_system_state(self) -> bool:
        """Verify overall system state after initialization."""
        try:
            # Check all components are initialized
            if not all(self.component_status.values()):
                return False

            # Verify blockchain state
            if not self.blockchain or not self.blockchain.genesis_block_created:
                return False

            # Verify network connectivity
            if not self.network or not self.network.is_running:
                return False

            # Verify consensus mechanism
            if not self.consensus:
                return False

            # Verify contract executor
            if not self.contract_executor:
                return False

            self.state.blockchain_ready = True
            self.state.network_ready = True
            self.state.consensus_ready = True
            self.state.contracts_ready = True
            self.state.state_ready = True

            return True

        except Exception as e:
            logger.error(f"System state verification failed: {str(e)}")
            return False

    async def _monitor_system_health(self) -> None:
        """Continuously monitor system health."""
        while True:
            try:
                # Perform health checks
                await self._check_component_health()
                self.state.last_health_check = datetime.now()

                # Wait before next check
                await asyncio.sleep(30)

            except Exception as e:
                logger.error(f"Health monitoring error: {str(e)}")
                await asyncio.sleep(5)

    async def _check_component_health(self) -> None:
        """Check health of individual components."""
        try:
            # Check blockchain health
            if self.blockchain:
                chain_metrics = self.blockchain.get_chain_metrics()
                if chain_metrics.get("chain_length", 0) < 1:
                    self.state.blockchain_ready = False
                    logger.warning("Blockchain health check failed")

            # Check network health
            if self.network:
                if not self.network.is_running:
                    self.state.network_ready = False
                    logger.warning("Network health check failed")

            # Check consensus health
            if self.consensus:
                consensus_metrics = self.consensus.get_metrics()
                if not consensus_metrics:
                    self.state.consensus_ready = False
                    logger.warning("Consensus health check failed")

            # Check contract executor health
            if self.contract_executor:
                if self.contract_executor.mana_pool <= 0:
                    self.state.contracts_ready = False
                    logger.warning("Contract executor health check failed")

        except Exception as e:
            logger.error(f"Component health check failed: {str(e)}")

    async def shutdown(self) -> None:
        """Gracefully shutdown all system components."""
        try:
            logger.info("Beginning system shutdown...")

            # Shutdown components in reverse order
            for component in reversed(self.initialization_order):
                await self._shutdown_component(component)

            logger.info("System shutdown completed")

        except Exception as e:
            logger.error(f"Error during shutdown: {str(e)}")

    async def _shutdown_component(self, component: str) -> None:
        """Shutdown a specific system component."""
        try:
            logger.info(f"Shutting down component: {component}")

            if component == "network" and self.network:
                await self.network.stop()
            elif component == "contracts" and self.contract_executor:
                # Cleanup contract executor
                pass
            elif component == "blockchain" and self.blockchain:
                # Ensure state is saved
                pass
            elif component == "consensus" and self.consensus:
                # Cleanup consensus
                pass
            elif component == "state_manager" and self.state_manager:
                # Ensure state is persisted
                pass

            self.component_status[component] = False

        except Exception as e:
            logger.error(f"Error shutting down {component}: {str(e)}")

    def get_system_status(self) -> Dict[str, Any]:
        """Get comprehensive system status."""
        return {
            "initialized": all(self.component_status.values()),
            "initialization_time": self.state.initialization_time.isoformat() 
                if self.state.initialization_time else None,
            "last_health_check": self.state.last_health_check.isoformat() 
                if self.state.last_health_check else None,
            "component_status": self.component_status.copy(),
            "errors": self.state.errors.copy(),
            "blockchain_height": self.blockchain.height if self.blockchain else 0,
            "network_peers": len(self.network.peers) if self.network else 0,
            "active_contracts": len(self.contract_executor.contracts) 
                if self.contract_executor else 0
        }