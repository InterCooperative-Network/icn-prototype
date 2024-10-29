"""
blockchain/core/blockchain.py

Core blockchain implementation for the ICN system. This class coordinates the interaction
between shards, nodes, consensus, and smart contracts while maintaining state consistency
and enforcing cooperative principles.

Key responsibilities:
- Coordinating shard creation and management
- Transaction routing and validation 
- Block creation and validation
- Smart contract execution
- Mana (resource) management
- Cross-shard transaction handling
- State consistency verification
"""

from __future__ import annotations
from typing import List, Dict, Optional, Any, Set
from datetime import datetime
import logging
import hashlib
import json
import asyncio

from .block import Block
from .node import Node 
from .shard import Shard
from .transaction import Transaction
from ..consensus.proof_of_cooperation import ProofOfCooperation
from ..contracts.smart_contract import SmartContract
from ..contracts.contract_executor import ContractExecutor
from ..core.state.unified_state import UnifiedStateManager

logger = logging.getLogger(__name__)

class Blockchain:
    """
    Main blockchain implementation for the ICN system.
    
    This class serves as the central coordinator for the blockchain network,
    managing shards, transactions, consensus, and cross-shard operations while
    enforcing cooperative principles and resource management through the mana system.

    Attributes:
        nodes (Dict[str, Node]): Active nodes in the network
        shards (Dict[int, Shard]): Active shards indexed by shard ID
        chain (List[Block]): Main chain of blocks
        smart_contracts (Dict[str, SmartContract]): Deployed smart contracts
        cooperative_mana (int): Available mana for cooperative operations
        mana_regen_rate (int): Rate of mana regeneration per cycle
    """

    def __init__(self, num_shards: int = 4, initial_mana: int = 1000, mana_regen_rate: int = 10):
        """
        Initialize the blockchain with specified shards and mana settings.

        Args:
            num_shards (int): Number of shards to create initially
            initial_mana (int): Starting mana pool for operations
            mana_regen_rate (int): Rate of mana regeneration
        """
        # Core components
        self.nodes: Dict[str, Node] = {}
        self.shards: Dict[int, Shard] = {}
        self.chain: List[Block] = []
        self.smart_contracts: Dict[str, SmartContract] = {}

        # Consensus and execution
        self.consensus_mechanism = ProofOfCooperation()
        self.contract_executor = ContractExecutor()
        self.state_manager = UnifiedStateManager(num_shards)

        # Resource management
        self.cooperative_mana = initial_mana
        self.mana_regen_rate = mana_regen_rate
        self.max_mana = initial_mana * 2

        # Track cross-shard operations
        self.cross_shard_queue: Dict[int, List[Transaction]] = {
            i: [] for i in range(num_shards)
        }

        # Initialize system
        self._initialize_shards(num_shards)
        self.create_genesis_block()

        # Metadata
        self.metadata = {
            "creation_time": datetime.now(),
            "last_update": datetime.now(),
            "version": "1.0",
            "network_name": "ICN MainNet",
            "network_id": hashlib.sha256(str(datetime.now().timestamp()).encode()).hexdigest()[:8]
        }

        logger.info(f"Initialized ICN blockchain with {num_shards} shards")

    def _initialize_shards(self, num_shards: int) -> None:
        """
        Initialize shards for parallel transaction processing.

        Args:
            num_shards (int): Number of shards to create
        """
        for shard_id in range(num_shards):
            self.create_shard(shard_id)
        logger.info(f"Initialized {num_shards} shards")

    def create_genesis_block(self) -> None:
        """Create the genesis block with initial system state."""
        if self.chain:
            logger.warning("Genesis block already exists")
            return

        genesis_block = Block(
            index=0,
            previous_hash="0" * 64,
            timestamp=datetime.now(),
            transactions=[],
            validator="genesis",
            shard_id=-1  # Special shard ID for genesis block
        )

        self.chain.append(genesis_block)
        logger.info("Genesis block created")

    def register_node(self, node: Node) -> bool:
        """
        Register a new node in the network.

        Args:
            node (Node): Node to register

        Returns:
            bool: True if registration successful
        """
        if not isinstance(node, Node) or node.node_id in self.nodes:
            logger.error(f"Invalid or duplicate node: {node.node_id}")
            return False

        self.nodes[node.node_id] = node
        logger.info(f"Registered node {node.node_id}")
        return True

    def create_shard(self, shard_id: int) -> bool:
        """
        Create a new shard with the given ID.

        Args:
            shard_id (int): ID for the new shard

        Returns:
            bool: True if shard created successfully
        """
        if shard_id in self.shards:
            logger.error(f"Shard {shard_id} already exists")
            return False

        self.shards[shard_id] = Shard(shard_id=shard_id)
        logger.info(f"Created shard {shard_id}")
        return True

    def add_transaction(self, transaction: Dict) -> bool:
        """
        Add a new transaction to the appropriate shard.

        Args:
            transaction (Dict): Transaction data to add

        Returns:
            bool: True if transaction added successfully
        """
        try:
            # Create Transaction object
            tx = Transaction(
                sender=transaction['sender'],
                receiver=transaction['receiver'],
                action=transaction['action'],
                data=transaction['data']
            )

            # Calculate shard assignment
            shard_id = self._calculate_shard_id(tx)
            tx.shard_id = shard_id

            # Validate and add to shard
            if shard_id not in self.shards:
                logger.error(f"Invalid shard ID: {shard_id}")
                return False

            if self.shards[shard_id].add_transaction(tx):
                logger.info(f"Added transaction {tx.transaction_id} to shard {shard_id}")
                return True

            return False

        except Exception as e:
            logger.error(f"Error adding transaction: {str(e)}")
            return False

    def _calculate_shard_id(self, transaction: Transaction) -> int:
        """
        Calculate which shard should handle a transaction.

        Uses a hash-based approach to distribute transactions across shards while
        maintaining consistency for related transactions.

        Args:
            transaction (Transaction): Transaction to assign to a shard

        Returns:
            int: Calculated shard ID
        """
        # Use consistent hashing for shard assignment
        tx_hash = hashlib.sha256(
            json.dumps(transaction.to_dict(), sort_keys=True).encode()
        ).hexdigest()
        return int(tx_hash, 16) % len(self.shards)

    async def create_block(self, shard_id: int) -> Optional[Block]:
        """
        Create a new block in the specified shard.

        Args:
            shard_id (int): Shard ID for block creation

        Returns:
            Optional[Block]: Created block if successful
        """
        try:
            shard = self.shards.get(shard_id)
            if not shard:
                logger.error(f"Invalid shard ID: {shard_id}")
                return None

            # Select validator
            validator = self.consensus_mechanism.select_validator(
                list(self.nodes.values()), 
                shard_id
            )
            if not validator:
                logger.error("No eligible validator available")
                return None

            # Create block
            block = shard.create_block(validator.node_id)
            if not block:
                return None

            # Handle cross-shard references
            self._add_cross_shard_refs(block)

            return block

        except Exception as e:
            logger.error(f"Error creating block: {str(e)}")
            return None

    def add_block(self, block: Block) -> bool:
        """
        Add a validated block to the chain.

        Args:
            block (Block): Block to add

        Returns:
            bool: True if block added successfully
        """
        try:
            # Validate block
            if not isinstance(block, Block):
                logger.error("Invalid block type")
                return False

            validator = self.nodes.get(block.validator)
            if not validator:
                logger.error(f"Unknown validator: {block.validator}")
                return False

            if not self.consensus_mechanism.validate_block(
                block, 
                self.chain[-1] if self.chain else None,
                validator
            ):
                logger.error("Block validation failed")
                return False

            # Add to chain
            self.chain.append(block)
            logger.info(f"Added block {block.index} to chain")

            # Process cross-shard references
            if block.cross_shard_refs:
                self._process_cross_shard_refs(block)

            return True

        except Exception as e:
            logger.error(f"Error adding block: {str(e)}")
            return False

    def regenerate_mana(self) -> None:
        """Regenerate cooperative mana up to the maximum limit."""
        self.cooperative_mana = min(
            self.max_mana,
            self.cooperative_mana + self.mana_regen_rate
        )

    async def deploy_smart_contract(self, contract: SmartContract) -> bool:
        """
        Deploy a new smart contract.

        Args:
            contract (SmartContract): Contract to deploy

        Returns:
            bool: True if deployment successful
        """
        try:
            # Verify contract doesn't already exist
            if contract.contract_id in self.smart_contracts:
                logger.error(f"Contract {contract.contract_id} already exists")
                return False

            # Check mana availability
            if self.cooperative_mana < contract.mana_cost:
                logger.error("Insufficient mana for contract deployment")
                return False

            # Deploy contract
            self.smart_contracts[contract.contract_id] = contract
            self.cooperative_mana -= contract.mana_cost

            logger.info(f"Deployed contract {contract.contract_id}")
            return True

        except Exception as e:
            logger.error(f"Error deploying contract: {str(e)}")
            return False

    async def execute_smart_contract(
        self, 
        contract_id: str,
        input_data: Dict,
        caller: str
    ) -> Optional[Dict]:
        """
        Execute a smart contract.

        Args:
            contract_id (str): ID of contract to execute
            input_data (Dict): Input parameters for execution
            caller (str): ID of calling entity

        Returns:
            Optional[Dict]: Execution results if successful
        """
        try:
            # Get contract
            contract = self.smart_contracts.get(contract_id)
            if not contract:
                logger.error(f"Contract {contract_id} not found")
                return None

            # Verify mana availability
            if self.cooperative_mana < contract.mana_cost:
                logger.error("Insufficient mana for contract execution")
                return None

            # Execute contract
            result = await self.contract_executor.execute_contract(
                contract_id,
                input_data,
                caller
            )

            if result:
                # Update mana and return result
                self.cooperative_mana -= contract.mana_cost
                logger.info(f"Executed contract {contract_id}")
                return result

            return None

        except Exception as e:
            logger.error(f"Error executing contract: {str(e)}")
            return None

    def get_chain_metrics(self) -> Dict:
        """
        Get comprehensive blockchain metrics.

        Returns:
            Dict: Dictionary of metrics
        """
        return {
            "chain_length": len(self.chain),
            "total_transactions": sum(len(block.transactions) for block in self.chain),
            "average_block_time": self._calculate_average_block_time(),
            "active_nodes": len(self.nodes),
            "active_shards": len(self.shards),
            "cooperative_mana": self.cooperative_mana,
            "contract_count": len(self.smart_contracts),
            "network_id": self.metadata["network_id"],
            "last_update": datetime.now().isoformat()
        }

    def _calculate_average_block_time(self) -> float:
        """
        Calculate the average time between blocks.

        Returns:
            float: Average block time in seconds
        """
        if len(self.chain) <= 1:
            return 0.0

        total_time = sum(
            (self.chain[i].timestamp - self.chain[i-1].timestamp).total_seconds()
            for i in range(1, len(self.chain))
        )
        return total_time / (len(self.chain) - 1)

    def validate_chain(self) -> bool:
        """
        Validate the integrity of the entire chain.

        Returns:
            bool: True if chain is valid
        """
        try:
            for i in range(1, len(self.chain)):
                if not self.chain[i].validate(self.chain[i-1]):
                    logger.error(f"Invalid block at height {i}")
                    return False

            logger.info("Chain validation successful")
            return True

        except Exception as e:
            logger.error(f"Error validating chain: {str(e)}")
            return False