from __future__ import annotations
from typing import List, Dict, Optional, Tuple, Any
from datetime import datetime
import hashlib
import logging
import asyncio

from .block import Block
from .node import Node
from .shard import Shard
from .transaction import Transaction
from ..consensus.proof_of_cooperation import ProofOfCooperation
from ..contracts.smart_contract import SmartContract
from ..contracts.contract_executor import ContractExecutor

logger = logging.getLogger(__name__)

class Blockchain:
    """
    Represents the decentralized blockchain for the ICN.

    Key Responsibilities:
    - Manage global chain state and shards
    - Coordinate node registration and transactions
    - Implement consensus and contract execution
    - Maintain cooperative mana economy
    - Ensure system integrity and performance
    """

    def __init__(self, num_shards: int = 4, initial_mana: int = 1000, 
                 mana_regen_rate: int = 10):
        """
        Initialize a new blockchain instance.

        Attributes:
            nodes (Dict[str, Node]): Registered nodes by node_id
            shards (Dict[int, Shard]): Active shards by shard_id
            chain (List[Block]): Main blockchain
            consensus_mechanism (ProofOfCooperation): Consensus implementation
            contract_executor (ContractExecutor): Smart contract execution engine
            cooperative_mana (int): Available system mana
            mana_regen_rate (int): Rate of mana regeneration
            genesis_block_created (bool): Whether genesis block exists
        """
        self.nodes: Dict[str, Node] = {}
        self.shards: Dict[int, Shard] = {}
        self.chain: List[Block] = []
        self.transaction_pool: List[Dict] = []
        self.smart_contracts: Dict[str, SmartContract] = {}

        # Initialize consensus and contract execution
        self.consensus_mechanism = ProofOfCooperation()
        self.contract_executor = ContractExecutor()

        # Mana management
        self.cooperative_mana = initial_mana
        self.mana_regen_rate = mana_regen_rate
        self.genesis_block_created = False

        # Set up shards and genesis block
        self._initialize_shards(num_shards)
        self.create_genesis_block()

    def _initialize_shards(self, num_shards: int) -> None:
        """
        Initialize shards for the blockchain.

        Args:
            num_shards (int): Number of shards to create

        Each shard is assigned a unique ID and initialized with a genesis block.
        """
        for shard_id in range(num_shards):
            self.create_shard(shard_id)

    def create_genesis_block(self) -> None:
        """
        Create the genesis block for the blockchain.

        The genesis block has:
        - Previous hash of zeros
        - No transactions
        - Special validator ID
        - Timestamp of chain creation

        It is idempotent—calling it multiple times has no effect.
        """
        if self.genesis_block_created:
            logger.warning("Genesis block already created")
            return

        genesis_block = Block(
            index=0,
            previous_hash="0" * 64,
            timestamp=datetime.now(),
            transactions=[],
            validator="genesis",
            shard_id=-1  # Global shard
        )

        self.chain.append(genesis_block)
        self.genesis_block_created = True
        logger.info("Genesis block created")

    def register_node(self, node: Node) -> bool:
        """
        Register a new node in the network.

        Args:
            node (Node): The node to register

        Returns:
            bool: True if registration is successful, False otherwise

        Registration allows nodes to participate in consensus, block creation,
        and smart contract execution.
        """
        if not isinstance(node, Node):
            logger.error("Invalid node object")
            return False

        if node.node_id in self.nodes:
            logger.error(f"Node {node.node_id} is already registered")
            return False

        self.nodes[node.node_id] = node
        logger.info(f"Node {node.node_id} registered")
        return True

    def create_shard(self, shard_id: int) -> bool:
        """
        Create a new shard.

        Args:
            shard_id (int): Unique identifier for the new shard

        Returns:
            bool: True if shard created successfully, False otherwise
        """
        if shard_id in self.shards:
            logger.error(f"Shard {shard_id} already exists")
            return False

        shard = Shard(shard_id=shard_id)
        self.shards[shard_id] = shard
        logger.info(f"Shard {shard_id} created")
        return True

    def add_transaction(self, transaction: Dict) -> bool:
        """
        Add a transaction to the blockchain.

        Args:
            transaction (Dict): Transaction data to add

        Returns:
            bool: True if transaction added successfully, False otherwise
        """
        if not isinstance(transaction, dict):
            logger.error("Invalid transaction format")
            return False

        try:
            tx = Transaction(
                sender=transaction['sender'],
                receiver=transaction['receiver'],
                action=transaction['action'],
                data=transaction['data'].copy()
            )

            shard_id = self._calculate_shard_id(transaction)

            if shard_id not in self.shards:
                logger.error(f"Target shard {shard_id} not found")
                return False

            return self.shards[shard_id].add_transaction(tx)

        except Exception as e:
            logger.error(f"Failed to add transaction: {str(e)}")
            return False

    def _calculate_shard_id(self, transaction: Dict) -> int:
        """
        Calculate target shard for a transaction.

        Args:
            transaction (Dict): Transaction to assign to a shard

        Returns:
            int: Calculated shard ID
        """
        tx_hash = hashlib.sha256(str(transaction).encode()).hexdigest()
        return int(tx_hash, 16) % len(self.shards)

    def create_block(self, shard_id: Optional[int] = None) -> Optional[Block]:
        """
        Create a block in the specified shard.

        Args:
            shard_id (Optional[int]): Target shard, or None for global block

        Returns:
            Optional[Block]: The created block or None if creation fails
        """
        shard = self.shards.get(shard_id)
        if not shard:
            logger.error(f"Shard {shard_id} not found")
            return None

        validator = self.consensus_mechanism.select_validator(
            list(self.nodes.values()), shard_id
        )
        if not validator:
            logger.error(f"No eligible validator for shard {shard_id}")
            return None

        new_block = shard.create_block(validator.node_id)
        if new_block and self.add_block(new_block):
            return new_block

        return None

    def add_block(self, block: Block) -> bool:
        """
        Add a block to the blockchain.

        Args:
            block (Block): The block to add

        Returns:
            bool: True if block added successfully, False otherwise
        """
        if not isinstance(block, Block):
            logger.error("Invalid block object")
            return False

        if not block.validate(self.chain[-1]):
            logger.error("Block validation failed")
            return False

        self.chain.append(block)
        logger.info(f"Block {block.index} added")
        return True

    async def deploy_smart_contract(self, contract: SmartContract) -> bool:
        """
        Deploy a smart contract.

        Args:
            contract (SmartContract): Contract to deploy

        Returns:
            bool: True if deployment is successful, False otherwise
        """
        try:
            if not await self.contract_executor.deploy_contract(contract):
                logger.error(f"Failed to deploy contract {contract.contract_id}")
                return False

            self.smart_contracts[contract.contract_id] = contract
            logger.info(f"Contract {contract.contract_id} deployed")
            return True

        except Exception as e:
            logger.error(f"Failed to deploy smart contract: {str(e)}")
            return False

    async def execute_smart_contract(
        self, contract_id: str, input_data: Dict, caller: str
    ) -> Optional[Dict]:
        """
        Execute a smart contract.

        Args:
            contract_id (str): ID of the contract to execute
            input_data (Dict): Input data for execution
            caller (str): ID of the calling entity

        Returns:
            Optional[Dict]: Execution results or None if execution fails
        """
        try:
            contract = self.smart_contracts.get(contract_id)
            if not contract:
                logger.error(f"Contract {contract_id} not found")
                return None

            if self.cooperative_mana < contract.mana_cost:
                logger.error("Insufficient mana for execution")
                return None

            result = await self.contract_executor.execute_contract(
                contract_id, input_data, caller
            )

            if result:
                self.cooperative_mana -= contract.mana_cost
                logger.info(f"Contract {contract_id} executed by {caller}")

            return result

        except Exception as e:
            logger.error(f"Failed to execute smart contract: {str(e)}")
            return None

    def regenerate_mana(self) -> None:
        """
        Regenerate cooperative mana.
        """
        self.cooperative_mana = min(
            1000,  # Maximum mana cap
            self.cooperative_mana + self.mana_regen_rate
        )

    def get_chain_metrics(self) -> Dict:
        """
        Get blockchain metrics.

        Returns:
            Dict: Dictionary of system metrics
        """
        metrics = {
            "chain_length": len(self.chain),
            "total_transactions": sum(len(block.transactions) for block in self.chain),
            "average_block_time": self._calculate_average_block_time(),
            "active_nodes": len(self.nodes),
            "active_shards": len(self.shards),
            "cooperative_mana": self.cooperative_mana,
            "contract_count": len(self.smart_contracts),
        }
        return metrics

    def _calculate_average_block_time(self) -> float:
        """
        Calculate average time between blocks.

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
        Validate the entire blockchain.

        Returns:
            bool: True if the chain is valid, False otherwise
        """
        for i in range(1, len(self.chain)):
            if not self.chain[i].validate(self.chain[i-1]):
                logger.error(f"Block {i} validation failed")
                return False

        logger.info("Blockchain is valid")
        return True
