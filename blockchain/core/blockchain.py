# ================================================================
# File: blockchain/core/blockchain.py
# Description: This file contains the core Blockchain class,
# which manages the network's decentralized ledger, handles blocks,
# transactions, nodes, and shards, and coordinates interactions
# within the ICN ecosystem.
# ================================================================

from __future__ import annotations
from typing import List, Dict, Optional, Tuple, Set
from datetime import datetime, timedelta
import logging
from .block import Block
from .node import Node
from .shard import Shard
from ..consensus.proof_of_cooperation import ProofOfCooperation
from ..contracts.smart_contract import SmartContract
from ..contracts.contract_executor import ContractExecutor

logger = logging.getLogger(__name__)

class Blockchain:
    """
    Represents the decentralized blockchain for the ICN.

    This blockchain manages the global state, coordinates block creation,
    handles transactions, shards, nodes, and consensus. It emphasizes cooperative
    principles, integrating decentralized governance and ensuring equitable participation.
    """

    def __init__(self):
        """
        Initialize the Blockchain instance.
        """
        self.nodes: Dict[str, Node] = {}
        self.shards: Dict[int, Shard] = {}
        self.consensus_mechanism = ProofOfCooperation()
        self.contract_executor = ContractExecutor()
        self.chain: List[Block] = []
        self.transaction_pool: List[Dict] = []
        self.smart_contracts: Dict[str, SmartContract] = {}
        self.genesis_block_created = False

    def create_genesis_block(self) -> None:
        """
        Create the genesis block for the blockchain, marking the start of the chain.
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
            shard_id=-1,
        )

        self.chain.append(genesis_block)
        self.genesis_block_created = True
        logger.info("Genesis block created")

    def register_node(self, node: Node) -> bool:
        """
        Register a new node to the blockchain.

        Args:
            node (Node): The node to be registered.

        Returns:
            bool: True if the node is successfully registered, False otherwise.
        """
        try:
            if node.node_id in self.nodes:
                logger.error(f"Node {node.node_id} is already registered")
                return False

            self.nodes[node.node_id] = node
            logger.info(f"Node {node.node_id} registered")
            return True

        except Exception as e:
            logger.error(f"Failed to register node: {str(e)}")
            return False

    def create_shard(self, shard_id: int) -> bool:
        """
        Create a new shard to handle transactions in parallel.

        Args:
            shard_id (int): The ID of the shard to be created.

        Returns:
            bool: True if the shard is successfully created, False otherwise.
        """
        try:
            if shard_id in self.shards:
                logger.error(f"Shard {shard_id} already exists")
                return False

            shard = Shard(shard_id=shard_id)
            self.shards[shard_id] = shard
            logger.info(f"Shard {shard_id} created")
            return True

        except Exception as e:
            logger.error(f"Failed to create shard: {str(e)}")
            return False

    def add_transaction(self, transaction: Dict) -> bool:
        """
        Add a new transaction to the transaction pool.

        Args:
            transaction (Dict): The transaction to be added.

        Returns:
            bool: True if the transaction is successfully added, False otherwise.
        """
        try:
            # Validate transaction format
            if not isinstance(transaction, dict):
                logger.error("Invalid transaction format")
                return False

            self.transaction_pool.append(transaction)
            logger.info(f"Transaction added to pool: {transaction.get('transaction_id')}")
            return True

        except Exception as e:
            logger.error(f"Failed to add transaction: {str(e)}")
            return False

    def create_block(self, shard_id: Optional[int] = None) -> Optional[Block]:
        """
        Create a new block from pending transactions in a shard.

        Args:
            shard_id (Optional[int]): The shard ID to create the block for, if applicable.

        Returns:
            Optional[Block]: The newly created block or None if creation fails.
        """
        try:
            if shard_id is None:
                # Create global block from all shards
                transactions = self.transaction_pool[:100]
                new_block = Block(
                    index=len(self.chain),
                    previous_hash=self.chain[-1].hash,
                    timestamp=datetime.now(),
                    transactions=transactions,
                    validator="global",
                    shard_id=-1,
                )

                self.transaction_pool = self.transaction_pool[100:]
                self.chain.append(new_block)
                logger.info(f"Global block {new_block.index} created")
                return new_block

            # Create shard-specific block
            shard = self.shards.get(shard_id)
            if not shard:
                logger.error(f"Shard {shard_id} not found")
                return None

            validator = self.consensus_mechanism.select_validator(list(self.nodes.values()), shard_id)
            if not validator:
                logger.error(f"No eligible validator for shard {shard_id}")
                return None

            new_block = shard.create_block(validator.node_id)
            if new_block:
                if self.add_block(new_block):
                    return new_block

            return None

        except Exception as e:
            logger.error(f"Failed to create block: {str(e)}")
            return None

    def add_block(self, block: Block) -> bool:
        """
        Add a validated block to the blockchain.

        Args:
            block (Block): The block to be added.

        Returns:
            bool: True if the block is successfully added, False otherwise.
        """
        try:
            previous_block = self.chain[-1]
            if not self.consensus_mechanism.validate_block(block, previous_block, self.nodes[block.validator]):
                logger.error("Block validation failed")
                return False

            self.chain.append(block)
            logger.info(f"Block {block.index} added to chain")
            return True

        except Exception as e:
            logger.error(f"Failed to add block: {str(e)}")
            return False

    def deploy_smart_contract(self, contract: SmartContract) -> bool:
        """
        Deploy a new smart contract to the blockchain.

        Args:
            contract (SmartContract): The smart contract to be deployed.

        Returns:
            bool: True if the contract is successfully deployed, False otherwise.
        """
        try:
            if not self.contract_executor.deploy_contract(contract):
                logger.error(f"Failed to deploy contract {contract.contract_id}")
                return False

            self.smart_contracts[contract.contract_id] = contract
            logger.info(f"Contract {contract.contract_id} deployed")
            return True

        except Exception as e:
            logger.error(f"Failed to deploy smart contract: {str(e)}")
            return False

    def execute_smart_contract(self, contract_id: str, input_data: Dict, caller: str) -> Optional[Dict]:
        """
        Execute a smart contract on the blockchain.

        Args:
            contract_id (str): The ID of the contract to execute.
            input_data (Dict): The input data for the contract execution.
            caller (str): The ID of the entity calling the contract.

        Returns:
            Optional[Dict]: The result of the execution or None if it fails.
        """
        try:
            result = self.contract_executor.execute_contract(contract_id, input_data, caller)
            logger.info(f"Contract {contract_id} executed by {caller}")
            return result

        except Exception as e:
            logger.error(f"Failed to execute smart contract: {str(e)}")
            return None

    def get_chain_metrics(self) -> Dict:
        """
        Get blockchain-wide metrics including transaction counts, block times, and validator statistics.

        Returns:
            Dict: Dictionary of blockchain metrics.
        """
        try:
            total_transactions = sum(len(block.transactions) for block in self.chain)
            avg_block_time = (
                sum(
                    (self.chain[i].timestamp - self.chain[i-1].timestamp).total_seconds()
                    for i in range(1, len(self.chain))
                ) / (len(self.chain) - 1) if len(self.chain) > 1 else 0
            )

            metrics = {
                "chain_length": len(self.chain),
                "total_transactions": total_transactions,
                "average_block_time": avg_block_time,
                "active_nodes": len(self.nodes),
                "active_shards": len(self.shards),
                "contract_count": len(self.smart_contracts),
            }

            return metrics

        except Exception as e:
            logger.error(f"Failed to get chain metrics: {str(e)}")
            return {}

    def validate_chain(self) -> bool:
        """
        Validate the entire blockchain, checking block integrity, sequence, and timestamps.

        Returns:
            bool: True if the chain is valid, False otherwise.
        """
        try:
            for i in range(1, len(self.chain)):
                current_block = self.chain[i]
                previous_block = self.chain[i - 1]

                if not current_block.validate(previous_block):
                    logger.error(f"Invalid block at index {i}")
                    return False

                if current_block.index != previous_block.index + 1:
                    logger.error(f"Non-sequential blocks at index {i}")
                    return False

                if current_block.timestamp <= previous_block.timestamp:
                    logger.error(f"Invalid timestamp at index {i}")
                    return False

            logger.info("Blockchain is valid")
            return True

        except Exception as e:
            logger.error(f"Blockchain validation failed: {str(e)}")
            return False
