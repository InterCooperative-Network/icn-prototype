# ================================================================
# File: blockchain/core/blockchain.py
# ================================================================
# Description: Core Blockchain implementation for the ICN system.
# 
# This module manages the ICN blockchain, coordinating shards, nodes,
# transactions, consensus, and smart contract execution.
# ================================================================

from __future__ import annotations
from typing import List, Dict, Optional, Any
from datetime import datetime, timedelta
import logging
import hashlib
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
    Core Blockchain implementation for the ICN system.
    Manages shards, transactions, blocks, nodes, consensus, and contracts.
    """

    def __init__(self, num_shards: int = 4, initial_mana: int = 1000, mana_regen_rate: int = 10):
        """
        Initialize the blockchain with specified shards, mana, and consensus.
        """
        self.nodes: Dict[str, Node] = {}
        self.shards: Dict[int, Shard] = {}
        self.chain: List[Block] = []
        self.transaction_pool: List[Transaction] = []
        self.smart_contracts: Dict[str, SmartContract] = {}

        self.consensus_mechanism = ProofOfCooperation()
        self.contract_executor = ContractExecutor()

        self.cooperative_mana = initial_mana
        self.mana_regen_rate = mana_regen_rate
        self.genesis_block_created = False

        self._initialize_shards(num_shards)
        self.create_genesis_block()

    def _initialize_shards(self, num_shards: int) -> None:
        """
        Initialize shards for parallel transaction processing.
        """
        for shard_id in range(num_shards):
            self.create_shard(shard_id)

    def create_genesis_block(self) -> None:
        """
        Create the genesis block with no transactions and a special validator.
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
            shard_id=-1
        )

        self.chain.append(genesis_block)
        self.genesis_block_created = True
        logger.info("Genesis block created")

    def register_node(self, node: Node) -> bool:
        """
        Register a node and make it eligible for validation.
        """
        if not isinstance(node, Node) or node.node_id in self.nodes:
            logger.error(f"Invalid or duplicate node: {node.node_id}")
            return False

        node.is_validator = True
        self.nodes[node.node_id] = node
        logger.info(f"Node {node.node_id} registered as validator")
        return True

    def create_shard(self, shard_id: int) -> bool:
        """
        Create a new shard with the given ID.
        """
        if shard_id in self.shards:
            logger.error(f"Shard {shard_id} already exists")
            return False

        self.shards[shard_id] = Shard(shard_id=shard_id)
        logger.info(f"Shard {shard_id} created")
        return True

    def add_transaction(self, transaction: Dict) -> bool:
        """
        Add a transaction after initializing and validating it.
        """
        if not isinstance(transaction, dict):
            logger.error("Invalid transaction format")
            return False

        tx = Transaction(
            sender=transaction['sender'],
            receiver=transaction['receiver'],
            action=transaction['action'],
            data=transaction['data']
        )

        shard_id = self._calculate_shard_id(tx)
        tx.shard_id = shard_id
        tx.transaction_id = self._calculate_transaction_id(tx)

        if shard_id not in self.shards or not self.shards[shard_id].add_transaction(tx):
            logger.error(f"Failed to add transaction {tx.transaction_id} to shard {shard_id}")
            return False

        logger.info(f"Transaction {tx.transaction_id} added to shard {shard_id}")
        return True

    def _calculate_shard_id(self, transaction: Transaction) -> int:
        """
        Calculate the shard ID for the transaction using its hash.
        """
        tx_hash = hashlib.sha256(str(transaction).encode()).hexdigest()
        return int(tx_hash, 16) % len(self.shards)

    def _calculate_transaction_id(self, transaction: Transaction) -> str:
        """
        Calculate the transaction ID using the hash of its contents.
        """
        tx_hash = hashlib.sha256(str(transaction).encode()).hexdigest()
        return tx_hash

    def create_block(self, shard_id: Optional[int] = None) -> Optional[Block]:
        """
        Create a new block in the specified shard.
        """
        shard = self.shards.get(shard_id)
        if not shard:
            logger.error(f"Shard {shard_id} not found")
            return None

        validator = self.consensus_mechanism.select_validator(list(self.nodes.values()), shard_id)
        if not validator:
            logger.error(f"No eligible validator for shard {shard_id}")
            return None

        new_block = shard.create_block(validator.node_id)
        if new_block and self.add_block(new_block):
            return new_block

        return None

    def add_block(self, block: Block) -> bool:
        """
        Add a validated block to the chain.
        """
        if not isinstance(block, Block) or not block.validate(self.chain[-1]):
            logger.error("Block validation failed")
            return False

        self.chain.append(block)
        logger.info(f"Block {block.index} added to chain")
        return True

    def regenerate_mana(self) -> None:
        """
        Regenerate cooperative mana up to the cap.
        """
        self.cooperative_mana = min(1000, self.cooperative_mana + self.mana_regen_rate)

    def get_chain_metrics(self) -> Dict:
        """
        Return blockchain metrics including chain length and mana.
        """
        return {
            "chain_length": len(self.chain),
            "total_transactions": sum(len(block.transactions) for block in self.chain),
            "average_block_time": self._calculate_average_block_time(),
            "active_nodes": len(self.nodes),
            "active_shards": len(self.shards),
            "cooperative_mana": self.cooperative_mana,
            "contract_count": len(self.smart_contracts),
        }

    def _calculate_average_block_time(self) -> float:
        """
        Calculate the average time between blocks.
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
        """
        for i in range(1, len(self.chain)):
            if not self.chain[i].validate(self.chain[i-1]):
                logger.error(f"Block {i} validation failed")
                return False

        logger.info("Blockchain is valid")
        return True

    async def deploy_smart_contract(self, contract: SmartContract) -> bool:
        """
        Deploy a smart contract and register it.
        """
        if contract.contract_id in self.smart_contracts or self.cooperative_mana < contract.mana_cost:
            logger.error(f"Contract {contract.contract_id} deployment failed")
            return False

        self.smart_contracts[contract.contract_id] = contract
        self.cooperative_mana -= contract.mana_cost
        logger.info(f"Contract {contract.contract_id} deployed")
        return True

    async def execute_smart_contract(self, contract_id: str, input_data: Dict, caller: str) -> Optional[Dict]:
        """
        Execute a smart contract with the given input data.
        """
        contract = self.smart_contracts.get(contract_id)
        if not contract or self.cooperative_mana < contract.mana_cost:
            logger.error(f"Failed to execute contract {contract_id}")
            return None

        result = await self.contract_executor.execute_contract(contract_id, input_data, caller)
        if result is not None:
            self.cooperative_mana -= contract.mana_cost
            logger.info(f"Contract {contract_id} executed by {caller}")
        else:
            logger.error(f"Failed to execute contract {contract_id}")

        return result
