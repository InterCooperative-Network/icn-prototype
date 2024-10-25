# ================================================================
# File: blockchain/core/shard.py
# Description: Core shard implementation for the ICN blockchain.
# Handles a partition of the blockchain, managing its own chain of
# blocks, transaction pool, and validation processes.
# ================================================================

from __future__ import annotations
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Set, Tuple
import logging
from .transaction import Transaction
from .block import Block

logger = logging.getLogger(__name__)

class Shard:
    """
    Represents a blockchain shard.

    A shard is a partition of the blockchain that processes a subset of transactions,
    enabling parallel processing and improved scalability. Each shard maintains its
    own chain of blocks, transaction pool, and validation state.

    Attributes:
        shard_id (int): Unique identifier for this shard
        chain (List[Block]): The shard's blockchain
        pending_transactions (List[Transaction]): Pool of unconfirmed transactions
        height (int): Current height of the chain
        max_transactions_per_block (int): Maximum transactions allowed per block
        last_block_time (datetime): Timestamp of the last block
        state (Dict): Shard-specific state storage
        metrics (Dict): Performance and operational metrics
    """

    def __init__(self, shard_id: int, max_transactions_per_block: int = 100):
        """
        Initialize a new shard.

        Args:
            shard_id (int): Unique identifier for the shard
            max_transactions_per_block (int): Maximum transactions per block
        """
        self.shard_id = shard_id
        self.chain: List[Block] = []
        self.pending_transactions: List[Transaction] = []
        self.height = 0
        self.max_transactions_per_block = max_transactions_per_block
        self.last_block_time = datetime.now()
        self.state: Dict = {}  # Shard-specific state
        self.metrics: Dict = {
            "total_transactions": 0,
            "average_block_time": 0,
            "blocks_created": 0,
            "pending_count": 0,
            "validation_failures": 0,
            "successful_blocks": 0,
            "rejected_transactions": 0,
            "total_size_bytes": 0,
            "average_transactions_per_block": 0
        }
        self.validation_cache: Dict[str, bool] = {}  # Cache validation results
        self.known_validators: Set[str] = set()  # Track known validators
        self.cross_shard_references: Dict[str, List[str]] = {}  # Track cross-shard txs
        self.last_prune_time = datetime.now()
        self._create_genesis_block()

    def _create_genesis_block(self) -> None:
        """Create genesis block for this shard."""
        try:
            genesis_block = Block(
                index=0,
                previous_hash="0" * 64,
                timestamp=datetime.now(),
                transactions=[],
                validator="genesis",
                shard_id=self.shard_id,
            )
            self.chain.append(genesis_block)
            self.height = 1
            self.last_block_time = genesis_block.timestamp
            self.metrics["blocks_created"] = 1
            self.known_validators.add("genesis")
            self._update_size_metrics(genesis_block)
            logger.info(f"Created genesis block for shard {self.shard_id}")

        except Exception as e:
            logger.error(f"Failed to create genesis block: {str(e)}")
            raise RuntimeError("Failed to initialize shard with genesis block")

    def add_transaction(self, transaction: Transaction) -> bool:
        """
        Add a new transaction to pending pool.

        Args:
            transaction (Transaction): Transaction to add to the pool

        Returns:
            bool: True if transaction was added successfully
        """
        try:
            if transaction.shard_id != self.shard_id:
                logger.error(
                    f"Transaction shard_id {transaction.shard_id} "
                    f"doesn't match shard {self.shard_id}"
                )
                self.metrics["rejected_transactions"] += 1
                return False

            if len(self.pending_transactions) >= self.max_transactions_per_block * 2:
                logger.warning(f"Shard {self.shard_id} transaction pool full")
                self.metrics["rejected_transactions"] += 1
                return False

            # Check if transaction already exists
            tx_id = transaction.transaction_id
            if any(tx.transaction_id == tx_id for tx in self.pending_transactions):
                logger.warning(f"Transaction {tx_id} already in pending pool")
                return False

            # Validate transaction
            if not self._validate_transaction(transaction):
                self.metrics["rejected_transactions"] += 1
                return False

            self.pending_transactions.append(transaction)
            self.metrics["pending_count"] = len(self.pending_transactions)
            
            # Check and record cross-shard references
            if self._is_cross_shard_transaction(transaction):
                self._record_cross_shard_reference(transaction)

            return True

        except Exception as e:
            logger.error(f"Failed to add transaction: {str(e)}")
            self.metrics["rejected_transactions"] += 1
            return False

    def _validate_transaction(self, transaction: Transaction) -> bool:
        """
        Validate a transaction before adding to pool.

        Args:
            transaction (Transaction): Transaction to validate

        Returns:
            bool: True if transaction is valid
        """
        try:
            # Check cache first
            tx_id = transaction.transaction_id
            if tx_id in self.validation_cache:
                return self.validation_cache[tx_id]

            # Basic validation
            if not transaction.validate():
                logger.error(f"Transaction {tx_id} failed basic validation")
                self.validation_cache[tx_id] = False
                return False

            # Check if transaction already exists in chain
            if self.get_transaction_by_id(tx_id):
                logger.error(f"Transaction {tx_id} already exists in chain")
                self.validation_cache[tx_id] = False
                return False

            # Validate timestamp
            if transaction.timestamp > datetime.now() + timedelta(minutes=5):
                logger.error(f"Transaction {tx_id} has future timestamp")
                self.validation_cache[tx_id] = False
                return False

            # Cache and return result
            self.validation_cache[tx_id] = True
            return True

        except Exception as e:
            logger.error(f"Transaction validation failed: {str(e)}")
            return False

    def create_block(self, validator: str) -> Optional[Block]:
        """
        Create a new block from pending transactions.

        Args:
            validator (str): ID of the validator creating the block

        Returns:
            Optional[Block]: New block if created successfully, None otherwise
        """
        if not self.pending_transactions:
            logger.debug(f"No pending transactions in shard {self.shard_id}")
            return None

        try:
            # Select transactions for the block
            selected_transactions = self._select_transactions_for_block()
            if not selected_transactions:
                return None

            # Create the block
            new_block = Block(
                index=self.height,
                previous_hash=self.chain[-1].hash,
                timestamp=datetime.now(),
                transactions=selected_transactions,
                validator=validator,
                shard_id=self.shard_id,
            )

            # Update validator tracking
            self.known_validators.add(validator)

            return new_block

        except Exception as e:
            logger.error(f"Failed to create block: {str(e)}")
            self.metrics["validation_failures"] += 1
            return None

    def _select_transactions_for_block(self) -> List[Transaction]:
        """
        Select and sort transactions for a new block.

        Returns:
            List[Transaction]: Selected transactions for the block
        """
        try:
            # Get initial selection
            candidates = self.pending_transactions[:self.max_transactions_per_block]
            
            # Sort by timestamp and fee (if applicable)
            candidates.sort(key=lambda tx: tx.timestamp)
            
            # Ensure all transactions are still valid
            valid_transactions = [
                tx for tx in candidates
                if self._validate_transaction(tx)
            ]

            return valid_transactions

        except Exception as e:
            logger.error(f"Failed to select transactions: {str(e)}")
            return []

    def add_block(self, block: Block) -> bool:
        """
        Add a validated block to the shard chain.

        Args:
            block (Block): Block to add to the chain

        Returns:
            bool: True if block was added successfully
        """
        try:
            # Validate block attributes
            if not self._validate_block_attributes(block):
                return False

            # Validate block against previous block
            if not block.validate(self.chain[-1]):
                logger.error("Block validation failed")
                self.metrics["validation_failures"] += 1
                return False

            # Update metrics
            self._update_metrics_for_new_block(block)

            # Remove included transactions from pending pool
            self._remove_included_transactions(block)

            # Add block to chain
            self.chain.append(block)
            self.height += 1
            self.last_block_time = block.timestamp

            # Update cross-shard references
            self._update_cross_shard_references(block)

            logger.info(f"Added block {block.index} to shard {self.shard_id}")
            self.metrics["successful_blocks"] += 1
            return True

        except Exception as e:
            logger.error(f"Failed to add block: {str(e)}")
            self.metrics["validation_failures"] += 1
            return False

    def _validate_block_attributes(self, block: Block) -> bool:
        """
        Validate block attributes before adding to chain.

        Args:
            block (Block): Block to validate

        Returns:
            bool: True if block attributes are valid
        """
        if block.shard_id != self.shard_id:
            logger.error(
                f"Block shard_id {block.shard_id} "
                f"doesn't match shard {self.shard_id}"
            )
            return False

        if block.index != self.height:
            logger.error(
                f"Block index {block.index} "
                f"doesn't match current height {self.height}"
            )
            return False

        return True

    def _update_metrics_for_new_block(self, block: Block) -> None:
        """
        Update shard metrics when adding a new block.

        Args:
            block (Block): New block being added
        """
        block_time = (block.timestamp - self.last_block_time).total_seconds()
        blocks_created = self.metrics["blocks_created"]
        
        # Update average block time
        self.metrics["average_block_time"] = (
            (self.metrics["average_block_time"] * blocks_created + block_time)
            / (blocks_created + 1)
        )
        
        # Update other metrics
        self.metrics["blocks_created"] += 1
        self.metrics["total_transactions"] += len(block.transactions)
        
        # Update average transactions per block
        total_tx = sum(len(b.transactions) for b in self.chain) + len(block.transactions)
        self.metrics["average_transactions_per_block"] = total_tx / (blocks_created + 1)
        
        # Update size metrics
        self._update_size_metrics(block)

    def _update_size_metrics(self, block: Block) -> None:
        """
        Update size-related metrics for the shard.

        Args:
            block (Block): Block to measure
        """
        try:
            # Estimate block size
            block_dict = block.to_dict()
            block_size = len(str(block_dict))  # Simple size estimation
            self.metrics["total_size_bytes"] += block_size

        except Exception as e:
            logger.error(f"Failed to update size metrics: {str(e)}")

    def _remove_included_transactions(self, block: Block) -> None:
        """
        Remove transactions included in a block from the pending pool.

        Args:
            block (Block): Block containing transactions to remove
        """
        tx_ids = {tx.transaction_id for tx in block.transactions}
        self.pending_transactions = [
            tx for tx in self.pending_transactions 
            if tx.transaction_id not in tx_ids
        ]
        self.metrics["pending_count"] = len(self.pending_transactions)

    def _is_cross_shard_transaction(self, transaction: Transaction) -> bool:
        """
        Check if a transaction involves multiple shards.

        Args:
            transaction (Transaction): Transaction to check

        Returns:
            bool: True if transaction involves multiple shards
        """
        return 'target_shard' in transaction.data

    def _record_cross_shard_reference(self, transaction: Transaction) -> None:
        """
        Record a cross-shard transaction reference.

        Args:
            transaction (Transaction): Cross-shard transaction to record
        """
        target_shard = transaction.data.get('target_shard')
        if target_shard:
            if target_shard not in self.cross_shard_references:
                self.cross_shard_references[target_shard] = []
            self.cross_shard_references[target_shard].append(transaction.transaction_id)

    def _update_cross_shard_references(self, block: Block) -> None:
        """
        Update cross-shard references when adding a block.

        Args:
            block (Block): Block to process for cross-shard references
        """
        for tx in block.transactions:
            if self._is_cross_shard_transaction(tx):
                target_shard = tx.data.get('target_shard')
                if target_shard in self.cross_shard_references:
                    self.cross_shard_references[target_shard] = [
                        tx_id for tx_id in self.cross_shard_references[target_shard]
                        if tx_id != tx.transaction_id
                    ]

    def get_latest_block(self) -> Block:
        """
        Get the latest block in this shard.

        Returns:
            Block: The most recent block in the chain
        """
        return self.chain[-1]

    def validate_chain(self) -> bool:
        """
        Validate the entire shard chain.

        Returns:
            bool: True if the chain is valid
        """
        try:
            for i in range(1, len(self.chain)):
                current_block = self.chain[i]
                previous_block = self.chain[i - 1]

                # Validate block
                if not current_block.validate(previous_block):
                    logger.error(f"Invalid block at height {i}")
                    return False

                # Validate sequence
                if current_block.index != previous_block.index + 1:
                    logger.error(f"Non-sequential blocks at height {i}")
                    return False

                # Validate timestamps
                if current_block.timestamp <= previous_block.timestamp:
                    logger.error(f"Invalid timestamp at height {i}")
                    return False

            return True

        except Exception as e:
            logger.error(f"Chain validation failed: {str(e)}")
            return False

    def get_block_by_hash(self, block_hash: str) -> Optional[Block]:
        """
        Retrieve a block by its hash.

        Args:
            block_hash (str): Hash of the block to retrieve

        Returns:
            Optional[Block]: The block if found, None otherwise
        """
        try:
            # Use next() with generator expression for efficient search
            return next(
                (block for block in self.chain if block.hash == block_hash),
                None
            )
        except Exception as e:
            logger.error(f"Failed to retrieve block: {str(e)}")
            return None

    def get_block_by_height(self, height: int) -> Optional[Block]:
        """
        Retrieve a block by its height.

        Args:
            height (int): Height of the block to retrieve

        Returns:
            Optional[Block]: The block if found, None otherwise
        """
        try:
            if 0 <= height < len(self.chain):
                return self.chain[height]
            logger.warning(f"Block height {height} out of range")
            return None
        except Exception as e:
            logger.error(f"Failed to retrieve block: {str(e)}")
            return None

    def get_transaction_by_id(self, transaction_id: str) -> Optional[Transaction]:
        """
        Find a transaction by its ID in the chain.

        Args:
            transaction_id (str): ID of the transaction to find

        Returns:
            Optional[Transaction]: The transaction if found, None otherwise
        """
        try:
            # First check pending transactions
            pending_tx = next(
                (tx for tx in self.pending_transactions if tx.transaction_id == transaction_id),
                None
            )
            if pending_tx:
                return pending_tx

            # Then search through blocks from newest to oldest
            for block in reversed(self.chain):
                tx = next(
                    (tx for tx in block.transactions if tx.transaction_id == transaction_id),
                    None
                )
                if tx:
                    return tx
            return None
        except Exception as e:
            logger.error(f"Failed to retrieve transaction: {str(e)}")
            return None

    def get_transactions_in_range(self, start_height: int, end_height: int) -> List[Transaction]:
        """
        Get all transactions within a specified block height range.

        Args:
            start_height (int): Starting block height (inclusive)
            end_height (int): Ending block height (inclusive)

        Returns:
            List[Transaction]: List of transactions in the specified range
        """
        transactions = []
        try:
            start_idx = max(0, start_height)
            end_idx = min(end_height + 1, len(self.chain))
            
            for i in range(start_idx, end_idx):
                transactions.extend(self.chain[i].transactions)
            
            return transactions
        except Exception as e:
            logger.error(f"Failed to get transactions in range: {str(e)}")
            return []

    def prune_pending_transactions(self, max_age_minutes: int = 60) -> None:
        """
        Remove old pending transactions.

        Args:
            max_age_minutes (int): Maximum age of transactions to keep in minutes
        """
        try:
            current_time = datetime.now()
            original_count = len(self.pending_transactions)
            
            self.pending_transactions = [
                tx for tx in self.pending_transactions
                if (current_time - tx.timestamp).total_seconds() < max_age_minutes * 60
            ]
            
            pruned_count = original_count - len(self.pending_transactions)
            self.metrics["pending_count"] = len(self.pending_transactions)
            
            if pruned_count > 0:
                logger.info(f"Pruned {pruned_count} old transactions")

            # Clear validation cache for pruned transactions
            self._prune_validation_cache()

        except Exception as e:
            logger.error(f"Failed to prune transactions: {str(e)}")

    def _prune_validation_cache(self) -> None:
        """Clean up the validation cache for efficiency."""
        try:
            # Keep cache entries only for existing transactions
            valid_tx_ids = {tx.transaction_id for tx in self.pending_transactions}
            self.validation_cache = {
                tx_id: result
                for tx_id, result in self.validation_cache.items()
                if tx_id in valid_tx_ids
            }
        except Exception as e:
            logger.error(f"Failed to prune validation cache: {str(e)}")

    def get_metrics(self) -> Dict:
        """
        Get comprehensive shard metrics.

        Returns:
            Dict: Dictionary containing current metrics
        """
        try:
            return {
                "shard_id": self.shard_id,
                "height": self.height,
                "pending_transactions": len(self.pending_transactions),
                "last_block_time": self.last_block_time.isoformat(),
                "chain_size": len(self.chain),
                "total_transactions_in_chain": sum(
                    len(block.transactions) for block in self.chain
                ),
                "known_validators": len(self.known_validators),
                "cross_shard_references": {
                    k: len(v) for k, v in self.cross_shard_references.items()
                },
                "validation_cache_size": len(self.validation_cache),
                **self.metrics,
            }
        except Exception as e:
            logger.error(f"Failed to get metrics: {str(e)}")
            return {}

    def get_chain_stats(self) -> Dict:
        """
        Get detailed statistics about the shard's chain.

        Returns:
            Dict: Dictionary containing chain statistics
        """
        try:
            block_times = []
            tx_counts = []
            validator_blocks = {}

            for i in range(1, len(self.chain)):
                current_block = self.chain[i]
                prev_block = self.chain[i-1]
                
                # Calculate block time
                block_time = (current_block.timestamp - prev_block.timestamp).total_seconds()
                block_times.append(block_time)
                
                # Track transactions
                tx_count = len(current_block.transactions)
                tx_counts.append(tx_count)
                
                # Track validator activity
                validator = current_block.validator
                validator_blocks[validator] = validator_blocks.get(validator, 0) + 1

            return {
                "average_block_time": sum(block_times) / len(block_times) if block_times else 0,
                "min_block_time": min(block_times) if block_times else 0,
                "max_block_time": max(block_times) if block_times else 0,
                "average_transactions": sum(tx_counts) / len(tx_counts) if tx_counts else 0,
                "max_transactions": max(tx_counts) if tx_counts else 0,
                "validator_distribution": validator_blocks,
                "total_blocks": len(self.chain),
                "total_validators": len(validator_blocks)
            }
        except Exception as e:
            logger.error(f"Failed to get chain stats: {str(e)}")
            return {}

    def to_dict(self) -> Dict:
        """
        Convert shard state to dictionary.

        Returns:
            Dict: Dictionary representation of the shard
        """
        try:
            return {
                "shard_id": self.shard_id,
                "height": self.height,
                "chain": [block.to_dict() for block in self.chain],
                "pending_transactions": [
                    tx.to_dict() for tx in self.pending_transactions
                ],
                "max_transactions_per_block": self.max_transactions_per_block,
                "last_block_time": self.last_block_time.isoformat(),
                "state": self.state,
                "metrics": self.metrics,
                "cross_shard_references": self.cross_shard_references,
                "known_validators": list(self.known_validators),
            }
        except Exception as e:
            logger.error(f"Failed to convert shard to dictionary: {str(e)}")
            raise ValueError(f"Failed to serialize shard: {str(e)}")

    @classmethod
    def from_dict(cls, data: Dict) -> 'Shard':
        """
        Create shard from dictionary.

        Args:
            data (Dict): Dictionary containing shard data

        Returns:
            Shard: New shard instance

        Raises:
            ValueError: If the data is invalid or incomplete
        """
        try:
            shard = cls(
                shard_id=data["shard_id"],
                max_transactions_per_block=data["max_transactions_per_block"],
            )

            # Restore chain
            shard.chain = [Block.from_dict(block) for block in data["chain"]]

            # Restore pending transactions
            shard.pending_transactions = [
                Transaction.from_dict(tx) for tx in data["pending_transactions"]
            ]

            # Restore other attributes
            shard.height = data["height"]
            shard.last_block_time = datetime.fromisoformat(data["last_block_time"])
            shard.state = data["state"]
            shard.metrics = data["metrics"]
            shard.cross_shard_references = data.get("cross_shard_references", {})
            shard.known_validators = set(data.get("known_validators", []))

            return shard

        except Exception as e:
            logger.error(f"Failed to create shard from dictionary: {str(e)}")
            raise ValueError(f"Invalid shard data: {str(e)}")

    def __str__(self) -> str:
        """
        Return a human-readable string representation of the shard.

        Returns:
            str: String representation of the shard
        """
        return (
            f"Shard(id={self.shard_id}, "
            f"height={self.height}, "
            f"pending_tx={len(self.pending_transactions)}, "
            f"blocks={len(self.chain)}, "
            f"validators={len(self.known_validators)})"
        )

    def __repr__(self) -> str:
        """
        Return a detailed string representation of the shard.

        Returns:
            str: Detailed string representation of the shard
        """
        return (
            f"Shard(shard_id={self.shard_id}, "
            f"height={self.height}, "
            f"chain_length={len(self.chain)}, "
            f"pending_transactions={len(self.pending_transactions)}, "
            f"last_block_time='{self.last_block_time.isoformat()}', "
            f"metrics={self.metrics})"
        )