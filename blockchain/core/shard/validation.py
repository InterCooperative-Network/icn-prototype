# blockchain/core/shard/validation.py

from typing import Dict, Optional, List
import logging
from datetime import datetime, timedelta
from .types import ShardMetrics, ShardConfig
from ..transaction import Transaction
from ..block import Block

logger = logging.getLogger(__name__)

class ValidationManager:
    """Handles transaction and chain validation logic."""
    
    def __init__(self, shard_id: int, config: ShardConfig):
        self.shard_id = shard_id
        self.config = config
        self.validation_cache: Dict[str, bool] = {}
        self.metrics = ShardMetrics()
        self.last_validation_time: Dict[str, datetime] = {}

    def validate_transaction(self, transaction: Transaction) -> bool:
        """
        Validate a transaction before adding to pool.
        
        Args:
            transaction: Transaction to validate
            
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

            # Validate shard assignment
            if transaction.shard_id != self.shard_id:
                logger.error(f"Transaction {tx_id} has incorrect shard_id")
                self.validation_cache[tx_id] = False
                return False

            # Validate timestamp
            if transaction.timestamp > datetime.now() + timedelta(minutes=5):
                logger.error(f"Transaction {tx_id} has future timestamp")
                self.validation_cache[tx_id] = False
                return False

            # Cache and return result
            self.validation_cache[tx_id] = True
            self.last_validation_time[tx_id] = datetime.now()
            return True

        except Exception as e:
            logger.error(f"Transaction validation failed: {str(e)}")
            return False

    def validate_block(self, block: Block, previous_block: Optional[Block] = None) -> bool:
        """
        Validate a block before adding to chain.
        
        Args:
            block: Block to validate
            previous_block: Previous block in chain
            
        Returns:
            bool: True if block is valid
        """
        try:
            # Validate block structure
            if not block.validate(previous_block):
                logger.error("Block failed structural validation")
                return False

            # Validate shard assignment
            if block.shard_id != self.shard_id:
                logger.error(f"Block has incorrect shard_id: {block.shard_id}")
                return False

            # Validate transactions
            if not self._validate_block_transactions(block):
                return False

            # Validate block size
            block_size = len(str(block.to_dict()))
            if block_size > self.config.max_block_size:
                logger.error(f"Block size {block_size} exceeds limit")
                return False

            return True

        except Exception as e:
            logger.error(f"Block validation failed: {str(e)}")
            return False

    def _validate_block_transactions(self, block: Block) -> bool:
        """
        Validate all transactions in a block.
        
        Args:
            block: Block containing transactions
            
        Returns:
            bool: True if all transactions are valid
        """
        try:
            # Validate transaction count
            if len(block.transactions) > self.config.max_transactions_per_block:
                logger.error("Block exceeds maximum transaction limit")
                return False

            # Validate individual transactions
            for tx in block.transactions:
                if not self.validate_transaction(tx):
                    logger.error(f"Invalid transaction in block: {tx.transaction_id}")
                    return False

            # Validate transaction ordering (by priority)
            for i in range(len(block.transactions) - 1):
                if block.transactions[i].priority < block.transactions[i + 1].priority:
                    logger.error("Block transactions not properly ordered by priority")
                    return False

            return True

        except Exception as e:
            logger.error(f"Block transaction validation failed: {str(e)}")
            return False

    def validate_chain_sequence(self, blocks: List[Block]) -> bool:
        """
        Validate a sequence of blocks.
        
        Args:
            blocks: List of blocks to validate
            
        Returns:
            bool: True if sequence is valid
        """
        try:
            for i in range(1, len(blocks)):
                current_block = blocks[i]
                previous_block = blocks[i - 1]

                # Validate block
                if not self.validate_block(current_block, previous_block):
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
            logger.error(f"Chain sequence validation failed: {str(e)}")
            return False

    def clear_old_validations(self, max_age_minutes: int = 60) -> None:
        """
        Clear old validation cache entries.
        
        Args:
            max_age_minutes: Maximum age of cache entries
        """
        try:
            current_time = datetime.now()
            cutoff_time = current_time - timedelta(minutes=max_age_minutes)

            # Clear old entries
            self.validation_cache = {
                tx_id: result
                for tx_id, result in self.validation_cache.items()
                if self.last_validation_time.get(tx_id, current_time) > cutoff_time
            }

            # Clear old timestamps
            self.last_validation_time = {
                tx_id: timestamp
                for tx_id, timestamp in self.last_validation_time.items()
                if timestamp > cutoff_time
            }

        except Exception as e:
            logger.error(f"Failed to clear old validations: {str(e)}")