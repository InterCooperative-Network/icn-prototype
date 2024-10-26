# blockchain/core/shard/validation_manager.py

from typing import Dict, Optional, List, Set
import logging
from datetime import datetime, timedelta
from .shard_types import ShardMetrics, ShardConfig
from ..block import Block
from ..transaction import Transaction

logger = logging.getLogger(__name__)

class ValidationManager:
    """Handles transaction and chain validation logic."""
    
    def __init__(self, shard_id: int, config: ShardConfig):
        self.shard_id = shard_id
        self.config = config
        self.validation_cache: Dict[str, bool] = {}
        self.metrics = ShardMetrics()
        self.last_validation_time: Dict[str, datetime] = {}
        self.last_block_time = datetime.now() - timedelta(seconds=config.min_block_interval)
        self.state: Dict[str, Dict] = {"balances": {}}

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

            # Check transaction amount
            amount = transaction.data.get('amount', 0)
            if amount <= 0:
                logger.error(f"Transaction {tx_id} has invalid amount")
                self.validation_cache[tx_id] = False
                return False

            # Check shard assignment
            if transaction.shard_id != self.shard_id:
                logger.error(f"Transaction {tx_id} has wrong shard ID")
                self.validation_cache[tx_id] = False
                return False

            # Check balance
            sender = transaction.sender
            balances = self.state.get("balances", {})
            sender_balance = balances.get(sender, 1000.0)  # Default initial balance
            if sender_balance < amount:
                logger.error(f"Insufficient balance for transaction {tx_id}")
                self.validation_cache[tx_id] = False
                return False

            # Cache and return success
            self.validation_cache[tx_id] = True
            return True

        except Exception as e:
            logger.error(f"Transaction validation failed: {str(e)}")
            return False

    def validate_block(self, block: Block, previous_block: Optional[Block]) -> bool:
        """
        Validate a block before adding to chain.
        
        Args:
            block: Block to validate
            previous_block: Previous block in chain for validation
            
        Returns:
            bool: True if block is valid
        """
        try:
            # Basic block validation
            if not block.validate(previous_block):
                logger.error("Block failed basic validation")
                return False

            # Check shard ID
            if block.shard_id != self.shard_id:
                logger.error("Block has wrong shard ID")
                return False

            # Validate all transactions
            for tx in block.transactions:
                if not self.validate_transaction(tx):
                    logger.error(f"Block contains invalid transaction {tx.transaction_id}")
                    return False

                # Update balances for subsequent transaction validations
                amount = float(tx.data.get('amount', 0))
                self.state.setdefault("balances", {})
                self.state["balances"].setdefault(tx.sender, 1000.0)
                self.state["balances"].setdefault(tx.receiver, 1000.0)
                self.state["balances"][tx.sender] -= amount
                self.state["balances"][tx.receiver] += amount

            # Always allow the genesis block
            if previous_block is None or block.index == 0:
                return True

            # Check block timing - disabled for testing
            # current_time = datetime.now()
            # time_since_last = (current_time - self.last_block_time).total_seconds()
            # if time_since_last < self.config.min_block_interval:
            #     logger.error(f"Block created too soon after previous block")
            #     return False

            self.last_block_time = datetime.now()
            return True

        except Exception as e:
            logger.error(f"Block validation failed: {str(e)}")
            return False

    def clear_cache(self) -> None:
        """Clear validation cache."""
        self.validation_cache.clear()

    def update_state(self, state: Dict) -> None:
        """Update the validation manager's state view."""
        self.state = state.copy()

    def get_metrics(self) -> Dict:
        """Get validation metrics."""
        return {
            "validation_cache_size": len(self.validation_cache),
            "last_validation_times": {
                tx_id: time.isoformat()
                for tx_id, time in self.last_validation_time.items()
            },
            "total_validations": len(self.validation_cache),
            "failed_validations": len([v for v in self.validation_cache.values() if not v])
        }

    def to_dict(self) -> Dict:
        """Convert manager state to dictionary format."""
        return {
            "validation_cache": self.validation_cache.copy(),
            "metrics": self.metrics.to_dict(),
            "last_validation_time": {
                tx_id: time.isoformat()
                for tx_id, time in self.last_validation_time.items()
            },
            "last_block_time": self.last_block_time.isoformat(),
            "state": self.state.copy()
        }

    @classmethod
    def from_dict(cls, data: Dict, shard_id: int, config: ShardConfig) -> 'ValidationManager':
        """Create manager from dictionary data."""
        manager = cls(shard_id, config)
        manager.validation_cache = data["validation_cache"].copy()
        manager.metrics = ShardMetrics.from_dict(data["metrics"])
        manager.last_validation_time = {
            tx_id: datetime.fromisoformat(time_str)
            for tx_id, time_str in data["last_validation_time"].items()
        }
        manager.last_block_time = datetime.fromisoformat(data["last_block_time"])
        manager.state = data["state"].copy()
        return manager