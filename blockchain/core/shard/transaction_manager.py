# blockchain/core/shard/transaction_manager.py

from typing import List, Optional, Dict, Set
import logging
from datetime import datetime, timedelta
from .types import ShardMetrics, ShardConfig
from ..transaction import Transaction

logger = logging.getLogger(__name__)

class TransactionManager:
    """Handles transaction processing and management within a shard."""
    
    def __init__(self, shard_id: int, config: ShardConfig):
        self.shard_id = shard_id
        self.config = config
        self.pending_transactions: List[Transaction] = []
        self.processed_transactions: Set[str] = set()  # Track processed tx IDs
        self.metrics = ShardMetrics()
        self.last_prune_time = datetime.now()

    def add_transaction(self, transaction: Transaction) -> bool:
        """
        Add a new transaction to the pending pool.
        
        Args:
            transaction: Transaction to add
            
        Returns:
            bool: True if transaction was added successfully
        """
        try:
            # Check shard assignment
            if transaction.shard_id != self.shard_id:
                logger.error(
                    f"Transaction shard_id {transaction.shard_id} "
                    f"doesn't match shard {self.shard_id}"
                )
                self.metrics.rejected_transactions += 1
                return False

            # Check pool capacity
            if len(self.pending_transactions) >= self.config.max_pending_transactions:
                logger.warning(f"Shard {self.shard_id} transaction pool full")
                self.metrics.rejected_transactions += 1
                return False

            # Check for duplicate
            tx_id = transaction.transaction_id
            if tx_id in self.processed_transactions:
                logger.warning(f"Transaction {tx_id} already processed")
                return False

            if any(tx.transaction_id == tx_id for tx in self.pending_transactions):
                logger.warning(f"Transaction {tx_id} already in pending pool")
                return False

            # Add transaction
            self.pending_transactions.append(transaction)
            self.metrics.pending_count = len(self.pending_transactions)
            
            # Sort by priority after adding
            self._sort_pending_transactions()

            return True

        except Exception as e:
            logger.error(f"Failed to add transaction: {str(e)}")
            self.metrics.rejected_transactions += 1
            return False

    def select_transactions_for_block(self) -> List[Transaction]:
        """
        Select and sort transactions for a new block.
        
        Returns:
            List[Transaction]: Selected transactions for the block
        """
        try:
            # Get initial selection
            candidates = self.pending_transactions[:self.config.max_transactions_per_block]
            
            # Sort by priority and timestamp
            candidates.sort(key=lambda tx: (-tx.priority, tx.timestamp))

            # Track selected transactions
            for tx in candidates:
                self.processed_transactions.add(tx.transaction_id)

            return candidates

        except Exception as e:
            logger.error(f"Failed to select transactions: {str(e)}")
            return []

    def remove_transactions(self, transaction_ids: Set[str]) -> None:
        """
        Remove transactions from the pending pool.
        
        Args:
            transaction_ids: Set of transaction IDs to remove
        """
        try:
            self.pending_transactions = [
                tx for tx in self.pending_transactions 
                if tx.transaction_id not in transaction_ids
            ]
            self.metrics.pending_count = len(self.pending_transactions)

        except Exception as e:
            logger.error(f"Failed to remove transactions: {str(e)}")

    def prune_old_transactions(self, max_age_minutes: int = 60) -> None:
        """
        Remove old transactions from the pending pool.
        
        Args:
            max_age_minutes: Maximum age of transactions to keep
        """
        try:
            current_time = datetime.now()
            
            # Only prune if enough time has passed
            if (current_time - self.last_prune_time).total_seconds() < 60:
                return

            cutoff_time = current_time - timedelta(minutes=max_age_minutes)
            original_count = len(self.pending_transactions)
            
            self.pending_transactions = [
                tx for tx in self.pending_transactions
                if tx.timestamp > cutoff_time
            ]
            
            # Update metrics
            self.metrics.pending_count = len(self.pending_transactions)
            pruned_count = original_count - len(self.pending_transactions)
            
            if pruned_count > 0:
                logger.info(f"Pruned {pruned_count} old transactions")
                
            self.last_prune_time = current_time

        except Exception as e:
            logger.error(f"Failed to prune transactions: {str(e)}")

    def _sort_pending_transactions(self) -> None:
        """Sort pending transactions by priority and timestamp."""
        try:
            self.pending_transactions.sort(
                key=lambda tx: (-tx.priority, tx.timestamp)
            )
        except Exception as e:
            logger.error(f"Failed to sort transactions: {str(e)}")

    def get_transaction_by_id(self, transaction_id: str) -> Optional[Transaction]:
        """
        Find a transaction by its ID in the pending pool.
        
        Args:
            transaction_id: ID of the transaction to find
            
        Returns:
            Optional[Transaction]: The transaction if found
        """
        try:
            return next(
                (tx for tx in self.pending_transactions 
                 if tx.transaction_id == transaction_id),
                None
            )
        except Exception as e:
            logger.error(f"Failed to retrieve transaction: {str(e)}")
            return None

    def get_pending_transaction_count(self) -> int:
        """Get count of pending transactions."""
        return len(self.pending_transactions)

    def clear_all(self) -> None:
        """Clear all pending transactions."""
        self.pending_transactions = []
        self.metrics.pending_count = 0
        logger.info(f"Cleared all pending transactions from shard {self.shard_id}")

    def to_dict(self) -> Dict:
        """Convert manager state to dictionary format."""
        return {
            "pending_transactions": [tx.to_dict() for tx in self.pending_transactions],
            "processed_transactions": list(self.processed_transactions),
            "metrics": self.metrics.to_dict(),
        }

    @classmethod
    def from_dict(cls, data: Dict, shard_id: int, config: ShardConfig) -> 'TransactionManager':
        """Create manager from dictionary data."""
        manager = cls(shard_id, config)
        manager.pending_transactions = [
            Transaction.from_dict(tx) for tx in data["pending_transactions"]
        ]
        manager.processed_transactions = set(data["processed_transactions"])
        manager.metrics = ShardMetrics.from_dict(data["metrics"])
        return manager