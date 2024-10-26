# blockchain/core/shard/transaction_manager.py

from typing import List, Optional, Dict, Set
import logging
from datetime import datetime, timedelta
from .shard_types import ShardMetrics, ShardConfig
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
        """Add a new transaction to the pending pool."""
        try:
            # Check shard assignment
            if transaction.shard_id != self.shard_id:
                logger.error(f"Transaction shard_id {transaction.shard_id} doesn't match shard {self.shard_id}")
                return False

            # Check pool capacity
            if len(self.pending_transactions) >= self.config.max_pending_transactions:
                logger.warning(f"Shard {self.shard_id} transaction pool full")
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
            
            # Sort by priority
            self._sort_pending_transactions()

            return True

        except Exception as e:
            logger.error(f"Failed to add transaction: {str(e)}")
            return False

    def select_transactions_for_block(self) -> List[Transaction]:
        """Select and sort transactions for a new block."""
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
        """Remove transactions from the pending pool."""
        try:
            self.pending_transactions = [
                tx for tx in self.pending_transactions 
                if tx.transaction_id not in transaction_ids
            ]
            self.metrics.pending_count = len(self.pending_transactions)

            # Add to processed set
            self.processed_transactions.update(transaction_ids)

        except Exception as e:
            logger.error(f"Failed to remove transactions: {str(e)}")

    def get_metrics(self) -> Dict:
        """Get transaction pool metrics."""
        return {
            "pending_count": len(self.pending_transactions),
            "processed_count": len(self.processed_transactions),
            "total_transactions": self.metrics.total_transactions
        }

    def _sort_pending_transactions(self) -> None:
        """Sort pending transactions by priority and timestamp."""
        try:
            self.pending_transactions.sort(key=lambda tx: (-tx.priority, tx.timestamp))
        except Exception as e:
            logger.error(f"Failed to sort transactions: {str(e)}")

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