"""
blockchain/core/shard/transaction_manager.py

Manages transaction processing and selection within a shard. This module handles
transaction queuing, prioritization, and selection for block creation while 
enforcing resource limits and maintaining mempool health.

Key responsibilities:
- Managing the transaction mempool
- Prioritizing transactions for block inclusion
- Enforcing transaction limits and resource constraints
- Handling transaction cleanup and pruning
- Maintaining transaction metrics
"""

from typing import List, Optional, Dict, Set, Any
import logging
from datetime import datetime, timedelta
from heapq import heappush, heappop
from .shard_types import ShardMetrics, ShardConfig
from ..transaction import Transaction

logger = logging.getLogger(__name__)

class TransactionManager:
    """
    Manages transaction processing within a shard.
    
    Implements efficient transaction queuing and selection mechanisms while
    enforcing resource limits and maintaining mempool health. Uses a priority
    queue system for transaction selection and includes periodic cleanup of
    old transactions.
    """

    def __init__(self, shard_id: int, config: ShardConfig):
        """
        Initialize the transaction manager.

        Args:
            shard_id: ID of the shard this manager belongs to
            config: Configuration parameters for the shard
        """
        self.shard_id = shard_id
        self.config = config
        
        # Transaction storage
        self.pending_transactions: List[Transaction] = []
        self.priority_queue: List[tuple] = []  # (priority, timestamp, transaction)
        self.processed_transactions: Set[str] = set()  # Track processed tx IDs
        
        # Transaction indexing
        self.tx_index: Dict[str, Transaction] = {}  # Quick lookup by ID
        self.sender_txs: Dict[str, Set[str]] = {}  # Track txs by sender
        
        # Metrics tracking
        self.metrics = ShardMetrics()
        self.last_prune_time = datetime.now()
        self.last_metrics_update = datetime.now()
        
        # Resource tracking
        self.current_mempool_size = 0  # Size in bytes
        self.sender_nonce_tracking: Dict[str, int] = {}  # Track sender nonces

    def add_transaction(self, transaction: Transaction) -> bool:
        """
        Add a new transaction to the pending pool.

        Args:
            transaction: Transaction to add

        Returns:
            bool: True if transaction added successfully
        """
        try:
            # Verify shard assignment
            if transaction.shard_id != self.shard_id:
                logger.error(f"Transaction shard_id {transaction.shard_id} doesn't match shard {self.shard_id}")
                return False

            # Check pool capacity
            if len(self.pending_transactions) >= self.config.max_pending_transactions:
                self._prune_old_transactions()
                if len(self.pending_transactions) >= self.config.max_pending_transactions:
                    logger.warning(f"Shard {self.shard_id} transaction pool full")
                    return False

            # Check for duplicate
            tx_id = transaction.transaction_id
            if tx_id in self.processed_transactions or tx_id in self.tx_index:
                logger.warning(f"Transaction {tx_id} already processed or pending")
                return False

            # Verify transaction nonce
            if not self._verify_transaction_nonce(transaction):
                logger.error(f"Invalid nonce for transaction {tx_id}")
                return False

            # Update memory pool size
            tx_size = len(str(transaction.to_dict()))
            if self.current_mempool_size + tx_size > self.config.max_state_size:
                logger.warning("Memory pool size limit reached")
                return False

            # Add to storage structures
            self.pending_transactions.append(transaction)
            self.tx_index[tx_id] = transaction
            
            # Update sender tracking
            sender = transaction.sender
            if sender not in self.sender_txs:
                self.sender_txs[sender] = set()
            self.sender_txs[sender].add(tx_id)

            # Add to priority queue
            heappush(
                self.priority_queue,
                (-transaction.priority, datetime.now(), transaction)
            )

            # Update metrics
            self.current_mempool_size += tx_size
            self.metrics.pending_count = len(self.pending_transactions)
            
            logger.info(f"Added transaction {tx_id} to shard {self.shard_id}")
            return True

        except Exception as e:
            logger.error(f"Failed to add transaction: {str(e)}")
            return False

    def select_transactions_for_block(self) -> List[Transaction]:
        """
        Select transactions for inclusion in a new block.

        Selects transactions based on priority, age, and resource constraints
        while maintaining fairness and system health.

        Returns:
            List[Transaction]: Selected transactions for block creation
        """
        try:
            selected_transactions = []
            selected_size = 0
            used_resources = {}  # Track resource usage per sender

            while (
                self.priority_queue and 
                len(selected_transactions) < self.config.max_transactions_per_block
            ):
                # Get highest priority transaction
                _, _, transaction = heappop(self.priority_queue)
                tx_id = transaction.transaction_id

                # Skip if transaction was already selected or processed
                if tx_id in self.processed_transactions:
                    continue

                # Verify resource limits
                sender = transaction.sender
                if not self._verify_sender_resources(sender, transaction, used_resources):
                    continue

                # Check transaction size
                tx_size = len(str(transaction.to_dict()))
                if selected_size + tx_size > self.config.max_block_size:
                    break

                # Add transaction to selection
                selected_transactions.append(transaction)
                selected_size += tx_size
                
                # Update resource tracking
                if sender not in used_resources:
                    used_resources[sender] = {"count": 0, "size": 0}
                used_resources[sender]["count"] += 1
                used_resources[sender]["size"] += tx_size

            return selected_transactions

        except Exception as e:
            logger.error(f"Error selecting transactions: {str(e)}")
            return []

    def remove_transactions(self, transaction_ids: Set[str]) -> None:
        """
        Remove transactions after they've been included in a block.

        Args:
            transaction_ids: Set of transaction IDs to remove
        """
        try:
            # Update sender nonces for processed transactions
            for tx_id in transaction_ids:
                if tx_id in self.tx_index:
                    tx = self.tx_index[tx_id]
                    self.sender_nonce_tracking[tx.sender] = max(
                        self.sender_nonce_tracking.get(tx.sender, 0),
                        tx.data.get("nonce", 0)
                    )

            # Remove from pending pool
            self.pending_transactions = [
                tx for tx in self.pending_transactions 
                if tx.transaction_id not in transaction_ids
            ]

            # Update indices and tracking
            for tx_id in transaction_ids:
                if tx_id in self.tx_index:
                    tx = self.tx_index[tx_id]
                    sender = tx.sender
                    
                    # Update sender tracking
                    if sender in self.sender_txs:
                        self.sender_txs[sender].discard(tx_id)
                        if not self.sender_txs[sender]:
                            del self.sender_txs[sender]
                    
                    # Update mempool size
                    self.current_mempool_size -= len(str(tx.to_dict()))
                    
                    # Clean up index
                    del self.tx_index[tx_id]

                # Mark as processed
                self.processed_transactions.add(tx_id)

            # Update metrics
            self.metrics.pending_count = len(self.pending_transactions)
            
        except Exception as e:
            logger.error(f"Error removing transactions: {str(e)}")

    def _verify_transaction_nonce(self, transaction: Transaction) -> bool:
        """
        Verify transaction nonce to prevent double spending and ensure order.

        Args:
            transaction: Transaction to verify

        Returns:
            bool: True if nonce is valid
        """
        sender = transaction.sender
        nonce = transaction.data.get("nonce", 0)
        
        # Get current highest nonce for sender
        current_nonce = self.sender_nonce_tracking.get(sender, 0)
        
        # Nonce must be higher than current tracked nonce
        return nonce > current_nonce

    def _verify_sender_resources(
        self,
        sender: str,
        transaction: Transaction,
        used_resources: Dict[str, Dict[str, int]]
    ) -> bool:
        """
        Verify sender has sufficient resources for transaction.

        Args:
            sender: Transaction sender
            transaction: Transaction to verify
            used_resources: Currently used resources in block

        Returns:
            bool: True if sender has sufficient resources
        """
        # Get current resource usage
        sender_resources = used_resources.get(sender, {"count": 0, "size": 0})
        
        # Check transaction count per sender
        max_tx_per_sender = 10  # Configure based on requirements
        if sender_resources["count"] >= max_tx_per_sender:
            return False

        # Check total size per sender
        max_size_per_sender = self.config.max_block_size // 4  # 25% of block size
        tx_size = len(str(transaction.to_dict()))
        if sender_resources["size"] + tx_size > max_size_per_sender:
            return False

        return True

    def _prune_old_transactions(self) -> None:
        """Remove old transactions from the pool to prevent memory bloat."""
        try:
            current_time = datetime.now()
            
            # Only prune periodically
            if (current_time - self.last_prune_time) < timedelta(minutes=5):
                return

            # Remove transactions older than 3 hours
            max_age = timedelta(hours=3)
            self.pending_transactions = [
                tx for tx in self.pending_transactions
                if (current_time - tx.timestamp) <= max_age
            ]

            # Rebuild indices
            self._rebuild_indices()
            
            # Update metrics
            self.metrics.pending_count = len(self.pending_transactions)
            self.last_prune_time = current_time

        except Exception as e:
            logger.error(f"Error pruning transactions: {str(e)}")

    def _rebuild_indices(self) -> None:
        """Rebuild transaction indices after pruning."""
        try:
            # Clear existing indices
            self.tx_index.clear()
            self.sender_txs.clear()
            self.priority_queue.clear()
            
            # Rebuild from pending transactions
            self.current_mempool_size = 0
            
            for tx in self.pending_transactions:
                tx_id = tx.transaction_id
                sender = tx.sender
                
                # Update tx index
                self.tx_index[tx_id] = tx
                
                # Update sender tracking
                if sender not in self.sender_txs:
                    self.sender_txs[sender] = set()
                self.sender_txs[sender].add(tx_id)
                
                # Update priority queue
                heappush(
                    self.priority_queue,
                    (-tx.priority, tx.timestamp, tx)
                )
                
                # Update mempool size
                self.current_mempool_size += len(str(tx.to_dict()))

        except Exception as e:
            logger.error(f"Error rebuilding indices: {str(e)}")

    def get_metrics(self) -> Dict[str, Any]:
        """
        Get transaction pool metrics and statistics.

        Returns:
            Dict[str, Any]: Dictionary of metrics
        """
        try:
            return {
                "pending_count": len(self.pending_transactions),
                "processed_count": len(self.processed_transactions),
                "mempool_size": self.current_mempool_size,
                "unique_senders": len(self.sender_txs),
                "average_tx_size": (
                    self.current_mempool_size / len(self.pending_transactions)
                    if self.pending_transactions else 0
                ),
                "priority_queue_size": len(self.priority_queue)
            }

        except Exception as e:
            logger.error(f"Error getting metrics: {str(e)}")
            return {}

    def to_dict(self) -> Dict[str, Any]:
        """Convert manager state to dictionary format."""
        try:
            return {
                "pending_transactions": [tx.to_dict() for tx in self.pending_transactions],
                "processed_transactions": list(self.processed_transactions),
                "sender_nonce_tracking": self.sender_nonce_tracking.copy(),
                "metrics": self.metrics.to_dict(),
            }

        except Exception as e:
            logger.error(f"Error converting to dictionary: {str(e)}")
            return {}

    @classmethod
    def from_dict(cls, data: Dict[str, Any], shard_id: int, config: ShardConfig) -> 'TransactionManager':
        """
        Create manager from dictionary data.

        Args:
            data: Dictionary containing manager data
            shard_id: Shard ID for the manager
            config: Shard configuration

        Returns:
            TransactionManager: Reconstructed manager instance
        """
        try:
            manager = cls(shard_id, config)
            
            # Restore transactions
            manager.pending_transactions = [
                Transaction.from_dict(tx) for tx in data["pending_transactions"]
            ]
            manager.processed_transactions = set(data["processed_transactions"])
            manager.sender_nonce_tracking = data["sender_nonce_tracking"]
            manager.metrics = ShardMetrics.from_dict(data["metrics"])
            
            # Rebuild indices
            manager._rebuild_indices()
            
            return manager

        except Exception as e:
            logger.error(f"Error creating from dictionary: {str(e)}")
            raise