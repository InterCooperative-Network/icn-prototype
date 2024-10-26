from typing import Dict, Optional, List, Set, Any
import logging
from datetime import datetime
from copy import deepcopy
from .shard_types import ShardConfig, ShardMetrics
from ..transaction import Transaction
from ..block import Block

logger = logging.getLogger(__name__)

class StateManager:
    """
    Manages state and state transitions within a shard.
    
    This class handles:
    - State updates from transactions and blocks
    - State validation and consistency checks
    - State snapshots and rollbacks
    - State metrics tracking
    """

    def __init__(self, shard_id: int, config: ShardConfig):
        """
        Initialize the state manager.
        
        Args:
            shard_id: ID of the shard this manager belongs to
            config: Configuration parameters
        """
        self.shard_id = shard_id
        self.config = config
        self.state: Dict[str, Any] = {
            "balances": {},
            "metadata": {
                "shard_id": shard_id,
                "created_at": datetime.now().isoformat(),
                "last_updated": datetime.now().isoformat(),
                "total_transactions": 0,
                "total_volume": 0.0,
                "version": "1.0"
            }
        }
        self._state_snapshots: List[Dict] = []
        self.metrics = ShardMetrics()
        self.processed_transactions: Set[str] = set()
        self._backup_state: Optional[Dict] = None

    def update_state(self, block: Block) -> bool:
        """
        Update state based on a new block.
        
        Args:
            block: Block containing transactions to process
            
        Returns:
            bool: True if state update successful
        """
        try:
            # Create backup
            self._backup_state = deepcopy(self.state)

            # Process all transactions
            for tx in block.transactions:
                if not self._process_transaction(tx):
                    self._rollback_state()
                    return False

            # Update metadata
            self.state["metadata"].update({
                "last_updated": datetime.now().isoformat(),
                "total_transactions": self.state["metadata"]["total_transactions"] + len(block.transactions),
                "last_block": block.index
            })

            # Update metrics
            self.metrics.total_transactions += len(block.transactions)
            
            # Take snapshot if needed
            if self._should_snapshot():
                self._take_snapshot()

            return True

        except Exception as e:
            logger.error(f"Failed to update state: {str(e)}")
            self._rollback_state()
            return False

    def _process_transaction(self, transaction: Transaction) -> bool:
        """
        Process a single transaction's state changes.
        
        Args:
            transaction: Transaction to process
            
        Returns:
            bool: True if transaction processed successfully
        """
        try:
            sender = transaction.sender
            receiver = transaction.receiver
            amount = float(transaction.data.get("amount", 0))
            
            # Initialize balances if needed
            if sender not in self.state["balances"]:
                self.state["balances"][sender] = 1000.0  # Initial balance
            if receiver not in self.state["balances"]:
                self.state["balances"][receiver] = 1000.0  # Initial balance

            # Check balance
            if self.state["balances"][sender] < amount:
                logger.error(f"Insufficient funds for transaction {transaction.transaction_id}")
                return False

            # Update balances
            self.state["balances"][sender] -= amount
            self.state["balances"][receiver] += amount

            # Track total volume
            self.state["metadata"]["total_volume"] += amount

            # Mark as processed
            self.processed_transactions.add(transaction.transaction_id)

            return True

        except Exception as e:
            logger.error(f"Failed to process transaction {transaction.transaction_id}: {str(e)}")
            return False

    def _should_snapshot(self) -> bool:
        """Determine if state snapshot should be taken."""
        state_size = len(str(self.state))
        return (
            len(self._state_snapshots) == 0 or 
            state_size > self.config.max_state_size * 0.9
        )

    def _take_snapshot(self) -> None:
        """Take a snapshot of current state."""
        try:
            snapshot = {
                'timestamp': datetime.now().isoformat(),
                'state': deepcopy(self.state),
                'metrics': self.metrics.to_dict()
            }
            self._state_snapshots.append(snapshot)
            
            # Keep only last 10 snapshots
            if len(self._state_snapshots) > 10:
                self._state_snapshots = self._state_snapshots[-10:]

        except Exception as e:
            logger.error(f"Failed to take state snapshot: {str(e)}")

    def _rollback_state(self) -> None:
        """Rollback to last backup state."""
        if self._backup_state is not None:
            self.state = deepcopy(self._backup_state)
            self._backup_state = None

    def get_balance(self, address: str) -> float:
        """
        Get balance for an address.
        
        Args:
            address: Address to get balance for
            
        Returns:
            float: Current balance
        """
        return self.state["balances"].get(address, 0.0)

    def get_metrics(self) -> Dict[str, Any]:
        """
        Get state metrics.
        
        Returns:
            Dict[str, Any]: Dictionary of metrics
        """
        return {
            "total_transactions": self.state["metadata"]["total_transactions"],
            "total_volume": self.state["metadata"]["total_volume"],
            "state_size": len(str(self.state)),
            "processed_transactions": len(self.processed_transactions)
        }

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert state manager to dictionary format.
        
        Returns:
            Dict[str, Any]: Dictionary representation
        """
        return {
            "state": deepcopy(self.state),
            "metrics": self.metrics.to_dict(),
            "processed_transactions": list(self.processed_transactions),
            "snapshots": self._state_snapshots.copy()
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any], shard_id: int, config: ShardConfig) -> 'StateManager':
        """
        Create state manager from dictionary data.
        
        Args:
            data: Dictionary containing state data
            shard_id: ID of the shard
            config: Configuration parameters
            
        Returns:
            StateManager: New state manager instance
        """
        manager = cls(shard_id, config)
        manager.state = deepcopy(data.get("state", {}))
        manager._state_snapshots = data.get("snapshots", []).copy()
        manager.processed_transactions = set(data.get("processed_transactions", []))
        manager.metrics = ShardMetrics.from_dict(data.get("metrics", {}))
        return manager

    def __str__(self) -> str:
        """String representation."""
        return f"StateManager(shard={self.shard_id}, tx_count={self.state['metadata']['total_transactions']})"