from typing import Dict, Optional, List, Set
import logging
from datetime import datetime
from copy import deepcopy
from .shard_types import ShardMetrics, ShardConfig
from ..block import Block
from ..transaction import Transaction

logger = logging.getLogger(__name__)

class StateManager:
    """Manages shard state and state transitions."""
    
    def __init__(self, shard_id: int, config: ShardConfig):
        """Initialize the state manager."""
        self.shard_id = shard_id
        self.config = config
        self.state: Dict = {
            "balances": {},
            "metadata": {
                "shard_id": shard_id,
                "created_at": datetime.now().isoformat(),
                "last_updated": datetime.now().isoformat(),
                "total_transactions": 0,
                "total_volume": 0.0
            }
        }
        self.metrics = ShardMetrics()
        self.state_history: List[Dict] = []
        self.last_state_update = datetime.now()
        self._backup_state: Optional[Dict] = None
        self.processed_transactions: Set[str] = set()
        self._initialize_default_balances()

    def _initialize_default_balances(self) -> None:
        """Initialize default balances for testing."""
        if not self.state["balances"]:
            self.state["balances"] = {f"user{i}": 1000.0 for i in range(10)}

    def update_state(self, block: Block) -> bool:
        """Update state based on a new block."""
        try:
            self._backup_state = deepcopy(self.state)

            for tx in block.transactions:
                if not self._process_transaction(tx):
                    logger.error(f"Failed to process transaction {tx.transaction_id}")
                    self.rollback_state()
                    return False

            self.state["metadata"].update({
                "last_updated": datetime.now().isoformat(),
                "last_block_index": block.index,
                "total_transactions": self.state["metadata"].get("total_transactions", 0) + len(block.transactions)
            })

            if self._should_save_snapshot():
                self._save_state_snapshot()

            return True
        except Exception as e:
            logger.error(f"Failed to update state: {str(e)}")
            self.rollback_state()
            return False

    def _process_transaction(self, transaction: Transaction) -> bool:
        """Process a single transaction's state changes."""
        sender = transaction.sender
        receiver = transaction.receiver
        amount = transaction.data.get("amount", 0)

        if self.state["balances"].get(sender, 0) < amount:
            logger.error(f"Insufficient funds for transaction {transaction.transaction_id}")
            return False

        self.state["balances"][sender] -= amount
        self.state["balances"][receiver] = self.state["balances"].get(receiver, 0) + amount
        return True

    def rollback_state(self) -> bool:
        """Rollback to last backup state."""
        if self._backup_state is not None:
            self.state = deepcopy(self._backup_state)
            self._backup_state = None
            return True
        return False

    def _should_save_snapshot(self) -> bool:
        """Determine if a snapshot should be saved."""
        return len(self.state_history) == 0 or len(str(self.state)) > self.config.max_state_size * 0.9

    def _save_state_snapshot(self) -> None:
        """Save a snapshot of the current state."""
        try:
            snapshot = {
                'timestamp': datetime.now().isoformat(),
                'state': deepcopy(self.state),
                'metrics': self.metrics.to_dict(),
            }
            self.state_history.append(snapshot)
            
            if len(self.state_history) > 10:
                self.state_history = self.state_history[-10:]

        except Exception as e:
            logger.error(f"Failed to save state snapshot: {str(e)}")

    def get_metrics(self) -> Dict:
        """Get current metrics."""
        return {
            "total_transactions": self.state["metadata"].get("total_transactions", 0),
            "total_volume": self.state["metadata"].get("total_volume", 0.0),
            "state_size": len(str(self.state)),
            "processed_transactions": len(self.processed_transactions),
            "pending_transactions": 0
        }

    def to_dict(self) -> Dict:
        """Convert manager state to dictionary."""
        return {
            'state': deepcopy(self.state),
            'metrics': self.metrics.to_dict(),
            'last_update': self.last_state_update.isoformat(),
            'processed_transactions': list(self.processed_transactions)
        }
