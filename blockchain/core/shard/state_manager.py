# blockchain/core/shard/state_manager.py

from typing import Dict, Any, Optional
import logging
from datetime import datetime
from copy import deepcopy
from .types import ShardMetrics, ShardConfig
from ..block import Block

logger = logging.getLogger(__name__)

class StateManager:
    """Manages shard state, metrics, and state transitions."""
    
    def __init__(self, shard_id: int, config: ShardConfig):
        self.shard_id = shard_id
        self.config = config
        self.state: Dict[str, Any] = {}
        self.metrics = ShardMetrics()
        self.state_history: List[Dict] = []
        self.last_state_update = datetime.now()
        self._backup_state: Optional[Dict] = None

    def update_state(self, block: Block) -> bool:
        """
        Update state based on a new block.
        
        Args:
            block: New block to process
            
        Returns:
            bool: True if state was updated successfully
        """
        try:
            # Create state backup
            self._backup_state = deepcopy(self.state)
            
            # Process each transaction
            for tx in block.transactions:
                self._process_transaction_state(tx)
            
            # Update metrics
            self._update_metrics_for_block(block)
            
            # Save state snapshot if needed
            if self._should_save_snapshot():
                self._save_state_snapshot()
            
            self.last_state_update = datetime.now()
            return True

        except Exception as e:
            logger.error(f"Failed to update state: {str(e)}")
            if self._backup_state is not None:
                self.state = self._backup_state
            return False

    def _process_transaction_state(self, transaction) -> None:
        """Process a transaction's impact on state."""
        # Update account balances, contract states, etc.
        if transaction.action == "transfer":
            self._update_balances(transaction)
        elif transaction.action == "contract":
            self._update_contract_state(transaction)
        
        # Update general state metrics
        self.metrics.total_transactions += 1

    def _update_metrics_for_block(self, block: Block) -> None:
        """Update metrics based on a new block."""
        try:
            # Update block time metrics
            if len(self.state_history) > 0:
                block_time = (block.timestamp - self.last_state_update).total_seconds()
                blocks_created = self.metrics.blocks_created
                self.metrics.average_block_time = (
                    (self.metrics.average_block_time * blocks_created + block_time)
                    / (blocks_created + 1)
                )
            
            # Update other metrics
            self.metrics.blocks_created += 1
            self.metrics.total_transactions += len(block.transactions)
            
            # Calculate average transactions per block
            if self.metrics.blocks_created > 0:
                self.metrics.average_transactions_per_block = (
                    self.metrics.total_transactions / self.metrics.blocks_created
                )
            
            # Update size metrics
            self.metrics.total_size_bytes += len(str(block.to_dict()))
            self.metrics.state_size_bytes = len(str(self.state))

        except Exception as e:
            logger.error(f"Failed to update metrics: {str(e)}")

    def _should_save_snapshot(self) -> bool:
        """Determine if state snapshot should be saved."""
        # Save snapshot every 100 blocks or if state size changed significantly
        return (
            self.metrics.blocks_created % 100 == 0 or
            self.metrics.state_size_bytes > self.config.max_state_size * 0.9
        )

    def _save_state_snapshot(self) -> None:
        """Save a snapshot of current state."""
        try:
            snapshot = {
                'timestamp': datetime.now(),
                'state': deepcopy(self.state),
                'metrics': self.metrics.to_dict(),
                'block_height': self.metrics.blocks_created
            }
            self.state_history.append(snapshot)
            
            # Keep only last 10 snapshots
            if len(self.state_history) > 10:
                self.state_history = self.state_history[-10:]

        except Exception as e:
            logger.error(f"Failed to save state snapshot: {str(e)}")

    def rollback_state(self) -> bool:
        """
        Rollback to last backup state.
        
        Returns:
            bool: True if rollback was successful
        """
        try:
            if self._backup_state is not None:
                self.state = self._backup_state
                self._backup_state = None
                return True
            return False

        except Exception as e:
            logger.error(f"Failed to rollback state: {str(e)}")
            return False

    def get_metrics(self) -> Dict:
        """Get current metrics."""
        return self.metrics.to_dict()

    def get_state_size(self) -> int:
        """Get current state size in bytes."""
        return len(str(self.state))

    def clear_state(self) -> None:
        """Clear current state."""
        self._backup_state = deepcopy(self.state)
        self.state = {}
        self.metrics = ShardMetrics()

    def to_dict(self) -> Dict:
        """Convert manager state to dictionary format."""
        return {
            'state': deepcopy(self.state),
            'metrics': self.metrics.to_dict(),
            'last_update': self.last_state_update.isoformat(),
            'history': [
                {
                    'timestamp': snapshot['timestamp'].isoformat(),
                    'block_height': snapshot['block_height'],
                    'state_size': len(str(snapshot['state']))
                }
                for snapshot in self.state_history
            ]
        }

    @classmethod
    def from_dict(cls, data: Dict, shard_id: int, config: ShardConfig) -> 'StateManager':
        """Create manager from dictionary data."""
        manager = cls(shard_id, config)
        manager.state = deepcopy(data['state'])
        manager.metrics = ShardMetrics.from_dict(data['metrics'])
        manager.last_state_update = datetime.fromisoformat(data['last_update'])
        return manager