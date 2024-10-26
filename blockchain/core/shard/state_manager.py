# blockchain/core/shard/state_manager.py

from typing import Dict, Any, Optional, List
import logging
from datetime import datetime
from copy import deepcopy
from .shard_types import ShardMetrics, ShardConfig
from ..block import Block
from ..transaction import Transaction

logger = logging.getLogger(__name__)

class StateManager:
    """Manages shard state, metrics, and state transitions."""
    
    def __init__(self, shard_id: int, config: ShardConfig):
        self.shard_id = shard_id
        self.config = config
        self.state: Dict[str, Any] = {
            "balances": {},
            "contracts": {},
            "metadata": {},
            "last_processed_block": None
        }
        self.metrics = ShardMetrics()
        self.state_history: List[Dict] = []
        self.last_state_update = datetime.now()
        self._backup_state: Optional[Dict] = None
        
        # Track processed transactions to prevent duplicates
        self.processed_transactions: Set[str] = set()

    def update_state(self, block: Block) -> bool:
        """
        Update state based on a new block.
        
        Args:
            block: Block containing transactions to process
            
        Returns:
            bool: True if state was updated successfully
        """
        try:
            # Create state backup
            self._backup_state = deepcopy(self.state)
            
            # Process each transaction
            for tx in block.transactions:
                if not self._process_transaction_state(tx):
                    logger.error(f"Failed to process transaction {tx.transaction_id}")
                    self.rollback_state()
                    return False

            # Update metrics
            self._update_metrics_for_block(block)
            
            # Save state snapshot if needed
            if self._should_save_snapshot():
                self._save_state_snapshot()
            
            self.last_state_update = datetime.now()
            self.state["last_processed_block"] = block.index
            
            # Track processed transactions
            for tx in block.transactions:
                self.processed_transactions.add(tx.transaction_id)
            
            return True

        except Exception as e:
            logger.error(f"Failed to update state: {str(e)}")
            self.rollback_state()
            return False

    def _process_transaction_state(self, transaction: Transaction) -> bool:
        """Process a transaction's impact on state."""
        try:
            # Skip if already processed
            if transaction.transaction_id in self.processed_transactions:
                logger.warning(f"Transaction {transaction.transaction_id} already processed")
                return True

            # Ensure all accounts exist in state
            balances = self.state["balances"]
            if transaction.sender not in balances:
                balances[transaction.sender] = 1000.0  # Initial balance
            if transaction.receiver not in balances:
                balances[transaction.receiver] = 1000.0  # Initial balance

            # Verify sufficient balance
            amount = float(transaction.data.get("amount", 0))
            if amount <= 0:
                logger.error(f"Invalid transaction amount: {amount}")
                return False

            if balances[transaction.sender] < amount:
                logger.error(f"Insufficient balance for {transaction.sender}")
                return False

            # Update balances
            balances[transaction.sender] -= amount
            balances[transaction.receiver] += amount

            if transaction.action == "contract":
                return self._update_contract_state(transaction)
            elif transaction.action == "store":
                return self._update_storage_state(transaction)

            # Update metadata
            self._update_metadata(transaction)
            
            self.metrics.total_transactions += 1
            return True

        except Exception as e:
            logger.error(f"Failed to process transaction state: {str(e)}")
            return False

    def _update_contract_state(self, transaction: Transaction) -> bool:
        """Update smart contract state."""
        try:
            contracts = self.state["contracts"]
            contract_id = transaction.data.get("contract_id")
            
            if not contract_id:
                return False
                
            if contract_id not in contracts:
                contracts[contract_id] = {"state": {}, "calls": []}
                
            # Update contract state
            contracts[contract_id]["state"].update(transaction.data.get("state_updates", {}))
            contracts[contract_id]["calls"].append({
                "timestamp": transaction.timestamp.isoformat(),
                "sender": transaction.sender,
                "action": transaction.data.get("contract_action")
            })
            return True

        except Exception as e:
            logger.error(f"Failed to update contract state: {str(e)}")
            return False

    def _update_storage_state(self, transaction: Transaction) -> bool:
        """Update storage state for data transactions."""
        try:
            if "storage" not in self.state:
                self.state["storage"] = {"files": {}, "size": 0}
                
            storage = self.state["storage"]
            file_id = transaction.data.get("file_id")
            
            if file_id and "file_data" in transaction.data:
                storage["files"][file_id] = {
                    "owner": transaction.sender,
                    "size": len(transaction.data["file_data"]),
                    "timestamp": transaction.timestamp.isoformat()
                }
                storage["size"] += len(transaction.data["file_data"])
                return True
            return False

        except Exception as e:
            logger.error(f"Failed to update storage state: {str(e)}")
            return False

    def _update_metadata(self, transaction: Transaction) -> None:
        """Update state metadata."""
        metadata = self.state["metadata"]
        
        # Update last transaction timestamp
        metadata["last_transaction"] = transaction.timestamp.isoformat()
        
        # Track active addresses
        if "active_addresses" not in metadata:
            metadata["active_addresses"] = set()
        metadata["active_addresses"].add(transaction.sender)
        metadata["active_addresses"].add(transaction.receiver)
        
        # Update transaction counts
        if "transaction_counts" not in metadata:
            metadata["transaction_counts"] = {}
        metadata["transaction_counts"][transaction.action] = \
            metadata["transaction_counts"].get(transaction.action, 0) + 1

    def _update_metrics_for_block(self, block: Block) -> None:
        """Update metrics based on a new block."""
        try:
            self.metrics.blocks_created += 1
            self.metrics.total_transactions += len(block.transactions)
            
            if self.metrics.blocks_created > 0:
                self.metrics.average_transactions_per_block = (
                    self.metrics.total_transactions / self.metrics.blocks_created
                )
            
            # Update size metrics
            block_size = len(str(block.to_dict()))
            self.metrics.total_size_bytes += block_size
            self.metrics.state_size_bytes = len(str(self.state))

        except Exception as e:
            logger.error(f"Failed to update metrics: {str(e)}")

    def _should_save_snapshot(self) -> bool:
        """Determine if state snapshot should be saved."""
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
        """Rollback to last backup state."""
        try:
            if self._backup_state is not None:
                self.state = deepcopy(self._backup_state)
                self._backup_state = None
                return True
            return False

        except Exception as e:
            logger.error(f"Failed to rollback state: {str(e)}")
            return False

    def get_metrics(self) -> Dict:
        """Get current metrics."""
        return {
            "total_transactions": self.metrics.total_transactions,
            "blocks_created": self.metrics.blocks_created,
            "average_transactions_per_block": self.metrics.average_transactions_per_block,
            "total_size_bytes": self.metrics.total_size_bytes,
            "state_size_bytes": self.metrics.state_size_bytes,
            "processed_transactions": len(self.processed_transactions)
        }

    def to_dict(self) -> Dict:
        """Convert manager state to dictionary format."""
        return {
            'state': deepcopy(self.state),
            'metrics': self.metrics.to_dict(),
            'last_update': self.last_state_update.isoformat(),
            'processed_transactions': list(self.processed_transactions)
        }

    @classmethod
    def from_dict(cls, data: Dict, shard_id: int, config: ShardConfig) -> 'StateManager':
        """Create manager from dictionary data."""
        manager = cls(shard_id, config)
        manager.state = deepcopy(data['state'])
        manager.metrics = ShardMetrics.from_dict(data['metrics'])
        manager.last_state_update = datetime.fromisoformat(data['last_update'])
        manager.processed_transactions = set(data.get('processed_transactions', []))
        return manager