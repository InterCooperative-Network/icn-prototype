"""
blockchain/core/shard/state_manager.py

Manages state transitions and verification within shards of the ICN blockchain.
Handles state updates, merkle tree maintenance, and state root calculations
while ensuring atomicity and consistency of state changes.

Key responsibilities:
- Managing shard state transitions
- State root calculation and verification
- State snapshot creation and restoration
- Cross-shard state verification
- State pruning and optimization
"""

from typing import Dict, Optional, Any, List, Set
from datetime import datetime, timedelta
import logging
import hashlib
import json
from copy import deepcopy

from .shard_types import ShardConfig, ShardMetrics
from ..block import Block
from ..transaction import Transaction

logger = logging.getLogger(__name__)

class StateManager:
    """
    Manages state transitions and verification within a shard.
    
    Implements atomic state updates, maintains state history, and provides
    verification mechanisms for ensuring state consistency across the network.
    Uses merkle trees for efficient state verification and supports
    state rollback for error recovery.
    """

    def __init__(self, shard_id: int, config: ShardConfig):
        """
        Initialize the state manager.

        Args:
            shard_id: ID of the shard this manager belongs to
            config: Configuration parameters for the shard
        """
        self.shard_id = shard_id
        self.config = config
        
        # State storage
        self.state: Dict[str, Any] = {}
        self.state_history: List[Dict[str, Any]] = []
        self.state_roots: Dict[int, str] = {}  # height -> root
        
        # Pending changes
        self.pending_state: Optional[Dict[str, Any]] = None
        self.pending_changes: List[Dict[str, Any]] = []
        
        # State verification
        self.checkpoints: Dict[int, str] = {}  # height -> state root
        self.verified_states: Set[str] = set()  # Set of verified state roots
        
        # Metrics
        self.metrics = ShardMetrics()
        self.last_prune = datetime.now()
        
        # Initialize state
        self._initialize_state()

    def _initialize_state(self) -> None:
        """Initialize empty state structure with required fields."""
        self.state = {
            "accounts": {},      # Account balances and data
            "contracts": {},     # Smart contract states
            "metadata": {
                "shard_id": self.shard_id,
                "created_at": datetime.now().isoformat(),
                "last_updated": datetime.now().isoformat(),
                "version": "1.0",
                "state_version": 0
            }
        }
        self.state_roots[0] = self._calculate_state_root(self.state)

    async def update_state(self, block: Block) -> bool:
        """
        Update state based on transactions in a block.

        Args:
            block: Block containing state transitions

        Returns:
            bool: True if state update successful
        """
        try:
            # Start state transition
            self.pending_state = deepcopy(self.state)
            self.pending_changes.clear()

            # Apply all transactions
            for tx in block.transactions:
                if not await self._apply_transaction(tx):
                    await self._revert_pending_changes()
                    return False

            # Verify state size
            if len(str(self.pending_state)) > self.config.max_state_size:
                logger.error("State size limit exceeded")
                await self._revert_pending_changes()
                return False

            # Calculate new state root
            new_root = self._calculate_state_root(self.pending_state)
            
            # Commit state change
            self.state = self.pending_state
            self.state_history.append(deepcopy(self.state))
            self.state_roots[block.index] = new_root
            
            # Update metadata
            self.state["metadata"]["last_updated"] = datetime.now().isoformat()
            self.state["metadata"]["state_version"] += 1

            # Clear pending state
            self.pending_state = None
            self.pending_changes.clear()

            # Cleanup old state if needed
            await self._prune_old_state()

            logger.info(f"Updated state for block {block.index}")
            return True

        except Exception as e:
            logger.error(f"Error updating state: {str(e)}")
            await self._revert_pending_changes()
            return False

    async def _apply_transaction(self, transaction: Transaction) -> bool:
        """
        Apply a single transaction to the pending state.

        Args:
            transaction: Transaction to apply

        Returns:
            bool: True if transaction applied successfully
        """
        try:
            # Record current state for potential rollback
            self.pending_changes.append(deepcopy(self.pending_state))

            # Apply based on transaction type
            if transaction.action == "transfer":
                return await self._apply_transfer(transaction)
            elif transaction.action == "contract":
                return await self._apply_contract_change(transaction)
            elif transaction.action == "stake":
                return await self._apply_stake_change(transaction)
            else:
                logger.error(f"Unsupported transaction action: {transaction.action}")
                return False

        except Exception as e:
            logger.error(f"Error applying transaction: {str(e)}")
            return False

    async def _apply_transfer(self, transaction: Transaction) -> bool:
        """
        Apply a transfer transaction to the pending state.

        Args:
            transaction: Transfer transaction to apply

        Returns:
            bool: True if transfer applied successfully
        """
        try:
            amount = float(transaction.data.get('amount', 0))
            if amount <= 0:
                return False

            # Initialize accounts if needed
            accounts = self.pending_state["accounts"]
            for account_id in [transaction.sender, transaction.receiver]:
                if account_id not in accounts:
                    accounts[account_id] = {
                        "balance": 0.0,
                        "created_at": datetime.now().isoformat(),
                        "last_updated": datetime.now().isoformat(),
                        "transaction_count": 0
                    }

            # Verify sender balance
            sender_account = accounts[transaction.sender]
            if sender_account["balance"] < amount:
                return False

            # Apply transfer
            sender_account["balance"] -= amount
            accounts[transaction.receiver]["balance"] += amount

            # Update metadata
            for account_id in [transaction.sender, transaction.receiver]:
                accounts[account_id]["transaction_count"] += 1
                accounts[account_id]["last_updated"] = datetime.now().isoformat()

            return True

        except Exception as e:
            logger.error(f"Error applying transfer: {str(e)}")
            return False

    async def _apply_contract_change(self, transaction: Transaction) -> bool:
        """
        Apply a contract state change.

        Args:
            transaction: Contract transaction to apply

        Returns:
            bool: True if contract change applied successfully
        """
        try:
            contract_id = transaction.data.get('contract_id')
            if not contract_id:
                return False

            contracts = self.pending_state["contracts"]
            
            # Initialize contract state if needed
            if contract_id not in contracts:
                contracts[contract_id] = {
                    "state": {},
                    "created_at": datetime.now().isoformat(),
                    "last_updated": datetime.now().isoformat(),
                    "transaction_count": 0
                }

            # Apply state changes
            contract = contracts[contract_id]
            state_changes = transaction.data.get('state_changes', {})
            contract["state"].update(state_changes)

            # Update metadata
            contract["transaction_count"] += 1
            contract["last_updated"] = datetime.now().isoformat()

            return True

        except Exception as e:
            logger.error(f"Error applying contract change: {str(e)}")
            return False

    async def _apply_stake_change(self, transaction: Transaction) -> bool:
        """
        Apply a staking change to an account.

        Args:
            transaction: Stake transaction to apply

        Returns:
            bool: True if stake change applied successfully
        """
        try:
            amount = float(transaction.data.get('amount', 0))
            if amount <= 0:
                return False

            accounts = self.pending_state["accounts"]
            account_id = transaction.sender

            # Initialize account if needed
            if account_id not in accounts:
                accounts[account_id] = {
                    "balance": 0.0,
                    "stake": 0.0,
                    "created_at": datetime.now().isoformat(),
                    "last_updated": datetime.now().isoformat(),
                    "transaction_count": 0
                }

            account = accounts[account_id]

            # Handle stake or unstake
            if transaction.action == "stake":
                if account["balance"] < amount:
                    return False
                account["balance"] -= amount
                account["stake"] += amount
            else:  # unstake
                if account["stake"] < amount:
                    return False
                account["stake"] -= amount
                account["balance"] += amount

            account["transaction_count"] += 1
            account["last_updated"] = datetime.now().isoformat()

            return True

        except Exception as e:
            logger.error(f"Error applying stake change: {str(e)}")
            return False

    async def _revert_pending_changes(self) -> None:
        """Revert pending state changes if an error occurs."""
        if self.pending_changes:
            self.pending_state = self.pending_changes[-1]
            self.pending_changes.clear()

    def _calculate_state_root(self, state: Dict[str, Any]) -> str:
        """
        Calculate merkle root of current state.

        Args:
            state: State to calculate root for

        Returns:
            str: Calculated state root
        """
        state_json = json.dumps(state, sort_keys=True)
        return hashlib.sha256(state_json.encode()).hexdigest()

    async def create_snapshot(self, block_height: int) -> Optional[Dict[str, Any]]:
        """
        Create a snapshot of current state.

        Args:
            block_height: Block height to create snapshot for

        Returns:
            Optional[Dict[str, Any]]: State snapshot if successful
        """
        try:
            snapshot = {
                "state": deepcopy(self.state),
                "height": block_height,
                "timestamp": datetime.now().isoformat(),
                "state_root": self.state_roots.get(block_height),
                "metadata": {
                    "shard_id": self.shard_id,
                    "created_at": datetime.now().isoformat(),
                    "state_version": self.state["metadata"]["state_version"]
                }
            }
            return snapshot

        except Exception as e:
            logger.error(f"Error creating snapshot: {str(e)}")
            return None

    async def restore_snapshot(self, snapshot: Dict[str, Any]) -> bool:
        """
        Restore state from a snapshot.

        Args:
            snapshot: State snapshot to restore

        Returns:
            bool: True if restore successful
        """
        try:
            # Verify snapshot
            if not self._verify_snapshot(snapshot):
                return False

            # Restore state
            self.state = deepcopy(snapshot["state"])
            height = snapshot["height"]
            self.state_roots[height] = snapshot["state_root"]

            logger.info(f"Restored state from snapshot at height {height}")
            return True

        except Exception as e:
            logger.error(f"Error restoring snapshot: {str(e)}")
            return False

    def _verify_snapshot(self, snapshot: Dict[str, Any]) -> bool:
        """
        Verify integrity of a state snapshot.

        Args:
            snapshot: Snapshot to verify

        Returns:
            bool: True if snapshot is valid
        """
        try:
            # Verify required fields
            required_fields = ["state", "height", "state_root", "metadata"]
            if not all(field in snapshot for field in required_fields):
                return False

            # Verify state root
            calculated_root = self._calculate_state_root(snapshot["state"])
            if calculated_root != snapshot["state_root"]:
                return False

            return True

        except Exception as e:
            logger.error(f"Error verifying snapshot: {str(e)}")
            return False

    async def _prune_old_state(self) -> None:
        """Prune old state history to prevent memory bloat."""
        try:
            current_time = datetime.now()
            if (current_time - self.last_prune) < timedelta(hours=1):
                return

            # Keep last 100 states
            if len(self.state_history) > 100:
                self.state_history = self.state_history[-100:]

            # Remove old state roots
            current_height = max(self.state_roots.keys(), default=0)
            for height in list(self.state_roots.keys()):
                if height < current_height - 1000:  # Keep last 1000 state roots
                    del self.state_roots[height]

            self.last_prune = current_time

        except Exception as e:
            logger.error(f"Error pruning old state: {str(e)}")

    def get_metrics(self) -> Dict[str, Any]:
        """
        Get state manager metrics.

        Returns:
            Dict[str, Any]: Dictionary of metrics
        """
        try:
            return {
                "state_size": len(str(self.state)),
                "state_version": self.state["metadata"]["state_version"],
                "account_count": len(self.state["accounts"]),
                "contract_count": len(self.state["contracts"]),
                "state_history_length": len(self.state_history),
                "last_updated": self.state["metadata"]["last_updated"]
            }

        except Exception as e:
            logger.error(f"Error getting metrics: {str(e)}")
            return {}

    def to_dict(self) -> Dict[str, Any]:
        """Convert manager state to dictionary format."""
        try:
            return {
                "state": deepcopy(self.state),
                "state_roots": self.state_roots.copy(),
                "checkpoints": self.checkpoints.copy(),
                "metrics": self.metrics.to_dict()
            }

        except Exception as e:
            logger.error(f"Error converting to dictionary: {str(e)}")
            return {}

    @classmethod
    def from_dict(cls, data: Dict[str, Any], shard_id: int, config: ShardConfig) -> 'StateManager':
        """
        Create manager from dictionary data.

        Args:
            data: Dictionary containing manager data
            shard_id: Shard ID for the manager
            config: Shard configuration

        Returns:
            StateManager: Reconstructed manager instance
        """
        try:
            manager = cls(shard_id, config)
            manager.state = deepcopy(data["state"])
            manager.state_roots = data["state_roots"].copy()
            manager.checkpoints = data["checkpoints"].copy()
            manager.metrics = ShardMetrics.from_dict(data["metrics"])
            return manager

        except Exception as e:
            logger.error(f"Error creating from dictionary: {str(e)}")
            raise