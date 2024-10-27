from typing import Dict, Optional, Any, Set, List
from datetime import datetime
import json
import hashlib
import logging
from dataclasses import dataclass, field
from copy import deepcopy

logger = logging.getLogger(__name__)

@dataclass
class StateTransition:
    """Represents a transition in blockchain state."""
    old_state: Dict[str, Any]
    new_state: Dict[str, Any]
    transaction_id: str
    shard_id: Optional[int]
    timestamp: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def calculate_hash(self) -> str:
        """Calculate hash of the state transition."""
        data = {
            "old_state": self.old_state,
            "new_state": self.new_state,
            "transaction_id": self.transaction_id,
            "shard_id": self.shard_id,
            "timestamp": self.timestamp.isoformat(),
            "metadata": self.metadata
        }
        return hashlib.sha256(json.dumps(data, sort_keys=True).encode()).hexdigest()

class UnifiedStateManager:
    """
    Manages both global and shard-specific state in a unified way.
    Coordinates state transitions across shards while maintaining global consistency.
    """
    
    def __init__(self, shard_count: int):
        # Global state
        self.global_state: Dict[str, Any] = {
            "accounts": {},      # Global account states
            "contracts": {},     # Smart contract states
            "governance": {},    # Governance states
            "metadata": {        # Global metadata
                "last_updated": datetime.now().isoformat(),
                "version": "1.0",
                "shard_count": shard_count,
                "total_transactions": 0,
                "total_volume": 0.0
            }
        }
        
        # Shard states
        self.shard_states: Dict[int, Dict[str, Any]] = {
            shard_id: {
                "balances": {},  # Shard-specific balances
                "metadata": {    # Shard metadata
                    "shard_id": shard_id,
                    "created_at": datetime.now().isoformat(),
                    "last_updated": datetime.now().isoformat(),
                    "transaction_count": 0
                }
            }
            for shard_id in range(shard_count)
        }
        
        # State management
        self.state_transitions: List[StateTransition] = []
        self.pending_transitions: Dict[str, StateTransition] = {}
        self.state_roots: Dict[int, str] = {}  # height -> state root
        self.shard_roots: Dict[int, Dict[int, str]] = {}  # height -> shard_id -> root
        self.checkpoint_heights: Set[int] = set()
        
        self._backup_states: Dict[str, Any] = {}

    def begin_transition(self, transaction_id: str, shard_id: Optional[int] = None) -> None:
        """Begin a new state transition, either global or shard-specific."""
        if transaction_id in self.pending_transitions:
            raise ValueError(f"Transition already in progress for transaction {transaction_id}")
            
        # Backup relevant states
        if shard_id is not None:
            old_state = deepcopy(self.shard_states[shard_id])
            self._backup_states[f"shard_{shard_id}"] = deepcopy(self.shard_states[shard_id])
        else:
            old_state = deepcopy(self.global_state)
            self._backup_states["global"] = deepcopy(self.global_state)
            
        self.pending_transitions[transaction_id] = StateTransition(
            old_state=old_state,
            new_state=deepcopy(old_state),
            transaction_id=transaction_id,
            shard_id=shard_id
        )

    def update_shard_balance(self, shard_id: int, account_id: str, delta: float, transaction_id: str) -> bool:
        """Update an account balance within a specific shard."""
        try:
            if transaction_id not in self.pending_transitions:
                raise ValueError(f"No transition in progress for transaction {transaction_id}")
                
            transition = self.pending_transitions[transaction_id]
            if transition.shard_id != shard_id:
                raise ValueError(f"Transaction {transaction_id} not associated with shard {shard_id}")
                
            shard_state = transition.new_state
            
            # Initialize account if needed
            if account_id not in shard_state["balances"]:
                shard_state["balances"][account_id] = {
                    "balance": 0.0,
                    "created_at": datetime.now().isoformat(),
                    "transaction_count": 0
                }
                
            # Update balance
            account = shard_state["balances"][account_id]
            new_balance = account["balance"] + delta
            
            if new_balance < 0:
                return False
                
            account["balance"] = new_balance
            account["transaction_count"] += 1
            account["last_updated"] = datetime.now().isoformat()
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to update shard balance: {str(e)}")
            self.rollback_transition(transaction_id)
            return False

    def update_global_state(self, state_type: str, entity_id: str, 
                          state_update: Dict, transaction_id: str) -> bool:
        """Update global state (contracts, governance, etc)."""
        try:
            if transaction_id not in self.pending_transitions:
                raise ValueError(f"No transition in progress for transaction {transaction_id}")
                
            transition = self.pending_transitions[transaction_id]
            if transition.shard_id is not None:
                raise ValueError("Cannot update global state in shard-specific transaction")
                
            if state_type not in transition.new_state:
                raise ValueError(f"Invalid state type: {state_type}")
                
            # Initialize entity if needed
            if entity_id not in transition.new_state[state_type]:
                transition.new_state[state_type][entity_id] = {
                    "created_at": datetime.now().isoformat()
                }
                
            # Update state
            entity_state = transition.new_state[state_type][entity_id]
            entity_state.update(state_update)
            entity_state["last_updated"] = datetime.now().isoformat()
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to update global state: {str(e)}")
            self.rollback_transition(transaction_id)
            return False

    def commit_transition(self, transaction_id: str, block_height: int) -> bool:
        """Commit a state transition and update state roots."""
        try:
            if transaction_id not in self.pending_transitions:
                raise ValueError(f"No transition found for transaction {transaction_id}")
                
            transition = self.pending_transitions[transaction_id]
            
            # Update metadata
            if transition.shard_id is not None:
                # Shard-specific commit
                shard_state = self.shard_states[transition.shard_id]
                shard_state.update(transition.new_state)
                shard_state["metadata"]["last_updated"] = datetime.now().isoformat()
                shard_state["metadata"]["transaction_count"] += 1
                
                # Update shard root
                if block_height not in self.shard_roots:
                    self.shard_roots[block_height] = {}
                self.shard_roots[block_height][transition.shard_id] = self._calculate_state_root(shard_state)
            else:
                # Global commit
                self.global_state.update(transition.new_state)
                self.global_state["metadata"]["last_updated"] = datetime.now().isoformat()
                self.global_state["metadata"]["total_transactions"] += 1
                
                # Update global root
                self.state_roots[block_height] = self._calculate_state_root(self.global_state)
            
            # Store transition
            self.state_transitions.append(transition)
            
            # Cleanup
            del self.pending_transitions[transaction_id]
            if transition.shard_id is not None:
                del self._backup_states[f"shard_{transition.shard_id}"]
            else:
                del self._backup_states["global"]
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to commit transition: {str(e)}")
            self.rollback_transition(transaction_id)
            return False

    def rollback_transition(self, transaction_id: str) -> None:
        """Rollback a pending state transition."""
        if transaction_id in self.pending_transitions:
            transition = self.pending_transitions[transaction_id]
            
            # Restore from backup
            if transition.shard_id is not None:
                backup_key = f"shard_{transition.shard_id}"
                if backup_key in self._backup_states:
                    self.shard_states[transition.shard_id] = self._backup_states[backup_key]
                    del self._backup_states[backup_key]
            else:
                if "global" in self._backup_states:
                    self.global_state = self._backup_states["global"]
                    del self._backup_states["global"]
                    
            del self.pending_transitions[transaction_id]

    def create_checkpoint(self, block_height: int) -> Dict[str, str]:
        """Create a checkpoint with both global and shard state roots."""
        try:
            checkpoint = {
                "global": self._calculate_state_root(self.global_state),
                "shards": {
                    shard_id: self._calculate_state_root(state)
                    for shard_id, state in self.shard_states.items()
                }
            }
            
            self.state_roots[block_height] = checkpoint["global"]
            self.shard_roots[block_height] = checkpoint["shards"]
            self.checkpoint_heights.add(block_height)
            
            return checkpoint
            
        except Exception as e:
            logger.error(f"Failed to create checkpoint: {str(e)}")
            return {}

    def _calculate_state_root(self, state: Dict) -> str:
        """Calculate Merkle root of a state tree."""
        state_json = json.dumps(state, sort_keys=True)
        return hashlib.sha256(state_json.encode()).hexdigest()

    def get_shard_balance(self, shard_id: int, account_id: str) -> float:
        """Get account balance within a specific shard."""
        return self.shard_states[shard_id]["balances"].get(
            account_id, {"balance": 0.0}
        )["balance"]

    def get_global_state(self, state_type: str, entity_id: str) -> Optional[Dict]:
        """Get state of a global entity (contract, governance, etc)."""
        return self.global_state.get(state_type, {}).get(entity_id)