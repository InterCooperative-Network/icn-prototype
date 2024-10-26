# blockchain/core/shard/base.py

from typing import Dict, List, Optional, Set
import logging
from datetime import datetime
from .shard_types import ShardConfig, ShardMetrics
from .transaction_manager import TransactionManager
from .state_manager import StateManager
from .validation_manager import ValidationManager
from .cross_shard_manager import CrossShardManager
from ..block import Block
from ..transaction import Transaction

logger = logging.getLogger(__name__)


class Shard:
    """
    Main shard class that coordinates all shard components.
    
    This class serves as the primary interface for shard operations,
    delegating specific functionalities to specialized managers while
    maintaining backward compatibility with existing interfaces.
    """
    
    def __init__(self, shard_id: int, config: Optional[ShardConfig] = None):
        """
        Initialize a new shard.
        
        Args:
            shard_id: Unique identifier for this shard
            config: Optional configuration, uses defaults if not provided
        """
        self.shard_id = shard_id
        self.config = config or ShardConfig()
        
        # Initialize managers
        self.transaction_manager = TransactionManager(shard_id, self.config)
        self.state_manager = StateManager(shard_id, self.config)
        self.validation_manager = ValidationManager(shard_id, self.config)
        self.cross_shard_manager = CrossShardManager(shard_id, self.config)
        
        # Core properties
        self.chain: List[Block] = []
        self.height = 0
        self.known_validators: Set[str] = set()
        
        # Backward compatibility properties
        self._pending_transactions = property(
            lambda self: self.transaction_manager.pending_transactions
        )
        self._state = property(
            lambda self: self.state_manager.state
        )
        
        self._create_genesis_block()

    def _create_genesis_block(self) -> None:
        """Create and add the genesis block."""
        genesis_block = Block(
            index=0,
            previous_hash="0" * 64,
            timestamp=datetime.now(),
            transactions=[],
            validator="genesis",
            shard_id=self.shard_id
        )
        
        self.chain.append(genesis_block)
        self.height = 1
        self.known_validators.add("genesis")
        self.state_manager.update_state(genesis_block)

    # Property accessors for backward compatibility
    @property
    def pending_transactions(self) -> List[Transaction]:
        """Backward compatible access to pending transactions."""
        return self.transaction_manager.pending_transactions

    @property
    def state(self) -> Dict:
        """Backward compatible access to state."""
        return self.state_manager.state

    def add_transaction(self, transaction: Transaction) -> bool:
        """
        Add a new transaction to the shard.
        
        Args:
            transaction: Transaction to add
            
        Returns:
            bool: True if transaction was added successfully
        """
        if not self.validation_manager.validate_transaction(transaction):
            return False
        return self.transaction_manager.add_transaction(transaction)

    def create_block(self, validator: str) -> Optional[Block]:
        """
        Create a new block from pending transactions.
        
        Args:
            validator: ID of the validator creating the block
            
        Returns:
            Optional[Block]: New block if created successfully
        """
        transactions = self.transaction_manager.select_transactions_for_block()
        if not transactions:
            return None

        block = Block(
            index=self.height,
            previous_hash=self.chain[-1].hash if self.chain else "0" * 64,
            timestamp=datetime.now(),
            transactions=transactions,
            validator=validator,
            shard_id=self.shard_id
        )

        # Update cross-shard references
        self.cross_shard_manager.update_references(block)
        return block

    def add_block(self, block: Block) -> bool:
        """
        Add a validated block to the chain.
        
        Args:
            block: Block to add
            
        Returns:
            bool: True if block was added successfully
        """
        # Validate block
        if not self.validation_manager.validate_block(block, self.chain[-1] if self.chain else None):
            return False

        # Update state
        if not self.state_manager.update_state(block):
            return False

        # Add block to chain
        self.chain.append(block)
        self.height += 1
        self.known_validators.add(block.validator)

        # Remove included transactions
        tx_ids = {tx.transaction_id for tx in block.transactions}
        self.transaction_manager.remove_transactions(tx_ids)

        return True

    def validate_chain(self) -> bool:
        """
        Validate the entire chain.
        
        Returns:
            bool: True if chain is valid
        """
        if not self.chain:
            return True

        for i in range(1, len(self.chain)):
            if not self.validation_manager.validate_block(self.chain[i], self.chain[i-1]):
                return False

        return True

    def get_metrics(self) -> Dict:
        """Get comprehensive shard metrics."""
        metrics = {
            "shard_id": self.shard_id,
            "height": self.height,
            "chain_size": len(self.chain),
            "known_validators": len(self.known_validators),
            "pending_transactions": len(self.pending_transactions),
            "state_size": len(str(self.state))
        }

        # Add metrics from each manager
        metrics.update(self.state_manager.get_metrics())
        metrics.update(self.transaction_manager.get_metrics())
        
        # Get cross-shard metrics
        cross_shard_metrics = self.cross_shard_manager.get_metrics()
        metrics.update({
            "pending_validations": cross_shard_metrics["pending_validations"],
            "validated_refs": cross_shard_metrics.get("validated_refs", 0),
            "cross_shard_operations": cross_shard_metrics.get("cross_shard_operations", 0)
        })

        return metrics

    def to_dict(self) -> Dict:
        """Convert shard state to dictionary format."""
        return {
            "shard_id": self.shard_id,
            "height": self.height,
            "chain": [block.to_dict() for block in self.chain],
            "config": self.config.to_dict(),
            "known_validators": list(self.known_validators),
            "transaction_manager": self.transaction_manager.to_dict(),
            "state_manager": self.state_manager.to_dict(),
            "cross_shard_manager": self.cross_shard_manager.to_dict()
        }

    @classmethod
    def from_dict(cls, data: Dict) -> 'Shard':
        """Create shard from dictionary data."""
        config = ShardConfig.from_dict(data["config"])
        shard = cls(data["shard_id"], config)
        
        # Restore chain
        shard.chain = [Block.from_dict(block) for block in data["chain"]]
        shard.height = data["height"]
        shard.known_validators = set(data["known_validators"])
        
        # Restore managers
        shard.transaction_manager = TransactionManager.from_dict(
            data["transaction_manager"],
            shard.shard_id,
            config
        )
        shard.state_manager = StateManager.from_dict(
            data["state_manager"],
            shard.shard_id,
            config
        )
        shard.cross_shard_manager = CrossShardManager.from_dict(
            data["cross_shard_manager"],
            shard.shard_id,
            config
        )
        
        return shard