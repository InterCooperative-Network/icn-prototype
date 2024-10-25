# blockchain/core/shard/base.py

from typing import Dict, List, Optional, Set
import logging
from datetime import datetime
from .types import ShardConfig, ShardMetrics
from .transaction_manager import TransactionManager
from .state_manager import StateManager
from .validation import ValidationManager
from .cross_shard import CrossShardManager
from ..block import Block
from ..transaction import Transaction

logger = logging.getLogger(__name__)

class Shard:
    """
    Main shard class that coordinates all shard components.
    
    This class serves as the primary interface for shard operations,
    delegating specific functionalities to specialized managers.
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

    def add_transaction(self, transaction: Transaction) -> bool:
        """
        Add a new transaction to the shard.
        
        Args:
            transaction: Transaction to add
            
        Returns:
            bool: True if transaction was added successfully
        """
        # Validate transaction
        if not self.validation_manager.validate_transaction(transaction):
            return False
            
        # Check for cross-shard implications
        cross_shard_ref = self.cross_shard_manager.process_transaction(transaction)
        
        # Add to transaction pool
        return self.transaction_manager.add_transaction(transaction)

    def create_block(self, validator: str) -> Optional[Block]:
        """
        Create a new block from pending transactions.
        
        Args:
            validator: ID of the validator creating the block
            
        Returns:
            Optional[Block]: New block if created successfully
        """
        # Select transactions
        transactions = self.transaction_manager.select_transactions_for_block()
        if not transactions:
            return None

        # Create block
        block = Block(
            index=self.height,
            previous_hash=self.chain[-1].hash,
            timestamp=datetime.now(),
            transactions=transactions,
            validator=validator,
            shard_id=self.shard_id
        )

        # Add cross-shard references
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
        if not self.validation_manager.validate_block(block, self.chain[-1]):
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
        return self.validation_manager.validate_chain_sequence(self.chain)

    def get_metrics(self) -> Dict:
        """Get comprehensive shard metrics."""
        return {
            "shard_id": self.shard_id,
            "height": self.height,
            "chain_size": len(self.chain),
            "known_validators": len(self.known_validators),
            **self.state_manager.get_metrics(),
            **self.cross_shard_manager.get_metrics()
        }

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
        # Create shard instance
        config = ShardConfig.from_dict(data["config"])
        shard = cls(data["shard_id"], config)
        
        # Restore chain
        shard.chain = [Block.from_dict(block) for block in data["chain"]]
        shard.height = data["height"]
        shard.known_validators = set(data["known_validators"])
        
        # Restore managers
        shard.transaction_manager = TransactionManager.from_dict(
            data["transaction_manager"], shard.shard_id, config
        )
        shard.state_manager = StateManager.from_dict(
            data["state_manager"], shard.shard_id, config
        )
        shard.cross_shard_manager = CrossShardManager.from_dict(
            data["cross_shard_manager"], shard.shard_id, config
        )
        
        return shard