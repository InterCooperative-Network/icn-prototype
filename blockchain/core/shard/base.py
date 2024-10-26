# blockchain/core/shard/base.py

from typing import Dict, List, Optional, Set, Any, Union
import logging
from datetime import datetime
from copy import deepcopy
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

    # Maintain class-level defaults for backward compatibility
    DEFAULT_MAX_TRANSACTIONS = 100

    def __init__(self, shard_id: int, config: Optional[ShardConfig] = None, **kwargs):
        """
        Initialize a new shard.
        
        Args:
            shard_id: Unique identifier for this shard
            config: Optional configuration, uses defaults if not provided
            **kwargs: Legacy support for direct parameter passing
                     (e.g., max_transactions_per_block)
        """
        if not isinstance(shard_id, int) or shard_id < 0:
            raise ValueError("Invalid shard_id. Must be non-negative integer.")

        # Handle legacy initialization
        if config is None:
            config = ShardConfig()
            if 'max_transactions_per_block' in kwargs:
                config.max_transactions_per_block = kwargs['max_transactions_per_block']
            if 'max_pending_transactions' in kwargs:
                config.max_pending_transactions = kwargs['max_pending_transactions']

        self.shard_id = shard_id
        self.config = config
        
        # Initialize managers
        try:
            self.transaction_manager = TransactionManager(shard_id, self.config)
            self.state_manager = StateManager(shard_id, self.config)
            self.validation_manager = ValidationManager(shard_id, self.config)
            self.cross_shard_manager = CrossShardManager(shard_id, self.config)
        except Exception as e:
            logger.error(f"Failed to initialize shard managers: {str(e)}")
            raise
        
        # Core properties
        self.chain: List[Block] = []
        self.height = 0
        self.known_validators: Set[str] = set()
        
        # Initialize genesis block and state
        self._create_genesis_block()
        self._initialize_state()

    def _initialize_state(self) -> None:
        """Initialize the shard's state with required structure."""
        try:
            self.state_manager.state.update({
                "metadata": {
                    "shard_id": self.shard_id,
                    "created_at": datetime.now().isoformat(),
                    "last_updated": datetime.now().isoformat(),
                    "total_transactions": 0,
                    "total_volume": 0.0,
                    "version": "1.0"
                }
            })
            
            # Ensure balances are initialized
            if "balances" not in self.state_manager.state:
                self.state_manager.state["balances"] = {
                    f"user{i}": 1000.0 for i in range(10)
                }
        except Exception as e:
            logger.error(f"Failed to initialize state: {str(e)}")
            raise

    def _create_genesis_block(self) -> None:
        """Create and add the genesis block."""
        try:
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
        except Exception as e:
            logger.error(f"Failed to create genesis block: {str(e)}")
            raise

    @property
    def max_transactions_per_block(self) -> int:
        """Property accessor for backward compatibility."""
        return self.config.max_transactions_per_block

    @classmethod
    def get_max_transactions_per_block(cls) -> int:
        """Class method accessor for backward compatibility."""
        return cls.DEFAULT_MAX_TRANSACTIONS

    @property
    def cross_shard_references(self) -> Dict[int, List[str]]:
        """Property accessor for cross shard references."""
        return self.cross_shard_manager.cross_shard_refs

    @property
    def average_block_time(self) -> float:
        """Calculate average time between blocks."""
        if len(self.chain) < 2:
            return 0.0
        
        total_time = sum(
            (self.chain[i].timestamp - self.chain[i-1].timestamp).total_seconds()
            for i in range(1, len(self.chain))
        )
        return total_time / (len(self.chain) - 1)

    @property
    def pending_transactions(self) -> List[Transaction]:
        """Property accessor for backward compatibility."""
        return self.transaction_manager.pending_transactions

    @property
    def state(self) -> Dict:
        """Property accessor for backward compatibility."""
        return self.state_manager.state

    def add_transaction(self, transaction: Transaction) -> bool:
        """Add a new transaction to the shard."""
        if not isinstance(transaction, Transaction):
            logger.error("Invalid transaction type")
            return False

        try:
            # Validate transaction
            if not self.validation_manager.validate_transaction(transaction):
                logger.error(f"Transaction {transaction.transaction_id} failed validation")
                return False

            # Add to transaction pool
            if not self.transaction_manager.add_transaction(transaction):
                return False

            # Process any cross-shard aspects
            if transaction.cross_shard_refs or 'target_shard' in transaction.data:
                self.cross_shard_manager.process_transaction(transaction)

            return True

        except Exception as e:
            logger.error(f"Failed to add transaction: {str(e)}")
            return False

    def create_block(self, validator: str) -> Optional[Block]:
        """Create a new block from pending transactions."""
        if not validator:
            logger.error("Invalid validator ID")
            return None

        try:
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

            # Add cross-shard references
            cross_shard_refs = self.cross_shard_manager.get_pending_validations()
            if cross_shard_refs:
                block.cross_shard_refs.extend(ref.transaction_id for ref in cross_shard_refs)

            return block

        except Exception as e:
            logger.error(f"Failed to create block: {str(e)}")
            return None

    def add_block(self, block: Block) -> bool:
        """Add a validated block to the chain."""
        if not isinstance(block, Block):
            logger.error("Invalid block type")
            return False

        try:
            # Validate block
            if not self.validation_manager.validate_block(block, self.chain[-1] if self.chain else None):
                logger.error("Block validation failed")
                return False

            # Update state
            if not self.state_manager.update_state(block):
                logger.error("State update failed")
                return False

            # Add block to chain
            self.chain.append(block)
            self.height += 1
            self.known_validators.add(block.validator)

            # Update cross-shard references
            if block.cross_shard_refs:
                for ref_id in block.cross_shard_refs:
                    self.cross_shard_manager.validate_reference(ref_id)

            # Remove processed transactions
            tx_ids = {tx.transaction_id for tx in block.transactions}
            self.transaction_manager.remove_transactions(tx_ids)

            return True

        except Exception as e:
            logger.error(f"Failed to add block: {str(e)}")
            return False

    def validate_chain(self) -> bool:
        """Validate the entire chain."""
        try:
            if not self.chain:
                return True

            for i in range(1, len(self.chain)):
                if not self.validation_manager.validate_block(self.chain[i], self.chain[i-1]):
                    return False
            return True

        except Exception as e:
            logger.error(f"Chain validation failed: {str(e)}")
            return False

    def get_metrics(self) -> Dict[str, Any]:
        """Get comprehensive shard metrics."""
        try:
            # Core metrics
            metrics = {
                "shard_id": self.shard_id,
                "height": self.height,
                "chain_size": len(self.chain),
                "known_validators": len(self.known_validators),
                "last_block_time": self.chain[-1].timestamp.isoformat() if self.chain else None,
                "average_block_time": self.average_block_time
            }
            
            # Add metrics from each manager
            metrics.update(self.state_manager.get_metrics())
            metrics.update(self.transaction_manager.get_metrics())
            metrics.update(self.cross_shard_manager.get_metrics())
            
            return metrics

        except Exception as e:
            logger.error(f"Failed to get metrics: {str(e)}")
            return {"error": str(e)}

    def to_dict(self) -> Dict[str, Any]:
        """Convert shard state to dictionary format."""
        try:
            return {
                "shard_id": self.shard_id,
                "height": self.height,
                "chain": [block.to_dict() for block in self.chain],
                "config": self.config.to_dict(),
                "known_validators": list(self.known_validators),
                "transaction_manager": self.transaction_manager.to_dict(),
                "state_manager": self.state_manager.to_dict(),
                "cross_shard_manager": self.cross_shard_manager.to_dict(),
                "version": "1.0"
            }
        except Exception as e:
            logger.error(f"Failed to convert shard to dictionary: {str(e)}")
            raise

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Shard':
        """Create shard from dictionary data."""
        try:
            # Create instance with config
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
            
        except Exception as e:
            logger.error(f"Failed to create shard from dictionary: {str(e)}")
            raise

    def __str__(self) -> str:
        """String representation."""
        return f"Shard(id={self.shard_id}, height={self.height}, validators={len(self.known_validators)})"