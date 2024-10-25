# blockchain/core/shard/cross_shard_manager.py

from typing import Dict, List, Optional, Set
import logging
from datetime import datetime, timedelta
from .shard_types import ShardConfig, ShardMetrics, CrossShardRef
from ..block import Block
from ..transaction import Transaction

logger = logging.getLogger(__name__)

class CrossShardManager:
    """Manages cross-shard operations and references."""
    
    def __init__(self, shard_id: int, config: ShardConfig):
        self.shard_id = shard_id
        self.config = config
        self.cross_shard_refs: Dict[int, List[CrossShardRef]] = {}  # target_shard -> refs
        self.pending_validations: Dict[str, CrossShardRef] = {}  # tx_id -> ref
        self.validated_refs: Set[str] = set()  # Set of validated tx_ids
        self.metrics = ShardMetrics()
        self.last_cleanup = datetime.now()

    def process_transaction(self, transaction: Transaction) -> Optional[CrossShardRef]:
        """Process a transaction for cross-shard operations."""
        try:
            # Check if this is a cross-shard transaction
            target_shard = transaction.data.get('target_shard')
            if not target_shard or target_shard == self.shard_id:
                return None

            # Create cross-shard reference
            ref = CrossShardRef(
                source_shard=self.shard_id,
                target_shard=target_shard,
                transaction_id=transaction.transaction_id
            )

            # Add to references
            if target_shard not in self.cross_shard_refs:
                self.cross_shard_refs[target_shard] = []
            self.cross_shard_refs[target_shard].append(ref)
            
            # Add to pending validations
            self.pending_validations[transaction.transaction_id] = ref
            
            # Update metrics
            self.metrics.cross_shard_operations += 1
            
            return ref

        except Exception as e:
            logger.error(f"Failed to process cross-shard transaction: {str(e)}")
            return None

    def update_references(self, block: Block) -> None:
        """Update cross-shard references based on a new block."""
        try:
            for tx in block.transactions:
                # Process new cross-shard references
                if 'target_shard' in tx.data:
                    self.process_transaction(tx)
                
                # Check for validation confirmations
                if 'validate_ref' in tx.data:
                    self._handle_validation_confirmation(tx)

        except Exception as e:
            logger.error(f"Failed to update cross-shard references: {str(e)}")

    def validate_reference(self, ref_id: str) -> bool:
        """Validate a cross-shard reference."""
        try:
            if ref_id not in self.pending_validations:
                return False

            ref = self.pending_validations[ref_id]
            ref.status = "validated"
            ref.validation_time = datetime.now()
            
            # Move to validated set
            self.validated_refs.add(ref_id)
            del self.pending_validations[ref_id]
            
            return True

        except Exception as e:
            logger.error(f"Failed to validate reference: {str(e)}")
            return False

    def get_pending_validations(self, target_shard: Optional[int] = None) -> List[CrossShardRef]:
        """Get pending validations, optionally filtered by target shard."""
        try:
            if target_shard is not None:
                return [
                    ref for ref in self.pending_validations.values()
                    if ref.target_shard == target_shard
                ]
            return list(self.pending_validations.values())

        except Exception as e:
            logger.error(f"Failed to get pending validations: {str(e)}")
            return []

    def cleanup_expired_references(self) -> None:
        """Clean up expired cross-shard references."""
        try:
            current_time = datetime.now()
            
            # Only clean up periodically
            if (current_time - self.last_cleanup).total_seconds() < 60:
                return

            timeout = timedelta(seconds=self.config.cross_shard_timeout)
            expired_refs = []

            # Find expired references
            for tx_id, ref in self.pending_validations.items():
                if current_time - ref.created_at > timeout:
                    ref.status = "expired"
                    expired_refs.append(tx_id)

            # Remove expired references
            for tx_id in expired_refs:
                del self.pending_validations[tx_id]

            # Update cleanup timestamp
            self.last_cleanup = current_time

        except Exception as e:
            logger.error(f"Failed to cleanup expired references: {str(e)}")

    def _handle_validation_confirmation(self, transaction: Transaction) -> None:
        """Process a validation confirmation transaction."""
        try:
            ref_id = transaction.data.get('validate_ref')
            if not ref_id:
                return

            if ref_id in self.pending_validations:
                self.validate_reference(ref_id)

        except Exception as e:
            logger.error(f"Failed to handle validation confirmation: {str(e)}")

    def get_metrics(self) -> Dict:
        """Get cross-shard operation metrics."""
        return {
            'pending_validations': len(self.pending_validations),
            'validated_refs': len(self.validated_refs),
            'cross_shard_operations': self.metrics.cross_shard_operations,
            'refs_by_shard': {
                shard_id: len(refs)
                for shard_id, refs in self.cross_shard_refs.items()
            }
        }

    def to_dict(self) -> Dict:
        """Convert manager state to dictionary format."""
        return {
            'cross_shard_refs': {
                shard_id: [ref.to_dict() for ref in refs]
                for shard_id, refs in self.cross_shard_refs.items()
            },
            'pending_validations': {
                tx_id: ref.to_dict()
                for tx_id, ref in self.pending_validations.items()
            },
            'validated_refs': list(self.validated_refs),
            'metrics': self.metrics.to_dict()
        }

    @classmethod
    def from_dict(cls, data: Dict, shard_id: int, config: ShardConfig) -> 'CrossShardManager':
        """Create manager from dictionary data."""
        manager = cls(shard_id, config)
        
        # Restore cross-shard references
        for shard_id_str, refs_data in data['cross_shard_refs'].items():
            shard_id = int(shard_id_str)
            manager.cross_shard_refs[shard_id] = [
                CrossShardRef.from_dict(ref_data)
                for ref_data in refs_data
            ]
        
        # Restore pending validations
        manager.pending_validations = {
            tx_id: CrossShardRef.from_dict(ref_data)
            for tx_id, ref_data in data['pending_validations'].items()
        }
        
        # Restore validated refs and metrics
        manager.validated_refs = set(data['validated_refs'])
        manager.metrics = ShardMetrics.from_dict(data['metrics'])
        
        return manager