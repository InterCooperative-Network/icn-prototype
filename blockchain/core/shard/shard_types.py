from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Set

@dataclass
class ShardMetrics:
    """Metrics tracking for a shard."""
    total_transactions: int = 0
    average_block_time: float = 0.0
    blocks_created: int = 0
    pending_count: int = 0
    validation_failures: int = 0
    successful_blocks: int = 0
    rejected_transactions: int = 0
    total_size_bytes: int = 0
    average_transactions_per_block: float = 0.0
    cross_shard_operations: int = 0
    active_validators: int = 0
    state_size_bytes: int = 0

    def to_dict(self) -> Dict:
        """Convert metrics to dictionary format."""
        return {
            field.name: getattr(self, field.name)
            for field in self.__dataclass_fields__.values()
        }

    @classmethod
    def from_dict(cls, data: Dict) -> 'ShardMetrics':
        """Create metrics from dictionary."""
        return cls(**{
            k: v for k, v in data.items()
            if k in cls.__dataclass_fields__
        })

@dataclass
class ShardConfig:
    """Configuration for a shard."""
    max_transactions_per_block: int = 100
    max_pending_transactions: int = 200
    max_cross_shard_refs: int = 50
    pruning_interval: int = 60  # minutes
    min_block_interval: int = 1  # seconds
    max_block_size: int = 1024 * 1024  # 1MB
    max_state_size: int = 10 * 1024 * 1024  # 10MB
    max_validators: int = 100
    cross_shard_timeout: int = 300  # seconds

    def to_dict(self) -> Dict:
        """Convert config to dictionary format."""
        return {
            field.name: getattr(self, field.name)
            for field in self.__dataclass_fields__.values()
        }

    @classmethod
    def from_dict(cls, data: Dict) -> 'ShardConfig':
        """Create config from dictionary."""
        return cls(**{
            k: v for k, v in data.items()
            if k in cls.__dataclass_fields__
        })

@dataclass
class CrossShardRef:
    """Represents a cross-shard reference."""
    source_shard: int
    target_shard: int
    transaction_id: str
    created_at: datetime = field(default_factory=datetime.now)
    status: str = "pending"  # pending, validated, expired
    validation_time: Optional[datetime] = None

    def to_dict(self) -> Dict:
        """Convert reference to dictionary format."""
        return {
            "source_shard": self.source_shard,
            "target_shard": self.target_shard,
            "transaction_id": self.transaction_id,
            "created_at": self.created_at.isoformat(),
            "status": self.status,
            "validation_time": self.validation_time.isoformat() if self.validation_time else None
        }

    @classmethod
    def from_dict(cls, data: Dict) -> 'CrossShardRef':
        """Create reference from dictionary."""
        data = data.copy()
        data['created_at'] = datetime.fromisoformat(data['created_at'])
        if data.get('validation_time'):
            data['validation_time'] = datetime.fromisoformat(data['validation_time'])
        return cls(**data)