# blockchain/core/transaction.py

from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, Optional, Any, List, Set
import hashlib
import json
import logging
from copy import deepcopy
import math

logger = logging.getLogger(__name__)

@dataclass
class Transaction:
    """
    Represents a transaction in the ICN blockchain.
    
    A transaction is the fundamental unit of record in the blockchain, representing
    any action or data transfer between parties in the network. Transactions in ICN
    support cooperative principles through:
    - Cross-shard operations
    - Resource sharing tracking
    - Cooperative reputation impacts
    - Fair prioritization
    """
    
    sender: str
    receiver: str
    action: str
    data: Dict[str, Any]
    timestamp: datetime = field(default_factory=datetime.now)
    signature: Optional[bytes] = None
    shard_id: Optional[int] = None
    transaction_id: str = field(init=False)
    _is_deserialized: bool = field(default=False, init=False, repr=False)
    priority: int = field(default=1)  # 1-5, with 5 being highest
    cooperative_tags: Set[str] = field(default_factory=set)
    resource_cost: Dict[str, float] = field(default_factory=lambda: {
        "computation": 1.0,
        "storage": 1.0,
        "bandwidth": 1.0
    })
    cross_shard_refs: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    # Maximum sizes for various fields
    MAX_DATA_SIZE = 1024 * 1024  # 1MB
    MAX_CROSS_SHARD_REFS = 10
    VALID_PRIORITIES = {1, 2, 3, 4, 5}
    VALID_ACTIONS = {
        "transfer", "stake", "unstake", "vote", "propose",
        "deploy", "execute", "store", "share", "validate"
    }

    def __post_init__(self) -> None:
        """Initialize transaction ID and perform validation after creation."""
        # Validate basic inputs
        if not self.sender:
            raise ValueError("Sender cannot be empty")
        if not self.receiver:
            raise ValueError("Receiver cannot be empty")
        if not self.action:
            raise ValueError("Action cannot be empty")
        
        # Validate action
        if self.action not in self.VALID_ACTIONS:
            raise ValueError(f"Invalid action. Must be one of: {self.VALID_ACTIONS}")
            
        # Validate priority
        if self.priority not in self.VALID_PRIORITIES:
            raise ValueError(f"Invalid priority. Must be between 1-5")

        # Deep copy mutable fields
        self.data = deepcopy(self.data)
        self.cooperative_tags = set(self.cooperative_tags)
        self.resource_cost = deepcopy(self.resource_cost)
        self.cross_shard_refs = list(self.cross_shard_refs)
        self.metadata = deepcopy(self.metadata)

        # Add creation metadata
        self.metadata.update({
            "created_at": datetime.now().isoformat(),
            "data_size": len(json.dumps(self.data)),
            "version": "1.0"
        })
        
        # Calculate transaction ID
        if not hasattr(self, 'transaction_id') or not self.transaction_id:
            self.transaction_id = self.calculate_id()

        # Calculate and store resource costs
        self._calculate_resource_costs()

    def _calculate_resource_costs(self) -> None:
        """Calculate resource costs based on transaction characteristics."""
        data_size = len(json.dumps(self.data))
        
        # Base computation cost
        self.resource_cost["computation"] = 1.0
        
        # Storage cost based on data size
        self.resource_cost["storage"] = math.ceil(data_size / 1024)  # Cost per KB
        
        # Bandwidth cost including cross-shard overhead
        self.resource_cost["bandwidth"] = (
            math.ceil(data_size / 1024) * 
            (1 + 0.2 * len(self.cross_shard_refs))  # 20% overhead per cross-shard ref
        )

    def calculate_id(self) -> str:
        """
        Calculate unique transaction ID using transaction data.
        
        Returns:
            str: The calculated transaction ID
        """
        tx_data = {
            "sender": self.sender,
            "receiver": self.receiver,
            "action": self.action,
            "data": self.data,
            "timestamp": self.timestamp.isoformat(),
            "shard_id": self.shard_id,
            "priority": self.priority,
            "cooperative_tags": sorted(list(self.cooperative_tags))
        }
        tx_json = json.dumps(tx_data, sort_keys=True)
        return hashlib.sha256(tx_json.encode()).hexdigest()

    def calculate_hash(self) -> str:
        """
        Calculate cryptographic hash of the transaction.
        
        Returns:
            str: The calculated hash
        """
        tx_dict = self.to_dict()
        tx_dict.pop('signature', None)  # Remove signature from hash calculation
        tx_json = json.dumps(tx_dict, sort_keys=True)
        return hashlib.sha256(tx_json.encode()).hexdigest()

    def validate(self) -> bool:
        """
        Validate the transaction's structure and data.
        
        Returns:
            bool: True if the transaction is valid
        """
        try:
            # Validate required fields
            if not all([self.sender, self.receiver, self.action]):
                logger.error("Missing required transaction fields")
                return False

            # Validate timestamp
            now = datetime.now()
            if self.timestamp > now + timedelta(minutes=5):
                logger.error(f"Transaction timestamp {self.timestamp} is in the future")
                return False

            if self.timestamp < now - timedelta(days=1):
                logger.error(f"Transaction timestamp {self.timestamp} is too old")
                return False

            # Validate data structure and size
            if not isinstance(self.data, dict):
                logger.error("Transaction data must be a dictionary")
                return False

            if len(json.dumps(self.data)) > self.MAX_DATA_SIZE:
                logger.error("Transaction data exceeds maximum size")
                return False

            # Validate action
            if self.action not in self.VALID_ACTIONS:
                logger.error(f"Invalid action: {self.action}")
                return False

            # Validate shard_id if present
            if self.shard_id is not None:
                if not isinstance(self.shard_id, int) or self.shard_id < 0:
                    logger.error("Invalid shard_id value")
                    return False

            # Validate cross-shard references
            if len(self.cross_shard_refs) > self.MAX_CROSS_SHARD_REFS:
                logger.error("Too many cross-shard references")
                return False

            # Validate resource costs
            if not all(cost >= 0 for cost in self.resource_cost.values()):
                logger.error("Invalid resource costs")
                return False

            # Verify transaction ID consistency
            if self.transaction_id != self.calculate_id():
                logger.error("Transaction ID mismatch")
                return False

            return True

        except Exception as e:
            logger.error(f"Transaction validation failed: {str(e)}")
            return False

    def is_cross_shard(self) -> bool:
        """Check if this is a cross-shard transaction."""
        return bool(self.cross_shard_refs) or 'target_shard' in self.data

    def get_target_shards(self) -> Set[int]:
        """Get all shards involved in this transaction."""
        shards = {self.shard_id} if self.shard_id is not None else set()
        if 'target_shard' in self.data:
            shards.add(self.data['target_shard'])
        return shards

    def get_resource_impact(self) -> float:
        """Calculate total resource impact of the transaction."""
        return sum(self.resource_cost.values())

    def get_cooperative_score(self) -> float:
        """Calculate cooperative impact score of the transaction."""
        base_score = 1.0
        
        # Bonus for cooperative tags
        if self.cooperative_tags:
            base_score += 0.1 * len(self.cooperative_tags)
            
        # Penalty for high resource usage
        resource_impact = self.get_resource_impact()
        if resource_impact > 10:
            base_score *= 0.9
            
        # Bonus for cross-shard cooperation
        if self.is_cross_shard():
            base_score *= 1.1
            
        return base_score

    def to_dict(self) -> Dict:
        """
        Convert transaction to dictionary format.
        
        Returns:
            Dict: The dictionary representation
        """
        return {
            "transaction_id": self.transaction_id,
            "sender": self.sender,
            "receiver": self.receiver,
            "action": self.action,
            "data": deepcopy(self.data),
            "timestamp": self.timestamp.isoformat(),
            "signature": self.signature.hex() if self.signature else None,
            "shard_id": self.shard_id,
            "priority": self.priority,
            "cooperative_tags": sorted(list(self.cooperative_tags)),
            "resource_cost": deepcopy(self.resource_cost),
            "cross_shard_refs": self.cross_shard_refs.copy(),
            "metadata": deepcopy(self.metadata)
        }

    @classmethod
    def from_dict(cls, data: Dict) -> Transaction:
        """Create transaction instance from dictionary data."""
        try:
            # Extract and convert fields
            timestamp = datetime.fromisoformat(data["timestamp"])
            signature = bytes.fromhex(data["signature"]) if data.get("signature") else None
            cooperative_tags = set(data.get("cooperative_tags", []))
            resource_cost = deepcopy(data.get("resource_cost", {
                "computation": 1.0,
                "storage": 1.0,
                "bandwidth": 1.0
            }))
            
            # Create transaction
            tx = cls(
                sender=data["sender"],
                receiver=data["receiver"],
                action=data["action"],
                data=deepcopy(data["data"]),
                timestamp=timestamp,
                signature=signature,
                shard_id=data.get("shard_id"),
                priority=data.get("priority", 1),
                cooperative_tags=cooperative_tags,
                resource_cost=resource_cost,
                cross_shard_refs=data.get("cross_shard_refs", []),
                metadata=data.get("metadata", {})
            )
            
            # Set the original transaction_id
            tx.transaction_id = data["transaction_id"]
            tx._is_deserialized = True
            
            return tx

        except Exception as e:
            logger.error(f"Failed to create transaction from dictionary: {str(e)}")
            raise ValueError(f"Invalid transaction data: {str(e)}")

    def __str__(self) -> str:
        """Return a human-readable string representation."""
        return (
            f"Transaction(id={self.transaction_id[:8]}..., "
            f"action={self.action}, "
            f"sender={self.sender[:8]}..., "
            f"receiver={self.receiver[:8]}..., "
            f"shard={self.shard_id}, "
            f"priority={self.priority})"
        )