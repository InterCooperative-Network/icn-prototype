# blockchain/core/transaction.py

from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, Optional, Any
import hashlib
import json
import logging
from uuid import uuid4

logger = logging.getLogger(__name__)


@dataclass
class Transaction:
    """
    Represents a transaction in the ICN blockchain.

    A transaction is the fundamental unit of record in the blockchain, representing
    any action or data transfer between parties in the network.
    """

    sender: str
    receiver: str
    action: str
    data: Dict[str, Any]
    timestamp: datetime = field(default_factory=datetime.now)
    signature: Optional[bytes] = None
    shard_id: Optional[int] = None
    transaction_id: str = field(init=False)

    def __post_init__(self) -> None:
        """Initialize transaction ID and perform validation after creation."""
        self.transaction_id = self.calculate_id()
        if not self.validate():
            raise ValueError("Invalid transaction parameters")

    def calculate_id(self) -> str:
        """Calculate unique transaction ID using transaction data."""
        tx_data = {
            "sender": self.sender,
            "receiver": self.receiver,
            "action": self.action,
            "data": self.data,
            "timestamp": self.timestamp.isoformat(),
            "shard_id": self.shard_id,
            "nonce": str(uuid4()),
        }
        serialized = json.dumps(tx_data, sort_keys=True)
        return hashlib.sha256(serialized.encode()).hexdigest()

    def validate(self) -> bool:
        """Validate the transaction's structure and data."""
        try:
            # Check required fields
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

            # Validate data structure
            if not isinstance(self.data, dict):
                logger.error("Transaction data must be a dictionary")
                return False

            # Validate action format
            if not self.action.isalnum() or len(self.action) > 64:
                logger.error("Invalid action format")
                return False

            # Validate IDs format
            if not all(isinstance(x, str) for x in [self.sender, self.receiver]):
                logger.error("Sender and receiver must be strings")
                return False

            # Validate shard_id if present
            if self.shard_id is not None and not isinstance(self.shard_id, int):
                logger.error("Invalid shard_id type")
                return False

            return True

        except Exception as e:
            logger.error(f"Transaction validation failed: {str(e)}")
            return False

    def to_dict(self) -> Dict[str, Any]:
        """Convert transaction to dictionary format."""
        return {
            "transaction_id": self.transaction_id,
            "sender": self.sender,
            "receiver": self.receiver,
            "action": self.action,
            "data": self.data,
            "timestamp": self.timestamp.isoformat(),
            "signature": self.signature.hex() if self.signature else None,
            "shard_id": self.shard_id,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> Transaction:
        """Create a transaction instance from a dictionary."""
        try:
            timestamp = datetime.fromisoformat(data["timestamp"])
            signature = (
                bytes.fromhex(data["signature"]) if data.get("signature") else None
            )

            return cls(
                sender=data["sender"],
                receiver=data["receiver"],
                action=data["action"],
                data=data["data"],
                timestamp=timestamp,
                signature=signature,
                shard_id=data.get("shard_id"),
            )
        except Exception as e:
            logger.error(f"Failed to create transaction from dictionary: {str(e)}")
            raise ValueError("Invalid transaction data")

    def __str__(self) -> str:
        """Return a human-readable string representation of the transaction."""
        return (
            f"Transaction(id={self.transaction_id[:8]}..., "
            f"action={self.action}, "
            f"sender={self.sender[:8]}..., "
            f"receiver={self.receiver[:8]}...)"
        )
