# blockchain/core/transaction.py

from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, Optional, Any
import hashlib
import json
import logging
from copy import deepcopy

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
    _is_deserialized: bool = field(default=False, init=False, repr=False)

    def __post_init__(self) -> None:
        """Initialize transaction ID and perform validation after creation."""
        # Validate inputs
        if not self.sender:
            raise ValueError("Sender cannot be empty")
        if not self.receiver:
            raise ValueError("Receiver cannot be empty")
        if not self.action:
            raise ValueError("Action cannot be empty")
            
        # Deep copy data to prevent external modifications
        self.data = deepcopy(self.data)
        
        # Calculate transaction ID
        if not hasattr(self, 'transaction_id') or not self.transaction_id:
            self.transaction_id = self.calculate_id()

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
            "shard_id": self.shard_id
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
        # Remove signature from hash calculation
        tx_dict.pop('signature', None)
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

            # Validate data structure
            if not isinstance(self.data, dict):
                logger.error("Transaction data must be a dictionary")
                return False

            # Validate action format
            if not self.action.isalnum() or len(self.action) > 64:
                logger.error("Invalid action format")
                return False

            # Validate shard_id if present
            if self.shard_id is not None and not isinstance(self.shard_id, int):
                logger.error("Invalid shard_id type")
                return False
            
            if self.shard_id is not None and self.shard_id < 0:
                logger.error("Invalid shard_id value")
                return False

            # Verify transaction ID consistency
            if self.transaction_id != self.calculate_id():
                logger.error("Transaction ID mismatch")
                return False

            return True

        except Exception as e:
            logger.error(f"Transaction validation failed: {str(e)}")
            return False

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
        }

    @classmethod
    def from_dict(cls, data: Dict) -> Transaction:
        """
        Create a transaction instance from a dictionary.
        
        Args:
            data (Dict): The dictionary containing transaction data
            
        Returns:
            Transaction: The created transaction instance
            
        Raises:
            ValueError: If the data is invalid
        """
        try:
            timestamp = datetime.fromisoformat(data["timestamp"])
            signature = bytes.fromhex(data["signature"]) if data.get("signature") else None
            
            # Create transaction with original transaction_id
            tx = cls(
                sender=data["sender"],
                receiver=data["receiver"],
                action=data["action"],
                data=deepcopy(data["data"]),
                timestamp=timestamp,
                signature=signature,
                shard_id=data.get("shard_id")
            )
            
            # Set the original transaction_id
            tx.transaction_id = data["transaction_id"]
            tx._is_deserialized = True
            
            # Verify consistency
            if not tx._is_deserialized and tx.transaction_id != tx.calculate_id():
                raise ValueError("Transaction ID mismatch after deserialization")
            
            return tx

        except Exception as e:
            logger.error(f"Failed to create transaction from dictionary: {str(e)}")
            raise ValueError(f"Invalid transaction data: {str(e)}")

    def __str__(self) -> str:
        """
        Return a human-readable string representation.
        
        Returns:
            str: The string representation
        """
        return (
            f"Transaction(id={self.transaction_id[:8]}..., "
            f"action={self.action}, "
            f"sender={self.sender[:8]}..., "
            f"receiver={self.receiver[:8]}...)"
        )