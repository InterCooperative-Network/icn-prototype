# ============================================================
# File: blockchain/core/block.py
# Description: Core block structure for the ICN blockchain.
# This file defines the block class used within each shard of
# the ICN blockchain. A block contains validated transactions
# and includes cryptographic links to maintain chain integrity.
# ============================================================

# blockchain/core/block.py

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import hashlib
import json
import logging

from .transaction import Transaction

logger = logging.getLogger(__name__)

@dataclass
class Block:
    """
    Represents a block in the ICN blockchain.
    
    A block is the fundamental unit of the blockchain, containing a list
    of transactions and cryptographic links to ensure immutability and
    integrity. Each block is validated by a node within a specific shard.
    """

    index: int
    previous_hash: str
    timestamp: datetime
    transactions: List[Transaction]
    validator: str
    shard_id: int
    hash: str = ""
    nonce: int = 0
    merkle_root: str = ""
    cross_shard_refs: List[str] = field(default_factory=list)
    metadata: Dict = field(default_factory=lambda: {
        "created_at": datetime.now().isoformat(),
        "version": "1.0"
    })
    version: str = "1.0"
    
    def __post_init__(self) -> None:
        """Initialize block after creation."""
        # Sort transactions by priority
        self.transactions.sort(key=lambda tx: (-tx.priority, tx.timestamp))
        
        # Calculate merkle root if not provided
        if not self.merkle_root:
            self.merkle_root = self.calculate_merkle_root()
        
        # Calculate hash if not provided
        if not self.hash:
            self.hash = self.calculate_hash()
            
        # Initialize metadata if not provided
        if "created_at" not in self.metadata:
            self.metadata["created_at"] = datetime.now().isoformat()
        if "version" not in self.metadata:
            self.metadata["version"] = self.version

    def calculate_merkle_root(self) -> str:
        """Calculate the Merkle root of transactions."""
        if not self.transactions:
            return hashlib.sha256(b"empty").hexdigest()
        
        # Create leaf nodes from transactions
        leaves = [tx.calculate_hash() for tx in self.transactions]
        
        # Build Merkle tree
        while len(leaves) > 1:
            if len(leaves) % 2 == 1:
                leaves.append(leaves[-1])
            leaves = [
                hashlib.sha256((a + b).encode()).hexdigest()
                for a, b in zip(leaves[::2], leaves[1::2])
            ]
        
        return leaves[0]

    def calculate_hash(self) -> str:
        """Calculate the hash of the block."""
        block_dict = {
            "index": self.index,
            "previous_hash": self.previous_hash,
            "timestamp": self.timestamp.isoformat(),
            "merkle_root": self.merkle_root,
            "validator": self.validator,
            "nonce": self.nonce,
            "shard_id": self.shard_id,
            "cross_shard_refs": sorted(self.cross_shard_refs),
            "version": self.version,
            "transaction_ids": [tx.transaction_id for tx in self.transactions]
        }
        block_json = json.dumps(block_dict, sort_keys=True)
        return hashlib.sha256(block_json.encode()).hexdigest()

    def add_transaction(self, transaction: Transaction) -> bool:
        """Add a transaction to the block."""
        try:
            # Validate shard assignment
            if transaction.shard_id != self.shard_id:
                logger.error(f"Transaction shard_id mismatch: {transaction.shard_id} != {self.shard_id}")
                return False
            
            # Check for duplicate
            if any(tx.transaction_id == transaction.transaction_id for tx in self.transactions):
                logger.error(f"Duplicate transaction: {transaction.transaction_id}")
                return False
            
            # Add transaction
            self.transactions.append(transaction)
            
            # Resort transactions by priority
            self.transactions.sort(key=lambda tx: (-tx.priority, tx.timestamp))
            
            # Update merkle root and hash
            self.merkle_root = self.calculate_merkle_root()
            self.hash = self.calculate_hash()
            
            return True
            
        except Exception as e:
            logger.error(f"Error adding transaction: {str(e)}")
            return False

    def validate(self, previous_block: Optional['Block'] = None) -> bool:
        """Validate block structure and consistency."""
        try:
            # Validate hash
            current_hash = self.calculate_hash()
            if self.hash != current_hash:
                logger.error("Invalid block hash")
                return False

            # Validate merkle root
            current_merkle_root = self.calculate_merkle_root()
            if self.merkle_root != current_merkle_root:
                logger.error("Invalid merkle root")
                return False

            # Validate timestamp
            if self.timestamp > datetime.now() + timedelta(minutes=5):
                logger.error("Block timestamp is in the future")
                return False

            # Validate transactions
            if not all(tx.validate() for tx in self.transactions):
                logger.error("Invalid transactions in block")
                return False

            # Validate against previous block
            if previous_block:
                if self.previous_hash != previous_block.hash:
                    logger.error("Invalid previous hash")
                    return False
                
                if self.index != previous_block.index + 1:
                    logger.error("Invalid block index")
                    return False
                
                if self.timestamp <= previous_block.timestamp:
                    logger.error("Invalid timestamp sequence")
                    return False
                    
                if self.shard_id != previous_block.shard_id:
                    logger.error("Shard ID mismatch")
                    return False

            return True

        except Exception as e:
            logger.error(f"Block validation failed: {str(e)}")
            return False

    def to_dict(self) -> Dict:
        """Convert block to dictionary format."""
        return {
            "index": self.index,
            "previous_hash": self.previous_hash,
            "timestamp": self.timestamp.isoformat(),
            "transactions": [tx.to_dict() for tx in self.transactions],
            "validator": self.validator,
            "hash": self.hash,
            "nonce": self.nonce,
            "merkle_root": self.merkle_root,
            "shard_id": self.shard_id,
            "cross_shard_refs": self.cross_shard_refs.copy(),
            "metadata": self.metadata.copy(),
            "version": self.version
        }

    @classmethod
    def from_dict(cls, data: Dict) -> 'Block':
        """Create block instance from dictionary."""
        transactions = [Transaction.from_dict(tx) for tx in data["transactions"]]
        timestamp = datetime.fromisoformat(data["timestamp"])
        
        block = cls(
            index=data["index"],
            previous_hash=data["previous_hash"],
            timestamp=timestamp,
            transactions=transactions,
            validator=data["validator"],
            shard_id=data["shard_id"],
            hash=data["hash"],
            nonce=data["nonce"],
            merkle_root=data["merkle_root"],
            cross_shard_refs=data.get("cross_shard_refs", []).copy(),
            metadata=data.get("metadata", {}).copy(),
            version=data.get("version", "1.0")
        )
        
        return block

    def __str__(self) -> str:
        """Return human-readable string representation."""
        return (
            f"Block(index={self.index}, "
            f"hash={self.hash[:8]}..., "
            f"tx_count={len(self.transactions)}, "
            f"shard={self.shard_id})"
        )