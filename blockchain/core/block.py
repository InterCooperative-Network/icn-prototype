# ============================================================
# File: blockchain/core/block.py
# Description: Core block structure for the ICN blockchain.
# This file defines the block class used within each shard of
# the ICN blockchain. A block contains validated transactions
# and includes cryptographic links to maintain chain integrity.
# ============================================================

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Set
import hashlib
import json
import logging
from copy import deepcopy
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
    metadata: Dict = field(default_factory=dict)
    version: str = "1.0"
    
    # Track transaction IDs for duplicate prevention
    _transaction_ids: Set[str] = field(default_factory=set, init=False, repr=False)
    _is_deserialized: bool = field(default=False, init=False, repr=False)

    def __post_init__(self) -> None:
        """
        Post-initialization for the block instance.

        This method ensures that the block's hash and Merkle root are set
        if not provided during initialization. It also sets metadata
        indicating the block's creation time and initializes transaction tracking.
        """
        # Initialize transaction ID tracking
        self._transaction_ids = {tx.transaction_id for tx in self.transactions}
        
        # Sort transactions by priority
        self.transactions.sort(key=lambda tx: (-tx.priority, tx.timestamp))
        
        # Process cross-shard references from transactions
        self._process_cross_shard_refs()
        
        # Sort cross-shard references for consistency
        self.cross_shard_refs = sorted(set(self.cross_shard_refs))
        
        # Ensure metadata contains creation time and cross-shard info
        if "created_at" not in self.metadata:
            self.metadata["created_at"] = datetime.now().isoformat()
        self.metadata["cross_shard_count"] = len(self.cross_shard_refs)
        
        # Only calculate merkle root and hash if they're not provided
        if not self.merkle_root and not self._is_deserialized:
            self.merkle_root = self.calculate_merkle_root()
            
        if not self.hash and not self._is_deserialized:
            self.hash = self.calculate_hash()
            
        # Validate all initial transactions match shard
        for tx in self.transactions:
            if tx.shard_id != self.shard_id:
                logger.warning(f"Transaction {tx.transaction_id} shard_id doesn't match block")

    def _process_cross_shard_refs(self) -> None:
        """Process and collect cross-shard references from transactions."""
        for tx in self.transactions:
            # Add direct cross-shard references from transaction
            if tx.cross_shard_refs:
                self.cross_shard_refs.extend(tx.cross_shard_refs)
            
            # Add reference if transaction targets another shard
            if 'target_shard' in tx.data and tx.data['target_shard'] != self.shard_id:
                cross_ref = f"shard_{tx.data['target_shard']}_{tx.transaction_id}"
                self.cross_shard_refs.append(cross_ref)

    def calculate_merkle_root(self) -> str:
        """Calculate the Merkle root of the transactions."""
        if not self.transactions:
            return hashlib.sha256(b"empty").hexdigest()

        leaves = []
        # Ensure consistent transaction serialization
        for tx in sorted(self.transactions, key=lambda t: t.transaction_id):
            tx_dict = tx.to_dict()
            # Remove any non-deterministic fields
            tx_dict.pop('timestamp', None)
            tx_json = json.dumps(tx_dict, sort_keys=True)
            leaves.append(hashlib.sha256(tx_json.encode()).hexdigest())

        while len(leaves) > 1:
            if len(leaves) % 2 == 1:
                leaves.append(leaves[-1])
            leaves = [
                hashlib.sha256((a + b).encode()).hexdigest()
                for a, b in zip(leaves[::2], leaves[1::2])
            ]

        return leaves[0] if leaves else hashlib.sha256(b"empty").hexdigest()

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
            "version": self.version
        }
        return hashlib.sha256(
            json.dumps(block_dict, sort_keys=True).encode()
        ).hexdigest()

    def validate(self, previous_block: Optional["Block"] = None) -> bool:
        """Validate block structure and consistency."""
        try:
            # Validate transactions
            if not all(isinstance(tx, Transaction) for tx in self.transactions):
                logger.error("Invalid transaction type in block")
                return False

            if not all(tx.validate() for tx in self.transactions):
                logger.error("Invalid transaction in block")
                return False

            # Validate timestamp
            now = datetime.now()
            if self.timestamp > now + timedelta(minutes=5):
                logger.error("Block timestamp is in the future")
                return False

            # Validate shard consistency
            if not all(tx.shard_id == self.shard_id for tx in self.transactions):
                logger.error("Transaction shard_id mismatch")
                return False

            # Previous block validation
            if previous_block:
                if self.previous_hash != previous_block.hash:
                    logger.error("Block's previous hash doesn't match previous block")
                    return False

                if self.index != previous_block.index + 1:
                    logger.error("Block index is not sequential")
                    return False

                if self.timestamp <= previous_block.timestamp:
                    logger.error("Block timestamp is not after previous block")
                    return False

            # Validate transaction ordering
            for i in range(len(self.transactions) - 1):
                if self.transactions[i].priority < self.transactions[i + 1].priority:
                    logger.error("Transactions not properly ordered by priority")
                    return False

            # Only validate merkle root and hash if not deserializing
            if not self._is_deserialized and self.merkle_root != self.calculate_merkle_root():
                logger.error("Invalid merkle root")
                return False

            if not self._is_deserialized and self.hash != self.calculate_hash():
                logger.error("Invalid block hash")
                return False

            return True

        except Exception as e:
            logger.error(f"Block validation failed: {str(e)}")
            return False

    def add_transaction(self, transaction: Transaction) -> bool:
        """Add a transaction to the block."""
        try:
            # Validate transaction
            if not transaction.validate():
                logger.error("Cannot add invalid transaction to block")
                return False

            if transaction.shard_id != self.shard_id:
                logger.error("Transaction shard_id doesn't match block")
                return False

            if transaction.transaction_id in self._transaction_ids:
                logger.error(f"Duplicate transaction {transaction.transaction_id}")
                return False

            # Add transaction
            self.transactions.append(transaction)
            self._transaction_ids.add(transaction.transaction_id)

            # Process new cross-shard references
            self._process_cross_shard_refs()

            # Re-sort transactions by priority
            self.transactions.sort(key=lambda tx: (-tx.priority, tx.timestamp))

            # Update block state
            self.merkle_root = self.calculate_merkle_root()
            self.hash = self.calculate_hash()

            return True

        except Exception as e:
            logger.error(f"Failed to add transaction: {str(e)}")
            return False

    def get_cross_shard_info(self) -> Dict:
        """Get information about cross-shard interactions."""
        target_shards = set()
        ref_counts = {}
        for tx in self.transactions:
            if 'target_shard' in tx.data:
                target_shards.add(tx.data['target_shard'])
            for ref in tx.cross_shard_refs:
                ref_counts[ref] = ref_counts.get(ref, 0) + 1
                
        return {
            "target_shards": sorted(list(target_shards)),
            "ref_counts": ref_counts,
            "total_refs": len(self.cross_shard_refs)
        }

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
            "cross_shard_refs": sorted(self.cross_shard_refs),
            "metadata": deepcopy(self.metadata),
            "version": self.version,
        }

    @classmethod
    def from_dict(cls, data: Dict) -> "Block":
        """Create block instance from dictionary."""
        try:
            # Mark as being deserialized to prevent recalculation
            block = cls(
                index=data["index"],
                previous_hash=data["previous_hash"],
                timestamp=datetime.fromisoformat(data["timestamp"]),
                transactions=[Transaction.from_dict(tx) for tx in data["transactions"]],
                validator=data["validator"],
                shard_id=data["shard_id"],
                hash=data["hash"],
                nonce=data["nonce"],
                merkle_root=data["merkle_root"],
                cross_shard_refs=sorted(data.get("cross_shard_refs", [])),
                metadata=deepcopy(data.get("metadata", {})),
                version=data.get("version", "1.0")
            )
            block._is_deserialized = True

            return block

        except Exception as e:
            logger.error(f"Failed to create block from dictionary: {str(e)}")
            raise ValueError(f"Invalid block data: {str(e)}")

    def __str__(self) -> str:
        """Return human-readable string representation."""
        return (
            f"Block(index={self.index}, "
            f"hash={self.hash[:8]}..., "
            f"tx_count={len(self.transactions)}, "
            f"shard={self.shard_id}, "
            f"cross_refs={len(self.cross_shard_refs)})"
        )