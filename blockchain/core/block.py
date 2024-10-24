# ============================================================
# File: blockchain/core/block.py
# Description: Core block structure for the ICN blockchain.
# This file defines the block class used within each shard of
# the ICN blockchain. A block contains validated transactions
# and includes cryptographic links to maintain chain integrity.
# ============================================================

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
    metadata: Dict = field(default_factory=dict)
    version: str = "1.0"

    def __post_init__(self) -> None:
        """
        Post-initialization for the block instance.

        This method ensures that the block's hash and Merkle root are set
        if not provided during initialization. It also sets metadata
        indicating the block's creation time.
        """
        if not self.merkle_root:
            self.merkle_root = self.calculate_merkle_root()
        if not self.hash:
            self.hash = self.calculate_hash()
        self.metadata["created_at"] = datetime.now().isoformat()

    def calculate_merkle_root(self) -> str:
        """
        Calculate the Merkle root of the transactions in the block.

        The Merkle root is a cryptographic summary of the block's transactions,
        ensuring that any alteration in the transactions will result in a
        different root, thereby preserving integrity.

        Returns:
            str: The calculated Merkle root.
        """
        if not self.transactions:
            return hashlib.sha256(b"empty").hexdigest()

        leaves = [
            hashlib.sha256(json.dumps(tx.to_dict()).encode()).hexdigest()
            for tx in self.transactions
        ]

        while len(leaves) > 1:
            if len(leaves) % 2 == 1:
                leaves.append(leaves[-1])  # Duplicate the last leaf if odd count
            leaves = [
                hashlib.sha256((a + b).encode()).hexdigest()
                for a, b in zip(leaves[::2], leaves[1::2])
            ]

        return leaves[0]

    def calculate_hash(self) -> str:
        """
        Calculate the hash of the block.

        The block hash is a cryptographic representation of the block's contents,
        ensuring integrity and immutability within the blockchain. It uses SHA-256
        to provide a fixed-length hash.

        Returns:
            str: The calculated block hash.
        """
        block_dict = {
            "index": self.index,
            "previous_hash": self.previous_hash,
            "timestamp": self.timestamp.isoformat(),
            "merkle_root": self.merkle_root,
            "validator": self.validator,
            "nonce": self.nonce,
            "shard_id": self.shard_id,
            "cross_shard_refs": self.cross_shard_refs,
            "version": self.version,
        }
        return hashlib.sha256(
            json.dumps(block_dict, sort_keys=True).encode()
        ).hexdigest()

    def validate(self, previous_block: Optional["Block"] = None) -> bool:
        """
        Validate block structure and consistency.

        This method performs various checks to ensure that the block's
        structure, transactions, and cryptographic integrity are valid.

        Args:
            previous_block (Optional[Block]): The previous block in the chain.

        Returns:
            bool: True if the block is valid, False otherwise.
        """
        try:
            # Validate block hash
            if self.hash != self.calculate_hash():
                logger.error("Invalid block hash")
                return False

            # Validate Merkle root
            if self.merkle_root != self.calculate_merkle_root():
                logger.error("Invalid Merkle root")
                return False

            # Validate timestamp
            now = datetime.now()
            if self.timestamp > now + timedelta(minutes=5):
                logger.error("Block timestamp is in the future")
                return False

            # Validate transactions
            if not all(isinstance(tx, Transaction) for tx in self.transactions):
                logger.error("Invalid transaction type in block")
                return False

            if not all(tx.validate() for tx in self.transactions):
                logger.error("Invalid transaction in block")
                return False

            # Validate against previous block if provided
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

            return True

        except Exception as e:
            logger.error(f"Block validation failed: {str(e)}")
            return False

    def add_transaction(self, transaction: Transaction) -> bool:
        """
        Add a transaction to the block.

        Ensures that only valid transactions are added, maintaining consistency
        in terms of shard assignment and transaction integrity.

        Args:
            transaction (Transaction): The transaction to be added.

        Returns:
            bool: True if the transaction was added successfully, False otherwise.
        """
        if not transaction.validate():
            logger.error("Cannot add invalid transaction to block")
            return False

        if transaction.shard_id != self.shard_id:
            logger.error("Transaction shard_id doesn't match block")
            return False

        self.transactions.append(transaction)
        self.merkle_root = self.calculate_merkle_root()
        self.hash = self.calculate_hash()
        return True

    def to_dict(self) -> Dict:
        """
        Convert the block to a dictionary format.

        Returns:
            Dict: The dictionary representation of the block.
        """
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
            "cross_shard_refs": self.cross_shard_refs,
            "metadata": self.metadata,
            "version": self.version,
        }

    @classmethod
    def from_dict(cls, data: Dict) -> "Block":
        """
        Create a block instance from a dictionary.

        Args:
            data (Dict): The dictionary representation of the block.

        Returns:
            Block: The created block instance.

        Raises:
            ValueError: If the data is invalid or incomplete.
        """
        try:
            transactions = [Transaction.from_dict(tx) for tx in data["transactions"]]
            timestamp = datetime.fromisoformat(data["timestamp"])

            return cls(
                index=data["index"],
                previous_hash=data["previous_hash"],
                timestamp=timestamp,
                transactions=transactions,
                validator=data["validator"],
                shard_id=data["shard_id"],
                hash=data["hash"],
                nonce=data["nonce"],
                merkle_root=data["merkle_root"],
                cross_shard_refs=data.get("cross_shard_refs", []),
                metadata=data.get("metadata", {}),
                version=data.get("version", "1.0"),
            )
        except Exception as e:
            logger.error(f"Failed to create block from dictionary: {str(e)}")
            raise ValueError("Invalid block data")

    def __str__(self) -> str:
        """
        Return a human-readable string representation of the block.

        Provides a summary of the block's index, hash, transaction count,
        and shard assignment.

        Returns:
            str: The block's string representation.
        """
        return (
            f"Block(index={self.index}, "
            f"hash={self.hash[:8]}..., "
            f"tx_count={len(self.transactions)}, "
            f"shard={self.shard_id})"
        )
