# blockchain/core/shard.py

from datetime import datetime
from typing import List, Dict, Optional
import logging
from .transaction import Transaction
from .block import Block

logger = logging.getLogger(__name__)


class Shard:
    """
    Represents a blockchain shard.

    A shard is a partition of the blockchain that processes a subset of transactions,
    enabling parallel processing and improved scalability.
    """

    def __init__(self, shard_id: int, max_transactions_per_block: int = 100):
        self.shard_id = shard_id
        self.chain: List[Block] = []
        self.pending_transactions: List[Transaction] = []
        self.height = 0
        self.max_transactions_per_block = max_transactions_per_block
        self.last_block_time = datetime.now()
        self.state: Dict = {}  # Shard-specific state
        self.metrics: Dict = {
            "total_transactions": 0,
            "average_block_time": 0,
            "blocks_created": 0,
            "pending_count": 0,
        }
        self._create_genesis_block()

    def _create_genesis_block(self) -> None:
        """Create genesis block for this shard."""
        genesis_block = Block(
            index=0,
            previous_hash="0" * 64,
            timestamp=datetime.now(),
            transactions=[],
            validator="genesis",
            shard_id=self.shard_id,
        )
        self.chain.append(genesis_block)
        self.height = 1
        self.last_block_time = genesis_block.timestamp
        self.metrics["blocks_created"] = 1
        logger.info(f"Created genesis block for shard {self.shard_id}")

    def add_transaction(self, transaction: Transaction) -> bool:
        """Add a new transaction to pending pool."""
        try:
            if transaction.shard_id != self.shard_id:
                logger.error(
                    f"Transaction shard_id {transaction.shard_id} "
                    f"doesn't match shard {self.shard_id}"
                )
                return False

            if len(self.pending_transactions) >= self.max_transactions_per_block * 2:
                logger.warning(f"Shard {self.shard_id} transaction pool full")
                return False

            if not transaction.validate():
                logger.error("Invalid transaction")
                return False

            self.pending_transactions.append(transaction)
            self.metrics["pending_count"] = len(self.pending_transactions)
            return True

        except Exception as e:
            logger.error(f"Failed to add transaction: {str(e)}")
            return False

    def create_block(self, validator: str) -> Optional[Block]:
        """Create a new block from pending transactions."""
        if not self.pending_transactions:
            logger.debug(f"No pending transactions in shard {self.shard_id}")
            return None

        try:
            transactions = self.pending_transactions[: self.max_transactions_per_block]

            new_block = Block(
                index=self.height,
                previous_hash=self.chain[-1].hash,
                timestamp=datetime.now(),
                transactions=transactions,
                validator=validator,
                shard_id=self.shard_id,
            )

            return new_block

        except Exception as e:
            logger.error(f"Failed to create block: {str(e)}")
            return None

    def add_block(self, block: Block) -> bool:
        """Add a validated block to the shard chain."""
        try:
            if block.shard_id != self.shard_id:
                logger.error(
                    f"Block shard_id {block.shard_id} "
                    f"doesn't match shard {self.shard_id}"
                )
                return False

            if block.index != self.height:
                logger.error(
                    f"Block index {block.index} "
                    f"doesn't match current height {self.height}"
                )
                return False

            if not block.validate(self.chain[-1]):
                logger.error("Block validation failed")
                return False

            # Update metrics
            block_time = (block.timestamp - self.last_block_time).total_seconds()
            self.metrics["average_block_time"] = (
                self.metrics["average_block_time"] * self.metrics["blocks_created"]
                + block_time
            ) / (self.metrics["blocks_created"] + 1)
            self.metrics["blocks_created"] += 1
            self.metrics["total_transactions"] += len(block.transactions)

            # Remove included transactions from pending pool
            tx_ids = {tx.transaction_id for tx in block.transactions}
            self.pending_transactions = [
                tx
                for tx in self.pending_transactions
                if tx.transaction_id not in tx_ids
            ]
            self.metrics["pending_count"] = len(self.pending_transactions)

            # Add block to chain
            self.chain.append(block)
            self.height += 1
            self.last_block_time = block.timestamp

            logger.info(f"Added block {block.index} to shard {self.shard_id}")
            return True

        except Exception as e:
            logger.error(f"Failed to add block: {str(e)}")
            return False

    def get_latest_block(self) -> Block:
        """Get the latest block in this shard."""
        return self.chain[-1]

    # blockchain/core/shard.py (continued)

    def validate_chain(self) -> bool:
        """Validate the entire shard chain."""
        try:
            for i in range(1, len(self.chain)):
                current_block = self.chain[i]
                previous_block = self.chain[i - 1]

                # Validate block
                if not current_block.validate(previous_block):
                    logger.error(f"Invalid block at height {i}")
                    return False

                # Validate sequence
                if current_block.index != previous_block.index + 1:
                    logger.error(f"Non-sequential blocks at height {i}")
                    return False

                # Validate timestamps
                if current_block.timestamp <= previous_block.timestamp:
                    logger.error(f"Invalid timestamp at height {i}")
                    return False

            return True

        except Exception as e:
            logger.error(f"Chain validation failed: {str(e)}")
            return False

    def get_block_by_hash(self, block_hash: str) -> Optional[Block]:
        """Retrieve a block by its hash."""
        try:
            return next(
                (block for block in self.chain if block.hash == block_hash), None
            )
        except Exception as e:
            logger.error(f"Failed to retrieve block: {str(e)}")
            return None

    def get_block_by_height(self, height: int) -> Optional[Block]:
        """Retrieve a block by its height."""
        try:
            if 0 <= height < len(self.chain):
                return self.chain[height]
            return None
        except Exception as e:
            logger.error(f"Failed to retrieve block: {str(e)}")
            return None

    def get_transaction_by_id(self, transaction_id: str) -> Optional[Transaction]:
        """Find a transaction by its ID in the chain."""
        try:
            for block in reversed(self.chain):  # Search from newest to oldest
                for tx in block.transactions:
                    if tx.transaction_id == transaction_id:
                        return tx
            return None
        except Exception as e:
            logger.error(f"Failed to retrieve transaction: {str(e)}")
            return None

    def prune_pending_transactions(self, max_age_minutes: int = 60) -> None:
        """Remove old pending transactions."""
        try:
            current_time = datetime.now()
            self.pending_transactions = [
                tx
                for tx in self.pending_transactions
                if (current_time - tx.timestamp).total_seconds() < max_age_minutes * 60
            ]
            self.metrics["pending_count"] = len(self.pending_transactions)

        except Exception as e:
            logger.error(f"Failed to prune transactions: {str(e)}")

    def get_metrics(self) -> Dict:
        """Get comprehensive shard metrics."""
        try:
            return {
                "shard_id": self.shard_id,
                "height": self.height,
                "pending_transactions": len(self.pending_transactions),
                "last_block_time": self.last_block_time.isoformat(),
                "chain_size": len(self.chain),
                "total_transactions_in_chain": sum(
                    len(block.transactions) for block in self.chain
                ),
                **self.metrics,
            }
        except Exception as e:
            logger.error(f"Failed to get metrics: {str(e)}")
            return {}

    def to_dict(self) -> Dict:
        """Convert shard state to dictionary."""
        try:
            return {
                "shard_id": self.shard_id,
                "height": self.height,
                "chain": [block.to_dict() for block in self.chain],
                "pending_transactions": [
                    tx.to_dict() for tx in self.pending_transactions
                ],
                "max_transactions_per_block": self.max_transactions_per_block,
                "last_block_time": self.last_block_time.isoformat(),
                "state": self.state,
                "metrics": self.metrics,
            }
        except Exception as e:
            logger.error(f"Failed to convert shard to dictionary: {str(e)}")
            return {}

    @classmethod
    def from_dict(cls, data: Dict) -> "Shard":
        """Create shard from dictionary."""
        try:
            shard = cls(
                shard_id=data["shard_id"],
                max_transactions_per_block=data["max_transactions_per_block"],
            )

            # Restore chain
            shard.chain = [Block.from_dict(block) for block in data["chain"]]

            # Restore pending transactions
            shard.pending_transactions = [
                Transaction.from_dict(tx) for tx in data["pending_transactions"]
            ]

            # Restore other attributes
            shard.height = data["height"]
            shard.last_block_time = datetime.fromisoformat(data["last_block_time"])
            shard.state = data["state"]
            shard.metrics = data["metrics"]

            return shard

        except Exception as e:
            logger.error(f"Failed to create shard from dictionary: {str(e)}")
            raise ValueError("Invalid shard data")

    def __str__(self) -> str:
        """Return a human-readable string representation of the shard."""
        return (
            f"Shard(id={self.shard_id}, "
            f"height={self.height}, "
            f"pending_tx={len(self.pending_transactions)}, "
            f"blocks={len(self.chain)})"
        )
