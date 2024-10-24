# ================================================================
# File: blockchain/utils/validation.py
# Description: Contains validation functions for transactions,
# blocks, and cooperative interactions within the ICN. These functions
# ensure data integrity, compliance with cooperative rules, and secure
# operation of the ICN blockchain.
# ================================================================

from typing import Dict, List, Any, Optional
import hashlib
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

def validate_transaction(transaction: Dict) -> bool:
    """
    Validate a transaction based on predefined rules.

    This function checks transaction structure, required fields, and
    cryptographic integrity, ensuring that transactions comply with the
    ICN’s cooperative principles.

    Args:
        transaction (Dict): A dictionary representing a transaction.

    Returns:
        bool: True if the transaction is valid, False otherwise.
    """
    try:
        # Check required fields
        required_fields = ["transaction_id", "sender", "receiver", "amount", "signature", "timestamp"]
        for field in required_fields:
            if field not in transaction:
                logger.error(f"Transaction missing required field: {field}")
                return False

        # Validate amount
        if transaction["amount"] <= 0:
            logger.error("Transaction amount must be greater than zero")
            return False

        # Validate timestamp
        transaction_time = datetime.fromisoformat(transaction["timestamp"])
        current_time = datetime.now()
        if transaction_time > current_time + timedelta(minutes=5):
            logger.error("Transaction timestamp is in the future")
            return False

        # Verify transaction signature (placeholder logic)
        if not _verify_signature(transaction):
            logger.error("Transaction signature verification failed")
            return False

        logger.info(f"Transaction {transaction['transaction_id']} is valid")
        return True

    except Exception as e:
        logger.error(f"Transaction validation failed: {str(e)}")
        return False

def validate_block(block: Dict, previous_block: Optional[Dict] = None) -> bool:
    """
    Validate a block based on structure, integrity, and consistency rules.

    This function checks the block’s cryptographic hash, Merkle root,
    timestamp, and transactions to ensure compliance with cooperative principles.

    Args:
        block (Dict): A dictionary representing a block.
        previous_block (Optional[Dict]): The previous block in the chain.

    Returns:
        bool: True if the block is valid, False otherwise.
    """
    try:
        # Check required fields
        required_fields = ["index", "previous_hash", "timestamp", "transactions", "hash", "merkle_root"]
        for field in required_fields:
            if field not in block:
                logger.error(f"Block missing required field: {field}")
                return False

        # Validate block index
        if previous_block and block["index"] != previous_block["index"] + 1:
            logger.error("Block index is not sequential")
            return False

        # Validate previous hash
        if previous_block and block["previous_hash"] != previous_block["hash"]:
            logger.error("Block previous hash does not match")
            return False

        # Validate timestamp
        block_time = datetime.fromisoformat(block["timestamp"])
        if block_time > datetime.now() + timedelta(minutes=5):
            logger.error("Block timestamp is in the future")
            return False

        if previous_block and block_time <= datetime.fromisoformat(previous_block["timestamp"]):
            logger.error("Block timestamp is not after the previous block")
            return False

        # Validate Merkle root
        if not _validate_merkle_root(block["transactions"], block["merkle_root"]):
            logger.error("Block Merkle root validation failed")
            return False

        # Validate block hash
        if not _validate_block_hash(block):
            logger.error("Block hash validation failed")
            return False

        logger.info(f"Block {block['index']} is valid")
        return True

    except Exception as e:
        logger.error(f"Block validation failed: {str(e)}")
        return False

def _validate_merkle_root(transactions: List[Dict], expected_merkle_root: str) -> bool:
    """
    Validate the Merkle root of a list of transactions.

    Args:
        transactions (List[Dict]): List of transaction dictionaries.
        expected_merkle_root (str): The expected Merkle root hash.

    Returns:
        bool: True if the Merkle root is valid, False otherwise.
    """
    try:
        if not transactions:
            return expected_merkle_root == hashlib.sha256(b"empty").hexdigest()

        # Create leaf nodes from transactions
        leaves = [hashlib.sha256(json.dumps(tx).encode()).hexdigest() for tx in transactions]

        # Build Merkle tree
        while len(leaves) > 1:
            if len(leaves) % 2 == 1:
                leaves.append(leaves[-1])
            leaves = [
                hashlib.sha256((a + b).encode()).hexdigest()
                for a, b in zip(leaves[::2], leaves[1::2])
            ]

        return leaves[0] == expected_merkle_root

    except Exception as e:
        logger.error(f"Failed to validate Merkle root: {str(e)}")
        return False

def _validate_block_hash(block: Dict) -> bool:
    """
    Validate the block's hash by recalculating it.

    Args:
        block (Dict): A dictionary representing a block.

    Returns:
        bool: True if the hash is valid, False otherwise.
    """
    try:
        block_data = {
            "index": block["index"],
            "previous_hash": block["previous_hash"],
            "timestamp": block["timestamp"],
            "merkle_root": block["merkle_root"],
        }
        recalculated_hash = hashlib.sha256(
            json.dumps(block_data, sort_keys=True).encode()
        ).hexdigest()

        return recalculated_hash == block["hash"]

    except Exception as e:
        logger.error(f"Failed to validate block hash: {str(e)}")
        return False

def _verify_signature(transaction: Dict) -> bool:
    """
    Placeholder function to verify the transaction signature.

    This function simulates signature verification and should be replaced with
    actual cryptographic verification logic.

    Args:
        transaction (Dict): A dictionary representing a transaction.

    Returns:
        bool: True if the signature is valid, False otherwise.
    """
    # Placeholder for signature verification logic
    return True

def validate_cooperative_interaction(interaction: Dict) -> bool:
    """
    Validate cooperative interactions such as votes, proposals, and resource sharing.

    This function ensures that cooperative interactions comply with ICN rules,
    supporting fair governance and resource management.

    Args:
        interaction (Dict): A dictionary representing a cooperative interaction.

    Returns:
        bool: True if the interaction is valid, False otherwise.
    """
    try:
        # Check required fields
        required_fields = ["interaction_id", "type", "initiator", "target", "timestamp"]
        for field in required_fields:
            if field not in interaction:
                logger.error(f"Interaction missing required field: {field}")
                return False

        # Validate timestamp
        interaction_time = datetime.fromisoformat(interaction["timestamp"])
        if interaction_time > datetime.now() + timedelta(minutes=5):
            logger.error("Interaction timestamp is in the future")
            return False

        logger.info(f"Cooperative interaction {interaction['interaction_id']} is valid")
        return True

    except Exception as e:
        logger.error(f"Cooperative interaction validation failed: {str(e)}")
        return False
