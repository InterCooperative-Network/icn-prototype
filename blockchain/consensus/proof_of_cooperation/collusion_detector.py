"""
collusion_detector.py

This module detects collusion among validators and participants in the Proof of Cooperation (PoC) consensus mechanism.
It identifies patterns that may indicate fraudulent behavior or attempts to manipulate the consensus process.

Classes:
    CollusionDetector
"""

from typing import List, Dict, Tuple
from .types import Node, Transaction, Block

class CollusionDetector:
    """
    The CollusionDetector is responsible for analyzing transaction patterns and validator behavior
    to detect potential collusion within the PoC network.
    
    Key Responsibilities:
    - Analyzing transaction patterns to identify repeated suspicious behavior.
    - Monitoring validator interactions and decisions for signs of collusion.
    - Utilizing historical data to identify anomalous patterns that could indicate fraud.
    """

    def __init__(self, transaction_threshold: int = 10, validator_threshold: int = 3):
        """
        Initialize the CollusionDetector with detection thresholds.

        Args:
            transaction_threshold (int): Number of similar transactions required to trigger a collusion check.
            validator_threshold (int): Number of validators interacting repeatedly to trigger a collusion check.
        """
        self.transaction_threshold = transaction_threshold
        self.validator_threshold = validator_threshold
        self.suspicious_transactions = []  # Stores potentially collusive transactions

    def detect_collusion(self, validator: Node, block: Block) -> bool:
        """
        Detect potential collusion in a given block based on transaction patterns and validator behavior.

        Detection Criteria:
        - Repeated transactions between the same sender and receiver within the same block.
        - Validators repeatedly validating blocks with similar transactions from the same set of senders.
        - Validators interacting unusually frequently with each other.

        Args:
            validator (Node): The validator node to check for collusion.
            block (Block): The block to analyze for collusion.

        Returns:
            bool: True if collusion is detected, False otherwise.
        """
        # Analyze transaction patterns
        if self._check_transaction_patterns(block.transactions):
            self._mark_validator_as_suspicious(validator)
            return True

        # Analyze validator interactions
        if self._check_validator_interactions(validator):
            self._mark_validator_as_suspicious(validator)
            return True

        return False

    def _check_transaction_patterns(self, transactions: List[Transaction]) -> bool:
        """
        Check transaction patterns within a block to identify suspicious behavior.

        Criteria:
        - If the same sender-receiver pair appears repeatedly within a block, it is considered suspicious.
        - If a block contains an unusually high number of similar transactions, it may indicate collusion.

        Args:
            transactions (List[Transaction]): List of transactions in the block.

        Returns:
            bool: True if suspicious patterns are found, False otherwise.
        """
        transaction_count = {}  # Maps (sender, receiver) pairs to transaction counts
        
        for tx in transactions:
            pair = (tx.sender, tx.receiver)
            transaction_count[pair] = transaction_count.get(pair, 0) + 1
            
            if transaction_count[pair] >= self.transaction_threshold:
                self.suspicious_transactions.append(tx)
                return True  # Collusion detected in transaction patterns

        return False

    def _check_validator_interactions(self, validator: Node) -> bool:
        """
        Check if a validator is interacting unusually frequently with other validators.

        Criteria:
        - If a validator consistently validates blocks from the same set of validators, it may indicate collusion.

        Args:
            validator (Node): The validator to analyze for repeated interactions.

        Returns:
            bool: True if suspicious interactions are found, False otherwise.
        """
        interaction_count = {}  # Maps validator IDs to interaction counts
        
        for interaction in validator.validation_history:
            interacting_validator = interaction.get("validator_id")
            if interacting_validator:
                interaction_count[interacting_validator] = interaction_count.get(interacting_validator, 0) + 1

                if interaction_count[interacting_validator] >= self.validator_threshold:
                    return True  # Collusion detected in validator interactions

        return False

    def _mark_validator_as_suspicious(self, validator: Node) -> None:
        """
        Mark a validator as suspicious and log the event for auditing purposes.

        Args:
            validator (Node): The validator to mark as suspicious.
        """
        validator.metadata["status"] = "suspicious"
        validator.metadata["last_suspicious_activity"] = datetime.now()
        validator.reputation -= 5  # Apply a reputation penalty
        validator.cooldown += 1  # Increase cooldown period for extra precaution

    def report_suspicious_transactions(self) -> List[Transaction]:
        """
        Generate a report of all suspicious transactions detected during collusion checks.

        Returns:
            List[Transaction]: List of suspicious transactions.
        """
        return self.suspicious_transactions
