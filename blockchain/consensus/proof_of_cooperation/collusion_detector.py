"""
collusion_detector.py

This module detects collusion among validators and participants in the Proof of Cooperation (PoC) consensus mechanism.
It identifies patterns that may indicate fraudulent behavior or attempts to manipulate the consensus process.

Classes:
    CollusionDetector
"""

from typing import List, Dict, Tuple
from datetime import datetime
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
        self.suspicious_transactions: List[Transaction] = []  # Stores potentially collusive transactions
        self.suspicious_validators: Dict[str, int] = {}  # Track suspicious validator behavior

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
        transactions_suspicious = self._check_transaction_patterns(block.transactions)

        # Analyze validator interactions
        interactions_suspicious = self._check_validator_interactions(validator)

        # Mark validator as suspicious if any collusion criteria are met
        if transactions_suspicious or interactions_suspicious:
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
        transaction_count: Dict[Tuple[str, str], int] = {}  # Maps (sender, receiver) pairs to transaction counts
        
        for tx in transactions:
            pair = (tx.sender, tx.receiver)
            transaction_count[pair] = transaction_count.get(pair, 0) + 1
            
            # If a pair exceeds the threshold, mark as suspicious
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
        interaction_count: Dict[str, int] = {}  # Maps validator IDs to interaction counts
        
        for interaction in validator.validation_history:
            interacting_validator = interaction.get("validator_id")
            if interacting_validator:
                interaction_count[interacting_validator] = interaction_count.get(interacting_validator, 0) + 1

                # If repeated interactions exceed the threshold, mark as suspicious
                if interaction_count[interacting_validator] >= self.validator_threshold:
                    return True  # Collusion detected in validator interactions

        return False

    def _mark_validator_as_suspicious(self, validator: Node) -> None:
        """
        Mark a validator as suspicious and log the event for auditing purposes.

        Args:
            validator (Node): The validator to mark as suspicious.
        """
        validator_id = validator.node_id
        self.suspicious_validators[validator_id] = self.suspicious_validators.get(validator_id, 0) + 1
        
        # Update validator metadata and reputation
        validator.metadata["status"] = "suspicious"
        validator.metadata["last_suspicious_activity"] = datetime.now()
        validator.reputation -= 5  # Apply a reputation penalty
        validator.cooldown += 1  # Increase cooldown period for extra precaution
        
        # Log suspicious activity
        self._log_suspicious_activity(validator)

    def _log_suspicious_activity(self, validator: Node) -> None:
        """
        Log details of suspicious activity for auditing and monitoring.

        Args:
            validator (Node): The validator marked as suspicious.
        """
        validator_id = validator.node_id
        print(f"[{datetime.now()}] Suspicious activity detected for validator {validator_id}.")
        print(f"Reputation reduced to {validator.reputation}, status set to 'suspicious'.")

    def report_suspicious_transactions(self) -> List[Transaction]:
        """
        Generate a report of all suspicious transactions detected during collusion checks.

        Returns:
            List[Transaction]: List of suspicious transactions.
        """
        return self.suspicious_transactions

    def report_suspicious_validators(self) -> Dict[str, int]:
        """
        Generate a report of all validators marked as suspicious during collusion checks.

        Returns:
            Dict[str, int]: Dictionary of suspicious validators and their frequency of suspicious activity.
        """
        return self.suspicious_validators

    def reset_suspicion_data(self) -> None:
        """
        Reset the collusion detector's data for suspicious transactions and validators.

        This is useful for clearing past data when starting a new detection cycle.
        """
        self.suspicious_transactions.clear()
        self.suspicious_validators.clear()
