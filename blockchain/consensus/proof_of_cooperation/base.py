"""
base.py

This module provides base classes and interfaces for the Proof of Cooperation (PoC) consensus mechanism.
It establishes the fundamental structures for validators, reputation management, and transaction processing.

Classes:
    BaseValidator
    BaseReputationSystem
    BaseTransaction
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, Union

class BaseValidator(ABC):
    """
    BaseValidator is an abstract base class that defines the fundamental structure
    for validators in the Proof of Cooperation (PoC) consensus mechanism.

    Validators are responsible for verifying transactions, creating blocks, and participating
    in shard management, all while maintaining a cooperative reputation system.
    """
    
    def __init__(self, node_id: str, cooperative_id: str):
        """
        Initialize a base validator with a unique node ID and cooperative ID.

        Args:
            node_id (str): Unique identifier for the validator node.
            cooperative_id (str): Identifier for the cooperative the node belongs to.
        """
        self.node_id = node_id
        self.cooperative_id = cooperative_id
        self.reputation: Dict[str, float] = {"validation": 0.0, "cooperation": 0.0}  # Reputation scores
        self.performance_metrics: Dict[str, Union[float, int]] = {"availability": 0.0, "success_rate": 0.0}
        self.cooldown: int = 0  # Cooldown period after validation
        self.inactivity_count: int = 0  # Track validator inactivity

    @abstractmethod
    def validate_transaction(self, transaction: Any) -> bool:
        """
        Validate a given transaction. This method must be implemented by subclasses.

        Args:
            transaction (Any): The transaction to validate.

        Returns:
            bool: True if the transaction is valid, False otherwise.
        """
        pass

    @abstractmethod
    def create_block(self) -> Optional[Any]:
        """
        Create a new block. This method must be implemented by subclasses.

        Returns:
            Optional[Any]: The newly created block, or None if block creation fails.
        """
        pass

    def enter_cooldown(self, blocks: int) -> None:
        """
        Enter a cooldown period after successful validation.

        Args:
            blocks (int): Number of blocks the validator must wait before participating again.
        """
        self.cooldown = blocks

    def decrease_cooldown(self) -> None:
        """
        Decrease the cooldown period by one block.
        """
        if self.cooldown > 0:
            self.cooldown -= 1

    def reset_performance_metrics(self) -> None:
        """
        Reset performance metrics to prepare for the next validation cycle.
        """
        self.performance_metrics = {"availability": 0.0, "success_rate": 0.0}

    def update_reputation(self, category: str, score: float) -> None:
        """
        Update the validator's reputation score for a given category.

        Args:
            category (str): The category of reputation to update (e.g., 'validation', 'cooperation').
            score (float): The score to add or subtract from the category.
        """
        if category in self.reputation:
            self.reputation[category] += score
            self.reputation[category] = max(0.0, min(self.reputation[category], 100.0))  # Cap reputation

    def apply_inactivity_decay(self) -> None:
        """
        Apply decay to the validator's reputation if it has been inactive for multiple cycles.
        """
        if self.inactivity_count > 3:
            decay_factor = 0.9
            for category in self.reputation:
                self.reputation[category] *= decay_factor

    def increment_inactivity(self) -> None:
        """
        Increment the inactivity count when the validator fails to participate.
        """
        self.inactivity_count += 1

    def reset_inactivity(self) -> None:
        """
        Reset the inactivity count when the validator successfully participates.
        """
        self.inactivity_count = 0

class BaseReputationSystem(ABC):
    """
    BaseReputationSystem is an abstract base class that defines the structure for managing
    reputation within the PoC network.

    Reputation is a critical component of the consensus mechanism, influencing validator selection,
    transaction validation, and cooperative interactions.
    """
    
    def __init__(self):
        self.reputation_scores: Dict[str, float] = {}  # Overall reputation scores for nodes

    @abstractmethod
    def update_reputation(self, node_id: str, category: str, score: float, evidence: Optional[Dict] = None) -> None:
        """
        Update the reputation score for a specific category of a node.

        Args:
            node_id (str): Identifier for the node whose reputation is being updated.
            category (str): The category of reputation to update (e.g., 'validation', 'cooperation').
            score (float): The score to add or subtract from the category.
            evidence (Optional[Dict]): Optional evidence to support the reputation change.
        """
        pass

    @abstractmethod
    def get_reputation(self, node_id: str, category: str) -> float:
        """
        Get the current reputation score for a specific category of a node.

        Args:
            node_id (str): Identifier for the node whose reputation is being retrieved.
            category (str): The category of reputation to retrieve.

        Returns:
            float: The reputation score for the specified category.
        """
        pass

    def apply_global_decay(self, decay_rate: float = 0.95) -> None:
        """
        Apply global decay to all reputation scores to incentivize continuous participation.

        Args:
            decay_rate (float): The rate at which reputation decays (default is 0.95).
        """
        for node, scores in self.reputation_scores.items():
            for category in scores:
                scores[category] *= decay_rate

class BaseTransaction(ABC):
    """
    BaseTransaction is an abstract base class that defines the structure for transactions
    within the PoC network.

    Transactions represent the fundamental operations within the network, including transfers,
    cooperative actions, and smart contract interactions.
    """
    
    def __init__(self, sender: str, receiver: str, action: str, data: Dict[str, Any]):
        """
        Initialize a base transaction with sender, receiver, action, and data.

        Args:
            sender (str): The sender's identifier.
            receiver (str): The receiver's identifier.
            action (str): The action to be performed by the transaction.
            data (Dict[str, Any]): Additional data related to the transaction.
        """
        self.sender = sender
        self.receiver = receiver
        self.action = action
        self.data = data

    @abstractmethod
    def execute(self) -> bool:
        """
        Execute the transaction. This method must be implemented by subclasses.

        Returns:
            bool: True if the transaction is executed successfully, False otherwise.
        """
        pass

    @abstractmethod
    def validate(self) -> bool:
        """
        Validate the transaction. This method must be implemented by subclasses.

        Returns:
            bool: True if the transaction is valid, False otherwise.
        """
        pass

    def get_cooperative_score(self) -> float:
        """
        Calculate the cooperative score of the transaction, based on its data and action.

        Returns:
            float: The calculated cooperative score.
        """
        return self.data.get("cooperative_score", 0.0)

    def log_transaction(self) -> None:
        """
        Log transaction details for auditing and debugging purposes.
        """
        print(f"Transaction from {self.sender} to {self.receiver} | Action: {self.action} | Data: {self.data}")
