"""
types.py

This module defines data structures and types used within the Proof of Cooperation (PoC) consensus mechanism.
It includes configuration settings, validation results, metrics tracking, and the overall state of the consensus.

Classes:
    ConsensusConfig
    ValidationResult
    ValidatorHistory
    ValidationStats
    ConsensusMetrics
    ConsensusState
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Any
from datetime import datetime

@dataclass
class ConsensusConfig:
    """
    Configuration parameters for the PoC consensus mechanism.

    Attributes:
        min_reputation (float): Minimum reputation required for validation eligibility.
        cooldown_blocks (int): Number of blocks a validator must wait during cooldown.
        reputation_decay_factor (float): Rate at which reputation decays over time.
        collusion_threshold (float): Threshold for detecting collusion among validators.
        reputation_weights (Dict[str, float]): Weights for different reputation categories.
        validation_thresholds (Dict[str, float]): Thresholds for validation metrics.
    """
    min_reputation: float = 10.0
    cooldown_blocks: int = 3
    reputation_decay_factor: float = 0.95
    collusion_threshold: float = 0.75
    
    reputation_weights: Dict[str, float] = field(default_factory=lambda: {
        "cooperative_growth": 1.5,
        "proposal_participation": 1.2,
        "transaction_validation": 1.3,
        "resource_sharing": 1.3,
        "conflict_resolution": 1.1,
        "community_building": 1.2,
        "sustainability": 1.2,
        "innovation": 1.3,
        "network_stability": 1.4,
        "data_availability": 1.2,
    })
    
    validation_thresholds: Dict[str, float] = field(default_factory=lambda: {
        "min_participation": 0.05,
        "min_success_rate": 0.4,
        "min_availability": 0.6,
        "max_consecutive_validations": 3,
        "new_node_reputation_factor": 0.3,
        "min_interactions": 3,
    })

@dataclass
class ValidationResult:
    """
    Represents the result of a validation operation.

    Attributes:
        success (bool): Indicates whether the validation was successful.
        reason (Optional[str]): Reason for failure, if applicable.
        metrics (Dict[str, Any]): Additional metrics related to the validation.
    """
    success: bool
    reason: Optional[str] = None
    metrics: Dict[str, Any] = field(default_factory=dict)

@dataclass
class ValidatorHistory:
    """
    Tracks the activity of a validator within the PoC network.

    Attributes:
        node_id (str): Identifier of the validator node.
        timestamp (datetime): Time of the validation event.
        shard_id (Optional[int]): Shard ID where validation occurred.
        success (bool): Indicates whether the validation was successful.
        metrics (Dict[str, Any]): Additional metrics related to the validation event.
    """
    node_id: str
    timestamp: datetime
    shard_id: Optional[int]
    success: bool = True
    metrics: Dict[str, Any] = field(default_factory=dict)

@dataclass
class ValidationStats:
    """
    Tracks validation statistics for individual nodes.

    Attributes:
        selections (int): Number of times the node was selected for validation.
        successful_validations (int): Number of successful validations.
        consecutive_failures (int): Number of consecutive failed validations.
        last_validation (Optional[datetime]): Timestamp of the last validation.
        shard_validations (Dict[int, Dict[str, Any]]): Shard-specific validation metrics.
    """
    selections: int = 0
    successful_validations: int = 0
    consecutive_failures: int = 0
    last_validation: Optional[datetime] = None
    shard_validations: Dict[int, Dict[str, Any]] = field(default_factory=dict)

@dataclass
class ConsensusMetrics:
    """
    Tracks metrics related to the PoC consensus mechanism.

    Attributes:
        total_validations (int): Total number of validations performed.
        successful_validations (int): Total number of successful validations.
        failed_validations (int): Total number of failed validations.
        collusion_detections (int): Number of detected collusion events.
        total_blocks_validated (int): Total number of blocks validated.
        new_node_participations (int): Number of participations by new nodes.
        average_block_time (float): Average time taken to validate blocks.
        validator_counts (Dict[str, int]): Number of validations per validator.
        shard_metrics (Dict[int, Dict[str, Any]]): Shard-specific metrics.
    """
    total_validations: int = 0
    successful_validations: int = 0
    failed_validations: int = 0
    collusion_detections: int = 0
    total_blocks_validated: int = 0
    new_node_participations: int = 0
    average_block_time: float = 0.0
    validator_counts: Dict[str, int] = field(default_factory=dict)
    shard_metrics: Dict[int, Dict[str, Any]] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert metrics to dictionary format.

        Returns:
            Dict[str, Any]: Dictionary representation of the metrics.
        """
        return {
            "total_validations": self.total_validations,
            "successful_validations": self.successful_validations,
            "failed_validations": self.failed_validations,
            "collusion_detections": self.collusion_detections,
            "total_blocks_validated": self.total_blocks_validated,
            "new_node_participations": self.new_node_participations,
            "average_block_time": self.average_block_time,
            "validator_counts": self.validator_counts.copy(),
            "shard_metrics": {
                shard_id: metrics.copy() 
                for shard_id, metrics in self.shard_metrics.items()
            }
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ConsensusMetrics':
        """
        Create ConsensusMetrics from dictionary data.

        Args:
            data (Dict[str, Any]): Dictionary containing metrics data.

        Returns:
            ConsensusMetrics: An instance of ConsensusMetrics populated with the provided data.
        """
        metrics = cls()
        metrics.total_validations = data.get("total_validations", 0)
        metrics.successful_validations = data.get("successful_validations", 0)
        metrics.failed_validations = data.get("failed_validations", 0)
        metrics.collusion_detections = data.get("collusion_detections", 0)
        metrics.total_blocks_validated = data.get("total_blocks_validated", 0)
        metrics.new_node_participations = data.get("new_node_participations", 0)
        metrics.average_block_time = data.get("average_block_time", 0.0)
        metrics.validator_counts = data.get("validator_counts", {}).copy()
        metrics.shard_metrics = data.get("shard_metrics", {}).copy()
        return metrics

@dataclass
class ConsensusState:
    """
    Represents the current state of the PoC consensus mechanism.

    Attributes:
        config (ConsensusConfig): Configuration parameters of the consensus.
        metrics (ConsensusMetrics): Metrics tracking consensus operations.
        validator_history (List[ValidatorHistory]): History of validator activities.
        validation_stats (Dict[str, ValidationStats]): Validation statistics by node.
        active_validators (Set[str]): Set of currently active validators.
    """
    config: ConsensusConfig
    metrics: ConsensusMetrics = field(default_factory=ConsensusMetrics)
    validator_history: List[ValidatorHistory] = field(default_factory=list)
    validation_stats: Dict[str, ValidationStats] = field(default_factory=dict)
    active_validators: Set[str] = field(default_factory=set)

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert state to dictionary format.

        Returns:
            Dict[str, Any]: Dictionary representation of the consensus state.
        """
        return {
            "config": {
                "min_reputation": self.config.min_reputation,
                "cooldown_blocks": self.config.cooldown_blocks,
                "reputation_decay_factor": self.config.reputation_decay_factor,
                "collusion_threshold": self.config.collusion_threshold,
                "reputation_weights": self.config.reputation_weights.copy(),
                "validation_thresholds": self.config.validation_thresholds.copy()
            },
            "metrics": self.metrics.to_dict(),
            "validator_history": [
                {
                    "node_id": h.node_id,
                    "timestamp": h.timestamp.isoformat(),
                    "shard_id": h.shard_id,
                    "success": h.success,
                    "metrics": h.metrics.copy()
                }
                for h in self.validator_history
            ],
            "validation_stats": {
                node_id: {
                    "selections": stats.selections,
                    "successful_validations": stats.successful_validations,
                    "consecutive_failures": stats.consecutive_failures,
                    "last_validation": stats.last_validation.isoformat() if stats.last_validation else None,
                    "shard_validations": stats.shard_validations.copy()
                }
                for node_id, stats in self.validation_stats.items()
            },
            "active_validators": list(self.active_validators)
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ConsensusState':
        """
        Create ConsensusState from dictionary data.

        Args:
            data (Dict[str, Any]): Dictionary containing consensus state data.

        Returns:
            ConsensusState: An instance of ConsensusState populated with the provided data.
        """
        config = ConsensusConfig(
            min_reputation=data["config"]["min_reputation"],
            cooldown_blocks=data["config"]["cooldown_blocks"],
            reputation_decay_factor=data["config"]["reputation_decay_factor"],
            collusion_threshold=data["config"]["collusion_threshold"]
        )
        config.reputation_weights = data["config"]["reputation_weights"].copy()
        config.validation_thresholds = data["config"]["validation_thresholds"].copy()

        metrics = ConsensusMetrics.from_dict(data["metrics"])

        validator_history = [
            ValidatorHistory(
                node_id=h["node_id"],
                timestamp=datetime.fromisoformat(h["timestamp"]),
                shard_id=h["shard_id"],
                success=h["success"],
                metrics=h["metrics"].copy()
            )
            for h in data["validator_history"]
        ]

        validation_stats = {
            node_id: ValidationStats(
                selections=stats["selections"],
                successful_validations=stats["successful_validations"],
                consecutive_failures=stats["consecutive_failures"],
                last_validation=datetime.fromisoformat(stats["last_validation"]) if stats["last_validation"] else None,
                shard_validations=stats["shard_validations"].copy()
            )
            for node_id, stats in data["validation_stats"].items()
        }

        active_validators = set(data["active_validators"])

        return cls(
            config=config,
            metrics=metrics,
            validator_history=validator_history,
            validation_stats=validation_stats,
            active_validators=active_validators
        )
