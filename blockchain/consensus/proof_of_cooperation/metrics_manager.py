# blockchain/consensus/proof_of_cooperation/metrics_manager.py

import logging
from typing import Dict, Optional, Any
from datetime import datetime, timedelta
from .types import ConsensusMetrics, ValidationResult, ValidatorHistory

logger = logging.getLogger(__name__)

class MetricsManager:
    """
    Manages performance metrics and statistics for the consensus mechanism.
    Tracks validation success rates, block times, and other performance indicators.
    """

    def __init__(self):
        """Initialize the metrics manager."""
        self.metrics = ConsensusMetrics()
        self.last_metrics_reset = datetime.now()
        self.reset_interval = timedelta(hours=24)

    def record_validation(self, result: ValidationResult, validator_id: str, shard_id: Optional[int] = None) -> None:
        """
        Record the result of a validation attempt.

        Args:
            result: The validation result
            validator_id: ID of the validator
            shard_id: Optional shard ID where validation occurred
        """
        try:
            self.metrics.total_validations += 1
            
            if result.success:
                self.metrics.successful_validations += 1
            else:
                self.metrics.failed_validations += 1

            # Track per-validator metrics
            if validator_id not in self.metrics.validator_counts:
                self.metrics.validator_counts[validator_id] = 0
            self.metrics.validator_counts[validator_id] += 1

            # Track shard-specific metrics if applicable
            if shard_id is not None:
                if shard_id not in self.metrics.shard_metrics:
                    self.metrics.shard_metrics[shard_id] = {
                        "validations": 0,
                        "successful": 0,
                        "failed": 0,
                        "unique_validators": set()
                    }
                
                shard_metrics = self.metrics.shard_metrics[shard_id]
                shard_metrics["validations"] += 1
                if result.success:
                    shard_metrics["successful"] += 1
                else:
                    shard_metrics["failed"] += 1
                shard_metrics["unique_validators"].add(validator_id)

            # Add any custom metrics from the validation result
            self._update_custom_metrics(result.metrics)

        except Exception as e:
            logger.error(f"Failed to record validation metrics: {str(e)}")

    def record_block_time(self, block_time: float) -> None:
        """
        Record the time taken to create a block.

        Args:
            block_time: Time in seconds to create the block
        """
        try:
            current_avg = self.metrics.average_block_time
            total_blocks = self.metrics.total_blocks_validated
            
            # Update running average
            self.metrics.average_block_time = (
                (current_avg * total_blocks + block_time) / (total_blocks + 1)
            )
            self.metrics.total_blocks_validated += 1

        except Exception as e:
            logger.error(f"Failed to record block time: {str(e)}")

    def record_collusion_detection(self) -> None:
        """Record a collusion detection event."""
        self.metrics.collusion_detections += 1

    def record_new_node_participation(self) -> None:
        """Record participation by a new node."""
        self.metrics.new_node_participations += 1

    def get_validator_performance(self, validator_id: str) -> Dict[str, Any]:
        """
        Get performance metrics for a specific validator.

        Args:
            validator_id: ID of the validator

        Returns:
            Dict containing validator's performance metrics
        """
        try:
            total_validations = self.metrics.validator_counts.get(validator_id, 0)
            if total_validations == 0:
                return {
                    "total_validations": 0,
                    "success_rate": 0.0,
                    "shard_participation": {}
                }

            # Calculate success rate for this validator
            validator_successes = sum(
                1 for s in self.metrics.shard_metrics.values()
                if validator_id in s["unique_validators"] and s["successful"] > 0
            )
            success_rate = validator_successes / total_validations

            # Calculate shard participation
            shard_participation = {
                shard_id: {
                    "validations": metrics["validations"],
                    "success_rate": metrics["successful"] / metrics["validations"]
                    if metrics["validations"] > 0 else 0.0
                }
                for shard_id, metrics in self.metrics.shard_metrics.items()
                if validator_id in metrics["unique_validators"]
            }

            return {
                "total_validations": total_validations,
                "success_rate": success_rate,
                "shard_participation": shard_participation
            }

        except Exception as e:
            logger.error(f"Failed to get validator performance: {str(e)}")
            return {"error": str(e)}

    def get_shard_metrics(self, shard_id: int) -> Dict[str, Any]:
        """
        Get metrics for a specific shard.

        Args:
            shard_id: ID of the shard

        Returns:
            Dict containing shard metrics
        """
        try:
            if shard_id not in self.metrics.shard_metrics:
                return {
                    "validations": 0,
                    "successful": 0,
                    "failed": 0,
                    "validator_count": 0,
                    "success_rate": 0.0
                }

            metrics = self.metrics.shard_metrics[shard_id]
            total = metrics["validations"]
            
            return {
                "validations": total,
                "successful": metrics["successful"],
                "failed": metrics["failed"],
                "validator_count": len(metrics["unique_validators"]),
                "success_rate": metrics["successful"] / total if total > 0 else 0.0
            }

        except Exception as e:
            logger.error(f"Failed to get shard metrics: {str(e)}")
            return {"error": str(e)}

    def get_all_metrics(self) -> Dict[str, Any]:
        """
        Get all consensus metrics.

        Returns:
            Dict containing all metrics
        """
        try:
            total_validations = self.metrics.total_validations
            return {
                "total_validations": total_validations,
                "successful_validations": self.metrics.successful_validations,
                "failed_validations": self.metrics.failed_validations,
                "success_rate": (
                    self.metrics.successful_validations / total_validations 
                    if total_validations > 0 else 0.0
                ),
                "average_block_time": self.metrics.average_block_time,
                "collusion_detections": self.metrics.collusion_detections,
                "new_node_participations": self.metrics.new_node_participations,
                "total_blocks_validated": self.metrics.total_blocks_validated,
                "active_validators": len(self.metrics.validator_counts),
                "shard_metrics": {
                    shard_id: self.get_shard_metrics(shard_id)
                    for shard_id in self.metrics.shard_metrics
                }
            }

        except Exception as e:
            logger.error(f"Failed to get all metrics: {str(e)}")
            return {"error": str(e)}

    def _update_custom_metrics(self, custom_metrics: Dict[str, Any]) -> None:
        """
        Update metrics with custom values from validation results.

        Args:
            custom_metrics: Dictionary of custom metrics to update
        """
        try:
            for key, value in custom_metrics.items():
                if not hasattr(self.metrics, key):
                    setattr(self.metrics, key, value)
                else:
                    current_value = getattr(self.metrics, key)
                    if isinstance(current_value, (int, float)):
                        setattr(self.metrics, key, current_value + value)
                    elif isinstance(current_value, dict):
                        current_value.update(value)

        except Exception as e:
            logger.error(f"Failed to update custom metrics: {str(e)}")

    def check_metrics_reset(self) -> None:
        """Check if metrics should be reset based on reset interval."""
        if datetime.now() - self.last_metrics_reset > self.reset_interval:
            self.metrics = ConsensusMetrics()
            self.last_metrics_reset = datetime.now()

    def to_dict(self) -> Dict[str, Any]:
        """Convert metrics manager state to dictionary."""
        return {
            "metrics": self.metrics.to_dict(),
            "last_reset": self.last_metrics_reset.isoformat(),
            "reset_interval_seconds": self.reset_interval.total_seconds()
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'MetricsManager':
        """Create metrics manager from dictionary data."""
        manager = cls()
        manager.metrics = ConsensusMetrics.from_dict(data["metrics"])
        manager.last_metrics_reset = datetime.fromisoformat(data["last_reset"])
        manager.reset_interval = timedelta(seconds=data["reset_interval_seconds"])
        return manager