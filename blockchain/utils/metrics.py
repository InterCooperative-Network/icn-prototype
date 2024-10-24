# ================================================================
# File: blockchain/utils/metrics.py
# Description: This file contains functions and classes for managing
# performance and operational metrics within the ICN ecosystem.
# These metrics are used to track node performance, block creation,
# transaction validation, and overall network health.
# ================================================================

from typing import Dict, List
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

class MetricsManager:
    """
    The MetricsManager handles the collection, analysis, and reporting of
    performance metrics within the ICN. It tracks node performance, transaction
    throughput, validation success rates, and resource utilization to provide
    real-time feedback for improving cooperative efficiency.
    """

    def __init__(self):
        """
        Initialize the MetricsManager.

        This constructor sets up the basic metrics structure, including
        performance, transaction, and resource usage metrics.
        """
        self.metrics: Dict = {
            "total_blocks_created": 0,
            "total_transactions_processed": 0,
            "average_block_creation_time": 0.0,
            "average_transaction_validation_time": 0.0,
            "resource_utilization": {
                "cpu": 0.0,
                "memory": 0.0,
                "bandwidth": 0.0,
                "storage": 0.0,
            },
            "validation_success_rate": 0.0,
            "uptime": 0.0,
        }
        self.start_time = datetime.now()

    def update_block_creation(self, creation_time: float) -> None:
        """
        Update metrics related to block creation.

        Args:
            creation_time (float): Time taken to create a new block.
        """
        try:
            self.metrics["total_blocks_created"] += 1
            total_time = (
                self.metrics["average_block_creation_time"]
                * (self.metrics["total_blocks_created"] - 1)
            )
            self.metrics["average_block_creation_time"] = (
                total_time + creation_time
            ) / self.metrics["total_blocks_created"]
            logger.info("Updated block creation metrics")

        except Exception as e:
            logger.error(f"Failed to update block creation metrics: {str(e)}")

    def update_transaction_processing(self, processing_time: float) -> None:
        """
        Update metrics related to transaction processing.

        Args:
            processing_time (float): Time taken to validate a transaction.
        """
        try:
            self.metrics["total_transactions_processed"] += 1
            total_time = (
                self.metrics["average_transaction_validation_time"]
                * (self.metrics["total_transactions_processed"] - 1)
            )
            self.metrics["average_transaction_validation_time"] = (
                total_time + processing_time
            ) / self.metrics["total_transactions_processed"]
            logger.info("Updated transaction processing metrics")

        except Exception as e:
            logger.error(f"Failed to update transaction metrics: {str(e)}")

    def update_resource_utilization(self, utilization: Dict[str, float]) -> None:
        """
        Update resource utilization metrics.

        Args:
            utilization (Dict[str, float]): Resource utilization metrics for
            CPU, memory, bandwidth, and storage.
        """
        try:
            for resource, value in utilization.items():
                if resource in self.metrics["resource_utilization"]:
                    self.metrics["resource_utilization"][resource] = max(
                        0.0, value
                    )
            logger.info("Updated resource utilization metrics")

        except Exception as e:
            logger.error(f"Failed to update resource utilization: {str(e)}")

    def update_validation_success(self, successful: bool) -> None:
        """
        Update validation success rate.

        Args:
            successful (bool): True if the validation was successful, False otherwise.
        """
        try:
            total_validations = self.metrics.get("total_validations", 0) + 1
            successful_validations = self.metrics.get("successful_validations", 0)

            if successful:
                successful_validations += 1

            self.metrics["validation_success_rate"] = (
                successful_validations / total_validations * 100
            )
            self.metrics["total_validations"] = total_validations
            self.metrics["successful_validations"] = successful_validations

            logger.info("Updated validation success metrics")

        except Exception as e:
            logger.error(f"Failed to update validation success rate: {str(e)}")

    def calculate_uptime(self) -> None:
        """
        Calculate the node's uptime since the start of the MetricsManager.
        """
        try:
            uptime_seconds = (datetime.now() - self.start_time).total_seconds()
            self.metrics["uptime"] = uptime_seconds / 3600  # uptime in hours
            logger.info("Calculated node uptime")

        except Exception as e:
            logger.error(f"Failed to calculate uptime: {str(e)}")

    def get_metrics(self) -> Dict:
        """
        Retrieve the current metrics.

        Returns:
            Dict: A dictionary containing all current metrics.
        """
        try:
            self.calculate_uptime()
            return self.metrics

        except Exception as e:
            logger.error(f"Failed to get metrics: {str(e)}")
            return {}

    def reset_metrics(self) -> None:
        """
        Reset all metrics to initial values.
        """
        try:
            self.__init__()  # Re-initialize the metrics manager
            logger.info("Reset all metrics")

        except Exception as e:
            logger.error(f"Failed to reset metrics: {str(e)}")
