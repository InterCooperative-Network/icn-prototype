# blockchain/system/monitoring.py
"""
System monitoring and metrics collection for the ICN blockchain.
Provides real-time monitoring, alerting, and performance tracking.
"""

from typing import Dict, List, Any, Optional
import asyncio
import logging
from datetime import datetime, timedelta
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

@dataclass
class ComponentMetrics:
    """Metrics for individual system components."""
    component_name: str
    status: str = "unknown"
    last_update: datetime = field(default_factory=datetime.now)
    metrics: Dict[str, Any] = field(default_factory=dict)
    errors: List[str] = field(default_factory=list)

@dataclass
class SystemMetrics:
    """Overall system metrics."""
    start_time: datetime = field(default_factory=datetime.now)
    components: Dict[str, ComponentMetrics] = field(default_factory=dict)
    alerts: List[Dict[str, Any]] = field(default_factory=list)
    performance_metrics: Dict[str, float] = field(default_factory=dict)

class SystemMonitor:
    """
    Monitors system health, performance, and component status.
    Provides real-time metrics and alerting.
    """

    def __init__(self, alert_handlers: Optional[List[callable]] = None):
        self.metrics = SystemMetrics()
        self.alert_handlers = alert_handlers or []
        self.is_running = False
        self.collection_interval = 30  # seconds
        self.alert_thresholds = {
            "transaction_queue_size": 1000,
            "block_time": 60,  # seconds
            "memory_usage": 0.9,  # 90%
            "peer_count": 3,  # minimum peers
            "consensus_participation": 0.66  # minimum participation rate
        }

    async def start_monitoring(self) -> None:
        """Start the monitoring system."""
        self.is_running = True
        await asyncio.gather(
            self._collect_metrics(),
            self._process_alerts(),
            self._cleanup_old_metrics()
        )

    async def stop_monitoring(self) -> None:
        """Stop the monitoring system."""
        self.is_running = False

    async def _collect_metrics(self) -> None:
        """Collect metrics from all system components."""
        while self.is_running:
            try:
                # Collect blockchain metrics
                await self._collect_blockchain_metrics()
                
                # Collect consensus metrics
                await self._collect_consensus_metrics()
                
                # Collect network metrics
                await self._collect_network_metrics()
                
                # Collect resource metrics
                await self._collect_resource_metrics()
                
                # Update system state
                await self._update_system_state()
                
                await asyncio.sleep(self.collection_interval)
                
            except Exception as e:
                logger.error(f"Error collecting metrics: {e}")
                await asyncio.sleep(5)

    async def _collect_blockchain_metrics(self) -> None:
        """Collect blockchain-specific metrics."""
        try:
            metrics = ComponentMetrics(component_name="blockchain")
            
            # Collect chain metrics
            chain_metrics = self.blockchain.get_chain_metrics()
            metrics.metrics.update({
                "chain_length": chain_metrics["chain_length"],
                "total_transactions": chain_metrics["total_transactions"],
                "average_block_time": chain_metrics["average_block_time"],
                "active_nodes": chain_metrics["active_nodes"],
                "mana_pool": chain_metrics["cooperative_mana"]
            })
            
            # Collect shard metrics
            shard_metrics = {}
            for shard_id, shard in self.blockchain.shards.items():
                shard_metrics[shard_id] = shard.get_metrics()
            metrics.metrics["shards"] = shard_metrics
            
            self.metrics.components["blockchain"] = metrics
            
        except Exception as e:
            logger.error(f"Error collecting blockchain metrics: {e}")
            self._record_component_error("blockchain", str(e))

    async def _collect_consensus_metrics(self) -> None:
        """Collect consensus-specific metrics."""
        try:
            metrics = ComponentMetrics(component_name="consensus")
            
            # Collect consensus metrics
            consensus_metrics = self.consensus.get_metrics()
            metrics.metrics.update({
                "active_validators": consensus_metrics["active_validators"],
                "successful_validations": consensus_metrics["successful_validations"],
                "failed_validations": consensus_metrics["failed_validations"],
                "average_validation_time": consensus_metrics["average_validation_time"],
                "participation_rate": self._calculate_participation_rate(consensus_metrics)
            })
            
            self.metrics.components["consensus"] = metrics
            
        except Exception as e:
            logger.error(f"Error collecting consensus metrics: {e}")
            self._record_component_error("consensus", str(e))

    async def _collect_network_metrics(self) -> None:
        """Collect network-specific metrics."""
        try:
            metrics = ComponentMetrics(component_name="network")
            
            # Collect network metrics
            network_metrics = self.network.get_metrics()
            metrics.metrics.update({
                "connected_peers": network_metrics["connected_peers"],
                "messages_sent": network_metrics["messages_sent"],
                "messages_received": network_metrics["messages_received"],
                "bandwidth_usage": network_metrics["bandwidth_usage"],
                "average_latency": network_metrics["average_latency"]
            })
            
            self.metrics.components["network"] = metrics
            
        except Exception as e:
            logger.error(f"Error collecting network metrics: {e}")
            self._record_component_error("network", str(e))

    async def _collect_resource_metrics(self) -> None:
        """Collect system resource metrics."""
        try:
            metrics = ComponentMetrics(component_name="resources")
            
            # Collect resource usage metrics
            metrics.metrics.update({
                "cpu_usage": self._get_cpu_usage(),
                "memory_usage": self._get_memory_usage(),
                "disk_usage": self._get_disk_usage(),
                "network_bandwidth": self._get_network_bandwidth()
            })
            
            self.metrics.components["resources"] = metrics
            
        except Exception as e:
            logger.error(f"Error collecting resource metrics: {e}")
            self._record_component_error("resources", str(e))

    def _record_component_error(self, component: str, error: str) -> None:
        """Record component error and update status."""
        if component in self.metrics.components:
            self.metrics.components[component].errors.append({
                "timestamp": datetime.now(),
                "error": error
            })
            self.metrics.components[component].status = "error"

    async def _process_alerts(self) -> None:
        """Process and send alerts based on metrics."""
        while self.is_running:
            try:
                current_alerts = []
                
                # Check transaction queue
                if self._check_transaction_queue_alert():
                    current_alerts.append({
                        "type": "transaction_queue",
                        "severity": "warning",
                        "message": "Transaction queue size exceeds threshold"
                    })

                # Check block time
                if self._check_block_time_alert():
                    current_alerts.append({
                        "type": "block_time",
                        "severity": "warning",
                        "message": "Block time exceeds threshold"
                    })

                # Check peer count
                if self._check_peer_count_alert():
                    current_alerts.append({
                        "type": "peer_count",
                        "severity": "critical",
                        "message": "Insufficient peer connections"
                    })

                # Process alerts
                for alert in current_alerts:
                    await self._handle_alert(alert)

                await asyncio.sleep(60)  # Check alerts every minute
                
            except Exception as e:
                logger.error(f"Error processing alerts: {e}")
                await asyncio.sleep(5)

    async def _handle_alert(self, alert: Dict[str, Any]) -> None:
        """Handle and distribute alerts."""
        try:
            # Add alert to metrics
            self.metrics.alerts.append({
                **alert,
                "timestamp": datetime.now()
            })
            
            # Call alert handlers
            for handler in self.alert_handlers:
                try:
                    await handler(alert)
                except Exception as e:
                    logger.error(f"Error in alert handler: {e}")
                    
        except Exception as e:
            logger.error(f"Error handling alert: {e}")

    def get_system_status(self) -> Dict[str, Any]:
        """Get current system status and metrics."""
        return {
            "status": self._calculate_system_status(),
            "uptime": (datetime.now() - self.metrics.start_time).total_seconds(),
            "components": {
                name: {
                    "status": metrics.status,
                    "last_update": metrics.last_update.isoformat(),
                    "metrics": metrics.metrics,
                    "recent_errors": metrics.errors[-5:]  # Last 5 errors
                }
                for name, metrics in self.metrics.components.items()
            },
            "recent_alerts": self.metrics.alerts[-10:],  # Last 10 alerts
            "performance_metrics": self.metrics.performance_metrics
        }

    def _calculate_system_status(self) -> str:
        """Calculate overall system status."""
        component_statuses = [m.status for m in self.metrics.components.values()]
        if any(status == "error" for status in component_statuses):
            return "error"
        if any(status == "warning" for status in component_statuses):
            return "warning"
        return "healthy"