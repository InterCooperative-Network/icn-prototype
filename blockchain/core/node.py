from __future__ import annotations
from datetime import datetime, timedelta
from typing import Dict, List, Set, Optional
import logging
import json

logger = logging.getLogger(__name__)

class Node:
    """
    Represents a node in the ICN network.

    A node is a participant in the network that can validate transactions,
    participate in consensus, and maintain portions of the blockchain.
    """

    def __init__(
        self,
        node_id: str,
        cooperative_id: Optional[str] = None,
        initial_stake: float = 10.0,
    ):
        self.node_id = node_id
        self.cooperative_id = cooperative_id
        self.reputation_scores = {
            "validation": 0.0,
            "proposal_creation": 0.0,
            "voting": 0.0,
            "resource_sharing": 0.0,
            "cooperative_growth": 0.0,
            "community_building": 0.0,
            "conflict_resolution": 0.0,
            "transaction_validation": 0.0,
            "data_availability": 0.0,
            "network_stability": 0.0,
            "innovation": 0.0,
            "sustainability": 0.0,
        }
        self.stake = initial_stake
        self.cooperative_interactions: List[str] = []
        self.validation_history: List[Dict] = []
        self.resource_usage: Dict[str, float] = {
            "computation": 0.0,
            "storage": 0.0,
            "bandwidth": 0.0,
            "memory": 0.0,
            "energy": 0.0,
        }
        self.shard_assignments: Set[int] = set()
        self.active_shards: Dict[int, datetime] = {}
        self.last_validation = datetime.now().timestamp()
        self.total_validations = 0
        self.cooldown = 0
        self.performance_metrics: Dict[str, float] = {
            "response_time": 0.0,
            "availability": 100.0,
            "validation_success_rate": 100.0,
            "network_reliability": 100.0,
        }
        self.metadata: Dict = {
            "creation_time": datetime.now(),
            "last_active": datetime.now(),
            "version": "1.0",
            "capabilities": set(),
            "status": "active",
        }

    def update_reputation(
        self,
        category: str,
        score: float,
        cooperative_id: Optional[str] = None,
        evidence: Optional[Dict] = None,
    ) -> bool:
        """Update reputation score for a category with evidence."""
        try:
            if category not in self.reputation_scores:
                logger.error(f"Invalid reputation category: {category}")
                return False

            old_score = self.reputation_scores[category]
            self.reputation_scores[category] = max(0, old_score + score)

            if cooperative_id:
                self.cooperative_interactions.append(cooperative_id)

            if evidence:
                self.validation_history.append(
                    {
                        "timestamp": datetime.now(),
                        "category": category,
                        "score_change": score,
                        "evidence": evidence,
                    }
                )

            self.metadata["last_active"] = datetime.now()

            # Trim history if needed
            if len(self.cooperative_interactions) > 1000:
                self.cooperative_interactions = self.cooperative_interactions[-1000:]
            if len(self.validation_history) > 1000:
                self.validation_history = self.validation_history[-1000:]

            return True

        except Exception as e:
            logger.error(f"Failed to update reputation: {str(e)}")
            return False

    def assign_to_shard(self, shard_id: int) -> bool:
        """Assign node to a shard."""
        if len(self.active_shards) >= 3:  # Maximum 3 active shards per node
            logger.warning(f"Node {self.node_id} already assigned to maximum shards")
            return False

        self.shard_assignments.add(shard_id)
        self.active_shards[shard_id] = datetime.now()
        logger.info(f"Node {self.node_id} assigned to shard {shard_id}")
        return True

    def remove_from_shard(self, shard_id: int) -> bool:
        """Remove node from a shard."""
        if shard_id in self.active_shards:
            del self.active_shards[shard_id]
            self.shard_assignments.discard(shard_id)
            logger.info(f"Node {self.node_id} removed from shard {shard_id}")
            return True
        return False

    def can_validate(self, shard_id: Optional[int] = None) -> bool:
        """Check if node can validate blocks."""
        current_time = datetime.now().timestamp()

        # Basic validation checks
        if self.cooldown > 0:
            return False

        if (current_time - self.last_validation) < 10:  # 10-second minimum
            return False

        if self.metadata["status"] != "active":
            return False

        # Shard-specific validation
        if shard_id is not None:
            if shard_id not in self.active_shards:
                return False

            shard_time = self.active_shards[shard_id]
            if (datetime.now() - shard_time).total_seconds() > 3600:  # 1 hour timeout
                return False

        return True

    def enter_cooldown(self, cooldown_period: int) -> None:
        """Put node into a cooldown period."""
        self.cooldown = cooldown_period
        self.metadata["status"] = "cooldown"
        logger.info(
            f"Node {self.node_id} entered cooldown for {cooldown_period} periods"
        )

    def update_metrics(self, metrics: Dict[str, float]) -> None:
        """Update node performance metrics."""
        self.performance_metrics.update(metrics)
        self.metadata["last_active"] = datetime.now()

        # Calculate validation success rate
        if self.total_validations > 0:
            success_rate = (
                len(
                    [
                        v
                        for v in self.validation_history
                        if v.get("evidence", {}).get("success", False)
                    ]
                )
                / self.total_validations
                * 100
            )
            self.performance_metrics["validation_success_rate"] = success_rate

    def get_total_reputation(self) -> float:
        """Calculate total reputation across all categories."""
        return sum(self.reputation_scores.values())

    def record_resource_usage(self, usage: Dict[str, float]) -> None:
        """Record resource usage metrics."""
        for resource, amount in usage.items():
            if resource in self.resource_usage:
                self.resource_usage[resource] += amount

        # Update availability based on resource usage
        total_usage = sum(self.resource_usage.values())
        self.performance_metrics["availability"] = max(0, 100 - (total_usage / 5))

    def to_dict(self) -> Dict:
        """Convert node state to dictionary."""
        return {
            "node_id": self.node_id,
            "cooperative_id": self.cooperative_id,
            "reputation_scores": self.reputation_scores,
            "stake": self.stake,
            "shard_assignments": list(self.shard_assignments),
            "active_shards": {k: v.isoformat() for k, v in self.active_shards.items()},
            "performance_metrics": self.performance_metrics,
            "resource_usage": self.resource_usage,
            "metadata": {
                **self.metadata,
                "creation_time": self.metadata["creation_time"].isoformat(),
                "last_active": self.metadata["last_active"].isoformat(),
                "capabilities": list(self.metadata["capabilities"]),
            },
            "status": self.metadata["status"],
        }

    @classmethod
    def from_dict(cls, data: Dict) -> Node:
        """Create node from dictionary."""
        try:
            node = cls(
                node_id=data["node_id"],
                cooperative_id=data["cooperative_id"],
                initial_stake=data["stake"],
            )
            node.reputation_scores = data["reputation_scores"]
            node.shard_assignments = set(data["shard_assignments"])
            node.active_shards = {
                int(k): datetime.fromisoformat(v)
                for k, v in data["active_shards"].items()
            }
            node.performance_metrics = data["performance_metrics"]
            node.resource_usage = data["resource_usage"]

            # Restore metadata
            node.metadata.update(data["metadata"])
            node.metadata["creation_time"] = datetime.fromisoformat(
                data["metadata"]["creation_time"]
            )
            node.metadata["last_active"] = datetime.fromisoformat(
                data["metadata"]["last_active"]
            )
            node.metadata["capabilities"] = set(data["metadata"]["capabilities"])

            return node

        except Exception as e:
            logger.error(f"Failed to create node from dictionary: {str(e)}")
            raise ValueError("Invalid node data")

    def __str__(self) -> str:
        """Return a human-readable string representation of the node."""
        return (
            f"Node(id={self.node_id}, "
            f"coop={self.cooperative_id}, "
            f"status={self.metadata['status']}, "
            f"rep={self.get_total_reputation():.2f})"
        )
