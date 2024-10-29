"""
blockchain/consensus/consensus_state_manager.py

Manages the state of the Proof of Cooperation consensus mechanism.
Handles state persistence, event propagation, and recovery.
"""

import asyncio
import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set, Any, Tuple
from dataclasses import dataclass, field
import aiofiles

from .proof_of_cooperation.types import (
    ConsensusConfig, ConsensusState, ConsensusMetrics,
    ValidationResult, ValidatorHistory
)
from ..core.node import Node
from ..core.block import Block
from ..utils.crypto import hash_data

logger = logging.getLogger(__name__)

@dataclass
class ConsensusSnapshot:
    """Represents a snapshot of consensus state for persistence."""
    timestamp: datetime
    state: ConsensusState
    active_validators: Set[str]
    pending_validations: Dict[str, ValidationResult]
    checkpoint_hash: str

    @property
    def snapshot_id(self) -> str:
        """Generate unique ID for this snapshot."""
        data = f"{self.timestamp.isoformat()}:{self.checkpoint_hash}"
        return hash_data(data.encode())

class ConsensusStateManager:
    """
    Manages consensus state for the ICN blockchain.
    
    Responsibilities:
    - Track consensus state and metrics
    - Persist state to disk
    - Handle state recovery
    - Coordinate consensus events
    - Manage validator sets
    """

    def __init__(
        self, 
        config: ConsensusConfig,
        state_dir: str = "consensus_state",
        snapshot_interval: int = 100  # blocks
    ):
        """Initialize the consensus state manager."""
        self.config = config
        self.state = ConsensusState(config)
        self.state_dir = Path(state_dir)
        self.snapshot_interval = snapshot_interval
        
        # Runtime state
        self.active_validators: Set[str] = set()
        self.pending_validations: Dict[str, ValidationResult] = {}
        self.validation_queue: asyncio.Queue = asyncio.Queue()
        self.snapshots: List[ConsensusSnapshot] = []
        
        # Initialize metrics
        self.metrics = ConsensusMetrics()
        
        # Create state directory if needed
        self.state_dir.mkdir(parents=True, exist_ok=True)
        
        # Event handlers
        self.event_handlers: Dict[str, List[callable]] = {
            "state_updated": [],
            "validator_added": [],
            "validator_removed": [],
            "snapshot_created": [],
            "state_recovered": []
        }
        
        # Recovery state
        self.recovery_mode = False
        self.last_checkpoint: Optional[ConsensusSnapshot] = None

    async def initialize(self) -> bool:
        """Initialize consensus state and recover if needed."""
        try:
            # Attempt to load latest state
            if await self._load_latest_state():
                logger.info("Successfully loaded existing consensus state")
                return True

            # Initialize fresh state if none exists
            logger.info("No existing state found, initializing fresh state")
            await self._initialize_fresh_state()
            return True

        except Exception as e:
            logger.error(f"Failed to initialize consensus state: {str(e)}")
            return False

    async def _initialize_fresh_state(self) -> None:
        """Initialize a fresh consensus state."""
        self.state = ConsensusState(self.config)
        await self._create_snapshot()
        self._notify_event("state_updated", self.state)

    async def add_validator(self, validator: Node) -> bool:
        """Add a new validator to the consensus."""
        try:
            if validator.node_id in self.active_validators:
                return False

            self.active_validators.add(validator.node_id)
            self.state.active_validators.add(validator.node_id)
            
            # Initialize validator metrics
            if validator.node_id not in self.state.validation_stats:
                self.state.validation_stats[validator.node_id] = {
                    "selections": 0,
                    "successful_validations": 0,
                    "consecutive_failures": 0,
                    "last_validation": None
                }

            self._notify_event("validator_added", validator)
            await self._create_snapshot()
            return True

        except Exception as e:
            logger.error(f"Failed to add validator {validator.node_id}: {str(e)}")
            return False

    async def remove_validator(self, validator_id: str) -> bool:
        """Remove a validator from consensus."""
        try:
            if validator_id not in self.active_validators:
                return False

            self.active_validators.remove(validator_id)
            self.state.active_validators.remove(validator_id)
            self._notify_event("validator_removed", validator_id)
            await self._create_snapshot()
            return True

        except Exception as e:
            logger.error(f"Failed to remove validator {validator_id}: {str(e)}")
            return False

    async def record_validation(
        self,
        validator_id: str,
        result: ValidationResult,
        block: Optional[Block] = None
    ) -> None:
        """Record a validation result."""
        try:
            # Update validator stats
            if validator_id in self.state.validation_stats:
                stats = self.state.validation_stats[validator_id]
                stats["selections"] += 1
                
                if result.success:
                    stats["successful_validations"] += 1
                    stats["consecutive_failures"] = 0
                else:
                    stats["consecutive_failures"] += 1

                stats["last_validation"] = datetime.now()

            # Update metrics
            self.metrics.total_validations += 1
            if result.success:
                self.metrics.successful_validations += 1
            else:
                self.metrics.failed_validations += 1

            # Create validator history entry
            history_entry = ValidatorHistory(
                node_id=validator_id,
                timestamp=datetime.now(),
                shard_id=block.shard_id if block else None,
                success=result.success
            )
            self.state.validator_history.append(history_entry)

            # Trim history if needed
            if len(self.state.validator_history) > 1000:
                self.state.validator_history = self.state.validator_history[-1000:]

            await self._create_snapshot()

        except Exception as e:
            logger.error(f"Failed to record validation result: {str(e)}")

    async def _create_snapshot(self) -> Optional[ConsensusSnapshot]:
        """Create a new state snapshot."""
        try:
            # Generate snapshot
            snapshot = ConsensusSnapshot(
                timestamp=datetime.now(),
                state=self.state,
                active_validators=self.active_validators.copy(),
                pending_validations=self.pending_validations.copy(),
                checkpoint_hash=self._calculate_checkpoint_hash()
            )

            # Save snapshot
            await self._save_snapshot(snapshot)
            self.snapshots.append(snapshot)

            # Trim old snapshots
            if len(self.snapshots) > 10:
                self.snapshots = self.snapshots[-10:]

            self._notify_event("snapshot_created", snapshot)
            return snapshot

        except Exception as e:
            logger.error(f"Failed to create snapshot: {str(e)}")
            return None

    def _calculate_checkpoint_hash(self) -> str:
        """Calculate hash of current state for checkpointing."""
        state_dict = {
            "metrics": self.metrics.to_dict(),
            "active_validators": list(self.active_validators),
            "validation_stats": self.state.validation_stats,
            "timestamp": datetime.now().isoformat()
        }
        return hash_data(json.dumps(state_dict, sort_keys=True).encode())

    async def _save_snapshot(self, snapshot: ConsensusSnapshot) -> bool:
        """Save snapshot to disk."""
        try:
            snapshot_path = self.state_dir / f"snapshot_{snapshot.snapshot_id}.json"
            snapshot_data = {
                "timestamp": snapshot.timestamp.isoformat(),
                "state": snapshot.state.to_dict(),
                "active_validators": list(snapshot.active_validators),
                "pending_validations": {
                    k: v.to_dict() for k, v in snapshot.pending_validations.items()
                },
                "checkpoint_hash": snapshot.checkpoint_hash
            }

            async with aiofiles.open(snapshot_path, 'w') as f:
                await f.write(json.dumps(snapshot_data, indent=2))
            return True

        except Exception as e:
            logger.error(f"Failed to save snapshot: {str(e)}")
            return False

    async def _load_latest_state(self) -> bool:
        """Load the latest state from disk."""
        try:
            # Find latest snapshot
            snapshot_files = list(self.state_dir.glob("snapshot_*.json"))
            if not snapshot_files:
                return False

            latest_snapshot = max(
                snapshot_files,
                key=lambda p: os.path.getctime(p)
            )

            # Load snapshot
            async with aiofiles.open(latest_snapshot, 'r') as f:
                data = json.loads(await f.read())

            # Restore state
            self.state = ConsensusState.from_dict(data["state"])
            self.active_validators = set(data["active_validators"])
            self.pending_validations = {
                k: ValidationResult.from_dict(v)
                for k, v in data["pending_validations"].items()
            }

            # Create recovery checkpoint
            self.last_checkpoint = ConsensusSnapshot(
                timestamp=datetime.fromisoformat(data["timestamp"]),
                state=self.state,
                active_validators=self.active_validators,
                pending_validations=self.pending_validations,
                checkpoint_hash=data["checkpoint_hash"]
            )

            self._notify_event("state_recovered", self.state)
            return True

        except Exception as e:
            logger.error(f"Failed to load latest state: {str(e)}")
            return False

    def register_event_handler(self, event_type: str, handler: callable) -> None:
        """Register a handler for consensus events."""
        if event_type in self.event_handlers:
            self.event_handlers[event_type].append(handler)

    def _notify_event(self, event_type: str, event_data: Any) -> None:
        """Notify registered handlers of an event."""
        if event_type in self.event_handlers:
            for handler in self.event_handlers[event_type]:
                try:
                    handler(event_data)
                except Exception as e:
                    logger.error(f"Error in event handler: {str(e)}")

    async def enter_recovery_mode(self) -> None:
        """Enter recovery mode after error detection."""
        self.recovery_mode = True
        if self.last_checkpoint:
            # Restore from last checkpoint
            self.state = self.last_checkpoint.state
            self.active_validators = self.last_checkpoint.active_validators
            self.pending_validations = self.last_checkpoint.pending_validations
            self._notify_event("state_recovered", self.state)

    async def exit_recovery_mode(self) -> None:
        """Exit recovery mode once state is restored."""
        self.recovery_mode = False
        await self._create_snapshot()

    def get_metrics(self) -> Dict[str, Any]:
        """Get current consensus metrics."""
        return {
            "total_validations": self.metrics.total_validations,
            "successful_validations": self.metrics.successful_validations,
            "failed_validations": self.metrics.failed_validations,
            "active_validators": len(self.active_validators),
            "pending_validations": len(self.pending_validations),
            "recovery_mode": self.recovery_mode
        }