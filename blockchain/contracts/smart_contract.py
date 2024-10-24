"""
blockchain/contracts/smart_contract.py

This module implements the SmartContract class for the ICN blockchain, providing
a secure, sandboxed environment for executing decentralized code. The implementation
follows cooperative principles and ensures fair resource usage.

Key features:
- Secure execution environment with restricted capabilities
- Resource management through mana system
- State persistence and size limitations
- Execution history and metrics tracking
- Caller authorization management
- Cross-contract dependencies
- Daily execution limits and cooldowns
"""

from __future__ import annotations
from datetime import datetime, timedelta
from typing import Dict, List, Set, Optional, Any, Union
import time
import logging
import hashlib
import json
import sys
from io import StringIO
from copy import deepcopy

logger = logging.getLogger(__name__)

class ContractExecutionError(Exception):
    """Exception raised for contract execution failures.
    
    This includes:
    - Code execution errors
    - Resource limit violations
    - State size exceeded
    - Authorization failures
    - Timeout errors
    """
    pass

class SmartContract:
    """Smart contract implementation for the ICN blockchain.
    
    Smart contracts are self-executing code units that run in a secure sandbox.
    They maintain state, track execution metrics, and enforce resource limits
    to ensure fair usage of network resources.
    
    Attributes:
        contract_id (str): Unique identifier for the contract
        code (str): Python source code of the contract
        creator (str): Identity of contract creator
        state (Dict): Contract's persistent state storage
        mana_cost (int): Mana required per execution
        version (str): Contract version identifier
        
    Resource Limits:
        - Maximum state size (default: 1MB)
        - Maximum execution time (default: 5s)
        - Maximum mana per execution (default: 100)
        - Maximum daily executions (default: 1000)
    """

    # Safe built-ins allowed in contract execution
    SAFE_BUILTINS = {
        "abs": abs,
        "bool": bool,
        "dict": dict,
        "float": float,
        "int": int,
        "len": len,
        "list": list,
        "max": max,
        "min": min,
        "round": round,
        "sorted": sorted,
        "str": str,
        "sum": sum,
    }

    def __init__(
        self,
        contract_id: str,
        code: str,
        creator: str,
        mana_cost: int = 10,
        version: str = "1.0",
    ) -> None:
        """Initialize a new smart contract.
        
        Args:
            contract_id: Unique identifier for the contract
            code: Contract source code in Python
            creator: Identity of the contract creator
            mana_cost: Mana cost per execution
            version: Version string for the contract
            
        The constructor initializes execution tracking, state storage,
        and resource limits while setting up the secure execution environment.
        """
        # Basic contract information
        self.contract_id = contract_id
        self.code = code
        self.creator = creator
        self.state: Dict = {}
        self.mana_cost = mana_cost
        self.version = version
        
        # Execution tracking
        self.created_at = datetime.now()
        self.last_executed: Optional[datetime] = None
        self.execution_count = 0
        self.total_mana_consumed = 0
        self.execution_history: List[Dict] = []
        
        # Previous state for rollback
        self._previous_state: Optional[Dict] = None
        
        # Metadata and capabilities
        self.metadata: Dict = {
            "created_at": self.created_at,
            "version": version,
            "creator": creator,
            "description": "",
            "tags": set(),
            "last_updated": self.created_at
        }
        
        # Dependencies and authorization
        self.dependencies: Set[str] = set()
        self.allowed_callers: Set[str] = {creator}
        
        # Resource restrictions
        self.restrictions: Dict = {
            "max_state_size": 1024 * 1024,  # 1MB
            "max_execution_time": 5,         # seconds
            "max_mana_per_execution": 100,   # mana
            "max_daily_executions": 1000,    # executions
        }
        
        # Daily execution tracking
        self.daily_executions = 0
        self.last_reset = datetime.now()

    def execute(self, input_data: Dict, available_mana: int) -> Dict:
        """Execute the smart contract with given input data.
        
        Args:
            input_data: Dictionary of input parameters for the contract
            available_mana: Amount of mana available for execution
            
        Returns:
            Dictionary containing:
            - execution result
            - updated state
            - mana consumed
            - execution time
            - output captured
            
        Raises:
            ContractExecutionError: If execution fails or violates restrictions
        """
        self._reset_daily_executions()
        self._backup_state()
        
        try:
            # Validate execution conditions
            validation_result = self._validate_execution(input_data, available_mana)
            if validation_result.get("error"):
                raise ContractExecutionError(validation_result["error"])

            execution_start = time.time()
            stdout_capture = StringIO()
            original_stdout = sys.stdout
            sys.stdout = stdout_capture

            try:
                # Set up and execute
                local_namespace = self._setup_execution_environment(input_data)
                exec(self.code, {}, local_namespace)

                if "execute" not in local_namespace:
                    raise ContractExecutionError("Contract missing execute function")

                # Execute with timing check
                if time.time() - execution_start > self.restrictions["max_execution_time"]:
                    raise ContractExecutionError("Execution time limit exceeded")

                result = local_namespace["execute"](input_data, self.state)

                # Validate post-execution state
                if len(str(self.state)) > self.restrictions["max_state_size"]:
                    self._rollback_state()
                    raise ContractExecutionError("State size limit exceeded after execution")

                # Update metrics and return result
                self._update_execution_metrics(execution_start)
                output = stdout_capture.getvalue()

                return {
                    "state": self.state,
                    "result": result,
                    "mana_used": self.mana_cost,
                    "execution_time": time.time() - execution_start,
                    "output": output,
                }

            finally:
                sys.stdout = original_stdout

        except ContractExecutionError:
            self._rollback_state()
            raise
        except Exception as e:
            self._rollback_state()
            logger.error(f"Contract execution failed: {str(e)}")
            raise ContractExecutionError(str(e))

    def _validate_execution(self, input_data: Dict, available_mana: int) -> Dict:
        """Validate all conditions required for contract execution.
        
        Performs comprehensive validation including:
        - Daily execution limits
        - Mana availability
        - Current and projected state size
        - Input data format
        """
        try:
            if self.daily_executions >= self.restrictions["max_daily_executions"]:
                return {"error": "Daily execution limit exceeded"}

            if available_mana < self.mana_cost:
                return {"error": "Insufficient mana"}

            # Calculate potential state size
            current_state_size = len(str(self.state))
            potential_growth = len(str(input_data)) * 2  # Conservative estimate
            if current_state_size + potential_growth > self.restrictions["max_state_size"]:
                return {"error": "Projected state size would exceed limit"}

            if not isinstance(input_data, dict):
                return {"error": "Invalid input data format"}

            return {}

        except Exception as e:
            return {"error": f"Validation failed: {str(e)}"}

    def _backup_state(self) -> None:
        """Create a backup of the current state for potential rollback."""
        self._previous_state = deepcopy(self.state)

    def _rollback_state(self) -> None:
        """Rollback to the previous state if available."""
        if self._previous_state is not None:
            self.state = self._previous_state
            self._previous_state = None

    def _setup_execution_environment(self, input_data: Dict) -> Dict:
        """Create a secure execution environment for the contract.
        
        Sets up a restricted namespace with only safe operations allowed.
        Provides access to contract state and metadata while preventing
        access to system resources.
        """
        return {
            "input": input_data,
            "state": self.state,
            "contract_id": self.contract_id,
            "creator": self.creator,
            "version": self.version,
            "metadata": self.metadata.copy(),
            "__builtins__": self.SAFE_BUILTINS,
        }

    def _update_execution_metrics(self, execution_start: float) -> None:
        """Update all execution metrics after successful execution.
        
        Updates:
        - Execution count and history
        - Mana consumption
        - Timing information
        - State size tracking
        """
        self.last_executed = datetime.now()
        self.execution_count += 1
        self.daily_executions += 1
        self.total_mana_consumed += self.mana_cost

        execution_record = {
            "timestamp": self.last_executed,
            "execution_time": time.time() - execution_start,
            "mana_used": self.mana_cost,
            "state_size": len(str(self.state)),
        }

        self.execution_history.append(execution_record)
        if len(self.execution_history) > 1000:
            self.execution_history = self.execution_history[-1000:]

    def _reset_daily_executions(self) -> None:
        """Reset daily execution counter if a day has passed."""
        current_time = datetime.now()
        if (current_time - self.last_reset).days >= 1:
            self.daily_executions = 0
            self.last_reset = current_time

    def authorize_caller(self, caller_id: str) -> bool:
        """Add a new authorized caller for the contract."""
        self.allowed_callers.add(caller_id)
        return True

    def revoke_caller(self, caller_id: str) -> bool:
        """Revoke a caller's authorization (except creator)."""
        if caller_id == self.creator:
            return False
        self.allowed_callers.discard(caller_id)
        return True

    def update_restrictions(self, new_restrictions: Dict) -> bool:
        """Update contract restrictions if valid."""
        try:
            if not all(k in self.restrictions for k in new_restrictions):
                return False
            self.restrictions.update(new_restrictions)
            self.metadata["last_updated"] = datetime.now()
            return True
        except Exception as e:
            logger.error(f"Failed to update restrictions: {str(e)}")
            return False

    def get_metrics(self) -> Dict:
        """Get comprehensive contract metrics."""
        return {
            "contract_id": self.contract_id,
            "version": self.version,
            "creator": self.creator,
            "created_at": self.created_at.isoformat(),
            "last_executed": (
                self.last_executed.isoformat() if self.last_executed else None
            ),
            "execution_count": self.execution_count,
            "daily_executions": self.daily_executions,
            "total_mana_consumed": self.total_mana_consumed,
            "average_mana_per_execution": (
                self.total_mana_consumed / self.execution_count
                if self.execution_count > 0
                else 0
            ),
            "state_size": len(str(self.state)),
            "dependencies": list(self.dependencies),
            "authorized_callers": len(self.allowed_callers),
            "restrictions": self.restrictions,
        }

    def to_dict(self) -> Dict:
        """Convert contract to dictionary representation."""
        return {
            "contract_id": self.contract_id,
            "code": self.code,
            "creator": self.creator,
            "state": self.state,
            "mana_cost": self.mana_cost,
            "version": self.version,
            "metadata": {
                **self.metadata,
                "created_at": self.metadata["created_at"].isoformat(),
                "last_updated": self.metadata["last_updated"].isoformat(),
                "tags": list(self.metadata["tags"]),
            },
            "dependencies": list(self.dependencies),
            "allowed_callers": list(self.allowed_callers),
            "restrictions": self.restrictions,
            "metrics": self.get_metrics(),
        }

    @classmethod
    def from_dict(cls, data: Dict) -> "SmartContract":
        """Create contract instance from dictionary data."""
        contract = cls(
            contract_id=data["contract_id"],
            code=data["code"],
            creator=data["creator"],
            mana_cost=data["mana_cost"],
            version=data["version"],
        )

        contract.state = data["state"]
        contract.metadata = {
            **data["metadata"],
            "created_at": datetime.fromisoformat(data["metadata"]["created_at"]),
            "last_updated": datetime.fromisoformat(data["metadata"]["last_updated"]),
            "tags": set(data["metadata"]["tags"]),
        }
        contract.dependencies = set(data["dependencies"])
        contract.allowed_callers = set(data["allowed_callers"])
        contract.restrictions = data["restrictions"]

        return contract

    def __str__(self) -> str:
        """Human-readable string representation."""
        return (
            f"Contract(id={self.contract_id}, "
            f"creator={self.creator}, "
            f"executions={self.execution_count}, "
            f"mana_cost={self.mana_cost})"
        )