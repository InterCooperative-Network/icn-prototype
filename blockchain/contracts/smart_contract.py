# ================================================================
# File: blockchain/contracts/smart_contract.py
# Description: Defines the SmartContract class for the ICN blockchain.
# This file handles the creation, execution, and lifecycle of smart
# contracts, which operate within a secure and cooperative environment.
# The contracts are sandboxed to ensure safety, integrity, and fairness.
# ================================================================

from __future__ import annotations
from datetime import datetime, timedelta
from typing import Dict, List, Set, Optional, Any
import time
import logging
import hashlib
import json
import sys
from io import StringIO

logger = logging.getLogger(__name__)

class ContractExecutionError(Exception):
    """
    Custom exception for handling errors during contract execution.

    This exception is raised when contract execution fails, either due to
    code errors, resource constraints, or violation of restrictions.
    """
    pass

class SmartContract:
    """
    Represents a smart contract in the ICN blockchain.

    Smart contracts are self-executing codes with terms directly written into
    the code. They run in a secure, sandboxed environment to ensure restricted
    capabilities, maintaining the ICN's cooperative and decentralized principles.
    """

    def __init__(
        self,
        contract_id: str,
        code: str,
        creator: str,
        mana_cost: int = 10,
        version: str = "1.0",
    ):
        """
        Initialize a SmartContract instance.

        Args:
            contract_id (str): Unique identifier for the contract.
            code (str): Contract code in Python.
            creator (str): The creator's identifier.
            mana_cost (int): Mana required for each execution.
            version (str): Version of the contract.
        """
        self.contract_id = contract_id
        self.code = code
        self.creator = creator
        self.state: Dict = {}
        self.mana_cost = mana_cost
        self.version = version
        self.created_at = datetime.now()
        self.last_executed = None
        self.execution_count = 0
        self.total_mana_consumed = 0
        self.execution_history: List[Dict] = []
        self.metadata: Dict = {
            "created_at": self.created_at,
            "version": version,
            "creator": creator,
            "description": "",
            "tags": set(),
        }
        self.dependencies: Set[str] = set()
        self.allowed_callers: Set[str] = {creator}
        self.restrictions: Dict = {
            "max_state_size": 1024 * 1024,  # 1MB
            "max_execution_time": 5,        # seconds
            "max_mana_per_execution": 100,  # mana
            "max_daily_executions": 1000,   # executions
        }
        self.daily_executions = 0
        self.last_reset = datetime.now()

    def execute(self, input_data: Dict, available_mana: int) -> Dict:
        """
        Execute the smart contract.

        The contract code is executed within a sandboxed environment, ensuring
        safety and compliance with restrictions. The function also manages state
        updates and checks resource availability.

        Args:
            input_data (Dict): Data passed to the contract.
            available_mana (int): Available mana for execution.

        Returns:
            Dict: Contains execution results, state updates, and metrics.

        Raises:
            ContractExecutionError: If execution fails or violates restrictions.
        """
        # Reset daily execution counter if needed
        self._reset_daily_executions()

        # Validate execution conditions
        validation_result = self._validate_execution(input_data, available_mana)
        if validation_result.get("error"):
            raise ContractExecutionError(validation_result["error"])

        execution_start = time.time()
        stdout_capture = StringIO()
        original_stdout = sys.stdout
        sys.stdout = stdout_capture

        try:
            # Set up a secure execution environment
            local_namespace = self._setup_execution_environment(input_data)

            # Execute contract code
            exec(self.code, {}, local_namespace)

            # Validate the presence of an execute function
            if "execute" not in local_namespace:
                raise ContractExecutionError("Contract missing execute function")

            # Check execution time
            if time.time() - execution_start > self.restrictions["max_execution_time"]:
                raise ContractExecutionError("Execution time limit exceeded")

            # Execute the contract function
            result = local_namespace["execute"](input_data, self.state)

            # Update contract metrics
            self._update_execution_metrics(execution_start)

            # Capture any output
            output = stdout_capture.getvalue()

            return {
                "state": self.state,
                "result": result,
                "mana_used": self.mana_cost,
                "execution_time": time.time() - execution_start,
                "output": output,
            }

        except Exception as e:
            logger.error(f"Contract execution failed: {str(e)}")
            raise ContractExecutionError(str(e))

        finally:
            sys.stdout = original_stdout

    def _validate_execution(self, input_data: Dict, available_mana: int) -> Dict:
        """
        Validate conditions for contract execution.

        Checks include state size, mana availability, input data format,
        and daily execution limits.

        Args:
            input_data (Dict): Input data for validation.
            available_mana (int): Available mana for execution.

        Returns:
            Dict: Contains error message if validation fails.
        """
        try:
            if self.daily_executions >= self.restrictions["max_daily_executions"]:
                return {"error": "Daily execution limit exceeded"}

            if available_mana < self.mana_cost:
                return {"error": "Insufficient mana"}

            if len(str(self.state)) > self.restrictions["max_state_size"]:
                return {"error": "State size limit exceeded"}

            if not isinstance(input_data, dict):
                return {"error": "Invalid input data format"}

            return {}

        except Exception as e:
            return {"error": f"Validation failed: {str(e)}"}

    def _setup_execution_environment(self, input_data: Dict) -> Dict:
        """
        Set up a secure execution environment with allowed variables.

        Args:
            input_data (Dict): Data passed to the contract.

        Returns:
            Dict: Secure local namespace for contract execution.
        """
        # Basic built-ins that are safe to use
        safe_builtins = {
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

        return {
            "input": input_data,
            "state": self.state.copy(),
            "contract_id": self.contract_id,
            "creator": self.creator,
            "version": self.version,
            "metadata": self.metadata,
            "__builtins__": safe_builtins,
        }

    def _update_execution_metrics(self, execution_start: float) -> None:
        """
        Update metrics after each contract execution.

        Args:
            execution_start (float): Start time of the execution.
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
        """
        Reset the daily execution counter if a day has passed since the last reset.
        """
        current_time = datetime.now()
        if (current_time - self.last_reset).days >= 1:
            self.daily_executions = 0
            self.last_reset = current_time

    def authorize_caller(self, caller_id: str) -> bool:
        """
        Add a caller to the list of authorized entities for contract execution.

        Args:
            caller_id (str): ID of the caller to authorize.

        Returns:
            bool: True if authorization was successful, False otherwise.
        """
        self.allowed_callers.add(caller_id)
        return True

    def revoke_caller(self, caller_id: str) -> bool:
        """
        Revoke a caller's authorization for contract execution.

        Args:
            caller_id (str): ID of the caller to revoke.

        Returns:
            bool: True if revocation was successful, False otherwise.
        """
        if caller_id == self.creator:
            return False
        self.allowed_callers.discard(caller_id)
        return True

    def update_restrictions(self, new_restrictions: Dict) -> bool:
        """
        Update the contract's execution restrictions.

        Args:
            new_restrictions (Dict): New restrictions to apply.

        Returns:
            bool: True if update was successful, False otherwise.
        """
        try:
            # Validate new restrictions
            if not all(k in self.restrictions for k in new_restrictions):
                return False

            # Update only valid restrictions
            self.restrictions.update(new_restrictions)
            return True

        except Exception as e:
            logger.error(f"Failed to update restrictions: {str(e)}")
            return False

    def get_metrics(self) -> Dict:
        """
        Get the comprehensive metrics of the contract's performance.

        Returns:
            Dict: Metrics of the smart contract.
        """
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
        """
        Convert the smart contract to a dictionary format.

        Returns:
            Dict: The dictionary representation of the contract.
        """
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
                "tags": list(self.metadata["tags"]),
            },
            "dependencies": list(self.dependencies),
            "allowed_callers": list(self.allowed_callers),
            "restrictions": self.restrictions,
            "metrics": self.get_metrics(),
        }

    @classmethod
    def from_dict(cls, data: Dict) -> "SmartContract":
        """
        Create a SmartContract instance from a dictionary.

        Args:
            data (Dict): The dictionary containing contract data.

        Returns:
            SmartContract: The created SmartContract instance.
        """
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
            "tags": set(data["metadata"]["tags"]),
        }
        contract.dependencies = set(data["dependencies"])
        contract.allowed_callers = set(data["allowed_callers"])
        contract.restrictions = data["restrictions"]

        return contract

    def __str__(self) -> str:
        """
        Return a human-readable string representation of the contract.

        Returns:
            str: The string representation of the contract.
        """
        return (
            f"Contract(id={self.contract_id}, "
            f"creator={self.creator}, "
            f"executions={self.execution_count}, "
            f"mana_cost={self.mana_cost})"
        )
        
