# ================================================================
# File: blockchain/contracts/contract_executor.py
# Description: Manages the deployment, execution, and lifecycle
# of smart contracts within the ICN ecosystem. The ContractExecutor
# ensures secure and efficient contract operations, resource limits,
# and enforces decentralized governance principles.
# ================================================================

from typing import Dict, List, Optional, Set
import logging
from datetime import datetime
import asyncio
from .smart_contract import SmartContract, ContractExecutionError

logger = logging.getLogger(__name__)

class ContractExecutor:
    """
    Manages smart contract deployment, execution, and lifecycle.

    This class handles the secure deployment of contracts, manages their
    dependencies, and coordinates their execution while enforcing resource
    limits and security constraints. It ensures that the cooperative nature
    of the ICN is maintained, supporting fair resource sharing among nodes.
    """

    def __init__(self, initial_mana: int = 1000, mana_regen_rate: int = 10):
        """
        Initialize the ContractExecutor.

        Args:
            initial_mana (int): Initial mana pool available for execution.
            mana_regen_rate (int): Rate at which mana regenerates over time.

        The mana system represents a replenishing resource used to limit the
        frequency and extent of smart contract executions, aligning with the ICN's
        resource-sharing goals.
        """
        self.contracts: Dict[str, SmartContract] = {}
        self.mana_pool = initial_mana
        self.mana_regen_rate = mana_regen_rate
        self.execution_queue: List[Dict] = []
        self.max_queue_size = 1000
        self.dependency_graph: Dict[str, Set[str]] = {}
        self.metrics: Dict = {
            "total_executions": 0,
            "failed_executions": 0,
            "total_mana_consumed": 0,
            "average_execution_time": 0,
            "contracts_deployed": 0,
        }
        self.execution_lock = asyncio.Lock()

    async def deploy_contract(self, contract: SmartContract) -> bool:
        """
        Deploy a new smart contract.

        Ensures the contract is safe, manages dependencies, and
        updates the registry for cooperative access.

        Args:
            contract (SmartContract): SmartContract instance to deploy.

        Returns:
            bool: True if deployment is successful, False otherwise.
        """
        try:
            # Check if contract already exists
            if contract.contract_id in self.contracts:
                logger.error(f"Contract {contract.contract_id} already exists")
                return False

            # Validate contract code (basic safety checks)
            if not self._validate_contract_code(contract.code):
                return False

            # Check dependencies
            if not self._validate_dependencies(contract.dependencies):
                return False

            # Update dependency graph
            self.dependency_graph[contract.contract_id] = contract.dependencies

            # Store contract
            self.contracts[contract.contract_id] = contract
            self.metrics["contracts_deployed"] += 1

            logger.info(f"Deployed contract {contract.contract_id}")
            return True

        except Exception as e:
            logger.error(f"Contract deployment failed: {str(e)}")
            return False

    async def execute_contract(
        self, contract_id: str, input_data: Dict, caller: str
    ) -> Dict:
        """
        Execute a smart contract.

        Executes the specified contract with the given input data and manages
        resource consumption (mana), caller authorization, and execution safety.

        Args:
            contract_id (str): ID of the contract to execute.
            input_data (Dict): Input data for the contract execution.
            caller (str): ID of the calling entity.

        Returns:
            Dict: Results of the contract execution.

        Raises:
            ContractExecutionError: If execution fails or violates constraints.
        """
        async with self.execution_lock:
            try:
                # Get contract
                contract = self.contracts.get(contract_id)
                if not contract:
                    raise ContractExecutionError(f"Contract {contract_id} not found")

                # Validate caller
                if caller not in contract.allowed_callers:
                    raise ContractExecutionError("Caller not authorized")

                # Check mana availability
                if self.mana_pool < contract.mana_cost:
                    raise ContractExecutionError("Insufficient mana in pool")

                # Execute contract
                execution_start = datetime.now()
                result = contract.execute(input_data, self.mana_pool)

                # Update mana pool
                self.mana_pool -= result["mana_used"]
                self.metrics["total_mana_consumed"] += result["mana_used"]

                # Update metrics
                execution_time = (datetime.now() - execution_start).total_seconds()
                self._update_metrics(execution_time, True)

                return result

            except ContractExecutionError as e:
                logger.error(f"Contract execution error: {str(e)}")
                self._update_metrics(0, False)
                raise

            except Exception as e:
                logger.error(f"Execution failed: {str(e)}")
                self._update_metrics(0, False)
                raise ContractExecutionError(str(e))

    def _validate_contract_code(self, code: str) -> bool:
        """
        Validate the safety of the contract code.

        This method checks for dangerous imports, built-ins, and compilation
        errors, ensuring that only safe and compliant code is deployed.

        Args:
            code (str): The code to validate.

        Returns:
            bool: True if the code is safe, False otherwise.
        """
        try:
            # Check for dangerous imports
            dangerous_imports = ["os", "sys", "subprocess", "importlib"]
            for imp in dangerous_imports:
                if f"import {imp}" in code or f"from {imp}" in code:
                    logger.error(f"Dangerous import detected: {imp}")
                    return False

            # Check for dangerous built-ins
            dangerous_builtins = ["exec", "eval", "open", "__import__"]
            for builtin in dangerous_builtins:
                if builtin in code:
                    logger.error(f"Dangerous builtin detected: {builtin}")
                    return False

            # Validate code compilation
            compile(code, "<string>", "exec")
            return True

        except Exception as e:
            logger.error(f"Contract code validation failed: {str(e)}")
            return False

    def _validate_dependencies(self, dependencies: Set[str]) -> bool:
        """
        Validate the contract's dependencies.

        Ensures that all dependencies exist and checks for potential
        circular dependencies that could disrupt cooperative integrity.

        Args:
            dependencies (Set[str]): Set of dependency contract IDs.

        Returns:
            bool: True if dependencies are valid, False otherwise.
        """
        try:
            # Check if all dependencies exist
            for dep in dependencies:
                if dep not in self.contracts:
                    logger.error(f"Dependency not found: {dep}")
                    return False

            # Check for circular dependencies
            visited = set()
            path = []

            def check_cycle(contract_id: str) -> bool:
                if contract_id in path:
                    logger.error(
                        f"Circular dependency detected: {' -> '.join(path + [contract_id])}"
                    )
                    return False

                if contract_id in visited:
                    return True

                visited.add(contract_id)
                path.append(contract_id)

                for dep in self.dependency_graph.get(contract_id, set()):
                    if not check_cycle(dep):
                        return False

                path.pop()
                return True

            # Validate each dependency for cycles
            for dep in dependencies:
                if not check_cycle(dep):
                    return False

            return True

        except Exception as e:
            logger.error(f"Dependency validation failed: {str(e)}")
            return False

    def _update_metrics(self, execution_time: float, success: bool) -> None:
        """
        Update execution metrics after contract execution.

        Args:
            execution_time (float): Time taken for execution.
            success (bool): Whether the execution was successful or not.
        """
        try:
            self.metrics["total_executions"] += 1
            if not success:
                self.metrics["failed_executions"] += 1

            # Update average execution time
            total_time = self.metrics["average_execution_time"] * (
                self.metrics["total_executions"] - 1
            )
            self.metrics["average_execution_time"] = (
                total_time + execution_time
            ) / self.metrics["total_executions"]

        except Exception as e:
            logger.error(f"Failed to update metrics: {str(e)}")

    async def regenerate_mana(self) -> None:
        """
        Regenerate mana in the pool.

        Mana regeneration reflects cooperative principles of resource renewal,
        ensuring sustainable use of network resources.
        """
        try:
            old_mana = self.mana_pool
            self.mana_pool = min(1000, self.mana_pool + self.mana_regen_rate)

            if self.mana_pool > old_mana:
                logger.debug(f"Regenerated mana: {self.mana_pool - old_mana}")

        except Exception as e:
            logger.error(f"Mana regeneration failed: {str(e)}")

    async def process_execution_queue(self) -> None:
        """
        Process pending contract executions from the queue.

        The queue ensures that contract executions are handled fairly,
        maintaining cooperative resource allocation.
        """
        while True:
            try:
                if not self.execution_queue:
                    await asyncio.sleep(1)
                    continue

                # Get next execution request
                request = self.execution_queue.pop(0)

                # Execute contract
                await self.execute_contract(
                    request["contract_id"], request["input_data"], request["caller"]
                )

                # Regenerate some mana
                await self.regenerate_mana()

            except Exception as e:
                logger.error(f"Queue processing error: {str(e)}")
                await asyncio.sleep(1)

    async def queue_execution(
        self, contract_id: str, input_data: Dict, caller: str
    ) -> bool:
        """
        Queue a contract for execution.

        Adds the contract execution request to the queue, ensuring fairness
        and cooperative resource management.

        Args:
            contract_id (str): ID of the contract to be executed.
            input_data (Dict): Data for contract execution.
            caller (str): ID of the caller.

        Returns:
            bool: True if successfully queued, False otherwise.
        """
        try:
            if len(self.execution_queue) >= self.max_queue_size:
                logger.error("Execution queue full")
                return False

            self.execution_queue.append(
                {
                    "contract_id": contract_id,
                    "input_data": input_data,
                    "caller": caller,
                    "timestamp": datetime.now(),
                }
            )

            return True

        except Exception as e:
            logger.error(f"Failed to queue execution: {str(e)}")
            return False

    def get_metrics(self) -> Dict:
        """
        Get executor metrics.

        Provides insights into execution statistics, mana usage, and contract
        deployments, supporting cooperative transparency.

        Returns:
            Dict: Metrics of the contract executor.
        """
        try:
            return {
                **self.metrics,
                "current_mana": self.mana_pool,
                "queue_length": len(self.execution_queue),
                "active_contracts": len(self.contracts),
            }
        except Exception as e:
            logger.error(f"Failed to get metrics: {str(e)}")
            return {}

    def __str__(self) -> str:
        """
        Return a human-readable string representation of the executor.

        Provides a summary of active contracts, mana levels, and queue status.

        Returns:
            str: String representation of the contract executor.
        """
        return (
            f"ContractExecutor(contracts={len(self.contracts)}, "
            f"mana={self.mana_pool}, "
            f"queue={len(self.execution_queue)})"
        )
