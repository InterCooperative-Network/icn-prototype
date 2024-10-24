"""
blockchain/contracts/contract_executor.py

This module implements the ContractExecutor for the ICN blockchain, providing
secure contract deployment, execution, and lifecycle management. It enforces
resource limits, security constraints, and cooperative principles.

Key features:
- Secure sandbox execution environment
- Resource management via mana system
- Dependency resolution and validation
- Cross-contract communication
- State integrity protection
- Concurrent execution handling
"""

from typing import Dict, List, Optional, Set
import logging
from datetime import datetime
import asyncio
import re
from .smart_contract import SmartContract, ContractExecutionError

logger = logging.getLogger(__name__)

class ContractExecutor:
    """Manages smart contract deployment, execution, and lifecycle.

    The ContractExecutor ensures secure and fair contract operations within
    the ICN ecosystem. It handles:
    - Contract deployment and validation
    - Secure execution environment
    - Resource management (mana)
    - Dependency resolution
    - State management
    - Concurrent execution
    """

    # Safe imports that contracts are allowed to use
    SAFE_IMPORTS = {
        'math', 'datetime', 'json', 'collections',
        'typing', 'dataclasses', 'enum', 'decimal'
    }

    # Regular expressions for code validation
    CODE_PATTERNS = {
        'import': re.compile(r'^import\s+(\w+)'),
        'from_import': re.compile(r'^from\s+(\w+)\s+import'),
        'execute_func': re.compile(r'def\s+execute\s*\([^)]*\):')
    }

    def __init__(self, initial_mana: int = 1000, mana_regen_rate: int = 10):
        """Initialize the ContractExecutor.

        Args:
            initial_mana: Starting mana pool for contract execution
            mana_regen_rate: Rate at which mana regenerates per cycle
        """
        # Contract management
        self.contracts: Dict[str, SmartContract] = {}
        self.dependency_graph: Dict[str, Set[str]] = {}
        
        # Resource management
        self.mana_pool = initial_mana
        self.mana_regen_rate = mana_regen_rate
        self.max_mana = initial_mana * 2
        
        # Execution management
        self.execution_queue: List[Dict] = []
        self.max_queue_size = 1000
        self.execution_lock = asyncio.Lock()
        
        # Performance tracking
        self.metrics = {
            "total_executions": 0,
            "failed_executions": 0,
            "total_mana_consumed": 0,
            "average_execution_time": 0.0,
            "contracts_deployed": 0,
            "successful_deployments": 0,
            "failed_deployments": 0
        }

    async def deploy_contract(self, contract: SmartContract) -> bool:
        """Deploy a new smart contract.

        Args:
            contract: SmartContract instance to deploy

        Returns:
            bool: True if deployment successful

        Raises:
            ContractExecutionError: If deployment fails due to dependency issues
        """
        try:
            # Check for existing contract
            if contract.contract_id in self.contracts:
                logger.error(f"Contract {contract.contract_id} already exists")
                return False

            # Validate contract code
            if not await self._validate_contract_code(contract.code):
                return False

            # Check dependencies - now raises ContractExecutionError
            if not await self._validate_dependencies(contract.dependencies):
                raise ContractExecutionError("Invalid or missing dependencies")

            # Store contract and update graph
            self.contracts[contract.contract_id] = contract
            self.dependency_graph[contract.contract_id] = contract.dependencies.copy()
            
            # Update metrics
            self.metrics["contracts_deployed"] += 1
            self.metrics["successful_deployments"] += 1
            
            logger.info(f"Successfully deployed contract {contract.contract_id}")
            return True

        except ContractExecutionError:
            self.metrics["failed_deployments"] += 1
            raise

        except Exception as e:
            logger.error(f"Contract deployment failed: {str(e)}")
            self.metrics["failed_deployments"] += 1
            return False

    async def _validate_contract_code(self, code: str) -> bool:
        """Validate contract code safety and structure.

        Args:
            code: Contract source code to validate

        Returns:
            bool: True if code is safe and valid
        """
        try:
            # Check for execute function
            if not self.CODE_PATTERNS['execute_func'].search(code):
                logger.error("Contract missing execute function")
                return False

            # Validate imports
            for line in code.split('\n'):
                line = line.strip()
                if not line or line.startswith('#'):
                    continue

                # Check import statements
                import_match = self.CODE_PATTERNS['import'].match(line)
                from_match = self.CODE_PATTERNS['from_import'].match(line)

                if import_match:
                    module = import_match.group(1)
                    if module not in self.SAFE_IMPORTS:
                        logger.error(f"Unsafe import detected: {module}")
                        return False
                elif from_match:
                    module = from_match.group(1)
                    if module not in self.SAFE_IMPORTS:
                        logger.error(f"Unsafe import detected: {module}")
                        return False

            # Test compilation
            compile(code, '<string>', 'exec')
            return True

        except Exception as e:
            logger.error(f"Code validation failed: {str(e)}")
            return False

    async def execute_contract(
        self, contract_id: str, input_data: Dict, caller: str
    ) -> Dict:
        """Execute a smart contract.

        Args:
            contract_id: ID of contract to execute
            input_data: Input parameters for contract
            caller: ID of calling entity

        Returns:
            Dict containing execution results

        Raises:
            ContractExecutionError: If execution fails
        """
        async with self.execution_lock:
            try:
                # Get and validate contract
                contract = self.contracts.get(contract_id)
                if not contract:
                    raise ContractExecutionError(f"Contract {contract_id} not found")

                # Check authorization
                if caller not in contract.allowed_callers:
                    raise ContractExecutionError(f"Caller {caller} not authorized")

                # Check mana
                if self.mana_pool < contract.mana_cost:
                    raise ContractExecutionError("Insufficient mana")

                # Execute contract
                execution_start = datetime.now()
                result = contract.execute(input_data, self.mana_pool)

                # Update resources
                mana_used = result["mana_used"]
                self.mana_pool = max(0, self.mana_pool - mana_used)
                self.metrics["total_mana_consumed"] += mana_used

                # Update metrics
                execution_time = (datetime.now() - execution_start).total_seconds()
                await self._update_metrics(execution_time, True)

                return result

            except Exception as e:
                await self._update_metrics(0, False)
                if isinstance(e, ContractExecutionError):
                    raise
                raise ContractExecutionError(str(e))

    async def _validate_dependencies(self, dependencies: Set[str]) -> bool:
        """Validate contract dependencies.

        Args:
            dependencies: Set of contract IDs this contract depends on

        Returns:
            bool: True if dependencies are valid

        Raises:
            ContractExecutionError: If dependencies are invalid or missing
        """
        try:
            # Check existence
            for dep in dependencies:
                if dep not in self.contracts:
                    msg = f"Dependency not found: {dep}"
                    logger.error(msg)
                    raise ContractExecutionError(msg)

            # Check for cycles
            visited: Set[str] = set()
            path: List[str] = []

            async def check_cycle(contract_id: str) -> bool:
                if contract_id in path:
                    cycle = ' -> '.join(path + [contract_id])
                    logger.error(f"Circular dependency detected: {cycle}")
                    raise ContractExecutionError(f"Circular dependency: {cycle}")

                if contract_id in visited:
                    return True

                visited.add(contract_id)
                path.append(contract_id)

                for dep in self.dependency_graph.get(contract_id, set()):
                    if not await check_cycle(dep):
                        return False

                path.pop()
                return True

            # Check each dependency
            for dep in dependencies:
                if not await check_cycle(dep):
                    raise ContractExecutionError("Dependency cycle detected")

            return True

        except ContractExecutionError:
            raise

        except Exception as e:
            logger.error(f"Dependency validation failed: {str(e)}")
            raise ContractExecutionError(f"Dependency validation failed: {str(e)}")

    async def _update_metrics(self, execution_time: float, success: bool) -> None:
        """Update executor metrics.

        Args:
            execution_time: Time taken for execution
            success: Whether execution was successful
        """
        try:
            self.metrics["total_executions"] += 1
            if not success:
                self.metrics["failed_executions"] += 1

            # Update average execution time
            total = self.metrics["average_execution_time"] * (self.metrics["total_executions"] - 1)
            self.metrics["average_execution_time"] = (total + execution_time) / self.metrics["total_executions"]

        except Exception as e:
            logger.error(f"Failed to update metrics: {str(e)}")

    async def regenerate_mana(self) -> None:
        """Regenerate mana pool resources."""
        try:
            old_mana = self.mana_pool
            self.mana_pool = min(self.max_mana, self.mana_pool + self.mana_regen_rate)
            
            if self.mana_pool > old_mana:
                logger.debug(f"Regenerated mana: {self.mana_pool - old_mana}")

        except Exception as e:
            logger.error(f"Mana regeneration failed: {str(e)}")

    def get_metrics(self) -> Dict:
        """Get executor metrics and statistics."""
        return {
            **self.metrics,
            "current_mana": self.mana_pool,
            "queue_length": len(self.execution_queue),
            "active_contracts": len(self.contracts)
        }

    async def queue_execution(self, contract_id: str, input_data: Dict, caller: str) -> bool:
        """Queue a contract execution request.

        Args:
            contract_id: ID of contract to execute
            input_data: Input parameters
            caller: ID of calling entity

        Returns:
            bool: True if successfully queued
        """
        try:
            if len(self.execution_queue) >= self.max_queue_size:
                logger.error("Execution queue full")
                return False

            self.execution_queue.append({
                "contract_id": contract_id,
                "input_data": input_data,
                "caller": caller,
                "timestamp": datetime.now()
            })
            return True

        except Exception as e:
            logger.error(f"Failed to queue execution: {str(e)}")
            return False

    def __str__(self) -> str:
        """Return a human-readable string representation of the executor."""
        return (
            f"ContractExecutor(contracts={len(self.contracts)}, "
            f"mana={self.mana_pool}, "
            f"queue={len(self.execution_queue)})"
        )