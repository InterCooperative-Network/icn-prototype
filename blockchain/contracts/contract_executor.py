# blockchain/contracts/contract_executor.py

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
    dependencies, and coordinates their execution while enforcing
    resource limits and security constraints.
    """
    
    def __init__(self, initial_mana: int = 1000, mana_regen_rate: int = 10):
        self.contracts: Dict[str, SmartContract] = {}
        self.mana_pool = initial_mana
        self.mana_regen_rate = mana_regen_rate
        self.execution_queue: List[Dict] = []
        self.max_queue_size = 1000
        self.dependency_graph: Dict[str, Set[str]] = {}
        self.metrics: Dict = {
            'total_executions': 0,
            'failed_executions': 0,
            'total_mana_consumed': 0,
            'average_execution_time': 0,
            'contracts_deployed': 0
        }
        self.execution_lock = asyncio.Lock()

    async def deploy_contract(self, contract: SmartContract) -> bool:
        """
        Deploy a new smart contract.
        
        Args:
            contract: SmartContract instance to deploy
            
        Returns:
            bool: True if deployment successful, False otherwise
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
            self.metrics['contracts_deployed'] += 1
            
            logger.info(f"Deployed contract {contract.contract_id}")
            return True
            
        except Exception as e:
            logger.error(f"Contract deployment failed: {str(e)}")
            return False

    async def execute_contract(self, contract_id: str, input_data: Dict,
                             caller: str) -> Dict:
        """
        Execute a smart contract.
        
        Args:
            contract_id: ID of contract to execute
            input_data: Input data for contract execution
            caller: ID of the calling entity
            
        Returns:
            Dict containing execution results
            
        Raises:
            ContractExecutionError: If execution fails
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
                self.mana_pool -= result['mana_used']
                self.metrics['total_mana_consumed'] += result['mana_used']
                
                # Update metrics
                execution_time = (datetime.now() - execution_start).total_seconds()
                self._update_metrics(execution_time, True)
                
                return result
                
            except Exception as e:
                self._update_metrics(0, False)
                raise ContractExecutionError(str(e))

    def _validate_contract_code(self, code: str) -> bool:
        """Validate contract code for basic safety."""
        try:
            # Check for dangerous imports
            dangerous_imports = ['os', 'sys', 'subprocess', 'importlib']
            for imp in dangerous_imports:
                if f"import {imp}" in code or f"from {imp}" in code:
                    logger.error(f"Dangerous import detected: {imp}")
                    return False
            
            # Check for dangerous built-ins
            dangerous_builtins = ['exec', 'eval', 'open', '__import__']
            for builtin in dangerous_builtins:
                if builtin in code:
                    logger.error(f"Dangerous builtin detected: {builtin}")
                    # blockchain/contracts/contract_executor.py (continued)

                    return False
            
            # Validate code compiles
            compile(code, '<string>', 'exec')
            
            return True
            
        except Exception as e:
            logger.error(f"Contract code validation failed: {str(e)}")
            return False

    def _validate_dependencies(self, dependencies: Set[str]) -> bool:
        """Validate contract dependencies exist and are cyclical."""
        try:
            # Check all dependencies exist
            for dep in dependencies:
                if dep not in self.contracts:
                    logger.error(f"Dependency not found: {dep}")
                    return False
            
            # Check for circular dependencies
            visited = set()
            path = []
            
            def check_cycle(contract_id: str) -> bool:
                if contract_id in path:
                    logger.error(f"Circular dependency detected: {' -> '.join(path + [contract_id])}")
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
            
            # Check each dependency
            for dep in dependencies:
                if not check_cycle(dep):
                    return False
            
            return True
            
        except Exception as e:
            logger.error(f"Dependency validation failed: {str(e)}")
            return False

    def _update_metrics(self, execution_time: float, success: bool) -> None:
        """Update executor metrics after contract execution."""
        try:
            self.metrics['total_executions'] += 1
            
            if not success:
                self.metrics['failed_executions'] += 1
            
            # Update average execution time
            total_time = (self.metrics['average_execution_time'] * 
                         (self.metrics['total_executions'] - 1))
            self.metrics['average_execution_time'] = (
                (total_time + execution_time) / self.metrics['total_executions']
            )
            
        except Exception as e:
            logger.error(f"Failed to update metrics: {str(e)}")

    async def regenerate_mana(self) -> None:
        """Regenerate mana in the pool."""
        try:
            old_mana = self.mana_pool
            self.mana_pool = min(1000, self.mana_pool + self.mana_regen_rate)
            
            if self.mana_pool > old_mana:
                logger.debug(f"Regenerated mana: {self.mana_pool - old_mana}")
                
        except Exception as e:
            logger.error(f"Mana regeneration failed: {str(e)}")

    async def process_execution_queue(self) -> None:
        """Process pending contract executions."""
        while True:
            try:
                if not self.execution_queue:
                    await asyncio.sleep(1)
                    continue
                
                # Get next execution request
                request = self.execution_queue.pop(0)
                
                # Execute contract
                await self.execute_contract(
                    request['contract_id'],
                    request['input_data'],
                    request['caller']
                )
                
                # Regenerate some mana
                await self.regenerate_mana()
                
            except Exception as e:
                logger.error(f"Queue processing error: {str(e)}")
                await asyncio.sleep(1)

    async def queue_execution(self, contract_id: str, input_data: Dict,
                            caller: str) -> bool:
        """Queue a contract for execution."""
        try:
            if len(self.execution_queue) >= self.max_queue_size:
                logger.error("Execution queue full")
                return False
            
            self.execution_queue.append({
                'contract_id': contract_id,
                'input_data': input_data,
                'caller': caller,
                'timestamp': datetime.now()
            })
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to queue execution: {str(e)}")
            return False

    def get_contract(self, contract_id: str) -> Optional[SmartContract]:
        """Get a contract by ID."""
        return self.contracts.get(contract_id)

    def list_contracts(self) -> List[Dict]:
        """Get list of all deployed contracts."""
        return [
            {
                'contract_id': contract.contract_id,
                'creator': contract.creator,
                'version': contract.version,
                'dependencies': list(contract.dependencies),
                'metrics': contract.get_metrics()
            }
            for contract in self.contracts.values()
        ]

    def get_metrics(self) -> Dict:
        """Get executor metrics."""
        return {
            **self.metrics,
            'current_mana': self.mana_pool,
            'queue_length': len(self.execution_queue),
            'active_contracts': len(self.contracts)
        }

    async def start(self) -> None:
        """Start the contract executor."""
        logger.info("Starting contract executor")
        asyncio.create_task(self.process_execution_queue())
        asyncio.create_task(self._mana_regeneration_loop())

    async def _mana_regeneration_loop(self) -> None:
        """Continuously regenerate mana."""
        while True:
            await self.regenerate_mana()
            await asyncio.sleep(60)  # Regenerate every minute

    def __str__(self) -> str:
        """Return a human-readable string representation of the executor."""
        return (f"ContractExecutor(contracts={len(self.contracts)}, "
                f"mana={self.mana_pool}, "
                f"queue={len(self.execution_queue)})")