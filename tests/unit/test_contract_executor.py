"""
tests/unit/test_contract_executor.py

Unit tests for the ContractExecutor class, handling contract deployment,
execution, and lifecycle management.
"""

import pytest
from datetime import datetime, timedelta
import sys
import os
import asyncio
from typing import Dict, List
import logging

# Add project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from blockchain.contracts.contract_executor import ContractExecutor
from blockchain.contracts.smart_contract import SmartContract, ContractExecutionError

class TestContractExecutor:
    """Test cases for the ContractExecutor class."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup test instance with fresh ContractExecutor."""
        self.executor = ContractExecutor(initial_mana=1000, mana_regen_rate=10)

    @pytest.fixture
    def basic_contract_code(self):
        """Fixture providing basic test contract code."""
        return """
def execute(input_data, state):
    a = input_data.get('a', 0)
    b = input_data.get('b', 0)
    result = a + b
    state['last_result'] = result
    return result
"""

    @pytest.fixture
    def test_contract(self, basic_contract_code):
        """Fixture providing a test contract instance."""
        return SmartContract(
            contract_id="test_contract",
            code=basic_contract_code,
            creator="test_creator",
            mana_cost=10
        )

    @pytest.mark.asyncio
    async def test_deploy_contract(self, test_contract):
        """Test contract deployment functionality."""
        # Test successful deployment
        success = await self.executor.deploy_contract(test_contract)
        assert success
        assert test_contract.contract_id in self.executor.contracts
        
        # Test duplicate deployment
        success = await self.executor.deploy_contract(test_contract)
        assert not success
        
        # Test contract with invalid code
        invalid_contract = SmartContract(
            contract_id="invalid_contract",
            code="invalid python code :",
            creator="test_creator"
        )
        success = await self.executor.deploy_contract(invalid_contract)
        assert not success

    @pytest.mark.asyncio
    async def test_execute_contract(self, test_contract):
        """Test contract execution."""
        await self.executor.deploy_contract(test_contract)
        
        result = await self.executor.execute_contract(
            test_contract.contract_id,
            {"a": 5, "b": 3},
            "test_creator"
        )
        assert result is not None
        assert result.get("result") == 8
        
        # Test insufficient mana
        self.executor.mana_pool = 5
        with pytest.raises(ContractExecutionError):
            await self.executor.execute_contract(
                test_contract.contract_id,
                {"a": 1, "b": 2},
                "test_creator"
            )

    @pytest.mark.asyncio
    async def test_execution_queue(self, test_contract):
        """Test contract execution queue functionality."""
        await self.executor.deploy_contract(test_contract)
        
        queue_success = await self.executor.queue_execution(
            test_contract.contract_id,
            {"a": 1, "b": 2},
            "test_creator"
        )
        assert queue_success

        # Test queue size limit
        self.executor.max_queue_size = 1
        queue_success = await self.executor.queue_execution(
            test_contract.contract_id,
            {"a": 3, "b": 4},
            "test_creator"
        )
        assert not queue_success

    @pytest.mark.asyncio
    async def test_dependency_management(self, basic_contract_code):
        """Test contract dependency management."""
        base_contract = SmartContract(
            contract_id="base_contract",
            code=basic_contract_code,
            creator="test_creator"
        )
        
        dependent_contract = SmartContract(
            contract_id="dependent_contract",
            code="""
def execute(input_data, state):
    value = input_data.get('value', 0)
    state['processed'] = value * 2
    return state['processed']
""",
            creator="test_creator"
        )
        dependent_contract.dependencies.add(base_contract.contract_id)
        
        # Test deployment order validation
        with pytest.raises(ContractExecutionError):
            await self.executor.deploy_contract(dependent_contract)
        
        await self.executor.deploy_contract(base_contract)
        success = await self.executor.deploy_contract(dependent_contract)
        assert success

    @pytest.mark.asyncio
    async def test_mana_regeneration(self, test_contract):
        """Test mana regeneration functionality."""
        initial_mana = self.executor.mana_pool
        
        await self.executor.deploy_contract(test_contract)
        await self.executor.execute_contract(
            test_contract.contract_id,
            {"a": 1, "b": 2},
            "test_creator"
        )
        
        used_mana = initial_mana - self.executor.mana_pool
        await self.executor.regenerate_mana()
        assert self.executor.mana_pool > initial_mana - used_mana

    def test_metrics_collection(self):
        """Test metrics collection and reporting."""
        metrics = self.executor.get_metrics()
        
        assert "total_executions" in metrics
        assert "failed_executions" in metrics
        assert "total_mana_consumed" in metrics
        assert "average_execution_time" in metrics
        assert "contracts_deployed" in metrics
        assert "queue_length" in metrics

    @pytest.mark.asyncio
    async def test_execution_limits(self):
        """Test contract execution limits and restrictions."""
        long_running_contract = SmartContract(
            contract_id="long_running",
            code="""
def execute(input_data, state):
    import time
    time.sleep(6)  # Exceed time limit
    return True
""",
            creator="test_creator"
        )
        
        await self.executor.deploy_contract(long_running_contract)
        
        with pytest.raises(ContractExecutionError):
            await self.executor.execute_contract(
                long_running_contract.contract_id,
                {},
                "test_creator"
            )

    @pytest.mark.asyncio
    async def test_concurrent_execution(self, test_contract):
        """Test concurrent contract execution handling."""
        await self.executor.deploy_contract(test_contract)
        
        tasks = []
        for i in range(5):
            task = self.executor.execute_contract(
                test_contract.contract_id,
                {"a": i, "b": i},
                "test_creator"
            )
            tasks.append(task)
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        successful = [r for r in results if not isinstance(r, Exception)]
        assert len(successful) == 5

    @pytest.mark.asyncio
    async def test_error_recovery(self):
        """Test error recovery and state management."""
        failing_contract = SmartContract(
            contract_id="failing_contract",
            code="""
def execute(input_data, state):
    if input_data.get('fail', False):
        raise ValueError('Intended failure')
    state['value'] = input_data.get('value', 0)
    return state['value']
""",
            creator="test_creator"
        )
        
        await self.executor.deploy_contract(failing_contract)
        
        result = await self.executor.execute_contract(
            failing_contract.contract_id,
            {"value": 42},
            "test_creator"
        )
        assert result["result"] == 42
        
        with pytest.raises(ContractExecutionError):
            await self.executor.execute_contract(
                failing_contract.contract_id,
                {"fail": True},
                "test_creator"
            )
        
        result = await self.executor.execute_contract(
            failing_contract.contract_id,
            {"value": 100},
            "test_creator"
        )
        assert result["result"] == 100

    @pytest.mark.asyncio
    async def test_authorization(self, test_contract):
        """Test contract authorization controls."""
        await self.executor.deploy_contract(test_contract)
        
        with pytest.raises(ContractExecutionError):
            await self.executor.execute_contract(
                test_contract.contract_id,
                {"a": 1, "b": 2},
                "unauthorized_user"
            )
        
        test_contract.authorize_caller("new_user")
        result = await self.executor.execute_contract(
            test_contract.contract_id,
            {"a": 1, "b": 2},
            "new_user"
        )
        assert result is not None

    @pytest.mark.asyncio
    async def test_contract_cleanup(self, basic_contract_code):
        """Test contract cleanup and resource management."""
        contracts = []
        for i in range(5):
            contract = SmartContract(
                contract_id=f"contract_{i}",
                code=basic_contract_code,
                creator="test_creator"
            )
            contracts.append(contract)
            await self.executor.deploy_contract(contract)
        
        metrics = self.executor.get_metrics()
        assert metrics["contracts_deployed"] == 5
        
        for contract in contracts:
            assert contract.contract_id in self.executor.contracts