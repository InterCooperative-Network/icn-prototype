import unittest
from datetime import datetime, timedelta
import sys
import os
from typing import List, Dict, Optional
import asyncio
import json
import hashlib

# Add project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from blockchain.contracts.smart_contract import SmartContract, ContractExecutionError
from blockchain.contracts.contract_executor import ContractExecutor
from blockchain.core.node import Node
from blockchain.core.shard import Shard

class TestSmartContractExecution(unittest.TestCase):
    """Integration tests for smart contract execution in the ICN blockchain."""

    def setUp(self):
        """Set up test environment before each test."""
        self.executor = ContractExecutor(initial_mana=1000)
        self.test_node = self._create_test_node()
        
        # Basic test contract for reuse
        self.basic_contract = self._create_test_contract("basic_contract")
        
        # Set up async event loop
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

    def tearDown(self):
        """Clean up after tests."""
        self.loop.close()

    def _create_test_node(self) -> Node:
        """Create a test node for contract interactions."""
        node = Node(
            node_id="test_node",
            cooperative_id="test_coop",
            initial_stake=100.0
        )
        node.reputation_scores = {
            "validation": 25.0,
            "resource_sharing": 25.0,
            "cooperative_growth": 25.0,
            "innovation": 25.0
        }
        return node

    def _create_test_contract(self, contract_id: str, mana_cost: int = 10) -> SmartContract:
        """Create a test smart contract."""
        code = """
def execute(input_data, state):
    # Initialize state if needed
    if 'count' not in state:
        state['count'] = 0
    if 'values' not in state:
        state['values'] = []
        
    # Process input
    value = input_data.get('value', 0)
    operation = input_data.get('operation', 'add')
    
    if operation == 'add':
        state['values'].append(value)
        state['count'] += 1
        return {'result': value, 'count': state['count']}
    elif operation == 'sum':
        total = sum(state['values'])
        return {'result': total, 'count': state['count']}
    elif operation == 'clear':
        state['values'] = []
        state['count'] = 0
        return {'result': None, 'count': 0}
    else:
        raise ValueError(f"Unknown operation: {operation}")
"""
        return SmartContract(
            contract_id=contract_id,
            code=code,
            creator="test_creator",
            mana_cost=mana_cost
        )

    async def test_basic_contract_execution(self):
        """Test basic contract deployment and execution."""
        # Deploy contract
        success = await self.executor.deploy_contract(self.basic_contract)
        self.assertTrue(success)

        # Execute contract with different operations
        result = await self.executor.execute_contract(
            self.basic_contract.contract_id,
            {"value": 42, "operation": "add"},
            "test_creator"
        )
        self.assertEqual(result["result"]["result"], 42)
        self.assertEqual(result["result"]["count"], 1)

        result = await self.executor.execute_contract(
            self.basic_contract.contract_id,
            {"value": 58, "operation": "add"},
            "test_creator"
        )
        self.assertEqual(result["result"]["count"], 2)

        result = await self.executor.execute_contract(
            self.basic_contract.contract_id,
            {"operation": "sum"},
            "test_creator"
        )
        self.assertEqual(result["result"]["result"], 100)

    async def test_mana_consumption(self):
        """Test mana consumption and regeneration."""
        initial_mana = self.executor.mana_pool

        # Deploy and execute contract
        await self.executor.deploy_contract(self.basic_contract)
        result = await self.executor.execute_contract(
            self.basic_contract.contract_id,
            {"value": 42, "operation": "add"},
            "test_creator"
        )

        # Verify mana consumption
        self.assertEqual(
            self.executor.mana_pool,
            initial_mana - self.basic_contract.mana_cost
        )

        # Test mana regeneration
        await self.executor.regenerate_mana()
        self.assertGreater(self.executor.mana_pool, initial_mana - self.basic_contract.mana_cost)

    async def test_state_persistence(self):
        """Test contract state persistence across executions."""
        await self.executor.deploy_contract(self.basic_contract)

        # Add values
        for i in range(3):
            await self.executor.execute_contract(
                self.basic_contract.contract_id,
                {"value": i * 10, "operation": "add"},
                "test_creator"
            )

        # Verify state
        result = await self.executor.execute_contract(
            self.basic_contract.contract_id,
            {"operation": "sum"},
            "test_creator"
        )
        self.assertEqual(result["result"]["result"], 30)  # 0 + 10 + 20
        self.assertEqual(result["result"]["count"], 3)

        # Clear state
        result = await self.executor.execute_contract(
            self.basic_contract.contract_id,
            {"operation": "clear"},
            "test_creator"
        )
        self.assertEqual(result["result"]["count"], 0)

    async def test_concurrent_execution(self):
        """Test concurrent contract execution."""
        await self.executor.deploy_contract(self.basic_contract)

        # Create multiple execution tasks
        tasks = []
        for i in range(5):
            task = self.executor.execute_contract(
                self.basic_contract.contract_id,
                {"value": i * 10, "operation": "add"},
                "test_creator"
            )
            tasks.append(task)

        # Execute concurrently
        results = await asyncio.gather(*tasks)
        
        # Verify results
        self.assertEqual(len(results), 5)
        
        # Verify final state
        sum_result = await self.executor.execute_contract(
            self.basic_contract.contract_id,
            {"operation": "sum"},
            "test_creator"
        )
        self.assertEqual(sum_result["result"]["result"], 100)  # 0 + 10 + 20 + 30 + 40

    async def test_error_handling(self):
        """Test contract error handling."""
        await self.executor.deploy_contract(self.basic_contract)

        # Test invalid operation
        with self.assertRaises(ContractExecutionError):
            await self.executor.execute_contract(
                self.basic_contract.contract_id,
                {"operation": "invalid_op"},
                "test_creator"
            )

        # Test insufficient mana
        expensive_contract = self._create_test_contract("expensive", mana_cost=2000)
        await self.executor.deploy_contract(expensive_contract)
        
        with self.assertRaises(ContractExecutionError):
            await self.executor.execute_contract(
                expensive_contract.contract_id,
                {"value": 1, "operation": "add"},
                "test_creator"
            )

    async def test_resource_limits(self):
        """Test contract resource limits."""
        # Create contract that tests limits
        resource_heavy_code = """
def execute(input_data, state):
    # Test memory limit
    if input_data.get('test_memory', False):
        big_list = list(range(1000000))  # Should exceed memory limit
        
    # Test computation limit
    if input_data.get('test_computation', False):
        n = 100
        result = [[i*j for j in range(n)] for i in range(n)]
        
    return {"status": "completed"}
"""
        resource_contract = SmartContract(
            contract_id="resource_test",
            code=resource_heavy_code,
            creator="test_creator",
            mana_cost=20
        )

        await self.executor.deploy_contract(resource_contract)

        # Test memory limit
        with self.assertRaises(ContractExecutionError):
            await self.executor.execute_contract(
                resource_contract.contract_id,
                {"test_memory": True},
                "test_creator"
            )

        # Test computation limit
        with self.assertRaises(ContractExecutionError):
            await self.executor.execute_contract(
                resource_contract.contract_id,
                {"test_computation": True},
                "test_creator"
            )

    async def test_cooperative_features(self):
        """Test cooperative aspects of contract execution."""
        # Create contract with cooperative features
        coop_code = """
def execute(input_data, state):
    if 'shared_resources' not in state:
        state['shared_resources'] = {}
    
    action = input_data.get('action')
    resource = input_data.get('resource')
    amount = input_data.get('amount', 0)
    
    if action == 'share':
        if resource not in state['shared_resources']:
            state['shared_resources'][resource] = 0
        state['shared_resources'][resource] += amount
        return {
            'status': 'shared',
            'resource': resource,
            'total': state['shared_resources'][resource]
        }
    elif action == 'use':
        if resource not in state['shared_resources']:
            raise ValueError(f"Resource {resource} not available")
        if state['shared_resources'][resource] < amount:
            raise ValueError(f"Insufficient {resource}")
        state['shared_resources'][resource] -= amount
        return {
            'status': 'used',
            'resource': resource,
            'remaining': state['shared_resources'][resource]
        }
            
    return {'status': 'error', 'message': 'Invalid action'}
"""
        coop_contract = SmartContract(
            contract_id="cooperative_test",
            code=coop_code,
            creator="test_creator",
            mana_cost=15
        )

        await self.executor.deploy_contract(coop_contract)

        # Test resource sharing
        result = await self.executor.execute_contract(
            coop_contract.contract_id,
            {
                "action": "share",
                "resource": "cpu_time",
                "amount": 100
            },
            "test_creator"
        )
        self.assertEqual(result["result"]["total"], 100)

        # Test resource usage
        result = await self.executor.execute_contract(
            coop_contract.contract_id,
            {
                "action": "use",
                "resource": "cpu_time",
                "amount": 30
            },
            "test_creator"
        )
        self.assertEqual(result["result"]["remaining"], 70)

        # Test insufficient resources
        with self.assertRaises(ContractExecutionError):
            await self.executor.execute_contract(
                coop_contract.contract_id,
                {
                    "action": "use",
                    "resource": "cpu_time",
                    "amount": 100
                },
                "test_creator"
            )

    def test_contract_metrics(self):
        """Test contract execution metrics tracking."""
        async def run_metrics_test():
            await self.executor.deploy_contract(self.basic_contract)

            # Execute contract multiple times
            for i in range(5):
                await self.executor.execute_contract(
                    self.basic_contract.contract_id,
                    {"value": i, "operation": "add"},
                    "test_creator"
                )

            # Get metrics
            metrics = self.basic_contract.get_metrics()
            
            # Verify metrics
            self.assertEqual(metrics["execution_count"], 5)
            self.assertGreater(metrics["total_mana_consumed"], 0)
            self.assertIsNotNone(metrics["last_executed"])
            self.assertGreater(metrics["state_size"], 0)

        self.loop.run_until_complete(run_metrics_test())

    def test_contract_authorization(self):
        """Test contract authorization controls."""
        async def run_auth_test():
            await self.executor.deploy_contract(self.basic_contract)

            # Test unauthorized execution
            with self.assertRaises(ContractExecutionError):
                await self.executor.execute_contract(
                    self.basic_contract.contract_id,
                    {"value": 1, "operation": "add"},
                    "unauthorized_user"
                )

            # Add authorized user
            self.basic_contract.authorize_caller("new_user")
            
            # Test authorized execution
            result = await self.executor.execute_contract(
                self.basic_contract.contract_id,
                {"value": 1, "operation": "add"},
                "new_user"
            )
            self.assertIsNotNone(result)

        self.loop.run_until_complete(run_auth_test())

if __name__ == "__main__":
    unittest.main()