import unittest
from datetime import datetime, timedelta
import sys
import os
from typing import Dict, Optional

# Add project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from blockchain.contracts.smart_contract import SmartContract, ContractExecutionError

class TestSmartContract(unittest.TestCase):
    """Test cases for the SmartContract class."""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.sample_code = """
def execute(input_data, state):
    # Simple contract that adds numbers
    a = input_data.get('a', 0)
    b = input_data.get('b', 0)
    result = a + b
    state['last_result'] = result
    return result
"""
        self.contract = SmartContract(
            contract_id="test_contract",
            code=self.sample_code,
            creator="test_creator",
            mana_cost=10,
            version="1.0"
        )

    def test_initialization(self):
        """Test smart contract initialization and attributes."""
        self.assertEqual(self.contract.contract_id, "test_contract")
        self.assertEqual(self.contract.creator, "test_creator")
        self.assertEqual(self.contract.mana_cost, 10)
        self.assertEqual(self.contract.version, "1.0")
        self.assertEqual(self.contract.state, {})
        self.assertEqual(self.contract.execution_count, 0)
        self.assertEqual(self.contract.total_mana_consumed, 0)
        self.assertIsNone(self.contract.last_executed)
        self.assertIn(self.contract.creator, self.contract.allowed_callers)

    def test_execute_valid_contract(self):
        """Test execution of a valid contract."""
        input_data = {"a": 5, "b": 3}
        result = self.contract.execute(input_data, available_mana=20)
        
        self.assertEqual(result["result"], 8)
        self.assertEqual(result["mana_used"], 10)
        self.assertEqual(self.contract.state["last_result"], 8)
        self.assertEqual(self.contract.execution_count, 1)
        self.assertEqual(self.contract.total_mana_consumed, 10)
        self.assertIsNotNone(self.contract.last_executed)

    def test_execute_insufficient_mana(self):
        """Test execution with insufficient mana."""
        input_data = {"a": 5, "b": 3}
        
        with self.assertRaises(ContractExecutionError) as context:
            self.contract.execute(input_data, available_mana=5)
        
        self.assertIn("Insufficient mana", str(context.exception))
        self.assertEqual(self.contract.execution_count, 0)
        self.assertEqual(self.contract.total_mana_consumed, 0)

    def test_execute_invalid_code(self):
        """Test execution with invalid contract code."""
        invalid_contract = SmartContract(
            contract_id="invalid_contract",
            code="def invalid_function(): return 'no execute'",
            creator="test_creator",
            mana_cost=10
        )
        
        with self.assertRaises(ContractExecutionError) as context:
            invalid_contract.execute({}, available_mana=20)
        
        self.assertIn("Contract missing execute function", str(context.exception))

    def test_execute_with_state_updates(self):
        """Test contract execution with state updates."""
        # First execution
        result1 = self.contract.execute({"a": 5, "b": 3}, available_mana=20)
        self.assertEqual(self.contract.state["last_result"], 8)
        
        # Second execution
        result2 = self.contract.execute({"a": 2, "b": 4}, available_mana=20)
        self.assertEqual(self.contract.state["last_result"], 6)
        
        self.assertEqual(self.contract.execution_count, 2)
        self.assertEqual(self.contract.total_mana_consumed, 20)

    def test_execution_limits(self):
        """Test contract execution limits."""
        # Set low daily limit for testing
        self.contract.restrictions["max_daily_executions"] = 2
        
        # First execution
        self.contract.execute({"a": 1, "b": 2}, available_mana=20)
        # Second execution
        self.contract.execute({"a": 3, "b": 4}, available_mana=20)
        
        # Third execution should fail
        with self.assertRaises(ContractExecutionError) as context:
            self.contract.execute({"a": 5, "b": 6}, available_mana=20)
        
        self.assertIn("Daily execution limit exceeded", str(context.exception))
        self.assertEqual(self.contract.execution_count, 2)

    def test_authorize_and_revoke_caller(self):
        """Test caller authorization management."""
        new_caller = "new_caller"
        
        # Test authorization
        self.assertTrue(self.contract.authorize_caller(new_caller))
        self.assertIn(new_caller, self.contract.allowed_callers)
        
        # Test revocation
        self.assertTrue(self.contract.revoke_caller(new_caller))
        self.assertNotIn(new_caller, self.contract.allowed_callers)
        
        # Test creator cannot be revoked
        self.assertFalse(self.contract.revoke_caller(self.contract.creator))
        self.assertIn(self.contract.creator, self.contract.allowed_callers)

    def test_update_restrictions(self):
        """Test updating contract restrictions."""
        new_restrictions = {
            "max_state_size": 2048,
            "max_execution_time": 10
        }
        
        self.assertTrue(self.contract.update_restrictions(new_restrictions))
        self.assertEqual(self.contract.restrictions["max_state_size"], 2048)
        self.assertEqual(self.contract.restrictions["max_execution_time"], 10)
        
        # Test invalid restriction update
        invalid_restrictions = {"invalid_key": 100}
        self.assertFalse(self.contract.update_restrictions(invalid_restrictions))

    def test_serialization(self):
        """Test contract serialization and deserialization."""
        # Execute contract to populate some data
        self.contract.execute({"a": 5, "b": 3}, available_mana=20)
        
        # Convert to dictionary
        contract_dict = self.contract.to_dict()
        
        # Create new contract from dictionary
        new_contract = SmartContract.from_dict(contract_dict)
        
        # Verify attributes
        self.assertEqual(new_contract.contract_id, self.contract.contract_id)
        self.assertEqual(new_contract.creator, self.contract.creator)
        self.assertEqual(new_contract.code, self.contract.code)
        self.assertEqual(new_contract.mana_cost, self.contract.mana_cost)
        self.assertEqual(new_contract.version, self.contract.version)
        self.assertEqual(new_contract.state, self.contract.state)
        self.assertEqual(new_contract.restrictions, self.contract.restrictions)

    def test_execution_history(self):
        """Test execution history tracking."""
        # Multiple executions
        self.contract.execute({"a": 1, "b": 2}, available_mana=20)
        self.contract.execute({"a": 3, "b": 4}, available_mana=20)
        
        # Check history
        self.assertEqual(len(self.contract.execution_history), 2)
        
        # Verify history entries
        latest_execution = self.contract.execution_history[-1]
        self.assertIn("timestamp", latest_execution)
        self.assertIn("execution_time", latest_execution)
        self.assertIn("mana_used", latest_execution)
        self.assertIn("state_size", latest_execution)

    def test_get_metrics(self):
        """Test contract metrics calculation."""
        # Execute contract
        self.contract.execute({"a": 5, "b": 3}, available_mana=20)
        
        metrics = self.contract.get_metrics()
        
        self.assertEqual(metrics["contract_id"], "test_contract")
        self.assertEqual(metrics["version"], "1.0")
        self.assertEqual(metrics["creator"], "test_creator")
        self.assertEqual(metrics["execution_count"], 1)
        self.assertEqual(metrics["total_mana_consumed"], 10)
        self.assertGreater(metrics["state_size"], 0)

    def test_state_size_limit(self):
        """Test contract state size limitations."""
        # Create contract that grows state
        growing_code = """
def execute(input_data, state):
    # Add large data to state
    state['data'] = 'x' * input_data['size']
    return len(state['data'])
"""
        growing_contract = SmartContract(
            contract_id="growing_contract",
            code=growing_code,
            creator="test_creator",
            mana_cost=10
        )
        
        # Set small state size limit
        growing_contract.restrictions["max_state_size"] = 100
        
        # Execute with small state update
        growing_contract.execute({"size": 50}, available_mana=20)
        
        # Execute with too large state update
        with self.assertRaises(ContractExecutionError) as context:
            growing_contract.execute({"size": 200}, available_mana=20)
        
        self.assertIn("State size limit exceeded", str(context.exception))

    def test_dependencies(self):
        """Test contract dependency management."""
        dependency_id = "dependency_contract"
        
        # Add dependency
        self.contract.dependencies.add(dependency_id)
        self.assertIn(dependency_id, self.contract.dependencies)
        
        # Verify serialization includes dependencies
        contract_dict = self.contract.to_dict()
        self.assertIn(dependency_id, contract_dict["dependencies"])
        
        # Create new contract from dict and verify dependencies
        new_contract = SmartContract.from_dict(contract_dict)
        self.assertIn(dependency_id, new_contract.dependencies)

    def test_metadata_updates(self):
        """Test contract metadata management."""
        # Update metadata
        self.contract.metadata["description"] = "Test contract"
        self.contract.metadata["tags"].add("test")
        
        # Verify serialization includes metadata
        contract_dict = self.contract.to_dict()
        self.assertEqual(contract_dict["metadata"]["description"], "Test contract")
        self.assertIn("test", contract_dict["metadata"]["tags"])
        
        # Create new contract and verify metadata
        new_contract = SmartContract.from_dict(contract_dict)
        self.assertEqual(new_contract.metadata["description"], "Test contract")
        self.assertIn("test", new_contract.metadata["tags"])

if __name__ == '__main__':
    unittest.main()