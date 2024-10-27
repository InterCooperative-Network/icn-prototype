"""
blockchain/tests/integration/test_system.py

Integration tests for the entire ICN blockchain system.
Tests overall system functionality and component interactions.
"""

import pytest
import asyncio
from datetime import datetime
import logging
from typing import List, Dict, Optional

from blockchain.integration import BlockchainSystem, SystemConfig
from blockchain.network.config import NetworkConfig
from blockchain.core.node import Node
from blockchain.core.transaction import Transaction
from blockchain.core.block import Block
from blockchain.consensus.proof_of_cooperation import ProofOfCooperation

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@pytest.fixture
async def system():
    """Create a test blockchain system."""
    config = SystemConfig(
        network_config=NetworkConfig(node_id="test_system"),
        num_shards=2,
        initial_mana=1000,
        mana_regen_rate=10,
        min_nodes=3,
        max_nodes=100
    )
    system = BlockchainSystem(config)
    await system.initialize()
    yield system
    await system.shutdown()

@pytest.fixture
def test_node():
    """Create a test node."""
    node = Node(
        node_id="test_node",
        cooperative_id="test_coop",
        initial_stake=100.0
    )
    # Set up node capabilities
    node.reputation_scores = {
        "validation": 25.0,
        "transaction_validation": 25.0,
        "resource_sharing": 25.0,
        "cooperative_growth": 25.0,
        "innovation": 25.0
    }
    node.performance_metrics = {
        "availability": 98.0,
        "validation_success_rate": 95.0,
        "network_reliability": 97.0
    }
    node.metadata.update({
        "address": "127.0.0.1",
        "port": 30303
    })
    return node

@pytest.mark.asyncio
async def test_system_initialization(system):
    """Test complete system initialization."""
    # Verify core components
    assert system.started
    assert system.blockchain is not None
    assert system.consensus is not None
    assert system.network is not None
    assert system.contract_executor is not None

    # Verify initial state
    metrics = system.get_system_status()
    assert metrics["started"]
    assert metrics["node_count"] == 0
    assert metrics["shard_count"] == 2
    assert "blockchain_metrics" in metrics
    assert "consensus_metrics" in metrics
    assert "network_metrics" in metrics

@pytest.mark.asyncio
async def test_node_management(system, test_node):
    """Test comprehensive node management."""
    # Add node
    success = await system.add_node(test_node)
    assert success
    assert test_node.node_id in system.blockchain.nodes

    # Verify node capabilities
    node = system.blockchain.nodes[test_node.node_id]
    assert node.can_validate()
    assert node.get_total_reputation() > 0

    # Test node network integration
    assert await system.network.ping_peer(test_node.node_id)

    # Test node overload prevention
    nodes = []
    for i in range(system.config.max_nodes + 1):
        node = Node(
            node_id=f"node_{i}",
            cooperative_id="test_coop",
            initial_stake=100.0
        )
        success = await system.add_node(node)
        if i < system.config.max_nodes:
            assert success
            nodes.append(node)
        else:
            assert not success

@pytest.mark.asyncio
async def test_transaction_processing(system, test_node):
    """Test end-to-end transaction processing."""
    # Add node first
    await system.add_node(test_node)

    # Create test transaction
    transaction = {
        "sender": test_node.node_id,
        "receiver": "test_receiver",
        "action": "transfer",
        "data": {"amount": 50},
        "shard_id": 0
    }

    # Process transaction
    success = await system.process_transaction(transaction)
    assert success

    # Verify transaction in pool
    found = False
    for shard in system.blockchain.shards.values():
        if any(tx.sender == test_node.node_id for tx in shard.pending_transactions):
            found = True
            break
    assert found

    # Create block with transaction
    success = await system.create_block(test_node.node_id)
    assert success

    # Verify transaction is no longer pending
    for shard in system.blockchain.shards.values():
        assert not any(tx.sender == test_node.node_id for tx in shard.pending_transactions)

@pytest.mark.asyncio
async def test_block_creation_and_consensus(system, test_node):
    """Test block creation with consensus mechanism."""
    await system.add_node(test_node)

    # Add multiple transactions
    transactions = []
    for i in range(3):
        tx = {
            "sender": test_node.node_id,
            "receiver": f"receiver_{i}",
            "action": "transfer",
            "data": {"amount": 50 + i},
            "shard_id": 0
        }
        success = await system.process_transaction(tx)
        assert success
        transactions.append(tx)

    # Create block
    success = await system.create_block(test_node.node_id)
    assert success

    # Verify block in chain
    latest_block = None
    for shard in system.blockchain.shards.values():
        if shard.chain[-1].validator == test_node.node_id:
            latest_block = shard.chain[-1]
            break
    assert latest_block is not None
    assert len(latest_block.transactions) > 0

@pytest.mark.asyncio
async def test_cross_shard_operation(system, test_node):
    """Test cross-shard transaction handling."""
    await system.add_node(test_node)

    # Create cross-shard transaction
    transaction = {
        "sender": test_node.node_id,
        "receiver": "cross_shard_receiver",
        "action": "transfer",
        "data": {
            "amount": 75,
            "target_shard": 1
        },
        "shard_id": 0,
        "cross_shard_refs": ["test_ref"]
    }

    # Process transaction
    success = await system.process_transaction(transaction)
    assert success

    # Create blocks in both shards
    success1 = await system.create_block(test_node.node_id, shard_id=0)
    success2 = await system.create_block(test_node.node_id, shard_id=1)
    assert success1 and success2

    # Verify cross-shard references
    for shard in system.blockchain.shards.values():
        if shard.shard_id == 0:
            assert any(block.cross_shard_refs for block in shard.chain[-2:])

@pytest.mark.asyncio
async def test_network_communication(system, test_node):
    """Test network communication and message propagation."""
    await system.add_node(test_node)

    # Create test message
    message = {
        "type": "test_message",
        "content": "test content",
        "timestamp": datetime.now().isoformat()
    }

    # Broadcast message
    await system.network.broadcast_message(
        "test_message",
        message
    )

    # Verify network metrics
    metrics = system.network.get_metrics()
    assert metrics["messages_sent"] > 0
    assert "broadcast_messages" in metrics

@pytest.mark.asyncio
async def test_system_recovery(system, test_node):
    """Test system recovery from failure scenarios."""
    await system.add_node(test_node)

    # Simulate network interruption
    await system.network.stop()
    assert not system.network.is_running

    # Recover network
    await system.network.start()
    assert system.network.is_running

    # Verify node still accessible
    assert test_node.node_id in system.blockchain.nodes

@pytest.mark.asyncio
async def test_resource_management(system, test_node):
    """Test system resource management and limits."""
    await system.add_node(test_node)

    # Track initial mana
    initial_mana = system.blockchain.cooperative_mana

    # Create resource-intensive transaction
    transaction = {
        "sender": test_node.node_id,
        "receiver": "resource_test",
        "action": "compute",
        "data": {
            "computation_units": 100,
            "storage_units": 50
        }
    }

    # Process transaction
    success = await system.process_transaction(transaction)
    assert success

    # Verify mana consumption
    assert system.blockchain.cooperative_mana < initial_mana

    # Test mana regeneration
    system.blockchain.regenerate_mana()
    assert system.blockchain.cooperative_mana > initial_mana - (initial_mana * 0.1)

@pytest.mark.asyncio
async def test_system_metrics(system, test_node):
    """Test comprehensive system metrics collection."""
    await system.add_node(test_node)

    # Generate some activity
    for i in range(3):
        tx = {
            "sender": test_node.node_id,
            "receiver": f"receiver_{i}",
            "action": "transfer",
            "data": {"amount": 50 + i}
        }
        await system.process_transaction(tx)

    # Get system status
    status = system.get_system_status()
    
    # Verify metrics
    assert status["started"]
    assert status["node_count"] == 1
    assert status["shard_count"] == 2
    assert status["uptime"] > 0
    assert "blockchain_metrics" in status
    assert "consensus_metrics" in status
    assert "network_metrics" in status

    # Verify detailed metrics
    blockchain_metrics = status["blockchain_metrics"]
    assert blockchain_metrics["total_transactions"] >= 3
    assert "chain_length" in blockchain_metrics
    assert "cooperative_mana" in blockchain_metrics

@pytest.mark.asyncio
async def test_system_shutdown(system):
    """Test graceful system shutdown."""
    # Add some nodes first
    for i in range(3):
        node = Node(
            node_id=f"shutdown_test_node_{i}",
            cooperative_id="test_coop",
            initial_stake=100.0
        )
        await system.add_node(node)

    # Shutdown system
    await system.shutdown()
    
    # Verify shutdown state
    assert not system.started
    assert not system.network.is_running

    # Verify cleanup
    assert system.blockchain is not None  # Should preserve state
    assert len(system.blockchain.nodes) == 3  # Should preserve nodes

if __name__ == "__main__":
    pytest.main([__file__, "-v"])