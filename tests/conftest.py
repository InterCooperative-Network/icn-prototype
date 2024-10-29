# blockchain/tests/conftest.py
"""
Test configuration and fixtures for the ICN blockchain testing.
Provides common test setup and utilities.
"""

import pytest
import asyncio
from typing import List, Dict
from dataclasses import dataclass
from blockchain.core.blockchain import Blockchain
from blockchain.core.node import Node
from blockchain.consensus.proof_of_cooperation import ProofOfCooperation
from blockchain.network.config import NetworkConfig

@dataclass
class TestEnvironment:
    """Encapsulates test environment components."""
    blockchain: Blockchain
    nodes: List[Node]
    consensus: ProofOfCooperation
    network_configs: Dict[str, NetworkConfig]

@pytest.fixture
async def test_env():
    """Create complete test environment."""
    # Create blockchain instance
    blockchain = Blockchain(num_shards=3, initial_mana=1000)
    
    # Create test nodes
    nodes = [Node(f"node_{i}", initial_stake=100.0) for i in range(5)]
    
    # Create network configs
    network_configs = {}
    for node in nodes:
        config = NetworkConfig(
            node_id=node.node_id,
            host="127.0.0.1",
            port=30303 + len(network_configs)
        )
        network_configs[node.node_id] = config
        
    # Create consensus mechanism
    consensus = ProofOfCooperation(min_reputation=10.0)
    
    # Register nodes with blockchain
    for node in nodes:
        await blockchain.register_node(node)
        
    return TestEnvironment(
        blockchain=blockchain,
        nodes=nodes,
        consensus=consensus,
        network_configs=network_configs
    )

@pytest.fixture
def event_loop():
    """Create event loop for async tests."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()