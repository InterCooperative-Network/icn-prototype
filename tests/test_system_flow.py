# blockchain/tests/integration/test_system_flow.py
"""
Integration tests for complete system flow, from transaction creation to finalization.
"""

import pytest
import asyncio
from blockchain.core.transaction import Transaction
from blockchain.consensus.system.block_production import BlockProductionScheduler

async def test_complete_transaction_flow(test_env):
    """Test complete flow of transaction through system."""
    # Create test transaction
    tx = Transaction(
        sender=test_env.nodes[0].node_id,
        receiver=test_env.nodes[1].node_id,
        action="transfer",
        data={"amount": 50.0}
    )
    
    # Add transaction to blockchain
    assert await test_env.blockchain.add_transaction(tx.to_dict())
    
    # Create block production scheduler
    scheduler = BlockProductionScheduler(
        test_env.consensus,
        test_env.blockchain.state_manager
    )
    
    # Create new block
    block = await scheduler.create_block(test_env.nodes[0].node_id)
    assert block is not None
    
    # Validate and add block
    assert await test_env.blockchain.add_block(block)
    
    # Verify state update
    receiver_balance = await test_env.blockchain.state_manager.get_balance(
        test_env.nodes[1].node_id
    )
    assert receiver_balance == 50.0

async def test_cross_shard_transaction(test_env):
    """Test cross-shard transaction processing."""
    # Create cross-shard transaction
    tx = Transaction(
        sender=test_env.nodes[0].node_id,
        receiver=test_env.nodes[1].node_id,
        action="transfer",
        data={
            "amount": 50.0,
            "target_shard": 1
        }
    )
    
    # Process transaction
    assert await test_env.blockchain.add_transaction(tx.to_dict())
    
    # Verify cross-shard handling
    # ... To be implemented