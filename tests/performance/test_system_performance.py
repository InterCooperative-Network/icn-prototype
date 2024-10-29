# blockchain/tests/performance/test_system_performance.py
"""
Performance testing framework for the ICN blockchain system.
Tests system performance under various load conditions.
"""

import pytest
import asyncio
from typing import List
import random
from datetime import datetime

from blockchain.core.transaction import Transaction
from blockchain.system.monitoring import SystemMonitor
from blockchain.tests.conftest import TestEnvironment

async def generate_test_transactions(
    count: int,
    test_env: TestEnvironment
) -> List[Transaction]:
    """Generate test transactions."""
    transactions = []
    nodes = test_env.nodes
    
    for _ in range(count):
        sender = random.choice(nodes)
        receiver = random.choice([n for n in nodes if n != sender])
        
        tx = Transaction(
            sender=sender.node_id,
            receiver=receiver.node_id,
            action="transfer",
            data={
                "amount": random.uniform(1, 100),
                "timestamp": datetime.now()
            }
        )
        transactions.append(tx)
        
    return transactions

async def test_transaction_throughput(test_env: TestEnvironment):
    """Test system transaction processing throughput."""
    # Setup monitoring
    monitor = SystemMonitor()
    await monitor.start_monitoring()
    
    try:
        # Generate test transactions
        tx_count = 1000
        transactions = await generate_test_transactions(tx_count, test_env)
        
        # Record start time
        start_time = datetime.now()
        
        # Process transactions
        tasks = [
            test_env.blockchain.add_transaction(tx.to_dict())
            for tx in transactions
        ]
        results = await asyncio.gather(*tasks)
        
        # Calculate metrics
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        success_count = len([r for r in results if r])
        
        # Get monitoring metrics
        metrics = monitor.get_system_status()
        
        assert success_count > 0
        assert duration > 0
        
        throughput = success_count / duration
        print(f"Transaction throughput: {throughput:.2f} tx/s")
        print(f"Success rate: {(success_count/tx_count)*100:.2f}%")
        
    finally:
        await monitor.stop_monitoring()

async def test_network_capacity(test_env: TestEnvironment):
    """Test network capacity and message handling."""
    monitor = SystemMonitor()
    await monitor.start_monitoring()
    
    try:
        # Generate large number of messages
        message_count = 5000
        message_size = 1024  # 1KB per message
        
        # Send messages
        start_time = datetime.now()
        
        tasks = []
        for _ in range(message_count):
            tasks.append(
                test_env.blockchain.network.broadcast_message(
                    "test_message",
                    {"data": "x" * message_size}
                )
            )
            
        await asyncio.gather(*tasks)
        
        # Calculate metrics
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        # Get monitoring metrics
        metrics = monitor.get_system_status()
        
        bandwidth = (message_count * message_size) / duration / 1024  # KB/s
        print(f"Network bandwidth: {bandwidth:.2f} KB/s")
        
    finally:
        await monitor.stop_monitoring()