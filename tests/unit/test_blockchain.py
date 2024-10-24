import pytest
import asyncio
from datetime import datetime

from blockchain.core.blockchain import Blockchain
from blockchain.core.node import Node
from blockchain.core.block import Block
from blockchain.contracts.smart_contract import SmartContract
from blockchain.core.transaction import Transaction

@pytest.fixture
def blockchain():
    """
    Fixture to create a fresh instance of the Blockchain for each test.
    """
    return Blockchain(num_shards=3, initial_mana=1000, mana_regen_rate=10)

def test_initialization(blockchain):
    """
    Test the initialization of the Blockchain.
    """
    assert isinstance(blockchain, Blockchain)
    assert blockchain.cooperative_mana == 1000
    assert blockchain.mana_regen_rate == 10
    assert len(blockchain.shards) == 3
    assert blockchain.genesis_block_created is True

def test_register_node(blockchain):
    """
    Test node registration functionality.
    """
    node = Node(node_id="node_1")
    assert blockchain.register_node(node) is True
    assert node.node_id in blockchain.nodes

    # Duplicate registration should fail
    assert blockchain.register_node(node) is False

def test_create_shard(blockchain):
    """
    Test shard creation in the Blockchain.
    """
    assert blockchain.create_shard(3) is True
    assert 3 in blockchain.shards

    # Attempt to create an existing shard
    assert blockchain.create_shard(3) is False

def test_add_transaction(blockchain):
    """
    Test adding a transaction to the Blockchain.
    """
    transaction = {
        "sender": "alice",
        "receiver": "bob",
        "action": "transfer",
        "data": {"amount": 50}
    }

    # Add a valid transaction
    assert blockchain.add_transaction(transaction) is True

    # Add an invalid transaction
    invalid_transaction = "invalid_format"
    assert blockchain.add_transaction(invalid_transaction) is False

def test_create_block(blockchain):
    """
    Test block creation in the Blockchain.
    """
    node = Node(node_id="node_1")
    blockchain.register_node(node)

    # Create a block in a valid shard
    shard_id = 0
    block = blockchain.create_block(shard_id)
    assert block is not None
    assert isinstance(block, Block)

    # Attempt to create a block in an invalid shard
    invalid_shard_id = 99
    assert blockchain.create_block(invalid_shard_id) is None

def test_add_block(blockchain):
    """
    Test adding a block to the Blockchain.
    """
    node = Node(node_id="node_1")
    blockchain.register_node(node)

    block = Block(
        index=1,
        previous_hash=blockchain.chain[-1].hash,
        timestamp=datetime.now(),
        transactions=[],
        validator="node_1",
        shard_id=0
    )

    # Add a valid block
    assert blockchain.add_block(block) is True

    # Add an invalid block (invalid previous hash)
    invalid_block = Block(
        index=2,
        previous_hash="invalid_hash",
        timestamp=datetime.now(),
        transactions=[],
        validator="node_1",
        shard_id=0
    )
    assert blockchain.add_block(invalid_block) is False

def test_mana_regeneration(blockchain):
    """
    Test mana regeneration functionality.
    """
    # Deplete some mana
    blockchain.cooperative_mana -= 100
    blockchain.regenerate_mana()

    # Check if mana regenerated correctly
    assert blockchain.cooperative_mana == 910  # 1000 - 100 + 10

def test_get_chain_metrics(blockchain):
    """
    Test retrieving blockchain metrics.
    """
    metrics = blockchain.get_chain_metrics()
    assert isinstance(metrics, dict)
    assert metrics["chain_length"] == 1  # Genesis block
    assert metrics["cooperative_mana"] == 1000
    assert metrics["active_nodes"] == 0
    assert metrics["active_shards"] == 3

def test_validate_chain(blockchain):
    """
    Test the entire blockchain validation.
    """
    node = Node(node_id="node_1")
    blockchain.register_node(node)

    block = Block(
        index=1,
        previous_hash=blockchain.chain[-1].hash,
        timestamp=datetime.now(),
        transactions=[],
        validator="node_1",
        shard_id=0
    )
    blockchain.add_block(block)

    # Validate the blockchain
    assert blockchain.validate_chain() is True

    # Corrupt the chain
    blockchain.chain[-1].previous_hash = "corrupt_hash"
    assert blockchain.validate_chain() is False

def test_smart_contract_deployment(blockchain):
    """
    Test deploying a smart contract to the Blockchain.
    """
    contract = SmartContract(
        contract_id="contract_1",
        creator="node_1",
        code="dummy_code",
        mana_cost=100
    )

    result = asyncio.run(blockchain.deploy_smart_contract(contract))
    assert result is True
    assert "contract_1" in blockchain.smart_contracts

    # Deploy an invalid contract (e.g., insufficient mana)
    contract_2 = SmartContract(
        contract_id="contract_2",
        creator="node_1",
        code="dummy_code",
        mana_cost=2000  # Exceeds available mana
    )

    result = asyncio.run(blockchain.deploy_smart_contract(contract_2))
    assert result is False

def test_smart_contract_execution(blockchain):
    """
    Test executing a smart contract on the Blockchain.
    """
    contract = SmartContract(
        contract_id="contract_1",
        creator="node_1",
        code="dummy_code",
        mana_cost=50
    )

    asyncio.run(blockchain.deploy_smart_contract(contract))

    input_data = {"param": "value"}
    result = asyncio.run(blockchain.execute_smart_contract(
        contract_id="contract_1",
        input_data=input_data,
        caller="node_1"
    ))
    assert result is not None

    # Attempt to execute a non-existent contract
    result = asyncio.run(blockchain.execute_smart_contract(
        contract_id="non_existent",
        input_data=input_data,
        caller="node_1"
    ))
    assert result is None
