import pytest
from datetime import datetime
from blockchain.consensus.proof_of_cooperation.types import ConsensusConfig, ConsensusState, ValidatorHistory
from blockchain.consensus.proof_of_cooperation.reputation_manager import ReputationManager
from blockchain.consensus.proof_of_cooperation.collusion_detector import CollusionDetector
from blockchain.core.node import Node

@pytest.fixture
def consensus_state():
    config = ConsensusConfig(
        min_reputation=10.0,
        cooldown_blocks=3,
        reputation_decay_factor=0.95,
        collusion_threshold=0.75
    )
    return ConsensusState(config=config)

@pytest.fixture
def sample_node():
    node = Node(node_id="node_2")
    node.reputation_scores = {
        "cooperative_growth": 1.0,
        "proposal_participation": 1.0,
        "transaction_validation": 1.0,
    }
    return node

def test_validator_history_tracking(consensus_state, sample_node):
    history_entry = ValidatorHistory(
        node_id=sample_node.node_id,
        timestamp=datetime.now(),
        shard_id=None,
        success=True
    )
    consensus_state.validator_history.append(history_entry)
    assert len(consensus_state.validator_history) == 1, "Validator history should be tracked"

def test_metrics_update(consensus_state):
    consensus_state.metrics.total_validations += 1
    assert consensus_state.metrics.total_validations == 1, "Total validations should increment"

def test_reputation_integration(consensus_state, sample_node):
    collusion_detector = CollusionDetector()
    reputation_manager = ReputationManager(consensus_state.config, collusion_detector)

    score = reputation_manager.calculate_cooperation_score(sample_node)
    assert score > 0, "Integration with reputation manager should calculate a score"
