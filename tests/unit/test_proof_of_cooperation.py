import pytest
from datetime import datetime, timedelta
from blockchain.consensus.proof_of_cooperation.reputation_manager import ReputationManager
from blockchain.consensus.proof_of_cooperation.types import ConsensusConfig, ValidationResult, ValidationStats
from blockchain.core.node import Node
from blockchain.consensus.proof_of_cooperation.collusion_detector import CollusionDetector

@pytest.fixture
def sample_config():
    return ConsensusConfig(
        min_reputation=10.0,
        cooldown_blocks=3,
        reputation_decay_factor=0.95,
        collusion_threshold=0.75
    )

@pytest.fixture
def collusion_detector():
    return CollusionDetector()

@pytest.fixture
def reputation_manager(sample_config, collusion_detector):
    return ReputationManager(sample_config, collusion_detector)

@pytest.fixture
def sample_node():
    node = Node(node_id="node_1")
    node.reputation_scores = {
        "cooperative_growth": 1.0,
        "proposal_participation": 1.0,
        "transaction_validation": 1.0,
    }
    node.validation_history = [{"evidence": {"success": True}}] * 5
    node.performance_metrics = {
        "availability": 90,
        "validation_success_rate": 80,
        "network_reliability": 85
    }
    return node

def test_calculate_cooperation_score(reputation_manager, sample_node):
    score = reputation_manager.calculate_cooperation_score(sample_node)
    assert score >= 0, "Cooperation score should be non-negative"

def test_validation_eligibility(reputation_manager, sample_node):
    eligible = reputation_manager.can_validate(sample_node)
    assert isinstance(eligible, bool), "Validation eligibility should return a boolean"

def test_update_stats(reputation_manager, sample_node):
    result = ValidationResult(success=True)
    reputation_manager.update_stats(sample_node.node_id, result)
    stats = reputation_manager.get_node_stats(sample_node.node_id)
    assert stats.successful_validations == 1, "Successful validations should increment"

def test_collusion_factor(reputation_manager, sample_node):
    factor = reputation_manager._calculate_collusion_factor(sample_node)
    assert 0 <= factor <= 1, "Collusion factor should be between 0 and 1"
