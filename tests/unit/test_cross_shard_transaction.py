"""
tests/unit/test_cross_shard_transaction.py

Unit tests for cross-shard transaction functionality.
"""

import pytest
from datetime import datetime
from typing import Dict, Set
from unittest.mock import Mock, patch

from blockchain.core.shard.cross_shard_transaction import CrossShardTransaction, CrossShardTransactionPhase
from blockchain.core.transaction import Transaction
from did.base_did import BaseDID
from system.governance import GovernanceSystem

@pytest.fixture
def mock_governance():
    governance = Mock(spec=GovernanceSystem)
    governance.validate_cross_shard_action.return_value = True
    governance.record_cross_shard_completion.return_value = None
    return governance

@pytest.fixture
def mock_transaction():
    return Transaction(
        sender="sender123",
        receiver="receiver456",
        action="transfer",
        data={"amount": 100}
    )

@pytest.fixture
def mock_validator_did():
    did = Mock(spec=BaseDID)
    did.get_did.return_value = "did:icn:validator123"
    did.verify.return_value = True
    return did

@pytest.fixture
def cross_shard_tx(mock_transaction, mock_governance):
    return CrossShardTransaction(
        transaction_id="tx123",
        source_shard=0,
        target_shards={1, 2},
        primary_transaction=mock_transaction,
        governance_system=mock_governance
    )

class TestCrossShardTransaction:
    def test_initialization(self, cross_shard_tx):
        """Test proper initialization of cross-shard transaction."""
        assert cross_shard_tx.transaction_id == "tx123"
        assert cross_shard_tx.source_shard == 0
        assert cross_shard_tx.target_shards == {1, 2}
        assert cross_shard_tx.state == "pending"
        assert len(cross_shard_tx.phases) == 3  # source + target shards

    def test_prepare_phase_success(self, cross_shard_tx, mock_validator_did):
        """Test successful preparation of a transaction phase."""
        result = cross_shard_tx.prepare_phase(0, "validator1", mock_validator_did)
        assert not result  # First validation alone shouldn't complete preparation
        
        # Add more validations
        for i in range(2):
            cross_shard_tx.prepare_phase(0, f"validator{i+2}", mock_validator_did)
        
        # Check phase status after required validations
        phase = cross_shard_tx.phases[0]
        assert phase.status == "prepared"
        assert len(phase.validation_signatures) >= cross_shard_tx.required_validations
        assert mock_validator_did.get_did() in phase.did_attestations

    def test_prepare_phase_invalid_shard(self, cross_shard_tx, mock_validator_did):
        """Test preparation with invalid shard ID."""
        result = cross_shard_tx.prepare_phase(99, "validator1", mock_validator_did)
        assert not result

    def test_commit_phase_success(self, cross_shard_tx, mock_validator_did):
        """Test successful commit of a prepared phase."""
        # First prepare the phase
        for i in range(3):
            cross_shard_tx.prepare_phase(0, f"validator{i+1}", mock_validator_did)
            
        # Then commit
        result = cross_shard_tx.commit_phase(0, "validator1", mock_validator_did)
        assert result
        assert cross_shard_tx.phases[0].status == "committed"

    def test_commit_phase_without_preparation(self, cross_shard_tx, mock_validator_did):
        """Test commit attempt without preparation."""
        result = cross_shard_tx.commit_phase(0, "validator1", mock_validator_did)
        assert not result
        assert cross_shard_tx.phases[0].status == "pending"

    def test_abort_phase(self, cross_shard_tx, mock_validator_did):
        """Test abortion of a transaction phase."""
        cross_shard_tx.abort_phase(0, "test abort", mock_validator_did)
        assert cross_shard_tx.phases[0].status == "aborted"
        assert cross_shard_tx.state == "aborted"

    def test_governance_integration(self, cross_shard_tx, mock_validator_did, mock_governance):
        """Test integration with governance system."""
        cross_shard_tx.prepare_phase(0, "validator1", mock_validator_did)
        mock_governance.validate_cross_shard_action.assert_called_once()

    def test_transaction_completion(self, cross_shard_tx, mock_validator_did):
        """Test successful completion of all phases."""
        # Prepare and commit all phases
        for shard_id in [0, 1, 2]:
            for i in range(3):
                cross_shard_tx.prepare_phase(shard_id, f"validator{i+1}", mock_validator_did)
            cross_shard_tx.commit_phase(shard_id, "validator1", mock_validator_did)
            
        assert cross_shard_tx.state == "completed"
        assert cross_shard_tx.completed_at is not None

    def test_serialization(self, cross_shard_tx, mock_validator_did):
        """Test serialization and deserialization."""
        # Add some state to the transaction
        cross_shard_tx.prepare_phase(0, "validator1", mock_validator_did)
        
        # Convert to dict
        tx_dict = cross_shard_tx.to_dict()
        
        # Create new instance from dict
        new_tx = CrossShardTransaction.from_dict(tx_dict)
        
        assert new_tx.transaction_id == cross_shard_tx.transaction_id
        assert new_tx.source_shard == cross_shard_tx.source_shard
        assert new_tx.target_shards == cross_shard_tx.target_shards
        assert new_tx.state == cross_shard_tx.state

    def test_invalid_validator_did(self, cross_shard_tx, mock_validator_did):
        """Test behavior with invalid validator DID."""
        mock_validator_did.verify.return_value = False
        result = cross_shard_tx.commit_phase(0, "validator1", mock_validator_did)
        assert not result

    @pytest.mark.asyncio
    async def test_concurrent_validations(self, cross_shard_tx, mock_validator_did):
        """Test concurrent validation attempts."""
        import asyncio
        
        async def validate(validator_id: str):
            return cross_shard_tx.prepare_phase(0, validator_id, mock_validator_did)
            
        # Attempt concurrent validations
        tasks = [validate(f"validator{i}") for i in range(5)]
        results = await asyncio.gather(*tasks)
        
        # Check that exactly required_validations were accepted
        assert sum(results) == 1  # Only one should return True for completion
        assert len(cross_shard_tx.phases[0].validation_signatures) >= cross_shard_tx.required_validations

    def test_phase_timeout(self, cross_shard_tx, mock_validator_did):
        """Test phase timeout handling."""
        # Simulate a prepared phase
        for i in range(3):
            cross_shard_tx.prepare_phase(0, f"validator{i+1}", mock_validator_did)
        
        # Set phase timestamp to past timeout
        phase = cross_shard_tx.phases[0]
        phase.timestamp = datetime.now() - timedelta(hours=2)
        
        # Attempt to commit
        result = cross_shard_tx.commit_phase(0, "validator1", mock_validator_did)
        assert not result
        
    def test_cross_shard_ref_tracking(self, cross_shard_tx):
        """Test tracking of cross-shard references."""
        # Add some cross-shard references
        tx_refs = ["ref1", "ref2", "ref3"]
        cross_shard_tx.primary_transaction.cross_shard_refs = tx_refs

        # Verify references are tracked
        for phase in cross_shard_tx.phases.values():
            if phase.shard_id in cross_shard_tx.target_shards:
                assert any(ref in str(phase.data) for ref in tx_refs)

    def test_did_attestation_validation(self, cross_shard_tx, mock_validator_did):
        """Test DID attestation validation."""
        # Add attestation
        cross_shard_tx.prepare_phase(0, "validator1", mock_validator_did)
        phase = cross_shard_tx.phases[0]
        
        # Verify attestation
        assert mock_validator_did.get_did() in phase.did_attestations
        assert len(phase.did_attestations) == 1

    def test_validator_cooldown(self, cross_shard_tx, mock_validator_did):
        """Test validator cooldown after validation."""
        validator_id = "validator1"
        
        # First validation
        result1 = cross_shard_tx.prepare_phase(0, validator_id, mock_validator_did)
        
        # Immediate retry should fail
        result2 = cross_shard_tx.prepare_phase(0, validator_id, mock_validator_did)
        
        assert result1 != result2  # One should succeed, one should fail