"""
tests/unit/test_state_verifier.py

Unit tests for state verification functionality.
"""

import pytest
from datetime import datetime
from typing import Dict, Any
from unittest.mock import Mock, patch

from blockchain.core.shard.state_verifier import StateVerifier, StateCheckpoint
from blockchain.core.shard.cross_shard_transaction import CrossShardTransaction
from did.base_did import BaseDID
from system.governance import GovernanceSystem

@pytest.fixture
def mock_governance():
    governance = Mock(spec=GovernanceSystem)
    governance.verify_state_checkpoint.return_value = True
    governance.verify_state_transition.return_value = True
    return governance

@pytest.fixture
def mock_validator_did():
    did = Mock(spec=BaseDID)
    did.get_did.return_value = "did:icn:validator123"
    did.verify.return_value = True
    return did

@pytest.fixture
def sample_state():
    return {
        "account1": {"balance": 1000, "nonce": 1},
        "account2": {"balance": 2000, "nonce": 1}
    }

@pytest.fixture
def state_verifier(mock_governance):
    return StateVerifier(governance_system=mock_governance)

@pytest.fixture
def mock_cross_shard_tx():
    tx = Mock(spec=CrossShardTransaction)
    tx.transaction_id = "tx123"
    tx.source_shard = 0
    tx.target_shards = {1, 2}
    tx.state = "pending"
    return tx

class TestStateVerifier:
    @pytest.mark.asyncio
    async def test_create_checkpoint(self, state_verifier, sample_state, mock_validator_did):
        """Test creation of state checkpoint."""
        checkpoint_hash, success = await state_verifier.create_checkpoint(
            "tx123",
            0,
            sample_state,
            mock_validator_did
        )
        
        assert success
        assert checkpoint_hash
        assert "tx123" in state_verifier.state_checkpoints
        assert 0 in state_verifier.state_checkpoints["tx123"]
        assert state_verifier.state_checkpoints["tx123"][0].state == sample_state

    @pytest.mark.asyncio
    async def test_verify_valid_state_transition(self, state_verifier, sample_state, mock_validator_did):
        """Test verification of valid state transition."""
        # Create initial checkpoint
        await state_verifier.create_checkpoint("tx123", 0, sample_state, mock_validator_did)
        
        # Create new state with valid changes
        new_state = {
            "account1": {"balance": 900, "nonce": 1},
            "account2": {"balance": 2100, "nonce": 1}
        }
        
        # Add multiple verifications
        for i in range(3):
            success = await state_verifier._verify_shard_state(
                "tx123",
                0,
                new_state,
                mock_validator_did
            )
            if i == 2:  # Should succeed on third verification
                assert success
            else:
                assert not success

    @pytest.mark.asyncio
    async def test_verify_invalid_state_transition(self, state_verifier, sample_state, mock_validator_did):
        """Test verification of invalid state transition."""
        # Create initial checkpoint
        await state_verifier.create_checkpoint("tx123", 0, sample_state, mock_validator_did)
        
        # Create new state with invalid changes (sum not preserved)
        invalid_state = {
            "account1": {"balance": 900, "nonce": 1},
            "account2": {"balance": 2200, "nonce": 1}  # Invalid increase
        }
        
        success = await state_verifier._verify_shard_state(
            "tx123",
            0,
            invalid_state,
            mock_validator_did
        )
        assert not success

    @pytest.mark.asyncio
    async def test_rollback_state(self, state_verifier, sample_state, mock_validator_did):
        """Test state rollback functionality."""
        # Create checkpoint
        await state_verifier.create_checkpoint("tx123", 0, sample_state, mock_validator_did)
        
        # Attempt rollback
        rolled_back_state = await state_verifier.rollback_state("tx123", 0, mock_validator_did)
        
        assert rolled_back_state == sample_state
        assert rolled_back_state is not sample_state  # Should be a copy

    @pytest.mark.asyncio
    async def test_verify_cross_shard_transaction(
        self,
        state_verifier,
        mock_cross_shard_tx,
        sample_state,
        mock_validator_did
    ):
        """Test verification of complete cross-shard transaction."""
        states = {
            0: sample_state,
            1: {"account3": {"balance": 1000, "nonce": 1}},
            2: {"account4": {"balance": 1000, "nonce": 1}}
        }
        
        # Create checkpoints for all shards
        for shard_id, state in states.items():
            await state_verifier.create_checkpoint(
                mock_cross_shard_tx.transaction_id,
                shard_id,
                state,
                mock_validator_did
            )
        
        # Verify states
        success = await state_verifier.verify_state(
            mock_cross_shard_tx,
            states,
            mock_validator_did
        )
        assert success

    @pytest.mark.asyncio
    async def test_cleanup_old_checkpoints(self, state_verifier, sample_state, mock_validator_did):
        """Test cleanup of old checkpoints."""
        # Create checkpoint
        await state_verifier.create_checkpoint("tx123", 0, sample_state, mock_validator_did)
        
        # Run cleanup with small max age
        await state_verifier.cleanup_old_checkpoints(max_age_hours=0)
        
        assert "tx123" not in state_verifier.state_checkpoints

    def test_get_checkpoint_metrics(self, state_verifier, sample_state, mock_validator_did):
        """Test retrieval of checkpoint metrics."""
        state_verifier.create_checkpoint("tx123", 0, sample_state, mock_validator_did)
        
        metrics = state_verifier.get_checkpoint_metrics()
        assert metrics["active_checkpoints"] > 0
        assert "tx123" in metrics["transactions"]

    def test_serialization(self, state_verifier, sample_state, mock_validator_did):
        """Test serialization and deserialization of verifier state."""
        # Create some state
        state_verifier.create_checkpoint("tx123", 0, sample_state, mock_validator_did)
        
        # Convert to dict
        state_dict = state_verifier.to_dict()
        
        # Create new instance
        new_verifier = StateVerifier.from_dict(state_dict)
        
        assert len(new_verifier.state_checkpoints) == len(state_verifier.state_checkpoints)
        assert "tx123" in new_verifier.state_checkpoints

    @pytest.mark.asyncio
    async def test_governance_integration(
        self,
        state_verifier,
        mock_governance,
        sample_state,
        mock_validator_did
    ):
        """Test integration with governance system."""
        await state_verifier.create_checkpoint("tx123", 0, sample_state, mock_validator_did)
        
        mock_governance.verify_state_checkpoint.assert_called_once()
        assert mock_governance.verify_state_checkpoint.call_args[0][0] == "tx123"

if __name__ == "__main__":
    pytest.main([__file__])