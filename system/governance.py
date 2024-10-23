# system/governance.py

from typing import Dict, List, Optional, Union
from datetime import datetime, timedelta
import logging
from dataclasses import dataclass, field
import math
import json

logger = logging.getLogger(__name__)

@dataclass
class Proposal:
    """Represents a governance proposal in the ICN system."""
    id: str
    title: str
    description: str
    creator: str
    proposal_type: str
    options: List[str]
    amount: Optional[float] = None
    start_time: datetime = field(default_factory=datetime.now)
    end_time: Optional[datetime] = None
    status: str = "pending"
    metadata: Dict = field(default_factory=dict)
    votes: Dict[str, int] = field(default_factory=dict)
    vote_weights: Dict[str, float] = field(default_factory=dict)
    
    def to_dict(self) -> Dict:
        """Convert proposal to dictionary format."""
        return {
            'id': self.id,
            'title': self.title,
            'description': self.description,
            'creator': self.creator,
            'type': self.proposal_type,
            'options': self.options,
            'amount': self.amount,
            'start_time': self.start_time.isoformat(),
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'status': self.status,
            'metadata': self.metadata,
            'votes': self.votes,
            'vote_weights': self.vote_weights
        }

    @classmethod
    def from_dict(cls, data: Dict) -> 'Proposal':
        """Create proposal from dictionary."""
        proposal = cls(
            id=data['id'],
            title=data['title'],
            description=data['description'],
            creator=data['creator'],
            proposal_type=data['type'],
            options=data['options'],
            amount=data.get('amount'),
            start_time=datetime.fromisoformat(data['start_time']),
            status=data['status'],
            metadata=data.get('metadata', {})
        )
        if data.get('end_time'):
            proposal.end_time = datetime.fromisoformat(data['end_time'])
        proposal.votes = data.get('votes', {})
        proposal.vote_weights = data.get('vote_weights', {})
        return proposal

class VotingSystem:
    """Manages voting mechanics for proposals."""
    
    def __init__(self, quorum_percentage: float = 0.4):
        self.quorum_percentage = quorum_percentage
        self.vote_records: Dict[str, Dict[str, Dict]] = {}

    def cast_vote(self, proposal_id: str, voter_id: str, choice: str,
                  weight: float = 1.0) -> bool:
        """Cast a weighted vote on a proposal."""
        if proposal_id not in self.vote_records:
            self.vote_records[proposal_id] = {}
            
        self.vote_records[proposal_id][voter_id] = {
            'choice': choice,
            'weight': weight,
            'timestamp': datetime.now()
        }
        return True

    def get_results(self, proposal_id: str) -> Dict[str, float]:
        """Calculate weighted voting results."""
        if proposal_id not in self.vote_records:
            return {}
            
        results = {}
        for vote_info in self.vote_records[proposal_id].values():
            choice = vote_info['choice']
            weight = vote_info['weight']
            results[choice] = results.get(choice, 0) + weight
            
        return results

    def has_quorum(self, proposal_id: str, total_voters: int) -> bool:
        """Check if proposal has reached quorum."""
        if proposal_id not in self.vote_records:
            return False
            
        participation = len(self.vote_records[proposal_id]) / total_voters
        return participation >= self.quorum_percentage

class Governance:
    """Main governance system for the ICN."""
    
    def __init__(self, blockchain, reputation_system):
        self.blockchain = blockchain
        self.reputation_system = reputation_system
        self.proposals: Dict[str, Proposal] = {}
        self.voting_system = VotingSystem()
        self.bylaws: Dict[str, str] = {}
        self.funds: float = 0
        self.approval_thresholds = {
            "constitution": 0.75,  # Constitutional changes require 75% approval
            "bylaw": 0.66,        # Bylaw changes require 66% approval
            "budget": 0.60,       # Budget proposals require 60% approval
            "standard": 0.51      # Standard proposals require simple majority
        }

    def create_proposal(self, proposal: Proposal) -> bool:
        """Create a new governance proposal."""
        try:
            # Validate creator's reputation
            creator_rep = self.reputation_system.get_reputation(proposal.creator)
            if creator_rep.get('proposal_creation', 0) < 10:
                logger.warning(f"Insufficient reputation for proposal creation: {proposal.creator}")
                return False

            # Validate proposal type
            if proposal.proposal_type not in self.approval_thresholds:
                logger.error(f"Invalid proposal type: {proposal.proposal_type}")
                return False

            self.proposals[proposal.id] = proposal
            
            # Create blockchain transaction
            transaction = {
                'type': 'proposal_creation',
                'proposal_id': proposal.id,
                'creator': proposal.creator,
                'timestamp': datetime.now().isoformat()
            }
            
            self.blockchain.add_transaction(transaction)
            logger.info(f"Created proposal: {proposal.id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create proposal: {e}")
            return False

    def start_voting(self, proposal_id: str, duration_days: int = 7) -> bool:
        """Start the voting period for a proposal."""
        proposal = self.proposals.get(proposal_id)
        if not proposal or proposal.status != "pending":
            return False

        proposal.status = "active"
        proposal.start_time = datetime.now()
        proposal.end_time = proposal.start_time + timedelta(days=duration_days)

        transaction = {
            'type': 'voting_start',
            'proposal_id': proposal_id,
            'start_time': proposal.start_time.isoformat(),
            'end_time': proposal.end_time.isoformat()
        }

        self.blockchain.add_transaction(transaction)
        logger.info(f"Started voting for proposal: {proposal_id}")
        return True

    def cast_vote(self, proposal_id: str, voter_id: str, choice: str) -> bool:
        """Cast a vote on an active proposal."""
        proposal = self.proposals.get(proposal_id)
        if not proposal or proposal.status != "active":
            return False

        if datetime.now() > proposal.end_time:
            logger.warning(f"Voting period ended for proposal: {proposal_id}")
            return False

        # Calculate voting power based on reputation
        reputation = self.reputation_system.get_reputation(voter_id)
        voting_power = self._calculate_voting_power(reputation)

        # Record vote
        if self.voting_system.cast_vote(proposal_id, voter_id, choice, voting_power):
            transaction = {
                'type': 'vote_cast',
                'proposal_id': proposal_id,
                'voter': voter_id,
                'choice': choice,
                'weight': voting_power
            }
            self.blockchain.add_transaction(transaction)

            # Update voter's reputation
            self.reputation_system.update_reputation(
                voter_id,
                1.0,
                'voting',
                {'proposal_id': proposal_id}
            )

            logger.info(f"Recorded vote from {voter_id} on proposal {proposal_id}")
            return True

        return False

    def _calculate_voting_power(self, reputation: Dict[str, float]) -> float:
        """Calculate voting power based on reputation scores."""
        # Calculate base voting power from reputation
        base_power = sum(reputation.values()) / len(reputation)
        
        # Apply square root to prevent excessive concentration of power
        return math.sqrt(max(1.0, base_power))

    def finalize_proposal(self, proposal_id: str) -> bool:
        """Finalize a proposal after voting period."""
        proposal = self.proposals.get(proposal_id)
        if not proposal or proposal.status != "active":
            return False

        if datetime.now() < proposal.end_time:
            logger.warning(f"Voting period still active for proposal: {proposal_id}")
            return False

        results = self.voting_system.get_results(proposal_id)
        if not results:
            proposal.status = "failed"
            return False

        total_voters = len(self.reputation_system.get_all_users())
        if not self.voting_system.has_quorum(proposal_id, total_voters):
            proposal.status = "failed"
            logger.info(f"Proposal {proposal_id} failed due to lack of quorum")
            return False

        # Calculate approval percentage
        total_votes = sum(results.values())
        approval_percentage = results.get("approve", 0) / total_votes

        # Get required threshold
        threshold = self.approval_thresholds[proposal.proposal_type]

        if approval_percentage >= threshold:
            proposal.status = "approved"
            self._implement_proposal(proposal)
        else:
            proposal.status = "rejected"

        transaction = {
            'type': 'proposal_finalization',
            'proposal_id': proposal_id,
            'status': proposal.status,
            'results': results
        }
        self.blockchain.add_transaction(transaction)

        logger.info(f"Finalized proposal {proposal_id} with status: {proposal.status}")
        return True

    def _implement_proposal(self, proposal: Proposal) -> None:
        """Implement an approved proposal."""
        implementation_handlers = {
            "budget": self._implement_budget_proposal,
            "bylaw": self._implement_bylaw_proposal,
            "constitution": self._implement_constitution_proposal,
        }
        
        handler = implementation_handlers.get(proposal.proposal_type)
        if handler:
            handler(proposal)

    def _implement_budget_proposal(self, proposal: Proposal) -> None:
        """Implement a budget proposal."""
        if proposal.amount and self.funds >= proposal.amount:
            self.funds -= proposal.amount
            transaction = {
                'type': 'budget_execution',
                'proposal_id': proposal.id,
                'amount': proposal.amount
            }
            self.blockchain.add_transaction(transaction)

    def _implement_bylaw_proposal(self, proposal: Proposal) -> None:
        """Implement a bylaw proposal."""
        self.bylaws[proposal.title] = proposal.description
        transaction = {
            'type': 'bylaw_update',
            'proposal_id': proposal.id,
            'title': proposal.title
        }
        self.blockchain.add_transaction(transaction)

    def _implement_constitution_proposal(self, proposal: Proposal) -> None:
        """Implement a constitutional proposal."""
        transaction = {
            'type': 'constitution_update',
            'proposal_id': proposal.id,
            'changes': proposal.metadata.get('changes', {})
        }
        self.blockchain.add_transaction(transaction)

    def get_proposal(self, proposal_id: str) -> Optional[Proposal]:
        """Get a specific proposal."""
        return self.proposals.get(proposal_id)

    def get_active_proposals(self) -> List[Proposal]:
        """Get all currently active proposals."""
        return [p for p in self.proposals.values() if p.status == "active"]

    def get_proposal_metrics(self) -> Dict:
        """Get metrics about proposal activity."""
        return {
            'total_proposals': len(self.proposals),
            'active_proposals': len([p for p in self.proposals.values() if p.status == "active"]),
            'approved_proposals': len([p for p in self.proposals.values() if p.status == "approved"]),
            'rejected_proposals': len([p for p in self.proposals.values() if p.status == "rejected"]),
            'average_participation': self._calculate_average_participation()
        }

    def _calculate_average_participation(self) -> float:
        """Calculate average participation rate in voted proposals."""
        voted_proposals = [p for p in self.proposals.values() 
                         if p.status in ["approved", "rejected"]]
        if not voted_proposals:
            return 0.0

        total_users = len(self.reputation_system.get_all_users())
        participation_rates = []

        for proposal in voted_proposals:
            if proposal.id in self.voting_system.vote_records:
                participation = len(self.voting_system.vote_records[proposal.id]) / total_users
                participation_rates.append(participation)

        return sum(participation_rates) / len(participation_rates) if participation_rates else 0.0