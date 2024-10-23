# blockchain/blockchain.py

import hashlib
import time
import math
import random
import json
import signal
import logging
from typing import Dict, List, Optional, Tuple, Set, Any, Union
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from cryptography.exceptions import InvalidKey
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)

@dataclass
class Transaction:
    """Represents a transaction in the ICN blockchain."""
    sender: str
    receiver: str
    action: str
    data: Dict
    timestamp: datetime = field(default_factory=datetime.now)
    signature: Optional[bytes] = None
    shard_id: Optional[int] = None
    transaction_id: str = field(init=False)
    
    def __post_init__(self):
        """Initialize transaction ID after creation."""
        self.transaction_id = self.calculate_id()
    
    def calculate_id(self) -> str:
        """Calculate unique transaction ID."""
        tx_data = {
            'sender': self.sender,
            'receiver': self.receiver,
            'action': self.action,
            'data': self.data,
            'timestamp': self.timestamp.isoformat(),
            'shard_id': self.shard_id
        }
        return hashlib.sha256(json.dumps(tx_data, sort_keys=True).encode()).hexdigest()
    
    def to_dict(self) -> Dict:
        """Convert transaction to dictionary format."""
        return {
            'transaction_id': self.transaction_id,
            'sender': self.sender,
            'receiver': self.receiver,
            'action': self.action,
            'data': self.data,
            'timestamp': self.timestamp.isoformat(),
            'signature': self.signature.hex() if self.signature else None,
            'shard_id': self.shard_id
        }
        
    @classmethod
    def from_dict(cls, data: Dict) -> 'Transaction':
        """Create transaction from dictionary."""
        timestamp = datetime.fromisoformat(data['timestamp'])
        signature = bytes.fromhex(data['signature']) if data.get('signature') else None
        
        return cls(
            sender=data['sender'],
            receiver=data['receiver'],
            action=data['action'],
            data=data['data'],
            timestamp=timestamp,
            signature=signature,
            shard_id=data.get('shard_id')
        )

    def validate(self) -> bool:
        """Validate transaction structure and data."""
        try:
            # Validate basic structure
            if not all([self.sender, self.receiver, self.action]):
                return False
            
            # Validate timestamp
            if self.timestamp > datetime.now() + timedelta(minutes=5):
                return False
                
            # Validate data structure
            if not isinstance(self.data, dict):
                return False
                
            # Validate transaction ID
            if self.transaction_id != self.calculate_id():
                return False
                
            return True
            
        except Exception as e:
            logger.error(f"Transaction validation failed: {e}")
            return False

@dataclass
class Block:
    """Represents a block in the ICN blockchain."""
    index: int
    previous_hash: str
    timestamp: datetime
    transactions: List[Transaction]
    validator: str
    shard_id: int
    hash: str = ""
    nonce: int = 0
    merkle_root: str = ""
    cross_shard_refs: List[str] = field(default_factory=list)
    metadata: Dict = field(default_factory=dict)
    version: str = "1.0"
    
    def __post_init__(self):
        """Initialize block after creation."""
        if not self.merkle_root:
            self.merkle_root = self.calculate_merkle_root()
        if not self.hash:
            self.hash = self.calculate_hash()
        self.metadata['created_at'] = datetime.now().isoformat()

    def calculate_merkle_root(self) -> str:
        """Calculate the Merkle root of transactions."""
        if not self.transactions:
            return hashlib.sha256(b"empty").hexdigest()
        
        leaves = [hashlib.sha256(json.dumps(tx.to_dict()).encode()).hexdigest()
                 for tx in self.transactions]
        
        while len(leaves) > 1:
            if len(leaves) % 2 == 1:
                leaves.append(leaves[-1])
            leaves = [hashlib.sha256((a + b).encode()).hexdigest()
                     for a, b in zip(leaves[::2], leaves[1::2])]
        
        return leaves[0]

    def calculate_hash(self) -> str:
        """Calculate the hash of the block."""
        block_dict = {
            'index': self.index,
            'previous_hash': self.previous_hash,
            'timestamp': self.timestamp.isoformat(),
            'merkle_root': self.merkle_root,
            'validator': self.validator,
            'nonce': self.nonce,
            'shard_id': self.shard_id,
            'cross_shard_refs': self.cross_shard_refs,
            'version': self.version
        }
        return hashlib.sha256(json.dumps(block_dict, sort_keys=True).encode()).hexdigest()

    def to_dict(self) -> Dict:
        """Convert block to dictionary format."""
        return {
            'index': self.index,
            'previous_hash': self.previous_hash,
            'timestamp': self.timestamp.isoformat(),
            'transactions': [tx.to_dict() for tx in self.transactions],
            'validator': self.validator,
            'hash': self.hash,
            'nonce': self.nonce,
            'merkle_root': self.merkle_root,
            'shard_id': self.shard_id,
            'cross_shard_refs': self.cross_shard_refs,
            'metadata': self.metadata,
            'version': self.version
        }

    @classmethod
    def from_dict(cls, data: Dict) -> 'Block':
        """Create block from dictionary."""
        transactions = [Transaction.from_dict(tx) for tx in data['transactions']]
        timestamp = datetime.fromisoformat(data['timestamp'])
        
        return cls(
            index=data['index'],
            previous_hash=data['previous_hash'],
            timestamp=timestamp,
            transactions=transactions,
            validator=data['validator'],
            shard_id=data['shard_id'],
            hash=data['hash'],
            nonce=data['nonce'],
            merkle_root=data['merkle_root'],
            cross_shard_refs=data.get('cross_shard_refs', []),
            metadata=data.get('metadata', {}),
            version=data.get('version', "1.0")
        )

    def validate(self, previous_block: Optional['Block'] = None) -> bool:
        """Validate block structure and consistency."""
        try:
            # Validate hash
            if self.hash != self.calculate_hash():
                return False
                
            # Validate merkle root
            if self.merkle_root != self.calculate_merkle_root():
                return False
                
            # Validate timestamp
            if self.timestamp > datetime.now() + timedelta(minutes=5):
                return False
                
            # Validate transactions
            if not all(tx.validate() for tx in self.transactions):
                return False
                
            # Validate against previous block if provided
            if previous_block:
                if self.previous_hash != previous_block.hash:
                    return False
                if self.index != previous_block.index + 1:
                    return False
                if self.timestamp <= previous_block.timestamp:
                    return False
                    
            return True
            
        except Exception as e:
            logger.error(f"Block validation failed: {e}")
            return False
            class Shard:
    """Represents a blockchain shard."""
    
    def __init__(self, shard_id: int, max_transactions_per_block: int = 100):
        self.shard_id = shard_id
        self.chain: List[Block] = []
        self.pending_transactions: List[Transaction] = []
        self.height = 0
        self.max_transactions_per_block = max_transactions_per_block
        self.last_block_time = datetime.now()
        self.state: Dict = {}
        self.metrics: Dict = {
            'total_transactions': 0,
            'average_block_time': 0,
            'blocks_created': 0,
            'pending_count': 0
        }
        self._create_genesis_block()

    def _create_genesis_block(self) -> None:
        """Create genesis block for this shard."""
        genesis_block = Block(
            index=0,
            previous_hash="0" * 64,
            timestamp=datetime.now(),
            transactions=[],
            validator="genesis",
            shard_id=self.shard_id
        )
        self.chain.append(genesis_block)
        self.height = 1
        self.last_block_time = genesis_block.timestamp
        self.metrics['blocks_created'] = 1

    def add_transaction(self, transaction: Transaction) -> bool:
        """Add a new transaction to pending pool."""
        if transaction.shard_id != self.shard_id:
            return False
            
        if len(self.pending_transactions) >= self.max_transactions_per_block * 2:
            return False
            
        if not transaction.validate():
            return False
            
        self.pending_transactions.append(transaction)
        self.metrics['pending_count'] = len(self.pending_transactions)
        return True

    def create_block(self, validator: str) -> Optional[Block]:
        """Create a new block from pending transactions."""
        if not self.pending_transactions:
            return None
            
        transactions = self.pending_transactions[:self.max_transactions_per_block]
        
        new_block = Block(
            index=self.height,
            previous_hash=self.chain[-1].hash,
            timestamp=datetime.now(),
            transactions=transactions,
            validator=validator,
            shard_id=self.shard_id
        )
        
        return new_block

    def add_block(self, block: Block) -> bool:
        """Add a validated block to the shard chain."""
        if block.shard_id != self.shard_id:
            return False
            
        if block.index != self.height:
            return False
            
        if not block.validate(self.chain[-1]):
            return False
            
        # Update metrics
        block_time = (block.timestamp - self.last_block_time).total_seconds()
        self.metrics['average_block_time'] = (
            (self.metrics['average_block_time'] * self.metrics['blocks_created'] + block_time) /
            (self.metrics['blocks_created'] + 1)
        )
        self.metrics['blocks_created'] += 1
        self.metrics['total_transactions'] += len(block.transactions)
        
        # Remove included transactions from pending pool
        tx_ids = {tx.transaction_id for tx in block.transactions}
        self.pending_transactions = [
            tx for tx in self.pending_transactions 
            if tx.transaction_id not in tx_ids
        ]
        self.metrics['pending_count'] = len(self.pending_transactions)
        
        # Add block to chain
        self.chain.append(block)
        self.height += 1
        self.last_block_time = block.timestamp
        
        return True

    def get_latest_block(self) -> Block:
        """Get the latest block in this shard."""
        return self.chain[-1]

    def validate_chain(self) -> bool:
        """Validate the entire shard chain."""
        for i in range(1, len(self.chain)):
            if not self.chain[i].validate(self.chain[i-1]):
                return False
        return True

    def get_metrics(self) -> Dict:
        """Get shard metrics and statistics."""
        return {
            'shard_id': self.shard_id,
            'height': self.height,
            'pending_transactions': len(self.pending_transactions),
            'last_block_time': self.last_block_time.isoformat(),
            **self.metrics
        }


@dataclass
class Node:
    """Represents a node in the ICN network."""
    
    def __init__(self, node_id: str, cooperative_id: Optional[str] = None,
                 initial_stake: float = 10.0):
        self.node_id = node_id
        self.cooperative_id = cooperative_id
        self.reputation_scores = {
            'validation': 0.0,
            'proposal_creation': 0.0,
            'voting': 0.0,
            'resource_sharing': 0.0,
            'cooperative_growth': 0.0,
            'community_building': 0.0,
            'conflict_resolution': 0.0,
            'transaction_validation': 0.0,
            'data_availability': 0.0,
            'network_stability': 0.0,
            'innovation': 0.0,
            'sustainability': 0.0
        }
        self.stake = initial_stake
        self.cooperative_interactions: List[str] = []
        self.validation_history: List[Dict] = []
        self.resource_usage: Dict[str, float] = {
            'computation': 0.0,
            'storage': 0.0,
            'bandwidth': 0.0,
            'memory': 0.0,
            'energy': 0.0
        }
        self.shard_assignments: Set[int] = set()
        self.active_shards: Dict[int, datetime] = {}
        self.last_validation = 0
        self.total_validations = 0
        self.cooldown = 0
        self.total_cycles = 0
        self.cycles_since_update: Dict[str, int] = {}
        self.performance_metrics: Dict[str, float] = {
            'response_time': 0.0,
            'availability': 100.0,
            'validation_success_rate': 100.0,
            'network_reliability': 100.0
        }
        self.metadata: Dict = {
            'creation_time': datetime.now(),
            'last_active': datetime.now(),
            'version': "1.0",
            'capabilities': set(),
            'status': "active"
        }

    def update_reputation(self, category: str, score: float, 
                         cooperative_id: Optional[str] = None,
                         evidence: Optional[Dict] = None) -> None:
        """Update reputation score for a category with evidence."""
        if category in self.reputation_scores:
            old_score = self.reputation_scores[category]
            self.reputation_scores[category] = max(0, old_score + score)
            
            if cooperative_id:
                self.cooperative_interactions.append(cooperative_id)
                
            if evidence:
                self.validation_history.append({
                    'timestamp': datetime.now(),
                    'category': category,
                    'score_change': score,
                    'evidence': evidence
                })
            
            self.metadata['last_active'] = datetime.now()
            
            # Trim interaction history to last 1000 interactions
            if len(self.cooperative_interactions) > 1000:
                self.cooperative_interactions = self.cooperative_interactions[-1000:]

    def assign_to_shard(self, shard_id: int) -> bool:
        """Assign node to a shard."""
        if len(self.active_shards) >= 3:  # Maximum 3 active shards per node
            return False
            
        self.shard_assignments.add(shard_id)
        self.active_shards[shard_id] = datetime.now()
        return True

    def remove_from_shard(self, shard_id: int) -> bool:
        """Remove node from a shard."""
        if shard_id in self.active_shards:
            del self.active_shards[shard_id]
            self.shard_assignments.discard(shard_id)
            return True
        return False

    def can_validate(self, shard_id: Optional[int] = None) -> bool:
        """Check if node can validate blocks."""
        current_time = time.time()
        
        # Basic validation checks
        if self.cooldown > 0:
            return False
        if current_time - self.last_validation < 10:  # 10 second minimum between validations
            return False
        if self.metadata['status'] != "active":
            return False
            
        # Shard-specific validation
        if shard_id is not None:
            if shard_id not in self.active_shards:
                return False
            if (datetime.now() - self.active_shards[shard_id]).total_seconds() > 3600:  # 1 hour timeout
                return False
                
        return True

    def enter_cooldown(self, cooldown_period: int) -> None:
        """Put node into cooldown period."""
        self.cooldown = cooldown_period
        self.metadata['status'] = "cooldown"

    def update_metrics(self, metrics: Dict[str, float]) -> None:
        """Update node performance metrics."""
        self.performance_metrics.update(metrics)
        self.metadata['last_active'] = datetime.now()

    def get_total_reputation(self) -> float:
        """Calculate total reputation across all categories."""
        return sum(self.reputation_scores.values())

    def to_dict(self) -> Dict:
        """Convert node state to dictionary."""
        return {
            'node_id': self.node_id,
            'cooperative_id': self.cooperative_id,
            'reputation_scores': self.reputation_scores,
            'stake': self.stake,
            'shard_assignments': list(self.shard_assignments),
            'performance_metrics': self.performance_metrics,
            'resource_usage': self.resource_usage,
            'metadata': self.metadata,
            'status': self.metadata['status']
        }

    @classmethod
    def from_dict(cls, data: Dict) -> 'Node':
        """Create node from dictionary."""
        node = cls(
            node_id=data['node_id'],
            cooperative_id=data['cooperative_id'],
            initial_stake=data['stake']
        )
        node.reputation_scores = data['reputation_scores']
        node.shard_assignments = set(data['shard_assignments'])
        node.performance_metrics = data['performance_metrics']
        node.resource_usage = data['resource_usage']
        node.metadata = data['metadata']
        
        return node
        class ProofOfCooperation:
    """Implements the Proof of Cooperation consensus mechanism."""
    
    def __init__(self, min_reputation: float = 10.0, cooldown_blocks: int = 3):
        self.min_reputation = min_reputation
        self.cooldown_blocks = cooldown_blocks
        self.cooperation_scores: Dict[str, float] = {}
        self.reputation_weights = {
            'cooperative_growth': 1.5,
            'proposal_participation': 1.2,
            'transaction_validation': 1.0,
            'resource_sharing': 1.3,
            'conflict_resolution': 1.1,
            'community_building': 1.4,
            'sustainability': 1.2,
            'innovation': 1.3,
            'network_stability': 1.1,
            'data_availability': 1.2
        }
        self.validation_thresholds = {
            'min_participation': 0.1,
            'min_success_rate': 0.8,
            'min_availability': 0.95,
            'max_consecutive_validations': 3
        }
        self.reputation_decay_factor = 0.99
        self.collusion_threshold = 0.8
        self.validator_history: List[Tuple[str, datetime, int]] = []  # node_id, timestamp, shard_id
        self.validation_stats: Dict[str, Dict] = {}
        self.performance_metrics: Dict[str, float] = {
            'average_block_time': 0.0,
            'total_validations': 0,
            'successful_validations': 0,
            'collusion_detections': 0
        }

    def calculate_cooperation_score(self, node: Node, shard_id: Optional[int] = None) -> float:
        """Calculate a node's cooperation score based on multiple factors."""
        if not node.can_validate(shard_id):
            return 0.0

        base_score = sum(
            score * self.reputation_weights.get(category, 1.0)
            for category, score in node.reputation_scores.items()
        )
        
        diversity_factor = self._calculate_diversity_factor(node)
        consistency_factor = self._calculate_consistency_factor(node)
        performance_factor = self._calculate_performance_factor(node)
        shard_factor = self._calculate_shard_factor(node, shard_id) if shard_id else 1.0
        
        final_score = (base_score * diversity_factor * consistency_factor * 
                      performance_factor * shard_factor)
                      
        return max(0, final_score)

    def _calculate_diversity_factor(self, node: Node) -> float:
        """Calculate diversity factor based on cooperative interactions."""
        recent_interactions = node.cooperative_interactions[-100:]  # Last 100 interactions
        if not recent_interactions:
            return 1.0
            
        unique_coops = len(set(recent_interactions))
        total_interactions = len(recent_interactions)
        
        diversity_score = unique_coops / total_interactions
        normalized_score = 1.0 + math.log(diversity_score + 1)
        
        return max(self.validation_thresholds['min_participation'], normalized_score)

    def _calculate_consistency_factor(self, node: Node) -> float:
        """Calculate consistency factor based on validation history."""
        if not node.validation_history:
            return 1.0
            
        recent_validations = node.validation_history[-50:]  # Last 50 validations
        successful = sum(1 for v in recent_validations 
                        if v.get('evidence', {}).get('success', False))
        
        success_rate = successful / len(recent_validations)
        return max(self.validation_thresholds['min_success_rate'], success_rate)

    def _calculate_performance_factor(self, node: Node) -> float:
        """Calculate performance factor based on node metrics."""
        metrics = node.performance_metrics
        if not metrics:
            return 1.0

        factors = [
            metrics.get('availability', 0) / 100,
            metrics.get('validation_success_rate', 0) / 100,
            metrics.get('network_reliability', 0) / 100
        ]
        
        avg_performance = sum(factors) / len(factors)
        return max(self.validation_thresholds['min_availability'], avg_performance)

    def _calculate_shard_factor(self, node: Node, shard_id: int) -> float:
        """Calculate shard-specific performance factor."""
        if shard_id not in node.active_shards:
            return 0.0
            
        # Consider time spent in shard
        time_in_shard = (datetime.now() - node.active_shards[shard_id]).total_seconds()
        shard_experience = min(1.0, time_in_shard / (24 * 3600))  # Max out at 1 day
        
        return 0.5 + (0.5 * shard_experience)

    def select_validator(self, nodes: List[Node], shard_id: Optional[int] = None) -> Optional[Node]:
        """Select the next validator using weighted random selection."""
        eligible_nodes = [
            node for node in nodes 
            if self._is_eligible_validator(node, shard_id)
        ]
        
        if not eligible_nodes:
            return None
            
        # Calculate scores for eligible nodes
        scores = [
            self.calculate_cooperation_score(node, shard_id) 
            for node in eligible_nodes
        ]
        total_score = sum(scores)
        
        if total_score <= 0:
            # Fallback to random selection if all scores are 0
            selected = random.choice(eligible_nodes)
        else:
            # Weighted random selection
            selection_point = random.uniform(0, total_score)
            current_sum = 0
            selected = eligible_nodes[-1]  # Default to last node
            
            for node, score in zip(eligible_nodes, scores):
                current_sum += score
                if current_sum >= selection_point:
                    selected = node
                    break
        
        # Record selection
        self._record_validator_selection(selected, shard_id)
        selected.enter_cooldown(self.cooldown_blocks)
        
        return selected

    def _is_eligible_validator(self, node: Node, shard_id: Optional[int] = None) -> bool:
        """Check if a node is eligible to validate blocks."""
        if not node.can_validate(shard_id):
            return False
            
        # Check minimum reputation requirement
        if node.get_total_reputation() < self.min_reputation:
            return False
            
        # Check performance factors
        performance_factor = self._calculate_performance_factor(node)
        if performance_factor < self.validation_thresholds['min_availability']:
            return False
            
        # Check recent selections to prevent concentration
        recent_validations = [
            v[0] for v in self.validator_history[-10:]
            if v[0] == node.node_id
        ]
        if len(recent_validations) >= self.validation_thresholds['max_consecutive_validations']:
            return False
            
        return True

    def _record_validator_selection(self, node: Node, shard_id: Optional[int]) -> None:
        """Record validator selection for statistics."""
        self.validator_history.append((node.node_id, datetime.now(), shard_id))
        if len(self.validator_history) > 1000:
            self.validator_history = self.validator_history[-1000:]
            
        if node.node_id not in self.validation_stats:
            self.validation_stats[node.node_id] = {
                'selections': 0,
                'successful_validations': 0,
                'last_selected': None,
                'shard_validations': {}
            }
            
        stats = self.validation_stats[node.node_id]
        stats['selections'] += 1
        stats['last_selected'] = datetime.now()
        
        if shard_id is not None:
            shard_stats = stats['shard_validations'].setdefault(shard_id, {
                'selections': 0,
                'successful': 0
            })
            shard_stats['selections'] += 1

    def validate_block(self, block: Block, previous_block: Optional[Block], 
                      validator: Node) -> bool:
        """Validate a proposed block."""
        try:
            # Verify validator eligibility
            if not self._is_eligible_validator(validator, block.shard_id):
                return False
                
            # Perform block validation
            if not block.validate(previous_block):
                return False
                
            # Verify cross-shard references if present
            if block.cross_shard_refs and not self._validate_cross_shard_refs(block):
                return False
                
            # Update statistics
            self._update_validation_stats(validator, block, True)
            
            return True
            
        except Exception as e:
            logger.error(f"Block validation failed: {e}")
            self._update_validation_stats(validator, block, False)
            return False

    def _validate_cross_shard_refs(self, block: Block) -> bool:
        """Validate cross-shard references in a block."""
        # This would include validation logic for cross-shard references
        # Implementation depends on specific cross-shard protocol
        return True

    def _update_validation_stats(self, validator: Node, block: Block, 
                               success: bool) -> None:
        """Update validation statistics."""
        stats = self.validation_stats.get(validator.node_id, {
            'selections': 0,
            'successful_validations': 0,
            'shard_validations': {}
        })
        
        if success:
            stats['successful_validations'] += 1
            
        if block.shard_id is not None:
            shard_stats = stats['shard_validations'].setdefault(block.shard_id, {
                'selections': 0,
                'successful': 0
            })
            if success:
                shard_stats['successful'] += 1
                
        self.validation_stats[validator.node_id] = stats

class SmartContract:
    """Represents a smart contract in the ICN blockchain."""
    
    def __init__(self, contract_id: str, code: str, creator: str, 
                 mana_cost: int = 10, version: str = "1.0"):
        self.contract_id = contract_id
        self.code = code
        self.creator = creator
        self.state: Dict = {}
        self.mana_cost = mana_cost
        self.version = version
        self.created_at = datetime.now()
        self.last_executed = None
        self.execution_count = 0
        self.total_mana_consumed = 0
        self.execution_history: List[Dict] = []
        self.metadata: Dict = {
            'created_at': self.created_at,
            'version': version,
            'creator': creator,
            'description': '',
            'tags': set()
        }
        self.dependencies: Set[str] = set()
        self.allowed_callers: Set[str] = {creator}
        self.restrictions: Dict = {
            'max_state_size': 1024 * 1024,  # 1MB
            'max_execution_time': 5,  # seconds
            'max_mana_per_execution': 100,
            'max_daily_executions': 1000
        }
        self.daily_executions = 0
        self.last_reset = datetime.now()

    def execute(self, input_data: Dict, available_mana: int) -> Dict:
        """Execute the smart contract code with safety checks."""
        # Reset daily execution counter if needed
        self._reset_daily_executions()
        
        # Validate execution conditions
        validation_result = self._validate_execution(input_data, available_mana)
        if validation_result.get("error"):
            return validation_result
            
        execution_start = time.time()
        try:
            # Set up secure execution environment
            local_namespace = self._setup_execution_environment(input_data)
            
            # Execute contract code
            exec(self.code, {}, local_namespace)
            
            # Validate execution result
            if "execute" not in local_namespace:
                return {"error": "Contract missing execute function"}
                
            # Check execution time
            if time.time() - execution_start > self.restrictions['max_execution_time']:
                return {"error": "Execution time limit exceeded"}
                
            # Execute contract function
            result = local_namespace["execute"](input_data, self.state)
            
            # Update contract metrics
            self._update_execution_metrics(execution_start)
            
            return {
                "state": self.state,
                "result": result,
                "mana_used": self.mana_cost,
                "execution_time": time.time() - execution_start
            }
            
        except Exception as e:
            logger.error(f"Contract execution failed: {e}")
            return {"error": str(e)}

    def _validate_execution(self, input_data: Dict, available_mana: int) -> Dict:
        """Validate execution conditions."""
        if self.daily_executions >= self.restrictions['max_daily_executions']:
            return {"error": "Daily execution limit exceeded"}
            
        if available_mana < self.mana_cost:
            return {"error": "Insufficient mana"}
            
        if len(str(self.state)) > self.restrictions['max_state_size']:
            return {"error": "State size limit exceeded"}
            
        return {}

    def _setup_execution_environment(self, input_data: Dict) -> Dict:
        """Set up secure execution environment with allowed variables."""
        return {
            "input": input_data,
            "state": self.state.copy(),
            "contract_id": self.contract_id,
            "creator": self.creator,
            "version": self.version,
            "metadata": self.metadata
        }

    def _update_execution_metrics(self, execution_start: float) -> None:
        """Update contract execution metrics."""
        self.last_executed = datetime.now()
        self.execution_count += 1
        self.daily_executions += 1
        self.total_mana_consumed += self.mana_cost
        
        execution_record = {
            'timestamp': self.last_executed,
            'execution_time': time.time() - execution_start,
            'mana_used': self.mana_cost,
            'state_size': len(str(self.state))
        }
        
        self.execution_history.append(execution_record)
        if len(self.execution_history) > 1000:
            self.execution_history = self.execution_history[-1000:]

    def _reset_daily_executions(self) -> None:
        """Reset daily execution counter if needed."""
        current_time = datetime.now()
        if (current_time - self.last_reset).days >= 1:
            self.daily_executions = 0
            self.last_reset = current_time

    def update_metadata(self, metadata: Dict) -> bool:
        """Update contract metadata."""
        try:
            self.metadata.update(metadata)
            return True
        except Exception as e:
            logger.error(f"Failed to update metadata: {e}")
            return False

    def add_dependency(self, contract_id: str) -> bool:
        """Add a contract dependency."""
        self.dependencies.add(contract_id)
        return True

    def authorize_caller(self, caller_id: str) -> bool:
        """Add an authorized caller."""
        self.allowed_callers.add(caller_id)
        return True

    def revoke_caller(self, caller_id: str) -> bool:
        """Revoke caller authorization."""
        if caller_id == self.creator:
            return False
        self.allowed_callers.discard(caller_id)
        return True

    def get_metrics(self) -> Dict:
        """Get comprehensive contract metrics."""
        return {
            'contract_id': self.contract_id,
            'version': self.version,
            'creator': self.creator,
            'created_at': self.created_at.isoformat(),
            'last_executed': self.last_executed.isoformat() if self.last_executed else None,
            'execution_count': self.execution_count,
            'daily_executions': self.daily_executions,
            'total_mana_consumed': self.total_mana_consumed,
            'average_mana_per_execution': (
                self.total_mana_consumed / self.execution_count 
                if self.execution_count > 0 else 0
            ),
            'state_size': len(str(self.state)),
            'dependencies': list(self.dependencies),
            'authorized_callers': len(self.allowed_callers),
            'restrictions': self.restrictions
        }

    def to_dict(self) -> Dict:
        """Convert contract to dictionary format."""
        return {
            'contract_id': self.contract_id,
            'code': self.code,
            'creator': self.creator,
            'state': self.state,
            'mana_cost': self.mana_cost,
            'version': self.version,
            'metadata': self.metadata,
            'dependencies': list(self.dependencies),
            'allowed_callers': list(self.allowed_callers),
            'restrictions': self.restrictions,
            'metrics': self.get_metrics()
        }

    @classmethod
    def from_dict(cls, data: Dict) -> 'SmartContract':
        """Create contract from dictionary."""
        contract = cls(
            contract_id=data['contract_id'],
            code=data['code'],
            creator=data['creator'],
            mana_cost=data['mana_cost'],
            version=data['version']
        )
        contract.state = data['state']
        contract.metadata = data['metadata']
        contract.dependencies = set(data['dependencies'])
        contract.allowed_callers = set(data['allowed_callers'])
        contract.restrictions = data['restrictions']
        return contract


class Blockchain:
    """Main blockchain implementation for ICN."""
    
    def __init__(self, num_shards: int = 4, initial_mana: int = 1000, 
                 mana_regen_rate: int = 10):
        self.num_shards = num_shards
        self.shards: Dict[int, Shard] = {
            i: Shard(i) for i in range(num_shards)
        }
        self.nodes: List[Node] = []
        self.consensus = ProofOfCooperation()
        self.contracts: Dict[str, SmartContract] = {}
        self.cooperative_mana = initial_mana
        self.mana_regen_rate = mana_regen_rate
        self.cross_shard_queue: Dict[int, List[Transaction]] = {
            i: [] for i in range(num_shards)
        }
        self.metadata: Dict = {
            'creation_time': datetime.now(),
            'last_update': datetime.now(),
            'version': "1.0",
            'network_name': "ICN MainNet",
            'network_id': hashlib.sha256(str(time.time()).encode()).hexdigest()[:8]
        }
        self.metrics: Dict = {
            'total_transactions': 0,
            'total_blocks': 0,
            'average_block_time': 0,
            'active_nodes': 0,
            'total_mana_consumed': 0
        }
        self.state: str = "active"
        self._initialize_network()

    def _initialize_network(self) -> None:
        """Initialize network configuration."""
        logger.info(f"Initializing ICN network {self.metadata['network_id']}")
        self._update_metrics()

    def add_node(self, node: Node) -> bool:
        """Add a new node to the network."""
        if any(n.node_id == node.node_id for n in self.nodes):
            return False
            
        self.nodes.append(node)
        self.metrics['active_nodes'] = len(
            [n for n in self.nodes if n.metadata['status'] == "active"]
        )
        logger.info(f"Added node {node.node_id} to network")
        return True

    def remove_node(self, node_id: str) -> bool:
        """Remove a node from the network."""
        node = self._get_node(node_id)
        if not node:
            return False
            
        self.nodes = [n for n in self.nodes if n.node_id != node_id]
        self.metrics['active_nodes'] = len(
            [n for n in self.nodes if n.metadata['status'] == "active"]
        )
        logger.info(f"Removed node {node_id} from network")
        return True

    def _get_node(self, node_id: str) -> Optional[Node]:
        """Get a node by its ID."""
        return next((n for n in self.nodes if n.node_id == node_id), None)

    def add_transaction(self, transaction: Dict) -> bool:
        """Add a new transaction to the network."""
        # Determine shard for transaction
        shard_id = self._calculate_shard_id(transaction)
        
        tx = Transaction(
            sender=transaction['sender'],
            receiver=transaction['receiver'],
            action=transaction['action'],
            data=transaction['data'],
            shard_id=shard_id
        )
        
        # Add to appropriate shard
        if self.shards[shard_id].add_transaction(tx):
            self.metrics['total_transactions'] += 1
            return True
        return False

    def _calculate_shard_id(self, transaction: Dict) -> int:
        """Calculate which shard should handle a transaction."""
        # Simple hash-based sharding
        tx_hash = hashlib.sha256(
            json.dumps(transaction, sort_keys=True).encode()
        ).hexdigest()
        return int(tx_hash, 16) % self.num_shards

    def create_block(self, shard_id: int) -> Optional[Block]:
        """Create a new block in specified shard."""
        shard = self.shards.get(shard_id)
        if not shard:
            return None
            
        # Select validator
        validator = self.consensus.select_validator(self.nodes, shard_id)
        if not validator:
            return None
            
        # Create block
        block = shard.create_block(validator.node_id)
        if not block:
            return None
            
        # Process cross-shard references
        self._add_cross_shard_refs(block)
        
        return block

    def _add_cross_shard_refs(self, block: Block) -> None:
        """Add cross-shard references to block."""
        cross_shard_txs = [
            tx for tx in block.transactions 
            if self._is_cross_shard_transaction(tx)
        ]
        
        for tx in cross_shard_txs:
            ref = self._create_cross_shard_ref(tx)
            if ref:
                block.cross_shard_refs.append(ref)

    def _is_cross_shard_transaction(self, transaction: Transaction) -> bool:
        """Check if transaction involves multiple shards."""
        return 'target_shard' in transaction.data

    def _create_cross_shard_ref(self, transaction: Transaction) -> Optional[str]:
        """Create a reference for cross-shard transaction."""
        try:
            ref_data = {
                'transaction_id': transaction.transaction_id,
                'source_shard': transaction.shard_id,
                'target_shard': transaction.data.get('target_shard'),
                'timestamp': transaction.timestamp.isoformat()
            }
            return hashlib.sha256(
                json.dumps(ref_data, sort_keys=True).encode()
            ).hexdigest()
        except Exception as e:
            logger.error(f"Failed to create cross-shard reference: {e}")
            return None

    def add_block(self, block: Block) -> bool:
        """Add a validated block to the network."""
        shard = self.shards.get(block.shard_id)
        if not shard:
            return False
            
        # Validate block
        validator = self._get_node(block.validator)
        if not validator:
            return False
            
        if not self.consensus.validate_block(block, shard.chain[-1], validator):
            return False
            
        # Add block to shard
        if shard.add_block(block):
            self._process_block_addition(block)
            return True
            
        return False

    def _process_block_addition(self, block: Block) -> None:
        """Process successful block addition."""
        self.metrics['total_blocks'] += 1
        self._update_metrics()
        
        # Process cross-shard references
        if block.cross_shard_refs:
            self._process_cross_shard_refs(block)
            
        logger.info(
            f"Added block {block.index} to shard {block.shard_id}")
                def _process_cross_shard_refs(self, block: Block) -> None:
        """Process cross-shard references in a block."""
        for ref in block.cross_shard_refs:
            ref_data = self._parse_cross_shard_ref(ref)
            if ref_data:
                target_shard = self.shards.get(ref_data['target_shard'])
                if target_shard:
                    tx = self._create_cross_shard_transaction(ref_data)
                    target_shard.add_transaction(tx)

    def _parse_cross_shard_ref(self, ref: str) -> Optional[Dict]:
        """Parse cross-shard reference into transaction data."""
        try:
            ref_data = json.loads(hashlib.sha256(ref.encode()).hexdigest())
            return ref_data
        except Exception as e:
            logger.error(f"Failed to parse cross-shard reference: {e}")
            return None

    def _create_cross_shard_transaction(self, ref_data: Dict) -> Transaction:
        """Create a cross-shard transaction from reference data."""
        return Transaction(
            sender='cross-shard',
            receiver='target-shard',
            action='cross-shard-transfer',
            data=ref_data,
            shard_id=ref_data['target_shard']
        )

    def add_smart_contract(self, contract: SmartContract) -> bool:
        """Add a new smart contract to the network."""
        if contract.contract_id in self.contracts:
            return False

        self.contracts[contract.contract_id] = contract
        logger.info(f"Added smart contract {contract.contract_id} to network")
        return True

    def execute_smart_contract(self, contract_id: str, input_data: Dict) -> Dict:
        """Execute a smart contract with given input data."""
        contract = self.contracts.get(contract_id)
        if not contract:
            return {"error": "Contract not found"}
        
        # Check available mana
        if self.cooperative_mana < contract.mana_cost:
            return {"error": "Insufficient cooperative mana"}
        
        result = contract.execute(input_data, self.cooperative_mana)
        if "error" not in result:
            self.cooperative_mana -= contract.mana_cost
            self.metrics['total_mana_consumed'] += contract.mana_cost
        return result

    def regenerate_mana(self) -> None:
        """Regenerate cooperative mana over time."""
        self.cooperative_mana = min(
            self.cooperative_mana + self.mana_regen_rate,
            1000  # Max mana limit for demonstration purposes
        )
        self._update_metrics()

    def _update_metrics(self) -> None:
        """Update blockchain network metrics."""
        self.metadata['last_update'] = datetime.now().isoformat()
        self.metrics['average_block_time'] = sum(
            shard.metrics['average_block_time'] for shard in self.shards.values()
        ) / max(1, len(self.shards))

    def to_dict(self) -> Dict:
        """Convert blockchain to dictionary format."""
        return {
            'num_shards': self.num_shards,
            'nodes': [node.to_dict() for node in self.nodes],
            'contracts': {cid: contract.to_dict() for cid, contract in self.contracts.items()},
            'cooperative_mana': self.cooperative_mana,
            'metrics': self.metrics,
            'metadata': self.metadata
        }

    @classmethod
    def from_dict(cls, data: Dict) -> 'Blockchain':
        """Create blockchain from dictionary."""
        blockchain = cls(
            num_shards=data['num_shards'],
            initial_mana=data['cooperative_mana']
        )
        blockchain.nodes = [Node.from_dict(node) for node in data['nodes']]
        blockchain.contracts = {
            cid: SmartContract.from_dict(contract) 
            for cid, contract in data['contracts'].items()
        }
        blockchain.cooperative_mana = data['cooperative_mana']
        blockchain.metrics = data['metrics']
        blockchain.metadata = data['metadata']
        return blockchain


