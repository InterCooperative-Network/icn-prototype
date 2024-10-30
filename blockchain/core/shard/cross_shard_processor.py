"""
blockchain/core/shard/cross_shard_processor.py

Processes cross-shard transactions in the ICN blockchain.
Handles atomic transaction execution across multiple shards while maintaining
consistency and cooperative principles.
"""

import asyncio
import logging
from typing import Dict, List, Optional, Set, Any, Tuple
from dataclasses import dataclass, field
from datetime import datetime
import hashlib

from ..transaction import Transaction
from ..block import Block
from ..state.commitment import StateCommitmentManager
from ..state.validation import StateValidator
from ...monitoring.cooperative_metrics import CooperativeMetricsMonitor

logger = logging.getLogger(__name__)

@dataclass
class CrossShardContext:
   """Context for a cross-shard transaction."""
   transaction_id: str
   source_shard: int
   target_shards: Set[int]
   initiator_id: str
   timestamp: datetime = field(default_factory=datetime.now)
   status: str = "pending"  # pending, prepared, committed, aborted
   participants: Set[str] = field(default_factory=set)
   state_refs: Dict[int, str] = field(default_factory=dict)  # shard_id -> state_ref

@dataclass
class CrossShardOutcome:
   """Result of a cross-shard transaction."""
   context: CrossShardContext
   success: bool
   affected_shards: Set[int]
   state_updates: Dict[int, str]  # shard_id -> state_root
   completion_time: datetime = field(default_factory=datetime.now)
   error: Optional[str] = None

class CrossShardProcessor:
   """
   Processes transactions that span multiple shards.
   
   Key responsibilities:
   - Coordinate atomic cross-shard transactions
   - Ensure state consistency across shards
   - Handle transaction preparation and commitment
   - Manage rollbacks for failed transactions
   """

   def __init__(
       self,
       commitment_manager: StateCommitmentManager,
       state_validator: StateValidator,
       metrics_monitor: CooperativeMetricsMonitor,
       prepare_timeout: int = 30,  # seconds
       commit_timeout: int = 60  # seconds
   ):
       self.commitment_manager = commitment_manager
       self.state_validator = state_validator
       self.metrics_monitor = metrics_monitor
       self.prepare_timeout = prepare_timeout
       self.commit_timeout = commit_timeout
       
       # Transaction tracking
       self.active_transactions: Dict[str, CrossShardContext] = {}
       self.prepared_transactions: Set[str] = set()
       self.committed_transactions: Dict[str, CrossShardOutcome] = {}
       self.aborted_transactions: Dict[str, str] = {}  # tx_id -> reason
       
       # Shard coordination
       self.shard_locks: Dict[int, Set[str]] = {}  # shard_id -> tx_ids
       self.pending_preparations: Dict[str, Set[int]] = {}  # tx_id -> shard_ids
       self.pending_commits: Dict[str, Set[int]] = {}  # tx_id -> shard_ids

   async def process_transaction(
       self,
       transaction: Transaction,
       source_shard: int,
       target_shards: Set[int]
   ) -> Optional[CrossShardOutcome]:
       """
       Process a cross-shard transaction.
       
       Args:
           transaction: The transaction to process
           source_shard: Source shard ID
           target_shards: Set of target shard IDs
           
       Returns:
           Optional[CrossShardOutcome]: The transaction outcome if successful
       """
       try:
           # Create transaction context
           context = CrossShardContext(
               transaction_id=transaction.transaction_id,
               source_shard=source_shard,
               target_shards=target_shards,
               initiator_id=transaction.sender
           )
           
           # Check if transaction already exists
           if transaction.transaction_id in self.active_transactions:
               logger.warning(f"Transaction {transaction.transaction_id} already active")
               return None
               
           # Acquire shard locks
           if not await self._acquire_shard_locks(context):
               return None
               
           try:
               # Start transaction processing
               self.active_transactions[transaction.transaction_id] = context
               
               # Prepare phase
               if not await self._prepare_transaction(context, transaction):
                   await self._abort_transaction(context, "Preparation failed")
                   return None
                   
               # Commit phase
               if not await self._commit_transaction(context, transaction):
                   await self._abort_transaction(context, "Commitment failed")
                   return None
                   
               # Create successful outcome
               outcome = CrossShardOutcome(
                   context=context,
                   success=True,
                   affected_shards=target_shards | {source_shard},
                   state_updates=context.state_refs
               )
               
               # Record success
               self.committed_transactions[transaction.transaction_id] = outcome
               
               # Record metrics
               self._record_transaction_metrics(context, True)
               
               return outcome
               
           finally:
               # Release shard locks
               await self._release_shard_locks(context)
               
       except Exception as e:
           logger.error(f"Error processing cross-shard transaction: {str(e)}")
           if context:
               await self._abort_transaction(context, str(e))
           return None

   async def _prepare_transaction(
       self,
       context: CrossShardContext,
       transaction: Transaction
   ) -> bool:
       """Prepare transaction across all involved shards."""
       try:
           # Initialize preparation tracking
           self.pending_preparations[context.transaction_id] = (
               context.target_shards | {context.source_shard}
           )
           
           # Send prepare requests to all shards
           preparation_tasks = []
           for shard_id in self.pending_preparations[context.transaction_id]:
               task = asyncio.create_task(
                   self._prepare_shard(context, transaction, shard_id)
               )
               preparation_tasks.append(task)
           
           # Wait for all preparations with timeout
           try:
               await asyncio.wait_for(
                   asyncio.gather(*preparation_tasks),
                   timeout=self.prepare_timeout
               )
           except asyncio.TimeoutError:
               logger.error(f"Preparation timeout for transaction {context.transaction_id}")
               return False
           
           # Check all shards prepared successfully
           if self.pending_preparations[context.transaction_id]:
               return False
               
           # Mark as prepared
           self.prepared_transactions.add(context.transaction_id)
           context.status = "prepared"
           
           return True
           
       except Exception as e:
           logger.error(f"Error preparing transaction: {str(e)}")
           return False

   async def _prepare_shard(
       self,
       context: CrossShardContext,
       transaction: Transaction,
       shard_id: int
   ) -> bool:
       """Prepare transaction for a specific shard."""
       try:
           # Validate shard state
           validation_context = self._create_validation_context(context, shard_id)
           validation_result = await self.state_validator.validate_state_transition(
               {},  # Old state not needed for preparation
               transaction.data,
               validation_context
           )
           
           if not validation_result.is_valid:
               logger.error(
                   f"State validation failed for shard {shard_id}: {validation_result.reason}"
               )
               return False
               
           # Create state commitment
           commitment = await self.commitment_manager.create_commitment(
               transaction.data,
               context.timestamp,
               shard_id,
               context.initiator_id
           )
           
           if not commitment:
               return False
               
           # Store state reference
           context.state_refs[shard_id] = commitment.commitment_id
           
           # Update preparation tracking
           if context.transaction_id in self.pending_preparations:
               self.pending_preparations[context.transaction_id].discard(shard_id)
               
           return True
           
       except Exception as e:
           logger.error(f"Error preparing shard {shard_id}: {str(e)}")
           return False

   async def _commit_transaction(
       self,
       context: CrossShardContext,
       transaction: Transaction
   ) -> bool:
       """Commit the prepared transaction."""
       try:
           if context.transaction_id not in self.prepared_transactions:
               logger.error(f"Transaction {context.transaction_id} not prepared")
               return False
               
           # Initialize commit tracking
           self.pending_commits[context.transaction_id] = (
               context.target_shards | {context.source_shard}
           )
           
           # Send commit requests to all shards
           commit_tasks = []
           for shard_id in self.pending_commits[context.transaction_id]:
               task = asyncio.create_task(
                   self._commit_shard(context, transaction, shard_id)
               )
               commit_tasks.append(task)
           
           # Wait for all commits with timeout
           try:
               await asyncio.wait_for(
                   asyncio.gather(*commit_tasks),
                   timeout=self.commit_timeout
               )
           except asyncio.TimeoutError:
               logger.error(f"Commit timeout for transaction {context.transaction_id}")
               return False
               
           # Check all shards committed successfully
           if self.pending_commits[context.transaction_id]:
               return False
               
           # Update status
           context.status = "committed"
           
           return True
           
       except Exception as e:
           logger.error(f"Error committing transaction: {str(e)}")
           return False

   async def _commit_shard(
       self,
       context: CrossShardContext,
       transaction: Transaction,
       shard_id: int
   ) -> bool:
       """Commit transaction for a specific shard."""
       try:
           # Verify state commitment
           commitment_id = context.state_refs.get(shard_id)
           if not commitment_id:
               return False
               
           # Verify commitment
           commitment = self.commitment_manager.commitments.get(commitment_id)
           if not commitment:
               return False
               
           # Update commit tracking
           if context.transaction_id in self.pending_commits:
               self.pending_commits[context.transaction_id].discard(shard_id)
               
           return True
           
       except Exception as e:
           logger.error(f"Error committing shard {shard_id}: {str(e)}")
           return False

   async def _abort_transaction(
       self,
       context: CrossShardContext,
       reason: str
   ) -> None:
       """Abort a transaction and clean up resources."""
       try:
           # Update status
           context.status = "aborted"
           
           # Record abort reason
           self.aborted_transactions[context.transaction_id] = reason
           
           # Clean up tracking
           if context.transaction_id in self.active_transactions:
               del self.active_transactions[context.transaction_id]
           if context.transaction_id in self.prepared_transactions:
               self.prepared_transactions.remove(context.transaction_id)
           if context.transaction_id in self.pending_preparations:
               del self.pending_preparations[context.transaction_id]
           if context.transaction_id in self.pending_commits:
               del self.pending_commits[context.transaction_id]
               
           # Record metrics
           self._record_transaction_metrics(context, False)
           
           logger.warning(
               f"Aborted transaction {context.transaction_id}: {reason}"
           )
           
       except Exception as e:
           logger.error(f"Error aborting transaction: {str(e)}")

   async def _acquire_shard_locks(self, context: CrossShardContext) -> bool:
       """Acquire locks for all involved shards."""
       try:
           shards = context.target_shards | {context.source_shard}
           
           # Check if any shard is already locked
           for shard_id in shards:
               if shard_id in self.shard_locks:
                   return False
                   
           # Acquire locks
           for shard_id in shards:
               if shard_id not in self.shard_locks:
                   self.shard_locks[shard_id] = set()
               self.shard_locks[shard_id].add(context.transaction_id)
               
           return True
           
       except Exception as e:
           logger.error(f"Error acquiring shard locks: {str(e)}")
           return False

   async def _release_shard_locks(self, context: CrossShardContext) -> None:
       """Release locks for all involved shards."""
       try:
           shards = context.target_shards | {context.source_shard}
           
           for shard_id in shards:
               if shard_id in self.shard_locks:
                   self.shard_locks[shard_id].discard(context.transaction_id)
                   if not self.shard_locks[shard_id]:
                       del self.shard_locks[shard_id]
                       
       except Exception as e:
           logger.error(f"Error releasing shard locks: {str(e)}")

   def _create_validation_context(
       self,
       context: CrossShardContext,
       shard_id: int
   ) -> ValidationContext:
       """Create validation context for a shard."""
       return ValidationContext(
           block_height=0,  # Not needed for preparation
           shard_id=shard_id,
           validator_id=context.initiator_id,
           cross_shard_refs={
               f"{target}:{context.transaction_id}"
               for target in context.target_shards
               if target != shard_id
           }
       )

   def _record_transaction_metrics(
       self,
       context: CrossShardContext,
       success: bool
   ) -> None:
       """Record metrics for the transaction."""
       try:
           self.metrics_monitor.record_metric(
               participant_id=context.initiator_id,
               metric_type="cross_shard_transaction",
               value=1.0 if success else 0.0,
               metadata={
                   "transaction_id": context.transaction_id,
                   "source_shard": context.source_shard,
                   "target_shards": list(context.target_shards),
                   "status": context.status,
                   "duration": (
                       datetime.now() - context.timestamp
                   ).total_seconds()
               }
           )
       except Exception as e:
           logger.error(f"Error recording transaction metrics: {str(e)}")

   def get_transaction_status(self, transaction_id: str) -> Dict[str, Any]:
       """Get detailed status of a transaction."""
       try:
           if transaction_id in self.committed_transactions:
               outcome = self.committed_transactions[transaction_id]
               return {
                   "status": "committed",
                   "context": {
                       "source_shard": outcome.context.source_shard,
                       "target_shards": list(outcome.context.target_shards),
                       "initiator": outcome.context.initiator_id,
                       "timestamp": outcome.context.timestamp.isoformat()
                   },
                   "affected_shards": list(outcome.affected_shards),
                   "state_updates": outcome.state_updates,
                   "completion_time": outcome.completion_time.isoformat()
               }
               
           if transaction_id in self.aborted_transactions:
               return {
                   "status": "aborted",
                   "reason": self.aborted_transactions[transaction_id]
               }
               
           if transaction_id in self.active_transactions:
               context = self.active_transactions[transaction_id]
               return {
                   "status": context.status,
                   "context": {
                       "source_shard": context.source_shard,
                       "target_shards": list(context.target_shards),
                       "initiator": context.initiator_id,
                       "timestamp": context.timestamp.isoformat()
                   },
                   "state_refs": context.state_refs,
                   "prepared": transaction_id in self.prepared_transactions,
                   "pending_preparations": list(
                       self.pending_preparations.get(transaction_id, set())
                   ),
                   "pending_commits": list(
                       self.pending_commits.get(transaction_id, set())
                   )
               }
           
           return {
               "status": "unknown",
               "exists": False
           }
           
       except Exception as e:
           logger.error(f"Error getting transaction status: {str(e)}")
           return {
               "status": "error",
               "error": str(e)
           }

   def get_processor_metrics(self) -> Dict[str, Any]:
       """Get comprehensive metrics about the processor."""
       try:
           total_transactions = len(self.committed_transactions) + len(self.aborted_transactions)
           if total_transactions == 0:
               return {
                   "total_transactions": 0,
                   "success_rate": 0.0,
                   "active_transactions": 0,
                   "locked_shards": 0
               }

           return {
               "total_transactions": total_transactions,
               "committed_transactions": len(self.committed_transactions),
               "aborted_transactions": len(self.aborted_transactions),
               "success_rate": len(self.committed_transactions) / total_transactions,
               "active_transactions": len(self.active_transactions),
               "prepared_transactions": len(self.prepared_transactions),
               "locked_shards": len(self.shard_locks),
               "average_shards_per_transaction": self._calculate_average_shards(),
               "preparation_distribution": self._get_preparation_distribution(),
               "commit_distribution": self._get_commit_distribution()
           }

       except Exception as e:
           logger.error(f"Error getting processor metrics: {str(e)}")
           return {}

   def _calculate_average_shards(self) -> float:
       """Calculate average number of shards per transaction."""
       try:
           total_shards = sum(
               len(outcome.affected_shards)
               for outcome in self.committed_transactions.values()
           )
           return total_shards / len(self.committed_transactions) if self.committed_transactions else 0

       except Exception as e:
           logger.error(f"Error calculating average shards: {str(e)}")
           return 0.0

   def _get_preparation_distribution(self) -> Dict[int, int]:
       """Get distribution of pending preparations per shard."""
       try:
           distribution = {}
           for pending_shards in self.pending_preparations.values():
               for shard_id in pending_shards:
                   distribution[shard_id] = distribution.get(shard_id, 0) + 1
           return distribution

       except Exception as e:
           logger.error(f"Error getting preparation distribution: {str(e)}")
           return {}

   def _get_commit_distribution(self) -> Dict[int, int]:
       """Get distribution of pending commits per shard."""
       try:
           distribution = {}
           for pending_shards in self.pending_commits.values():
               for shard_id in pending_shards:
                   distribution[shard_id] = distribution.get(shard_id, 0) + 1
           return distribution

       except Exception as e:
           logger.error(f"Error getting commit distribution: {str(e)}")
           return {}

   def export_transaction_history(self) -> Dict[str, Any]:
       """Export complete transaction history for analysis."""
       try:
           return {
               "committed_transactions": {
                   tx_id: {
                       "context": {
                           "source_shard": outcome.context.source_shard,
                           "target_shards": list(outcome.context.target_shards),
                           "initiator": outcome.context.initiator_id,
                           "timestamp": outcome.context.timestamp.isoformat(),
                           "status": outcome.context.status
                       },
                       "outcome": {
                           "success": outcome.success,
                           "affected_shards": list(outcome.affected_shards),
                           "state_updates": outcome.state_updates,
                           "completion_time": outcome.completion_time.isoformat(),
                           "error": outcome.error
                       }
                   }
                   for tx_id, outcome in self.committed_transactions.items()
               },
               "aborted_transactions": {
                   tx_id: reason
                   for tx_id, reason in self.aborted_transactions.items()
               },
               "active_transactions": {
                   tx_id: {
                       "source_shard": context.source_shard,
                       "target_shards": list(context.target_shards),
                       "initiator": context.initiator_id,
                       "timestamp": context.timestamp.isoformat(),
                       "status": context.status,
                       "state_refs": context.state_refs
                   }
                   for tx_id, context in self.active_transactions.items()
               }
           }

       except Exception as e:
           logger.error(f"Error exporting transaction history: {str(e)}")
           return {}