# blockchain/core/shard/validation_manager.py

from typing import Dict, Optional, List
import logging
from datetime import datetime, timedelta
from .shard_types import ShardMetrics, ShardConfig
from ..block import Block
from ..transaction import Transaction

logger = logging.getLogger(__name__)

class ValidationManager:
    """Handles transaction and chain validation logic."""
    
    def __init__(self, shard_id: int, config: ShardConfig):
        self.shard_id = shard_id
        self.config = config
        self.validation_cache: Dict[str, bool] = {}
        self.metrics = ShardMetrics()
        self.last_validation_time: Dict[str, datetime] = {}

    def validate_transaction(self, transaction: Transaction) -> bool:
    """
    Validate a transaction before adding to pool.
    
    Args:
        transaction: Transaction to validate
        
    Returns:
        bool: True if transaction is valid
    """
    try:
        # Check cache first
        tx_id = transaction.transaction_id
        if tx_id in self.validation_cache:
            return self.validation_cache[tx_id]

        # Basic validation
        if not transaction.validate():
            logger.error(f"Transaction {tx_id} failed basic validation")
            self.validation_cache[tx_id] = False
            return False

        # NEW: Check transaction amount
        if transaction.data.get('amount', 0) <= 0:
            logger.error(f"Transaction {tx_id} has invalid amount")
            self.validation_cache[tx_id] = False
            return False

        # Rest of the validation logic...