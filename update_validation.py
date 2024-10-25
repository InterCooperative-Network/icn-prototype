# TARGET: blockchain/core/shard/validation_manager.py
# MODE: update
# SECTION: validate_transaction
# DESCRIPTION: Add new validation checks for transaction amounts

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