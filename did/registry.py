# registry.py

from typing import Dict, Optional
from base_did import BaseDID
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class DIDRegistry:
    """
    Registry for managing DIDs within the InterCooperative Network (ICN).
    
    This registry handles registration, resolution, verification, and revocation of DIDs,
    as well as integration with identity providers.
    """
    def __init__(self):
        self.dids: Dict[str, BaseDID] = {}
        self.revoked_dids: Dict[str, datetime] = {}
        self._identity_providers: Dict[str, IdentityProvider] = {}

    def register_did(self, did: BaseDID) -> str:
        """Register a new DID within the registry."""
        did_id = did.generate_did()
        if did_id in self.revoked_dids:
            raise ValueError(f"DID {did_id} has been revoked")
        self.dids[did_id] = did
        logger.info(f"Registered new DID: {did_id}")
        return did_id

    def resolve_did(self, did_id: str) -> Optional[BaseDID]:
        """Resolve a DID to its corresponding object."""
        if did_id in self.revoked_dids:
            logger.warning(f"Attempted to resolve revoked DID: {did_id}")
            return None
        return self.dids.get(did_id)

    def revoke_did(self, did_id: str, reason: str) -> None:
        """Revoke a DID."""
        if did_id in self.dids:
            self.revoked_dids[did_id] = datetime.now()
            del self.dids[did_id]
            logger.warning(f"DID revoked: {did_id}, reason: {reason}")
