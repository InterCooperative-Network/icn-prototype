# layered_did.py

from base_did import BaseDID
from typing import Dict, Optional
from dataclasses import field

class LayeredDID(BaseDID):
    """
    Layered Decentralized Identifier (Layered DID).
    
    This class extends the base DID and allows for the creation of sub-DIDs, 
    which are specific to each cooperative. This provides users with a layer of 
    privacy and separation when interacting with different cooperatives, without 
    linking their activities to their base DID.

    Attributes:
    - sub_dids: A dictionary mapping cooperative IDs to their respective sub-DIDs.
    """
    sub_dids: Dict[str, str] = field(default_factory=dict)

    def add_sub_did(self, cooperative_id: str) -> str:
        """
        Generate and store a sub-DID specific to a cooperative.

        Args:
        - cooperative_id: The unique identifier of the cooperative.

        Returns:
        - str: The generated sub-DID.
        """
        sub_did = f"{self.generate_did()}:{hashlib.sha256(cooperative_id.encode()).hexdigest()[:8]}"
        self.sub_dids[cooperative_id] = sub_did
        return sub_did

    def get_sub_did(self, cooperative_id: str) -> Optional[str]:
        """
        Retrieve the sub-DID for a specific cooperative.

        Args:
        - cooperative_id: The cooperative's unique ID.

        Returns:
        - str | None: The sub-DID if found, otherwise None.
        """
        return self.sub_dids.get(cooperative_id)
