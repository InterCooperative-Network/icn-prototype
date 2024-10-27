# membership_card.py

from dataclasses import dataclass, field
from typing import Dict

@dataclass
class MembershipCard:
    """
    Soulbound Membership Card representing cooperative membership.
    
    This card is issued by cooperatives to their members, allowing them to participate 
    in the cooperative and access its products and services. These tokens cannot be 
    transferred and are permanently bound to the member's DID.

    Attributes:
    - cooperative_id: The unique identifier of the cooperative issuing the card.
    - member_did: The DID of the member to whom the card is issued.
    - metadata: Additional data about the membership, such as roles or permissions.
    - is_revoked: A boolean indicating whether the membership has been revoked.
    """
    cooperative_id: str
    member_did: str
    metadata: Dict = field(default_factory=dict)
    is_revoked: bool = False

    def issue(self, member_did: str, metadata: Dict) -> MembershipCard:
        """
        Issue a new membership card for a cooperative member.

        Args:
        - member_did: The DID of the member receiving the card.
        - metadata: Additional data about the membership (e.g., roles, permissions).

        Returns:
        - MembershipCard: The issued membership card.
        """
        self.member_did = member_did
        self.metadata.update(metadata)
        return self

    def revoke(self) -> None:
        """
        Revoke the membership card, invalidating it.
        """
        self.is_revoked = True
