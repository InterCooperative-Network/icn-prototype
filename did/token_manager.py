# token_manager.py

from membership_card import MembershipCard
from typing import Dict, Optional, List

class TokenManager:
    """
    Token Manager for managing cooperative and community tokens within the ICN system.
    
    This manager handles the issuance, upgrading, and federation of membership cards, 
    supporting both cooperative and community memberships. It allows for staking, 
    federation, and enhanced token management features.
    
    Attributes:
    - tokens: Dictionary mapping member DIDs to their respective membership cards.
    """
    
    def __init__(self):
        """
        Initialize the TokenManager with an empty dictionary for tokens.
        """
        self.tokens: Dict[str, MembershipCard] = {}

    def create_membership_card(self, dao_type: str, dao_id: str, member_did: str, metadata: Dict) -> MembershipCard:
        """
        Create and issue a new membership card for a cooperative or community member.

        Args:
        - dao_type: 'cooperative' or 'community', indicating the type of DAO.
        - dao_id: The ID of the DAO issuing the card.
        - member_did: The DID of the member receiving the card.
        - metadata: Additional metadata related to the membership (e.g., roles, permissions).

        Returns:
        - MembershipCard: The newly issued membership card.

        Raises:
        - ValueError: If a membership card already exists for the given member DID.
        """
        if member_did in self.tokens:
            raise ValueError(f"Membership card already exists for member DID: {member_did}")
        
        card = MembershipCard(dao_id=dao_id, member_did=member_did, metadata=metadata, dao_type=dao_type)
        self.tokens[member_did] = card
        return card

    def upgrade_membership_card(self, member_did: str, upgrades: Dict) -> Optional[MembershipCard]:
        """
        Upgrade a membership card with new metadata.

        Args:
        - member_did: The DID of the member whose card is being upgraded.
        - upgrades: New metadata to add to the card (e.g., added roles, permissions).

        Returns:
        - MembershipCard | None: The upgraded membership card, or None if not found.

        Raises:
        - ValueError: If the membership card has been revoked.
        """
        card = self.tokens.get(member_did)
        if card is None:
            return None
        if card.is_revoked:
            raise ValueError(f"Cannot upgrade revoked membership card for member DID: {member_did}")

        card.metadata.update(upgrades)
        return card

    def revoke_membership_card(self, member_did: str) -> bool:
        """
        Revoke a membership card, making it invalid for cooperative or community access.

        Args:
        - member_did: The DID of the member whose card is being revoked.

        Returns:
        - bool: True if revocation was successful, False if the card was not found or already revoked.
        """
        card = self.tokens.get(member_did)
        if card is None or card.is_revoked:
            return False

        card.revoke()
        return True

    def federate_membership_card(self, member_did: str, federation_terms: Dict) -> Optional[MembershipCard]:
        """
        Federate a membership card with another DAO (cooperative or community).

        Args:
        - member_did: The DID of the member whose card is being federated.
        - federation_terms: Terms of the federation agreement (e.g., sharing resources, joint initiatives).

        Returns:
        - MembershipCard | None: The federated membership card, or None if not found.
        """
        card = self.tokens.get(member_did)
        if card is None or card.is_revoked:
            return None

        # Add federation terms to the membership card metadata
        if "federation_terms" not in card.metadata:
            card.metadata["federation_terms"] = []
        card.metadata["federation_terms"].append(federation_terms)

        return card

    def stake_membership_card(self, member_did: str, duration: int) -> bool:
        """
        Stake a membership card for cooperative or community rewards.

        Args:
        - member_did: The DID of the member staking the card.
        - duration: Duration of staking in days.

        Returns:
        - bool: True if staking was successful, False otherwise.
        """
        card = self.tokens.get(member_did)
        if card and not card.is_revoked:
            card.metadata["staked"] = True
            card.metadata["staking_duration"] = duration
            return True
        return False

    def unstake_membership_card(self, member_did: str) -> bool:
        """
        Unstake a previously staked membership card, making it active again.

        Args:
        - member_did: The DID of the member unstaking the card.

        Returns:
        - bool: True if unstaking was successful, False otherwise.
        """
        card = self.tokens.get(member_did)
        if card and card.metadata.get("staked"):
            card.metadata["staked"] = False
            del card.metadata["staking_duration"]
            return True
        return False

    def list_membership_cards(self, dao_type: Optional[str] = None) -> Dict[str, MembershipCard]:
        """
        List all membership cards, optionally filtered by DAO type.

        Args:
        - dao_type: Optional, 'cooperative' or 'community', to filter by type.

        Returns:
        - Dict[str, MembershipCard]: Dictionary of membership cards filtered by DAO type, if specified.
        """
        if dao_type:
            return {did: card for did, card in self.tokens.items() if card.dao_type == dao_type}
        return self.tokens

    def get_membership_card(self, member_did: str) -> Optional[MembershipCard]:
        """
        Retrieve a specific membership card by member DID.

        Args:
        - member_did: The DID of the member whose card is being retrieved.

        Returns:
        - MembershipCard | None: The membership card if found, otherwise None.
        """
        return self.tokens.get(member_did)

    def transfer_membership(self, from_did: str, to_did: str, dao_type: str, dao_id: str) -> bool:
        """
        Transfer a membership from one DID to another within a specific DAO.

        Args:
        - from_did: The DID of the member transferring the membership.
        - to_did: The DID of the recipient member.
        - dao_type: 'cooperative' or 'community'.
        - dao_id: The ID of the DAO where the membership is being transferred.

        Returns:
        - bool: True if transfer was successful, False otherwise.

        Raises:
        - ValueError: If the source DID has no valid membership or the membership is revoked.
        """
        card = self.tokens.get(from_did)
        if card is None or card.is_revoked or card.dao_id != dao_id or card.dao_type != dao_type:
            raise ValueError(f"No valid membership found for transfer from DID: {from_did}")

        # Create a new membership card for the recipient DID
        new_card = MembershipCard(
            dao_id=dao_id,
            member_did=to_did,
            metadata=card.metadata,
            dao_type=dao_type
        )
        self.tokens[to_did] = new_card
        del self.tokens[from_did]  # Remove the old card after transfer

        return True
