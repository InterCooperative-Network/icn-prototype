# identity_provider.py

from abc import ABC, abstractmethod
from credential import Credential
from typing import Dict

class IdentityProvider(ABC):
    """
    Abstract base class for identity providers in the ICN.
    
    This class defines the interface for issuing and verifying credentials for 
    cooperatives and communities, along with OAuth-like integration.
    """
    
    @abstractmethod
    def issue_credential(self, subject: str, claims: Dict, dao_type: str) -> Credential:
        """
        Issue a verifiable credential for a cooperative or community.

        Args:
        - subject: The subject DID.
        - claims: Claims related to the credential.
        - dao_type: 'cooperative' or 'community'.

        Returns:
        - Credential: The issued credential.
        """
        pass

    @abstractmethod
    def verify_credential(self, credential: Credential) -> bool:
        """
        Verify the cryptographic proof of a credential.

        Args:
        - credential: The credential to verify.

        Returns:
        - bool: True if the credential is valid, False otherwise.
        """
        pass

    def request_oauth_credential(self, client_id: str, redirect_uri: str, dao_type: str) -> str:
        """
        Implement OAuth-like flow for external applications to request credentials from cooperatives or communities.
        
        Args:
        - client_id: Client ID requesting access.
        - redirect_uri: Redirect URI after consent.
        - dao_type: 'cooperative' or 'community'.

        Returns:
        - str: Authorization URL for credential request.
        """
        return f"https://auth.icn.org/oauth/authorize?client_id={client_id}&redirect_uri={redirect_uri}&dao_type={dao_type}"
