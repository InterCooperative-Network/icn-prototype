"""
Consensus mechanism components.

This module serves as the entry point for the various consensus mechanisms 
implemented within the blockchain's consensus layer, specifically focusing on 
the Proof of Cooperation (PoC) consensus mechanism.
"""

from .proof_of_cooperation import ProofOfCooperation

__all__ = ["ProofOfCooperation"]
