# ================================================================
# File: blockchain/network/protocol/base.py
# Description: Base protocol implementation for ICN network communication.
# This module defines the core protocol structures and message types
# used for node communication in the InterCooperative Network.
# ================================================================

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List
from datetime import datetime
import json
import logging
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

@dataclass
class ProtocolMessage:
    """Base class for all protocol messages."""
    message_type: str
    version: str
    payload: Dict[str, Any]
    timestamp: datetime = field(default_factory=datetime.now)
    sequence: int = field(default_factory=lambda: 0)
    
    def serialize(self) -> bytes:
        """Convert message to bytes for transmission."""
        message_dict = {
            "type": self.message_type,
            "version": self.version,
            "payload": self.payload,
            "timestamp": self.timestamp.isoformat(),
            "sequence": self.sequence
        }
        return json.dumps(message_dict).encode()
    
    @classmethod
    def deserialize(cls, data: bytes) -> 'ProtocolMessage':
        """Create message from received bytes."""
        message_dict = json.loads(data.decode())
        return cls(
            message_type=message_dict["type"],
            version=message_dict["version"],
            payload=message_dict["payload"],
            timestamp=datetime.fromisoformat(message_dict["timestamp"]),
            sequence=message_dict["sequence"]
        )

class Protocol(ABC):
    """Base class for network protocols."""
    
    def __init__(self, version: str):
        self.version = version
        self.message_handlers: Dict[str, List[callable]] = {}
        self.sequence_counter = 0
    
    @abstractmethod
    async def handle_message(self, message: ProtocolMessage) -> Optional[ProtocolMessage]:
        """
        Handle an incoming protocol message.
        
        Args:
            message: The received message
            
        Returns:
            Optional[ProtocolMessage]: Response message if any
        """
        pass
    
    @abstractmethod
    async def create_message(self, message_type: str, payload: Dict[str, Any]) -> ProtocolMessage:
        """
        Create a new protocol message.
        
        Args:
            message_type: Type of message to create
            payload: Message payload
            
        Returns:
            ProtocolMessage: The created message
        """
        pass

class HandshakeProtocol(Protocol):
    """Protocol implementation for peer handshakes."""
    
    def __init__(self):
        super().__init__("icn/1.0")
        
    async def handle_message(self, message: ProtocolMessage) -> Optional[ProtocolMessage]:
        if message.message_type == "handshake_init":
            return await self.create_message("handshake_ack", {
                "accepted": True,
                "supported_protocols": ["icn/1.0", "icn/1.1"]
            })
        return None
        
    async def create_message(self, message_type: str, payload: Dict[str, Any]) -> ProtocolMessage:
        self.sequence_counter += 1
        return ProtocolMessage(
            message_type=message_type,
            version=self.version,
            payload=payload,
            sequence=self.sequence_counter
        )

class ConsensusProtocol(Protocol):
    """Protocol implementation for consensus-related messages."""
    
    def __init__(self):
        super().__init__("icn/1.0")
        
    async def handle_message(self, message: ProtocolMessage) -> Optional[ProtocolMessage]:
        handlers = {
            "block_proposal": self._handle_block_proposal,
            "block_validation": self._handle_block_validation,
            "validation_result": self._handle_validation_result
        }
        
        handler = handlers.get(message.message_type)
        if handler:
            return await handler(message)
        return None
        
    async def create_message(self, message_type: str, payload: Dict[str, Any]) -> ProtocolMessage:
        self.sequence_counter += 1
        return ProtocolMessage(
            message_type=message_type,
            version=self.version,
            payload=payload,
            sequence=self.sequence_counter
        )
    
    async def _handle_block_proposal(self, message: ProtocolMessage) -> Optional[ProtocolMessage]:
        """Handle incoming block proposal."""
        try:
            # Validate block proposal
            block_data = message.payload.get("block")
            if not block_data:
                return await self.create_message("block_validation", {
                    "accepted": False,
                    "reason": "Missing block data"
                })
            
            # TODO: Implement actual block validation logic
            
            return await self.create_message("block_validation", {
                "accepted": True,
                "block_hash": block_data.get("hash")
            })
            
        except Exception as e:
            logger.error(f"Error handling block proposal: {str(e)}")
            return await self.create_message("block_validation", {
                "accepted": False,
                "reason": str(e)
            })
    
    async def _handle_block_validation(self, message: ProtocolMessage) -> Optional[ProtocolMessage]:
        """Handle block validation message."""
        try:
            validation_result = message.payload.get("accepted", False)
            if validation_result:
                # TODO: Implement validation confirmation logic
                return None
            
            reason = message.payload.get("reason", "Unknown reason")
            logger.warning(f"Block validation failed: {reason}")
            return None
            
        except Exception as e:
            logger.error(f"Error handling block validation: {str(e)}")
            return None
    
    async def _handle_validation_result(self, message: ProtocolMessage) -> Optional[ProtocolMessage]:
        """Handle validation result message."""
        try:
            # Process validation result
            result = message.payload.get("result", {})
            block_hash = result.get("block_hash")
            is_valid = result.get("is_valid", False)
            
            if is_valid:
                # TODO: Implement logic for accepted validation
                pass
            else:
                reason = result.get("reason", "Unknown reason")
                logger.warning(f"Validation rejected for block {block_hash}: {reason}")
            
            return None
            
        except Exception as e:
            logger.error(f"Error handling validation result: {str(e)}")
            return None

class SyncProtocol(Protocol):
    """Protocol implementation for state synchronization."""
    
    def __init__(self):
        super().__init__("icn/1.0")
        
    async def handle_message(self, message: ProtocolMessage) -> Optional[ProtocolMessage]:
        handlers = {
            "sync_request": self._handle_sync_request,
            "sync_response": self._handle_sync_response,
            "sync_complete": self._handle_sync_complete
        }
        
        handler = handlers.get(message.message_type)
        if handler:
            return await handler(message)
        return None
        
    async def create_message(self, message_type: str, payload: Dict[str, Any]) -> ProtocolMessage:
        self.sequence_counter += 1
        return ProtocolMessage(
            message_type=message_type,
            version=self.version,
            payload=payload,
            sequence=self.sequence_counter
        )
    
    async def _handle_sync_request(self, message: ProtocolMessage) -> Optional[ProtocolMessage]:
        """Handle state sync request."""
        try:
            # Get sync parameters
            start_block = message.payload.get("start_block", 0)
            end_block = message.payload.get("end_block")
            
            # TODO: Implement state gathering logic
            
            return await self.create_message("sync_response", {
                "start_block": start_block,
                "end_block": end_block,
                "state": {}  # Add actual state data
            })
            
        except Exception as e:
            logger.error(f"Error handling sync request: {str(e)}")
            return await self.create_message("sync_response", {
                "error": str(e)
            })
    
    async def _handle_sync_response(self, message: ProtocolMessage) -> Optional[ProtocolMessage]:
        """Handle state sync response."""
        try:
            state_data = message.payload.get("state", {})
            if not state_data:
                return None
            
            # TODO: Implement state application logic
            
            return await self.create_message("sync_complete", {
                "success": True
            })
            
        except Exception as e:
            logger.error(f"Error handling sync response: {str(e)}")
            return await self.create_message("sync_complete", {
                "success": False,
                "error": str(e)
            })
    
    async def _handle_sync_complete(self, message: ProtocolMessage) -> Optional[ProtocolMessage]:
        """Handle sync completion message."""
        try:
            success = message.payload.get("success", False)
            if not success:
                error = message.payload.get("error", "Unknown error")
                logger.error(f"Sync failed: {error}")
            
            # TODO: Implement sync completion logic
            
            return None
            
        except Exception as e:
            logger.error(f"Error handling sync complete: {str(e)}")
            return None