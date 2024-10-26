# ================================================================
# File: blockchain/network/transport/transport.py
# Description: Transport layer implementation for ICN network.
# Handles low-level network communication, including message framing,
# encryption, and reliable delivery.
# ================================================================

import asyncio
import logging
import struct
import json
from typing import Dict, Optional, Tuple, Any, Callable
from dataclasses import dataclass
from datetime import datetime
import hashlib
from cryptography.fernet import Fernet
from ..config import NetworkConfig

logger = logging.getLogger(__name__)

@dataclass
class MessageFrame:
    """Represents a framed network message."""
    message_type: str
    payload: bytes
    sequence: int
    timestamp: datetime
    flags: int = 0
    
    # Message frame format:
    # | Magic (2) | Version (2) | Flags (2) | Sequence (4) | Length (4) | Type (2) | Payload (...) | CRC (4) |
    FRAME_MAGIC = 0x1C4E  # ICN in hex
    FRAME_VERSION = 1
    HEADER_SIZE = 20
    
    def pack(self) -> bytes:
        """Pack message into bytes for transmission."""
        try:
            type_id = hash(self.message_type) & 0xFFFF  # Convert type to 16-bit identifier
            payload_len = len(self.payload)
            
            # Create header
            header = struct.pack(
                ">HHHIH",  # Big-endian: magic, version, flags, sequence, length
                self.FRAME_MAGIC,
                self.FRAME_VERSION,
                self.flags,
                self.sequence,
                payload_len
            )
            
            # Pack message type
            header += struct.pack(">H", type_id)
            
            # Combine header and payload
            message = header + self.payload
            
            # Add CRC
            crc = self._calculate_crc(message)
            message += struct.pack(">I", crc)
            
            return message
            
        except Exception as e:
            logger.error(f"Error packing message frame: {str(e)}")
            raise
    
    @classmethod
    def unpack(cls, data: bytes) -> 'MessageFrame':
        """Unpack bytes into message frame."""
        try:
            # Verify minimum length
            if len(data) < cls.HEADER_SIZE + 4:  # Header + CRC
                raise ValueError("Message too short")
                
            # Parse header
            magic, version, flags, sequence, payload_len, type_id = struct.unpack(
                ">HHHIHH",
                data[:cls.HEADER_SIZE]
            )
            
            # Verify magic number and version
            if magic != cls.FRAME_MAGIC:
                raise ValueError("Invalid message magic number")
            if version != cls.FRAME_VERSION:
                raise ValueError("Unsupported message version")
                
            # Extract payload
            payload_start = cls.HEADER_SIZE
            payload_end = payload_start + payload_len
            payload = data[payload_start:payload_end]
            
            # Verify payload length
            if len(payload) != payload_len:
                raise ValueError("Incomplete message payload")
                
            # Verify CRC
            received_crc = struct.unpack(">I", data[payload_end:payload_end + 4])[0]
            calculated_crc = cls._calculate_crc(data[:payload_end])
            if received_crc != calculated_crc:
                raise ValueError("CRC check failed")
                
            # Reconstruct message type from type_id
            message_type = f"type_{type_id}"  # TODO: Implement proper type mapping
            
            return cls(
                message_type=message_type,
                payload=payload,
                sequence=sequence,
                timestamp=datetime.now(),
                flags=flags
            )
            
        except Exception as e:
            logger.error(f"Error unpacking message frame: {str(e)}")
            raise
    
    @staticmethod
    def _calculate_crc(data: bytes) -> int:
        """Calculate CRC32 checksum."""
        return struct.unpack(">I", hashlib.crc32(data).to_bytes(4, 'big'))[0]

class SecureChannel:
    """Handles encrypted communication channel."""
    
    def __init__(self, key: Optional[bytes] = None):
        """Initialize secure channel with optional encryption key."""
        self.key = key or Fernet.generate_key()
        self.cipher = Fernet(self.key)
        
    def encrypt(self, data: bytes) -> bytes:
        """Encrypt data."""
        try:
            return self.cipher.encrypt(data)
        except Exception as e:
            logger.error(f"Encryption error: {str(e)}")
            raise
            
    def decrypt(self, data: bytes) -> bytes:
        """Decrypt data."""
        try:
            return self.cipher.decrypt(data)
        except Exception as e:
            logger.error(f"Decryption error: {str(e)}")
            raise

class NetworkTransport:
    """
    Handles reliable network transport with message framing and encryption.
    
    Features:
    - Message framing and sequencing
    - Optional encryption
    - Flow control
    - Error detection
    - Message acknowledgment
    """
    
    def __init__(self, config: NetworkConfig):
        """Initialize transport layer."""
        self.config = config
        self.sequence_counter = 0
        self.pending_messages: Dict[int, MessageFrame] = {}
        self.secure_channels: Dict[str, SecureChannel] = {}  # peer_id -> channel
        self.message_handlers: Dict[str, Callable] = {}
        self.retransmit_interval = 5.0  # seconds
        self.max_retransmissions = 3
        
        # Performance tracking
        self.metrics = {
            "messages_sent": 0,
            "messages_received": 0,
            "bytes_sent": 0,
            "bytes_received": 0,
            "retransmissions": 0,
            "failed_deliveries": 0,
            "encryption_errors": 0
        }
    
    async def send_message(
        self,
        peer_id: str,
        message_type: str,
        payload: Dict[str, Any],
        require_ack: bool = True
    ) -> bool:
        """
        Send a message to a peer.
        
        Args:
            peer_id: ID of the peer to send to
            message_type: Type of message
            payload: Message payload
            require_ack: Whether to wait for acknowledgment
            
        Returns:
            bool: True if message sent successfully
        """
        try:
            # Serialize payload
            payload_bytes = json.dumps(payload).encode()
            
            # Create message frame
            self.sequence_counter += 1
            frame = MessageFrame(
                message_type=message_type,
                payload=payload_bytes,
                sequence=self.sequence_counter,
                timestamp=datetime.now()
            )
            
            # Encrypt if necessary
            if self.config.enable_encryption and peer_id in self.secure_channels:
                channel = self.secure_channels[peer_id]
                frame.payload = channel.encrypt(frame.payload)
                frame.flags |= 0x0001  # Set encryption flag
            
            # Pack frame
            message_data = frame.pack()
            
            # Send message
            writer = self._get_peer_writer(peer_id)
            if not writer:
                return False
                
            writer.write(message_data)
            await writer.drain()
            
            # Track metrics
            self.metrics["messages_sent"] += 1
            self.metrics["bytes_sent"] += len(message_data)
            
            # Handle acknowledgment
            if require_ack:
                self.pending_messages[frame.sequence] = frame
                acknowledgment = await self._wait_for_ack(frame.sequence)
                return acknowledgment
            
            return True
            
        except Exception as e:
            logger.error(f"Error sending message to {peer_id}: {str(e)}")
            return False
    
    async def handle_connection(
        self,
        peer_id: str,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter
    ) -> None:
        """
        Handle incoming peer connection.
        
        Args:
            peer_id: ID of the connected peer
            reader: StreamReader for receiving data
            writer: StreamWriter for sending data
        """
        try:
            # Initialize encryption if enabled
            if self.config.enable_encryption:
                await self._setup_secure_channel(peer_id, reader, writer)
            
            # Start message handling loop
            while True:
                # Read header size
                header_data = await reader.readexactly(MessageFrame.HEADER_SIZE)
                
                # Parse header
                magic, version, flags, sequence, payload_len, _ = struct.unpack(
                    ">HHHIHH",
                    header_data
                )
                
                # Verify magic number and version
                if magic != MessageFrame.FRAME_MAGIC:
                    logger.error(f"Invalid message magic from {peer_id}")
                    break
                    
                if version != MessageFrame.FRAME_VERSION:
                    logger.error(f"Unsupported message version from {peer_id}")
                    break
                
                # Read payload and CRC
                remaining_data = await reader.readexactly(payload_len + 4)
                message_data = header_data + remaining_data
                
                # Unpack message
                message = MessageFrame.unpack(message_data)
                
                # Decrypt if necessary
                if flags & 0x0001 and peer_id in self.secure_channels:
                    channel = self.secure_channels[peer_id]
                    message.payload = channel.decrypt(message.payload)
                
                # Track metrics
                self.metrics["messages_received"] += 1
                self.metrics["bytes_received"] += len(message_data)
                
                # Send acknowledgment
                await self._send_ack(writer, sequence)
                
                # Handle message
                await self._handle_message(peer_id, message)
                
        except asyncio.IncompleteReadError:
            logger.info(f"Connection closed by peer {peer_id}")
        except Exception as e:
            logger.error(f"Error handling connection from {peer_id}: {str(e)}")
        finally:
            writer.close()
            await writer.wait_closed()
            if peer_id in self.secure_channels:
                del self.secure_channels[peer_id]
    
    def register_handler(
        self,
        message_type: str,
        handler: Callable[[str, Dict[str, Any]], None]
    ) -> None:
        """
        Register a message handler.
        
        Args:
            message_type: Type of message to handle
            handler: Callback function
        """
        self.message_handlers[message_type] = handler
    
    async def _setup_secure_channel(
        self,
        peer_id: str,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter
    ) -> None:
        """Set up encrypted channel with peer."""
        try:
            # Exchange keys
            my_key = Fernet.generate_key()
            writer.write(my_key)
            await writer.drain()
            
            peer_key = await reader.readexactly(44)  # Fernet key length
            
            # Create secure channel
            channel = SecureChannel(peer_key)
            self.secure_channels[peer_id] = channel
            
        except Exception as e:
            logger.error(f"Error setting up secure channel with {peer_id}: {str(e)}")
            self.metrics["encryption_errors"] += 1
            raise
    
    async def _handle_message(self, peer_id: str, message: MessageFrame) -> None:
        """Handle received message."""
        try:
            # Deserialize payload
            payload = json.loads(message.payload.decode())
            
            # Call registered handler
            handler = self.message_handlers.get(message.message_type)
            if handler:
                await handler(peer_id, payload)
            else:
                logger.warning(f"No handler for message type: {message.message_type}")
                
        except Exception as e:
            logger.error(f"Error handling message: {str(e)}")
    
    async def _send_ack(self, writer: asyncio.StreamWriter, sequence: int) -> None:
        """Send acknowledgment message."""
        try:
            ack = struct.pack(">I", sequence)
            writer.write(ack)
            await writer.drain()
        except Exception as e:
            logger.error(f"Error sending acknowledgment: {str(e)}")
    
    async def _wait_for_ack(self, sequence: int) -> bool:
        """Wait for message acknowledgment."""
        try:
            retries = 0
            while retries < self.max_retransmissions:
                try:
                    # Wait for acknowledgment
                    await asyncio.sleep(self.retransmit_interval)
                    
                    # Check if message was acknowledged
                    if sequence not in self.pending_messages:
                        return True
                        
                    # Retransmit if necessary
                    if retries < self.max_retransmissions - 1:
                        frame = self.pending_messages[sequence]
                        message_data = frame.pack()
                        writer = self._get_peer_writer(frame.peer_id)
                        if writer:
                            writer.write(message_data)
                            await writer.drain()
                            self.metrics["retransmissions"] += 1
                    
                    retries += 1
                    
                except Exception as e:
                    logger.error(f"Error in acknowledgment wait: {str(e)}")
                    retries += 1
            
            # Message delivery failed
            if sequence in self.pending_messages:
                del self.pending_messages[sequence]
            self.metrics["failed_deliveries"] += 1
            return False
            
        except Exception as e:
            logger.error(f"Error waiting for acknowledgment: {str(e)}")
            return False
    
    def _get_peer_writer(self, peer_id: str) -> Optional[asyncio.StreamWriter]:
        """Get StreamWriter for a peer."""
        # TODO: Implement peer writer management
        return None
    
    def get_metrics(self) -> Dict[str, int]:
        """Get transport metrics."""
        return self.metrics.copy()

# Example usage
async def example_usage():
    config = NetworkConfig(node_id="test_node")
    transport = NetworkTransport(config)
    
    # Register message handler
    async def handle_message(peer_id: str, payload: Dict[str, Any]) -> None:
        print(f"Received message from {peer_id}: {payload}")
        
    transport.register_handler("test_message", handle_message)
    
    # Send message
    success = await transport.send_message(
        peer_id="peer1",
        message_type="test_message",
        payload={"hello": "world"}
    )
    print(f"Message sent: {success}")

if __name__ == "__main__":
    asyncio.run(example_usage())