# ================================================================
# File: blockchain/network/manager.py
# Description: Core network management for the ICN blockchain.
# This module handles P2P networking, message routing, and peer
# management for the InterCooperative Network.
# ================================================================

import asyncio
import logging
import json
from typing import Dict, Set, Optional, Callable, Any, List
from datetime import datetime
from dataclasses import dataclass, field
import hashlib
import random

logger = logging.getLogger(__name__)

@dataclass
class PeerInfo:
    """Information about a connected peer."""
    node_id: str
    address: str
    port: int
    last_seen: datetime = field(default_factory=datetime.now)
    reputation: float = 1.0
    cooperative_id: Optional[str] = None
    supported_protocols: Set[str] = field(default_factory=set)
    connection_attempts: int = 0
    is_active: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class Message:
    """Network message structure."""
    message_type: str
    payload: Dict[str, Any]
    sender: str
    timestamp: datetime = field(default_factory=datetime.now)
    message_id: str = field(init=False)

    def __post_init__(self):
        """Generate unique message ID after initialization."""
        message_data = f"{self.message_type}:{self.sender}:{self.timestamp.isoformat()}"
        self.message_id = hashlib.sha256(message_data.encode()).hexdigest()

class NetworkManager:
    """
    Manages P2P networking for the ICN blockchain.
    
    Responsibilities:
    - Peer discovery and connection management
    - Message routing and broadcasting
    - Network state monitoring
    - Protocol version management
    """

    def __init__(self, node_id: str, host: str, port: int, max_peers: int = 50):
        """
        Initialize the network manager.
        
        Args:
            node_id: Unique identifier for this node
            host: Host address to bind to
            port: Port to listen on
            max_peers: Maximum number of peer connections
        """
        self.node_id = node_id
        self.host = host
        self.port = port
        self.max_peers = max_peers

        # Connection management
        self.peers: Dict[str, PeerInfo] = {}
        self.blacklisted_peers: Set[str] = set()
        self.pending_connections: Set[str] = set()
        self.connection_timeout = 30  # seconds

        # Message handling
        self.message_handlers: Dict[str, List[Callable]] = {}
        self.processed_messages: Set[str] = set()
        self.message_cache_size = 1000

        # Protocol versioning
        self.supported_protocols = {"icn/1.0", "icn/1.1"}
        self.min_protocol_version = "icn/1.0"

        # State
        self.is_running = False
        self.server: Optional[asyncio.Server] = None
        self.metrics: Dict[str, Any] = {
            "messages_processed": 0,
            "messages_sent": 0,
            "failed_connections": 0,
            "active_connections": 0
        }

    async def start(self) -> None:
        """Start the network manager and begin accepting connections."""
        if self.is_running:
            return

        try:
            # Start TCP server
            self.server = await asyncio.start_server(
                self._handle_connection,
                self.host,
                self.port
            )
            self.is_running = True
            
            # Start background tasks
            asyncio.create_task(self._maintenance_loop())
            asyncio.create_task(self._metrics_loop())
            
            logger.info(f"Network manager started on {self.host}:{self.port}")
            
            # Serve forever
            async with self.server:
                await self.server.serve_forever()
                
        except Exception as e:
            logger.error(f"Failed to start network manager: {str(e)}")
            raise

    async def stop(self) -> None:
        """Stop the network manager and close all connections."""
        if not self.is_running:
            return

        try:
            # Close all peer connections
            close_tasks = [
                self._close_peer_connection(peer_id)
                for peer_id in list(self.peers.keys())
            ]
            await asyncio.gather(*close_tasks, return_exceptions=True)

            # Stop server
            if self.server:
                self.server.close()
                await self.server.wait_closed()

            self.is_running = False
            logger.info("Network manager stopped")

        except Exception as e:
            logger.error(f"Error stopping network manager: {str(e)}")
            raise

    async def connect_to_peer(self, peer_id: str, address: str, port: int) -> bool:
        """
        Establish connection to a new peer.
        
        Args:
            peer_id: Unique identifier of the peer
            address: Peer's network address
            port: Peer's port number
            
        Returns:
            bool: True if connection successful, False otherwise
        """
        if peer_id in self.blacklisted_peers:
            logger.warning(f"Attempted to connect to blacklisted peer: {peer_id}")
            return False

        if peer_id in self.peers:
            logger.debug(f"Already connected to peer: {peer_id}")
            return True

        if len(self.peers) >= self.max_peers:
            logger.warning("Maximum peer connections reached")
            return False

        try:
            # Attempt TCP connection
            reader, writer = await asyncio.open_connection(address, port)

            # Create peer info
            peer_info = PeerInfo(
                node_id=peer_id,
                address=address,
                port=port
            )

            # Perform handshake
            if not await self._perform_handshake(reader, writer, peer_info):
                writer.close()
                await writer.wait_closed()
                return False

            # Store peer connection
            self.peers[peer_id] = peer_info
            self.metrics["active_connections"] += 1

            # Start message handling loop
            asyncio.create_task(self._handle_peer_messages(reader, writer, peer_id))

            logger.info(f"Successfully connected to peer: {peer_id}")
            return True

        except Exception as e:
            logger.error(f"Failed to connect to peer {peer_id}: {str(e)}")
            self.metrics["failed_connections"] += 1
            return False

    async def broadcast_message(self, message_type: str, payload: Dict[str, Any]) -> None:
        """
        Broadcast a message to all connected peers.
        
        Args:
            message_type: Type of message to broadcast
            payload: Message payload data
        """
        message = Message(
            message_type=message_type,
            payload=payload,
            sender=self.node_id
        )

        try:
            send_tasks = []
            for peer_id, peer_info in self.peers.items():
                if peer_info.is_active:
                    send_tasks.append(
                        self._send_message_to_peer(peer_id, message)
                    )

            await asyncio.gather(*send_tasks, return_exceptions=True)
            self.metrics["messages_sent"] += len(send_tasks)

        except Exception as e:
            logger.error(f"Error broadcasting message: {str(e)}")

    async def send_message(self, peer_id: str, message_type: str, payload: Dict[str, Any]) -> bool:
        """
        Send a message to a specific peer.
        
        Args:
            peer_id: ID of the peer to send to
            message_type: Type of message to send
            payload: Message payload data
            
        Returns:
            bool: True if message sent successfully, False otherwise
        """
        if peer_id not in self.peers:
            logger.warning(f"Attempted to send message to unknown peer: {peer_id}")
            return False

        message = Message(
            message_type=message_type,
            payload=payload,
            sender=self.node_id
        )

        try:
            await self._send_message_to_peer(peer_id, message)
            self.metrics["messages_sent"] += 1
            return True

        except Exception as e:
            logger.error(f"Failed to send message to peer {peer_id}: {str(e)}")
            return False

    def register_message_handler(self, message_type: str, handler: Callable) -> None:
        """
        Register a handler function for a specific message type.
        
        Args:
            message_type: Type of message to handle
            handler: Callback function to handle messages of this type
        """
        if message_type not in self.message_handlers:
            self.message_handlers[message_type] = []
        self.message_handlers[message_type].append(handler)

    async def _handle_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """Handle incoming peer connection."""
        try:
            # Receive handshake
            handshake_data = await reader.read(1024)
            handshake = json.loads(handshake_data.decode())

            peer_id = handshake.get("node_id")
            if not peer_id:
                writer.close()
                return

            # Check if peer is blacklisted
            if peer_id in self.blacklisted_peers:
                writer.close()
                return

            # Create peer info
            peer_info = PeerInfo(
                node_id=peer_id,
                address=writer.get_extra_info('peername')[0],
                port=writer.get_extra_info('peername')[1]
            )

            # Send handshake response
            response = {
                "node_id": self.node_id,
                "protocols": list(self.supported_protocols)
            }
            writer.write(json.dumps(response).encode())
            await writer.drain()

            # Store peer connection
            self.peers[peer_id] = peer_info
            self.metrics["active_connections"] += 1

            # Start message handling loop
            asyncio.create_task(self._handle_peer_messages(reader, writer, peer_id))

        except Exception as e:
            logger.error(f"Error handling connection: {str(e)}")
            writer.close()

    async def _handle_peer_messages(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        peer_id: str
    ) -> None:
        """Handle incoming messages from a peer."""
        try:
            while True:
                # Read message length
                length_data = await reader.readexactly(4)
                message_length = int.from_bytes(length_data, 'big')

                # Read message data
                message_data = await reader.readexactly(message_length)
                message_dict = json.loads(message_data.decode())

                # Create message object
                message = Message(
                    message_type=message_dict["message_type"],
                    payload=message_dict["payload"],
                    sender=message_dict["sender"]
                )

                # Process message if not seen before
                if message.message_id not in self.processed_messages:
                    self.processed_messages.add(message.message_id)
                    if len(self.processed_messages) > self.message_cache_size:
                        self.processed_messages.pop()

                    # Call registered handlers
                    handlers = self.message_handlers.get(message.message_type, [])
                    for handler in handlers:
                        try:
                            await handler(message, peer_id)
                        except Exception as e:
                            logger.error(f"Error in message handler: {str(e)}")

                    self.metrics["messages_processed"] += 1

        except asyncio.IncompleteReadError:
            # Connection closed
            await self._close_peer_connection(peer_id)
        except Exception as e:
            logger.error(f"Error handling messages from peer {peer_id}: {str(e)}")
            await self._close_peer_connection(peer_id)

    async def _send_message_to_peer(self, peer_id: str, message: Message) -> None:
        """Send a message to a specific peer."""
        if peer_id not in self.peers:
            raise ValueError(f"Unknown peer: {peer_id}")

        # Serialize message
        message_dict = {
            "message_type": message.message_type,
            "payload": message.payload,
            "sender": message.sender,
            "timestamp": message.timestamp.isoformat()
        }
        message_data = json.dumps(message_dict).encode()

        # Send message length followed by message data
        message_length = len(message_data)
        length_bytes = message_length.to_bytes(4, 'big')

        writer = self.peers[peer_id].writer
        writer.write(length_bytes + message_data)
        await writer.drain()

    async def _perform_handshake(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        peer_info: PeerInfo
    ) -> bool:
        """Perform protocol handshake with a peer."""
        try:
            # Send handshake
            handshake = {
                "node_id": self.node_id,
                "protocols": list(self.supported_protocols)
            }
            writer.write(json.dumps(handshake).encode())
            await writer.drain()

            # Receive response
            response_data = await reader.read(1024)
            response = json.loads(response_data.decode())

            # Verify peer ID
            if response.get("node_id") != peer_info.node_id:
                return False

            # Check protocol compatibility
            peer_protocols = set(response.get("protocols", []))
            if not peer_protocols & self.supported_protocols:
                return False

            peer_info.supported_protocols = peer_protocols
            peer_info.is_active = True
            return True

        except Exception as e:
            logger.error(f"Handshake failed: {str(e)}")
            return False

    async def _close_peer_connection(self, peer_id: str) -> None:
        """Close connection with a peer."""
        if peer_id in self.peers:
            peer_info = self.peers[peer_id]
            if hasattr(peer_info, 'writer'):
                peer_info.writer.close()
                await peer_info.writer.wait_closed()
            del self.peers[peer_id]
            self.metrics["active_connections"] -= 1

    async def _maintenance_loop(self) -> None:
        """Periodic maintenance tasks."""
        while self.is_running:
            try:
                # Remove inactive peers
                now = datetime.now()
                for peer_id, peer_info in list(self.peers.items()):
                    if (now - peer_info.last_seen).total_seconds() > 300:  # 5 minutes
                        await self._close_peer_connection(peer_id)

                # Clear old processed messages
                if len(self.processed_messages) > self.message_cache_size:
                    self.processed_messages = set(
                        list(self.processed_messages)[-self.message_cache_size:]
                    )

            except Exception as e:
                logger.error(f"Error in maintenance loop: {str(e)}")

            await asyncio.sleep(60)
            # Continuing from the previous NetworkManager class...

    async def _metrics_loop(self) -> None:
        """Background task to update network metrics."""
        while self.is_running:
            try:
                # Update peer metrics
                active_peers = sum(1 for p in self.peers.values() if p.is_active)
                total_reputation = sum(p.reputation for p in self.peers.values())
                
                self.metrics.update({
                    "active_peers": active_peers,
                    "total_peers": len(self.peers),
                    "average_reputation": total_reputation / max(1, len(self.peers)),
                    "blacklisted_peers": len(self.blacklisted_peers),
                    "pending_connections": len(self.pending_connections)
                })

                # Log metrics
                if active_peers > 0:
                    logger.info(f"Network metrics: {self.metrics}")

            except Exception as e:
                logger.error(f"Error updating metrics: {str(e)}")

            await asyncio.sleep(30)  # Update every 30 seconds

    def get_peer_info(self, peer_id: str) -> Optional[PeerInfo]:
        """
        Get information about a specific peer.
        
        Args:
            peer_id: ID of the peer
            
        Returns:
            Optional[PeerInfo]: Peer information if found
        """
        return self.peers.get(peer_id)

    def get_active_peers(self) -> List[PeerInfo]:
        """
        Get list of currently active peers.
        
        Returns:
            List[PeerInfo]: List of active peer information
        """
        return [peer for peer in self.peers.values() if peer.is_active]

    def blacklist_peer(self, peer_id: str, reason: str) -> None:
        """
        Add a peer to the blacklist.
        
        Args:
            peer_id: ID of the peer to blacklist
            reason: Reason for blacklisting
        """
        self.blacklisted_peers.add(peer_id)
        if peer_id in self.peers:
            asyncio.create_task(self._close_peer_connection(peer_id))
        logger.warning(f"Blacklisted peer {peer_id}: {reason}")

    def get_network_info(self) -> Dict[str, Any]:
        """
        Get comprehensive information about the network state.
        
        Returns:
            Dict[str, Any]: Network state information
        """
        return {
            "node_id": self.node_id,
            "address": f"{self.host}:{self.port}",
            "is_running": self.is_running,
            "metrics": self.metrics.copy(),
            "peers": {
                peer_id: {
                    "address": f"{peer.address}:{peer.port}",
                    "last_seen": peer.last_seen.isoformat(),
                    "reputation": peer.reputation,
                    "is_active": peer.is_active,
                    "protocols": list(peer.supported_protocols)
                }
                for peer_id, peer in self.peers.items()
            },
            "protocols": list(self.supported_protocols),
            "min_protocol": self.min_protocol_version
        }

    async def ping_peer(self, peer_id: str) -> bool:
        """
        Send a ping message to check peer connectivity.
        
        Args:
            peer_id: ID of the peer to ping
            
        Returns:
            bool: True if ping successful, False otherwise
        """
        try:
            ping_payload = {
                "timestamp": datetime.now().isoformat()
            }
            return await self.send_message(peer_id, "ping", ping_payload)
        except Exception as e:
            logger.error(f"Error pinging peer {peer_id}: {str(e)}")
            return False

    def update_peer_reputation(self, peer_id: str, change: float) -> None:
        """
        Update a peer's reputation score.
        
        Args:
            peer_id: ID of the peer
            change: Amount to change reputation by (positive or negative)
        """
        if peer_id in self.peers:
            peer = self.peers[peer_id]
            peer.reputation = max(0.0, min(1.0, peer.reputation + change))
            
            # Blacklist peers with consistently bad reputation
            if peer.reputation <= 0.1:
                self.blacklist_peer(peer_id, "Low reputation score")