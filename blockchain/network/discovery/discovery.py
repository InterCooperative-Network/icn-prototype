# ================================================================
# File: blockchain/network/discovery/discovery.py
# Description: Peer discovery implementation for the ICN network.
# Handles automatic peer discovery, peer list management, and 
# connection bootstrapping for the InterCooperative Network.
# ================================================================

import asyncio
import logging
import json
import random
from typing import Dict, Set, List, Optional, Any
from datetime import datetime, timedelta
from dataclasses import dataclass, field
import socket
import struct
import ipaddress
from hashlib import sha256

logger = logging.getLogger(__name__)

@dataclass
class PeerRecord:
    """Information about a discovered peer."""
    node_id: str
    address: str
    port: int
    last_seen: datetime = field(default_factory=datetime.now)
    last_successful_connection: Optional[datetime] = None
    connection_attempts: int = 0
    is_bootstrap_node: bool = False
    cooperative_id: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def peer_score(self) -> float:
        """Calculate peer score based on reliability and history."""
        if self.is_bootstrap_node:
            return 1.0
            
        # Base score starts at 0.5
        score = 0.5
        
        # Increase score for successful connections
        if self.last_successful_connection:
            hours_since_connection = (datetime.now() - self.last_successful_connection).total_seconds() / 3600
            if hours_since_connection < 24:
                score += 0.3
            elif hours_since_connection < 72:
                score += 0.1
                
        # Decrease score for failed connection attempts
        score -= min(0.4, self.connection_attempts * 0.1)
        
        return max(0.0, min(1.0, score))

class PeerDiscovery:
    """
    Manages peer discovery and connection management.
    
    Features:
    - Automatic peer discovery using multiple methods
    - Bootstrap node support
    - Peer scoring and prioritization
    - Connection attempt management
    - Cooperative-aware peer selection
    """
    
    def __init__(
        self,
        node_id: str,
        host: str,
        port: int,
        bootstrap_nodes: List[Dict[str, Any]] = None,
        max_peers: int = 50,
        discovery_interval: int = 300
    ):
        """
        Initialize the peer discovery system.
        
        Args:
            node_id: Unique identifier for this node
            host: Host address to bind to
            port: Port to listen on
            bootstrap_nodes: List of known bootstrap nodes
            max_peers: Maximum number of peers to maintain
            discovery_interval: Seconds between discovery attempts
        """
        self.node_id = node_id
        self.host = host
        self.port = port
        self.max_peers = max_peers
        self.discovery_interval = discovery_interval
        
        # Peer management
        self.known_peers: Dict[str, PeerRecord] = {}
        self.connected_peers: Set[str] = set()
        self.blacklisted_peers: Set[str] = set()
        self.pending_connections: Set[str] = set()
        
        # Bootstrap configuration
        self.bootstrap_nodes = bootstrap_nodes or []
        for node in self.bootstrap_nodes:
            self._add_bootstrap_node(node)
            
        # Discovery state
        self.is_running = False
        self.last_discovery = datetime.now() - timedelta(seconds=discovery_interval)
        
        # UDP discovery
        self.discovery_socket: Optional[socket.socket] = None
        self.discovery_port = port + 1
        
        # Metrics
        self.metrics = {
            "total_discoveries": 0,
            "successful_discoveries": 0,
            "failed_discoveries": 0,
            "total_connection_attempts": 0,
            "successful_connections": 0,
            "failed_connections": 0
        }

    async def start(self) -> None:
        """Start the peer discovery service."""
        if self.is_running:
            return
            
        try:
            # Initialize UDP discovery socket
            self.discovery_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.discovery_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.discovery_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            self.discovery_socket.bind(('', self.discovery_port))
            
            self.is_running = True
            
            # Start background tasks
            asyncio.create_task(self._discovery_loop())
            asyncio.create_task(self._maintenance_loop())
            asyncio.create_task(self._listen_for_broadcasts())
            
            logger.info(f"Peer discovery started on {self.host}:{self.port}")
            
        except Exception as e:
            logger.error(f"Failed to start peer discovery: {str(e)}")
            raise

    async def stop(self) -> None:
        """Stop the peer discovery service."""
        if not self.is_running:
            return
            
        try:
            self.is_running = False
            
            if self.discovery_socket:
                self.discovery_socket.close()
                
            logger.info("Peer discovery stopped")
            
        except Exception as e:
            logger.error(f"Error stopping peer discovery: {str(e)}")

    async def get_peers(self, count: int = 10, cooperative_id: Optional[str] = None) -> List[PeerRecord]:
        """
        Get a list of recommended peers to connect to.
        
        Args:
            count: Number of peers to return
            cooperative_id: Optional cooperative ID to prioritize
            
        Returns:
            List[PeerRecord]: List of recommended peers
        """
        try:
            # Filter and sort peers
            available_peers = [
                peer for peer_id, peer in self.known_peers.items()
                if peer_id not in self.connected_peers
                and peer_id not in self.blacklisted_peers
                and peer_id not in self.pending_connections
                and (not cooperative_id or peer.cooperative_id == cooperative_id)
            ]
            
            # Sort by score
            available_peers.sort(key=lambda p: p.peer_score, reverse=True)
            
            return available_peers[:count]
            
        except Exception as e:
            logger.error(f"Error getting peers: {str(e)}")
            return []

    def add_peer(self, node_id: str, address: str, port: int, **kwargs) -> bool:
        """
        Add a new peer to the known peers list.
        
        Args:
            node_id: Unique identifier of the peer
            address: Peer's network address
            port: Peer's port number
            **kwargs: Additional peer metadata
            
        Returns:
            bool: True if peer was added, False otherwise
        """
        try:
            if node_id in self.blacklisted_peers:
                return False
                
            if node_id not in self.known_peers:
                self.known_peers[node_id] = PeerRecord(
                    node_id=node_id,
                    address=address,
                    port=port,
                    cooperative_id=kwargs.get('cooperative_id'),
                    metadata=kwargs
                )
            else:
                # Update existing peer record
                peer = self.known_peers[node_id]
                peer.address = address
                peer.port = port
                peer.last_seen = datetime.now()
                peer.metadata.update(kwargs)
                
            return True
            
        except Exception as e:
            logger.error(f"Error adding peer: {str(e)}")
            return False

    def blacklist_peer(self, node_id: str, reason: str) -> None:
        """
        Add a peer to the blacklist.
        
        Args:
            node_id: ID of the peer to blacklist
            reason: Reason for blacklisting
        """
        self.blacklisted_peers.add(node_id)
        if node_id in self.known_peers:
            del self.known_peers[node_id]
        logger.warning(f"Blacklisted peer {node_id}: {reason}")

    async def _discovery_loop(self) -> None:
        """Background task for periodic peer discovery."""
        while self.is_running:
            try:
                # Check if discovery is needed
                now = datetime.now()
                if (now - self.last_discovery).total_seconds() < self.discovery_interval:
                    await asyncio.sleep(5)
                    continue
                    
                self.last_discovery = now
                self.metrics["total_discoveries"] += 1
                
                # Perform discovery methods
                discovery_tasks = [
                    self._broadcast_discovery(),
                    self._query_bootstrap_nodes(),
                    self._query_known_peers()
                ]
                
                results = await asyncio.gather(*discovery_tasks, return_exceptions=True)
                
                # Update metrics
                success = sum(1 for r in results if r and not isinstance(r, Exception))
                self.metrics["successful_discoveries"] += success
                self.metrics["failed_discoveries"] += (len(results) - success)
                
                # Log results
                logger.info(f"Discovery round completed: {success} successful methods")
                
            except Exception as e:
                logger.error(f"Error in discovery loop: {str(e)}")
                
            await asyncio.sleep(5)

    async def _maintenance_loop(self) -> None:
        """Background task for peer list maintenance."""
        while self.is_running:
            try:
                now = datetime.now()
                
                # Remove old peers
                for peer_id in list(self.known_peers.keys()):
                    peer = self.known_peers[peer_id]
                    if not peer.is_bootstrap_node:
                        if (now - peer.last_seen).total_seconds() > 3600:  # 1 hour
                            del self.known_peers[peer_id]
                
                # Clear old pending connections
                self.pending_connections = {
                    peer_id for peer_id in self.pending_connections
                    if peer_id in self.known_peers
                }
                
            except Exception as e:
                logger.error(f"Error in maintenance loop: {str(e)}")
                
            await asyncio.sleep(60)

    async def _broadcast_discovery(self) -> bool:
        """Broadcast discovery message on local network."""
        try:
            # Prepare discovery message
            message = {
                "node_id": self.node_id,
                "address": self.host,
                "port": self.port,
                "timestamp": datetime.now().isoformat()
            }
            
            # Broadcast message
            data = json.dumps(message).encode()
            self.discovery_socket.sendto(data, ('<broadcast>', self.discovery_port))
            
            return True
            
        except Exception as e:
            logger.error(f"Error broadcasting discovery: {str(e)}")
            return False

    async def _listen_for_broadcasts(self) -> None:
        """Listen for discovery broadcasts from other nodes."""
        while self.is_running:
            try:
                # Receive broadcast
                data, addr = await asyncio.get_event_loop().sock_recvfrom(self.discovery_socket, 1024)
                message = json.loads(data.decode())
                
                # Validate message
                required_fields = {"node_id", "address", "port", "timestamp"}
                if not all(field in message for field in required_fields):
                    continue
                    
                # Add peer
                node_id = message["node_id"]
                if node_id != self.node_id:
                    self.add_peer(
                        node_id=node_id,
                        address=message["address"],
                        port=message["port"]
                    )
                    
            except Exception as e:
                logger.error(f"Error processing broadcast: {str(e)}")
                
            await asyncio.sleep(0.1)

    async def _query_bootstrap_nodes(self) -> bool:
        """Query bootstrap nodes for peer information."""
        try:
            # Query each bootstrap node
            for node in self.bootstrap_nodes:
                try:
                    # Create TCP connection
                    reader, writer = await asyncio.open_connection(
                        node["address"],
                        node["port"]
                    )
                    
                    # Send peer request
                    request = {
                        "message_type": "peer_request",
                        "node_id": self.node_id,
                        "timestamp": datetime.now().isoformat()
                    }
                    writer.write(json.dumps(request).encode())
                    await writer.drain()
                    
                    # Read response
                    data = await reader.read(8192)
                    response = json.loads(data.decode())
                    
                    # Process peers
                    for peer in response.get("peers", []):
                        self.add_peer(**peer)
                        
                    writer.close()
                    await writer.wait_closed()
                    
                except Exception as e:
                    logger.error(f"Error querying bootstrap node: {str(e)}")
                    
            return True
            
        except Exception as e:
            logger.error(f"Error in bootstrap node query: {str(e)}")
            return False

    async def _query_known_peers(self) -> bool:
        """Query known peers for their peer lists."""
        try:
            # Select random subset of known peers
            sample_size = min(5, len(self.known_peers))
            if sample_size == 0:
                return False
                
            selected_peers = random.sample(list(self.known_peers.values()), sample_size)
            
            # Query each selected peer
            for peer in selected_peers:
                try:
                    # Create TCP connection
                    reader, writer = await asyncio.open_connection(
                        peer.address,
                        peer.port
                    )
                    
                    # Send peer request
                    request = {
                        "message_type": "peer_request",
                        "node_id": self.node_id,
                        "timestamp": datetime.now().isoformat()
                    }
                    writer.write(json.dumps(request).encode())
                    await writer.drain()
                    
                    # Read response
                    data = await reader.read(8192)
                    response = json.loads(data.decode())
                    
                    # Process peers
                    for peer_data in response.get("peers", []):
                        self.add_peer(**peer_data)
                        
                    writer.close()
                    await writer.wait_closed()
                    
                except Exception as e:
                    logger.error(f"Error querying peer {peer.node_id}: {str(e)}")
                    peer.connection_attempts += 1
                    
            return True
            
        except Exception as e:
            logger.error(f"Error in known peer query: {str(e)}")
            return False

    def _add_bootstrap_node(self, node: Dict[str, Any]) -> None:
        """Add a bootstrap node to known peers."""
        self.add_peer(
            node_id=node["node_id"],
            address=node["address"],
            port=node["port"],
            is_bootstrap_node=True
        )