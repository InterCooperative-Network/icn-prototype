# ================================================================
# File: blockchain/network/config.py
# Description: Network configuration settings for the ICN network.
# Defines network parameters, protocols, and connection settings
# for the InterCooperative Network.
# ================================================================

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set
import os
import json
import logging

logger = logging.getLogger(__name__)

@dataclass
class NetworkConfig:
    """Network configuration parameters."""
    
    # Node identification
    node_id: str
    cooperative_id: Optional[str] = None
    
    # Network addresses
    host: str = "0.0.0.0"
    port: int = 30303
    discovery_port: int = 30304
    
    # Connection limits
    max_peers: int = 50
    min_peers: int = 10
    max_connections_per_ip: int = 5
    connection_timeout: int = 30
    
    # Protocol settings
    supported_protocols: Set[str] = field(default_factory=lambda: {"icn/1.0", "icn/1.1"})
    min_protocol_version: str = "icn/1.0"
    max_message_size: int = 1024 * 1024  # 1MB
    
    # Discovery settings
    discovery_interval: int = 300  # 5 minutes
    bootstrap_nodes: List[Dict[str, Any]] = field(default_factory=list)
    enable_auto_discovery: bool = True
    discovery_methods: Set[str] = field(default_factory=lambda: {"broadcast", "bootstrap", "peer"})
    
    # Resource limits
    max_inbound_connections: int = 40
    max_outbound_connections: int = 20
    max_pending_connections: int = 10
    buffer_size: int = 1024 * 16  # 16KB
    
    # Performance settings
    read_timeout: float = 30.0
    write_timeout: float = 30.0
    keepalive_interval: int = 60
    ping_interval: int = 120
    
    # Security settings
    enable_encryption: bool = True
    require_node_id_verification: bool = True
    blacklist_threshold: int = 3
    reputation_threshold: float = 0.3
    
    # Cooperative settings
    prioritize_cooperative_peers: bool = True
    cooperative_connection_bonus: float = 0.2
    min_cooperative_peers: int = 5
    
    # Metrics and monitoring
    metrics_interval: int = 60
    log_metrics: bool = True
    enable_peer_scoring: bool = True
    
    @classmethod
    def load_from_file(cls, filepath: str) -> 'NetworkConfig':
        """
        Load configuration from a JSON file.
        
        Args:
            filepath: Path to configuration file
            
        Returns:
            NetworkConfig: Loaded configuration
        """
        try:
            if not os.path.exists(filepath):
                raise FileNotFoundError(f"Configuration file not found: {filepath}")
                
            with open(filepath, 'r') as f:
                config_data = json.load(f)
                
            # Validate required fields
            if "node_id" not in config_data:
                raise ValueError("Configuration must include node_id")
                
            return cls(**config_data)
            
        except Exception as e:
            logger.error(f"Error loading network configuration: {str(e)}")
            raise
    
    def save_to_file(self, filepath: str) -> None:
        """
        Save configuration to a JSON file.
        
        Args:
            filepath: Path to save configuration to
        """
        try:
            # Convert config to dictionary
            config_dict = {
                "node_id": self.node_id,
                "cooperative_id": self.cooperative_id,
                "host": self.host,
                "port": self.port,
                "discovery_port": self.discovery_port,
                "max_peers": self.max_peers,
                "min_peers": self.min_peers,
                "max_connections_per_ip": self.max_connections_per_ip,
                "connection_timeout": self.connection_timeout,
                "supported_protocols": list(self.supported_protocols),
                "min_protocol_version": self.min_protocol_version,
                "max_message_size": self.max_message_size,
                "discovery_interval": self.discovery_interval,
                "bootstrap_nodes": self.bootstrap_nodes,
                "enable_auto_discovery": self.enable_auto_discovery,
                "discovery_methods": list(self.discovery_methods),
                "max_inbound_connections": self.max_inbound_connections,
                "max_outbound_connections": self.max_outbound_connections,
                "max_pending_connections": self.max_pending_connections,
                "buffer_size": self.buffer_size,
                "read_timeout": self.read_timeout,
                "write_timeout": self.write_timeout,
                "keepalive_interval": self.keepalive_interval,
                "ping_interval": self.ping_interval,
                "enable_encryption": self.enable_encryption,
                "require_node_id_verification": self.require_node_id_verification,
                "blacklist_threshold": self.blacklist_threshold,
                "reputation_threshold": self.reputation_threshold,
                "prioritize_cooperative_peers": self.prioritize_cooperative_peers,
                "cooperative_connection_bonus": self.cooperative_connection_bonus,
                "min_cooperative_peers": self.min_cooperative_peers,
                "metrics_interval": self.metrics_interval,
                "log_metrics": self.log_metrics,
                "enable_peer_scoring": self.enable_peer_scoring
            }
            
            # Save to file
            with open(filepath, 'w') as f:
                json.dump(config_dict, f, indent=4)
                
            logger.info(f"Network configuration saved to {filepath}")
            
        except Exception as e:
            logger.error(f"Error saving network configuration: {str(e)}")
            raise
    
    def validate(self) -> bool:
        """
        Validate configuration settings.
        
        Returns:
            bool: True if configuration is valid
        """
        try:
            # Validate node identification
            if not self.node_id or len(self.node_id) < 8:
                logger.error("Invalid node_id")
                return False
            
            # Validate network settings
            if self.port < 1024 or self.port > 65535:
                logger.error("Invalid port number")
                return False
                
            if self.discovery_port < 1024 or self.discovery_port > 65535:
                logger.error("Invalid discovery port")
                return False
                
            # Validate connection limits
            if self.max_peers < self.min_peers:
                logger.error("max_peers cannot be less than min_peers")
                return False
                
            if self.max_inbound_connections + self.max_outbound_connections > self.max_peers:
                logger.error("Total connections cannot exceed max_peers")
                return False
                
            # Validate timeouts
            if self.read_timeout <= 0 or self.write_timeout <= 0:
                logger.error("Timeouts must be positive")
                return False
                
            # Validate intervals
            if (self.keepalive_interval <= 0 or self.ping_interval <= 0 or 
                self.metrics_interval <= 0 or self.discovery_interval <= 0):
                logger.error("Intervals must be positive")
                return False
                
            # Validate thresholds
            if self.reputation_threshold < 0 or self.reputation_threshold > 1:
                logger.error("Reputation threshold must be between 0 and 1")
                return False
                
            # Validate cooperative settings
            if self.min_cooperative_peers > self.max_peers:
                logger.error("min_cooperative_peers cannot exceed max_peers")
                return False
                
            if self.cooperative_connection_bonus < 0 or self.cooperative_connection_bonus > 1:
                logger.error("cooperative_connection_bonus must be between 0 and 1")
                return False
                
            # Validate protocol settings
            if not self.supported_protocols:
                logger.error("Must support at least one protocol version")
                return False
                
            if self.min_protocol_version not in self.supported_protocols:
                logger.error("min_protocol_version must be in supported_protocols")
                return False
                
            return True
            
        except Exception as e:
            logger.error(f"Error validating configuration: {str(e)}")
            return False
    
    def get_discovery_config(self) -> Dict[str, Any]:
        """
        Get discovery-specific configuration.
        
        Returns:
            Dict[str, Any]: Discovery configuration parameters
        """
        return {
            "port": self.discovery_port,
            "interval": self.discovery_interval,
            "bootstrap_nodes": self.bootstrap_nodes,
            "enable_auto": self.enable_auto_discovery,
            "methods": list(self.discovery_methods)
        }
    
    def get_connection_limits(self) -> Dict[str, int]:
        """
        Get connection limit configuration.
        
        Returns:
            Dict[str, int]: Connection limits
        """
        return {
            "max_peers": self.max_peers,
            "min_peers": self.min_peers,
            "max_inbound": self.max_inbound_connections,
            "max_outbound": self.max_outbound_connections,
            "max_pending": self.max_pending_connections,
            "max_per_ip": self.max_connections_per_ip
        }

# Default configuration
DEFAULT_CONFIG = NetworkConfig(
    node_id="",  # Must be set by application
    host="0.0.0.0",
    port=30303,
    discovery_port=30304,
    bootstrap_nodes=[
        {
            "node_id": "bootstrap-1",
            "address": "bootstrap1.icn.network",
            "port": 30303
        },
        {
            "node_id": "bootstrap-2",
            "address": "bootstrap2.icn.network",
            "port": 30303
        }
    ]
)