# ================================================================
# File: blockchain/network/protocol/dispatcher.py
# Description: Message dispatcher for ICN network.
# Handles routing and processing of network messages, providing a
# centralized point for message handling and distribution.
# ================================================================

import asyncio
import logging
from typing import Dict, Set, List, Optional, Callable, Any, Union
from dataclasses import dataclass, field
from datetime import datetime
import json
from enum import Enum, auto
import hashlib
import traceback

from ..config import NetworkConfig
from ..transport.transport import NetworkTransport, MessageFrame

logger = logging.getLogger(__name__)

class MessagePriority(Enum):
    """Message priority levels."""
    HIGH = auto()    # Critical messages (consensus, block propagation)
    MEDIUM = auto()  # Important but not critical (transaction propagation)
    LOW = auto()     # Regular messages (peer discovery, metrics)

@dataclass
class MessageHandler:
    """Registered message handler information."""
    callback: Callable
    priority: MessagePriority
    requires_response: bool = False
    timeout: float = 30.0
    cooperative_only: bool = False
    max_size: Optional[int] = None
    version_requirements: Optional[Set[str]] = None

@dataclass
class PendingResponse:
    """Tracks pending message responses."""
    message_id: str
    sender: str
    timestamp: datetime
    timeout: float
    future: asyncio.Future
    message_type: str

@dataclass
class MessageRoute:
    """Message routing information."""
    message_type: str
    target_shards: Set[int]
    target_cooperatives: Set[str]
    exclude_peers: Set[str]
    broadcast: bool = False
    local_only: bool = False

class MessageDispatcher:
    """
    Central message handling and routing system.
    
    Features:
    - Priority-based message processing
    - Message routing and filtering
    - Request-response handling
    - Cross-shard message propagation
    - Cooperative-aware message distribution
    - Message validation and rate limiting
    """

    def __init__(self, config: NetworkConfig, transport: NetworkTransport):
        """
        Initialize the message dispatcher.
        
        Args:
            config: Network configuration
            transport: Network transport layer
        """
        self.config = config
        self.transport = transport
        
        # Message handling
        self.handlers: Dict[str, MessageHandler] = {}
        self.pending_responses: Dict[str, PendingResponse] = {}
        
        # Message queues (priority-based)
        self.high_priority_queue: asyncio.Queue = asyncio.Queue()
        self.medium_priority_queue: asyncio.Queue = asyncio.Queue()
        self.low_priority_queue: asyncio.Queue = asyncio.Queue()
        
        # Rate limiting
        self.message_counts: Dict[str, int] = {}  # peer_id -> count
        self.message_timestamps: Dict[str, List[datetime]] = {}  # peer_id -> timestamps
        self.rate_limit_window = 60  # seconds
        self.rate_limit_max = 1000  # messages per window
        
        # State
        self.is_running = False
        self.processing_tasks: Set[asyncio.Task] = set()
        
        # Metrics
        self.metrics = {
            "messages_processed": 0,
            "messages_routed": 0,
            "messages_dropped": 0,
            "responses_received": 0,
            "responses_timed_out": 0,
            "rate_limit_violations": 0
        }

    async def start(self) -> None:
        """Start the message dispatcher."""
        if self.is_running:
            return
            
        try:
            self.is_running = True
            
            # Start worker tasks for each priority queue
            self.processing_tasks.add(
                asyncio.create_task(self._process_queue(
                    self.high_priority_queue,
                    "high"
                ))
            )
            self.processing_tasks.add(
                asyncio.create_task(self._process_queue(
                    self.medium_priority_queue,
                    "medium"
                ))
            )
            self.processing_tasks.add(
                asyncio.create_task(self._process_queue(
                    self.low_priority_queue,
                    "low"
                ))
            )
            
            # Start maintenance task
            self.processing_tasks.add(
                asyncio.create_task(self._maintenance_loop())
            )
            
            logger.info("Message dispatcher started")
            
        except Exception as e:
            logger.error(f"Error starting message dispatcher: {str(e)}")
            raise

    async def stop(self) -> None:
        """Stop the message dispatcher."""
        if not self.is_running:
            return
            
        try:
            self.is_running = False
            
            # Cancel all pending responses
            for pending in self.pending_responses.values():
                if not pending.future.done():
                    pending.future.cancel()
            
            # Cancel all processing tasks
            for task in self.processing_tasks:
                task.cancel()
                
            await asyncio.gather(*self.processing_tasks, return_exceptions=True)
            self.processing_tasks.clear()
            
            logger.info("Message dispatcher stopped")
            
        except Exception as e:
            logger.error(f"Error stopping message dispatcher: {str(e)}")

    def register_handler(
        self,
        message_type: str,
        callback: Callable,
        priority: MessagePriority = MessagePriority.MEDIUM,
        requires_response: bool = False,
        timeout: float = 30.0,
        cooperative_only: bool = False,
        max_size: Optional[int] = None,
        version_requirements: Optional[Set[str]] = None
    ) -> None:
        """
        Register a message handler.
        
        Args:
            message_type: Type of message to handle
            callback: Handler function
            priority: Message processing priority
            requires_response: Whether handler expects a response
            timeout: Response timeout in seconds
            cooperative_only: Whether to only accept messages from cooperative peers
            max_size: Maximum message size in bytes
            version_requirements: Required protocol versions
        """
        self.handlers[message_type] = MessageHandler(
            callback=callback,
            priority=priority,
            requires_response=requires_response,
            timeout=timeout,
            cooperative_only=cooperative_only,
            max_size=max_size,
            version_requirements=version_requirements
        )

    async def dispatch_message(
        self,
        message_type: str,
        payload: Dict[str, Any],
        routing: Optional[MessageRoute] = None,
        wait_response: bool = False
    ) -> Optional[Dict[str, Any]]:
        """
        Dispatch a message to the network.
        
        Args:
            message_type: Type of message to send
            payload: Message payload
            routing: Optional routing information
            wait_response: Whether to wait for response
            
        Returns:
            Optional[Dict[str, Any]]: Response payload if wait_response is True
        """
        try:
            message_id = self._generate_message_id(message_type, payload)
            
            if routing and routing.local_only:
                # Handle message locally
                return await self._handle_local_message(
                    message_type,
                    message_id,
                    payload
                )
            
            # Prepare message for sending
            message = {
                "message_id": message_id,
                "message_type": message_type,
                "payload": payload,
                "timestamp": datetime.now().isoformat(),
                "sender": self.config.node_id
            }
            
            if wait_response:
                # Create future for response
                future = asyncio.Future()
                self.pending_responses[message_id] = PendingResponse(
                    message_id=message_id,
                    sender=self.config.node_id,
                    timestamp=datetime.now(),
                    timeout=30.0,
                    future=future,
                    message_type=message_type
                )
                
            # Route message
            success = await self._route_message(message, routing)
            if not success:
                if wait_response:
                    self.pending_responses[message_id].future.cancel()
                    del self.pending_responses[message_id]
                return None
            
            if wait_response:
                try:
                    # Wait for response
                    response = await asyncio.wait_for(
                        self.pending_responses[message_id].future,
                        timeout=30.0
                    )
                    return response
                except asyncio.TimeoutError:
                    logger.warning(f"Response timeout for message {message_id}")
                    self.metrics["responses_timed_out"] += 1
                    return None
                finally:
                    if message_id in self.pending_responses:
                        del self.pending_responses[message_id]
            
            return None
            
        except Exception as e:
            logger.error(f"Error dispatching message: {str(e)}")
            return None

    async def handle_message(
        self,
        peer_id: str,
        message_data: Dict[str, Any]
    ) -> None:
        """
        Handle received message from transport layer.
        
        Args:
            peer_id: ID of sending peer
            message_data: Received message data
        """
        try:
            # Extract message information
            message_type = message_data.get("message_type")
            message_id = message_data.get("message_id")
            payload = message_data.get("payload")
            
            if not all([message_type, message_id, payload]):
                logger.error("Invalid message format")
                return
                
            # Check rate limits
            if not self._check_rate_limit(peer_id):
                logger.warning(f"Rate limit exceeded for peer {peer_id}")
                self.metrics["rate_limit_violations"] += 1
                return
                
            # Get handler
            handler = self.handlers.get(message_type)
            if not handler:
                logger.warning(f"No handler for message type: {message_type}")
                return
                
            # Validate message
            if not await self._validate_message(
                message_data,
                handler,
                peer_id
            ):
                return
                
            # Queue message for processing
            await self._queue_message(
                handler.priority,
                peer_id,
                message_type,
                message_id,
                payload
            )
            
        except Exception as e:
            logger.error(f"Error handling message: {str(e)}")

    async def _handle_local_message(
        self,
        message_type: str,
        message_id: str,
        payload: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Handle message locally without network transmission."""
        try:
            handler = self.handlers.get(message_type)
            if not handler:
                logger.warning(f"No handler for local message type: {message_type}")
                return None
                
            # Execute handler
            response = await handler.callback(self.config.node_id, payload)
            return response
            
        except Exception as e:
            logger.error(f"Error handling local message: {str(e)}")
            return None

    async def _route_message(
        self,
        message: Dict[str, Any],
        routing: Optional[MessageRoute]
    ) -> bool:
        """Route message according to routing information."""
        try:
            if not routing:
                # Broadcast to all peers
                return await self.transport.broadcast_message(
                    message["message_type"],
                    message
                )
            
            success = True
            sent_to_peers = set()
            
            # Send to target shards
            if routing.target_shards:
                # TODO: Implement shard-based routing
                pass
            
            # Send to target cooperatives
            if routing.target_cooperatives:
                # TODO: Implement cooperative-based routing
                pass
            
            # Broadcast if specified
            if routing.broadcast:
                for peer_id in self.transport.get_peer_ids():
                    if (peer_id not in sent_to_peers and 
                        peer_id not in routing.exclude_peers):
                        success &= await self.transport.send_message(
                            peer_id,
                            message["message_type"],
                            message
                        )
                        sent_to_peers.add(peer_id)
            
            self.metrics["messages_routed"] += len(sent_to_peers)
            return success
            
        except Exception as e:
            logger.error(f"Error routing message: {str(e)}")
            return False

    async def _process_queue(
        self,
        queue: asyncio.Queue,
        priority: str
    ) -> None:
        """Process messages from a priority queue."""
        while self.is_running:
            try:
                # Get message from queue
                peer_id, message_type, message_id, payload = await queue.get()
                
                # Get handler
                handler = self.handlers[message_type]
                
                try:
                    # Execute handler
                    response = await handler.callback(peer_id, payload)
                    
                    # Handle response if needed
                    if message_id in self.pending_responses:
                        pending = self.pending_responses[message_id]
                        if not pending.future.done():
                            pending.future.set_result(response)
                            self.metrics["responses_received"] += 1
                    
                    self.metrics["messages_processed"] += 1
                    
                except Exception as e:
                    logger.error(
                        f"Error processing {priority} priority message: {str(e)}\n"
                        f"{''.join(traceback.format_exc())}"
                    )
                    self.metrics["messages_dropped"] += 1
                
                finally:
                    queue.task_done()
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in {priority} priority queue processing: {str(e)}")
                await asyncio.sleep(1)

    async def _maintenance_loop(self) -> None:
        """Periodic maintenance tasks."""
        while self.is_running:
            try:
                current_time = datetime.now()
                
                # Clean up pending responses
                for message_id in list(self.pending_responses.keys()):
                    pending = self.pending_responses[message_id]
                    if (current_time - pending.timestamp).total_seconds() > pending.timeout:
                        if not pending.future.done():
                            pending.future.cancel()
                        del self.pending_responses[message_id]
                        self.metrics["responses_timed_out"] += 1
                
                # Clean up rate limiting data
                window_start = current_time.timestamp() - self.rate_limit_window
                for peer_id in list(self.message_timestamps.keys()):
                    self.message_timestamps[peer_id] = [
                        ts for ts in self.message_timestamps[peer_id]
                        if ts.timestamp() > window_start
                    ]
                    self.message_counts[peer_id] = len(self.message_timestamps[peer_id])
                
            except Exception as e:
                logger.error(f"Error in maintenance loop: {str(e)}")
                
            await asyncio.sleep(1)

    def _check_rate_limit(self, peer_id: str) -> bool:
        """Check if peer has exceeded rate limit."""
        current_time = datetime.now()
        
        if peer_id not in self.message_timestamps:
            self.message_timestamps[peer_id] = []
            self.message_counts[peer_id] = 0
            
        # Add new timestamp
        self.message_timestamps[peer_id].append(current_time)
        
       # Update count
        window_start = current_time.timestamp() - self.rate_limit_window
        self.message_counts[peer_id] = sum(
            1 for ts in self.message_timestamps[peer_id]
            if ts.timestamp() > window_start
        )
        
        # Check against limit
        return self.message_counts[peer_id] <= self.rate_limit_max

    async def _validate_message(
        self,
        message_data: Dict[str, Any],
        handler: MessageHandler,
        peer_id: str
    ) -> bool:
        """
        Validate incoming message.
        
        Args:
            message_data: Message to validate
            handler: Handler for this message type
            peer_id: ID of sending peer
            
        Returns:
            bool: True if message is valid
        """
        try:
            # Check cooperative requirement
            if handler.cooperative_only:
                peer_info = self.transport.get_peer_info(peer_id)
                if not peer_info or not peer_info.cooperative_id:
                    logger.warning(f"Non-cooperative peer {peer_id} sent cooperative-only message")
                    return False
            
            # Check message size
            if handler.max_size:
                message_size = len(json.dumps(message_data))
                if message_size > handler.max_size:
                    logger.warning(f"Message from {peer_id} exceeds size limit")
                    return False
            
            # Check version requirements
            if handler.version_requirements:
                peer_info = self.transport.get_peer_info(peer_id)
                if not peer_info:
                    return False
                    
                peer_versions = peer_info.supported_protocols
                if not peer_versions & handler.version_requirements:
                    logger.warning(f"Peer {peer_id} doesn't support required protocol version")
                    return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error validating message: {str(e)}")
            return False

    async def _queue_message(
        self,
        priority: MessagePriority,
        peer_id: str,
        message_type: str,
        message_id: str,
        payload: Dict[str, Any]
    ) -> None:
        """Queue message for processing according to priority."""
        try:
            message_tuple = (peer_id, message_type, message_id, payload)
            
            if priority == MessagePriority.HIGH:
                await self.high_priority_queue.put(message_tuple)
            elif priority == MessagePriority.MEDIUM:
                await self.medium_priority_queue.put(message_tuple)
            else:
                await self.low_priority_queue.put(message_tuple)
                
        except Exception as e:
            logger.error(f"Error queuing message: {str(e)}")

    def _generate_message_id(self, message_type: str, payload: Dict[str, Any]) -> str:
        """Generate unique message ID."""
        data = f"{self.config.node_id}:{message_type}:{json.dumps(payload, sort_keys=True)}"
        return hashlib.sha256(data.encode()).hexdigest()

    def get_queue_sizes(self) -> Dict[str, int]:
        """Get current size of message queues."""
        return {
            "high_priority": self.high_priority_queue.qsize(),
            "medium_priority": self.medium_priority_queue.qsize(),
            "low_priority": self.low_priority_queue.qsize()
        }

    def get_metrics(self) -> Dict[str, int]:
        """Get dispatcher metrics."""
        metrics = self.metrics.copy()
        metrics.update({
            "pending_responses": len(self.pending_responses),
            "registered_handlers": len(self.handlers)
        })
        return metrics

# Example usage
async def example_usage():
    config = NetworkConfig(node_id="test_node")
    transport = NetworkTransport(config)
    dispatcher = MessageDispatcher(config, transport)
    
    # Register message handler
    async def handle_test_message(peer_id: str, payload: Dict[str, Any]) -> None:
        print(f"Received test message from {peer_id}: {payload}")
        
    dispatcher.register_handler(
        "test_message",
        handle_test_message,
        priority=MessagePriority.MEDIUM
    )
    
    # Start dispatcher
    await dispatcher.start()
    
    # Dispatch message
    response = await dispatcher.dispatch_message(
        "test_message",
        {"content": "Hello world"},
        routing=MessageRoute(
            message_type="test_message",
            target_shards={1},
            target_cooperatives={"coop1"},
            exclude_peers=set(),
            broadcast=True
        )
    )
    
    # Stop dispatcher
    await dispatcher.stop()

if __name__ == "__main__":
    asyncio.run(example_usage())