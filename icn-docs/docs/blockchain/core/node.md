# Node Management

## Introduction
The **Node Management** component is responsible for managing the nodes within the InterCooperative Network (ICN). Each node represents a cooperative and is involved in validating transactions and maintaining the blockchain. Nodes are essential to the **Proof of Cooperation (PoC)** consensus mechanism, as their reputation directly influences block validation.

## Core Responsibilities

### 1. Node Registration
Nodes are registered to the network based on cooperative membership. Key functions include:
- **Initial Setup**: Registers a new node, associating it with a cooperative-specific DID.
- **Reputation Tracking**: Tracks the cooperative’s reputation, which determines the node’s ability to validate blocks.

### 2. Node Communication
Nodes communicate with each other to propagate transactions, share blocks, and maintain network state. Communication includes:
- **Broadcasting Transactions**: Nodes broadcast newly received transactions to other nodes for validation.
- **Block Propagation**: After block creation, nodes propagate the new block to ensure consistent chain updates.
- **Consensus Messages**: Nodes exchange messages to confirm block validation and reach consensus.

### 3. Reputation Management
Reputation plays a critical role in block validation. It is influenced by factors such as:
- **Successful Validations**: Nodes earn reputation points for successful block validations.
- **Misbehavior Detection**: Nodes lose reputation points for double-signing, propagating invalid transactions, or failing to validate correctly.

### 4. Node Rotation
To prevent monopolization and ensure fair participation, nodes enter a **cooldown period** after successfully validating a block. This mechanism rotates the active validating nodes, promoting equal opportunities for block validation.

## Key Functions and Classes

### `register_node()`
- **Purpose**: Registers a new node to the network.
- **Parameters**:
  - `node_id`: Unique identifier for the node.
  - `cooperative_did`: DID associated with the cooperative.
- **Returns**: Confirmation of successful registration.
- **Example**:
  ```python
  register_node(node_id="node_123", cooperative_did="did:icn:cooperative_456")
