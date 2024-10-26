# Blockchain Overview

## Introduction
The **Blockchain** module of the InterCooperative Network (ICN) serves as the decentralized ledger that records all transactions and cooperative actions. It ensures data integrity, immutability, and transparency through a distributed network of nodes that validate blocks via the **Proof of Cooperation (PoC)** consensus mechanism.

## Main Components

### 1. Blockchain Management
This submodule handles the core functions of block management, including:
- **Block Creation**: Assembles transactions into blocks.
- **Block Validation**: Validates blocks based on cooperative reputation, ensuring consensus is achieved fairly.
- **Chain Maintenance**: Maintains the integrity of the chain, handling chain reorganization and fork resolution if necessary.

### 2. Core Module
The **Core** submodule focuses on node operations and transaction processing:
- **Node Management**: Manages nodes participating in block validation, tracking cooperative reputation and participation.
- **Transaction Handling**: Manages transaction creation, propagation, and validation across the network.
- **Shard Management**: Supports sharding, dividing the blockchain into smaller, manageable segments to improve performance and scalability.

### 3. Utilities
The **Utils** submodule provides helper functions for cryptographic operations, metrics collection, and validation:
- **Cryptographic Functions**: Supports signing, hashing, and verifying data to ensure secure transactions.
- **Metrics**: Collects metrics related to node performance, transaction throughput, and block creation times.
- **Validation Utilities**: Includes functions to validate transaction formats, signatures, and block structure.

## Workflow

1. **Transaction Initiation**: Users or cooperatives initiate transactions, which are broadcasted to the network.
2. **Transaction Propagation**: Nodes receive and validate the transactions, checking for correctness and completeness.
3. **Block Assembly**: Valid transactions are grouped into blocks by nodes, following the PoC consensus mechanism.
4. **Block Validation**: Nodes validate the blocks based on cooperative reputation, ensuring tamper-proof consensus.
5. **State Update**: Once a block is validated, the state of the blockchain is updated, with changes applied to relevant shards.

## Key Files in the Blockchain Module

- **blockchain.py**: The main file that manages the blockchain’s creation, validation, and maintenance.
- **core/**:
  - **node.py**: Manages nodes in the network, including node reputation tracking and communication.
  - **block.py**: Handles block creation, validation, and chain updates.
  - **transaction.py**: Manages transaction processing, ensuring validity and state consistency.
  - **shard/**: Contains files for managing shards, including transaction distribution and state management.
- **utils/**:
  - **crypto.py**: Provides cryptographic functions for transaction signing, hashing, and verification.
  - **metrics.py**: Collects and logs performance metrics related to blockchain operations.
  - **validation.py**: Includes utility functions for transaction and block validation.

## Planned Enhancements
- **Dynamic Sharding**: Improving sharding performance by dynamically adjusting shard sizes based on network load.
- **Zero-Knowledge Proofs**: Implementing advanced cryptography to enable more privacy-focused transactions.
- **Improved Node Communication**: Enhancing peer-to-peer communication for faster block propagation and reduced latency.

---

This overview provides a foundational understanding of how the blockchain module operates within the ICN. For more detailed information about specific files and submodules, please refer to their individual documentation files.
