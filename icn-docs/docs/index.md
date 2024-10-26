# Welcome to the InterCooperative Network Documentation

## Introduction

The **InterCooperative Network (ICN)** is a decentralized platform designed to foster cooperative management, democratic governance, and secure resource sharing among diverse communities. It aims to provide an alternative to traditional capitalist structures by promoting cooperative principles, decentralized decision-making, and equitable resource distribution.

The ICN utilizes advanced technologies like blockchain, Decentralized Identifiers (DIDs), and Proof of Cooperation consensus to enable transparent, democratic governance and collaborative resource management. This documentation offers a comprehensive guide to understanding the ICN's architecture, core modules, workflows, and testing processes.

## Table of Contents

- **[Architecture](architecture.md)** - Overview of the system's architecture and components.
- **[Workflows](workflows.md)** - Detailed descriptions of ICN's key workflows like transaction processing and consensus.
- **[Testing Overview](testing_overview.md)** - Information on testing strategies, including unit, integration, and performance tests.

### Blockchain
- **[Overview](blockchain/README.md)** - General information about the blockchain module.
- **[Blockchain](blockchain/blockchain.md)** - Core blockchain functionalities and logic.
- **Utils**
  - [Overview](blockchain/utils/README.md)
  - [Metrics](blockchain/utils/metrics.md)
  - [Validation](blockchain/utils/validation.md)
  - [Crypto](blockchain/utils/crypto.md)
- **Core**
  - [Overview](blockchain/core/README.md)
  - [Node](blockchain/core/node.md)
  - [Block](blockchain/core/block.md)
  - [Transaction](blockchain/core/transaction.md)
  - **Shard**
    - [Overview](blockchain/core/shard/README.md)
    - [Validation Manager](blockchain/core/shard/validation_manager.md)
    - [Transaction Manager](blockchain/core/shard/transaction_manager.md)
    - [State Manager](blockchain/core/shard/state_manager.md)

### DID
- [Overview](did/README.md)
- [DID](did/did.md)
- [Registry](did/registry.md)

### API
- [Overview](api/README.md)
- [Server](api/server.md)

### Tests
- **[Overview](tests/README.md)**
- **Unit Tests**
  - [Test Blockchain](tests/unit/test_blockchain.md)
  - [Test Node](tests/unit/test_node.md)
- **Integration Tests**
  - [Test Consensus Mechanism](tests/integration/test_consensus_mechanism.md)
  - [Test Shard Management](tests/integration/test_shard_management.md)
- **Performance Tests**
  - [Test Stress Resilience](tests/performance/test_stress_resilience.md)

---

For more detailed information, navigate through the sections using the links above.
