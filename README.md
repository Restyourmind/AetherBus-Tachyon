# AetherBus-Tachyon

**AetherBus-Tachyon** is a high-performance, lightweight message broker designed for the AetherBus ecosystem. It serves as a central routing point for events, ensuring efficient and reliable delivery from producers to consumers.

This project is currently under active development and aims to be a foundational component for building scalable, event-driven architectures.

## ✨ Features

- **High-Performance Routing:** Utilizes an **Adaptive Radix Tree** for fast and efficient topic-based routing, ensuring low-latency message delivery even with a large number of routes.
- **Extensible Media Handling:** Supports pluggable codecs and compressors to optimize message payloads.
  - **Codec:** Defaulting to `JSON` for structured data.
  - **Compressor:** Defaulting to `LZ4` for high-speed compression and decompression.
- **ZeroMQ Integration:** Built on top of ZeroMQ (using `pebbe/zmq4`), leveraging its powerful and battle-tested messaging patterns (ROUTER-DEALER, PUB-SUB).
- **Clean Architecture:** Organized with a clear separation of concerns (domain, use case, delivery, repository) for maintainability and testability.
- **Continuous Integration:** Includes a **GitHub Actions workflow** that automatically builds the application and runs tests (including race detection) on every push and pull request to the `main` branch.

## 🚀 Getting Started

### Prerequisites

- [Go](https://golang.org/dl/) (version 1.22 or later)
- [ZeroMQ](https://zeromq.org/download/) (version 4.x)

On Debian/Ubuntu, you can install ZeroMQ development libraries with:

```bash
sudo apt-get update && sudo apt-get install -y libzmq3-dev
```

### Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/aetherbus/aetherbus-tachyon.git
   cd aetherbus-tachyon
   ```

2. **Install dependencies:**
   ```bash
   go mod tidy
   ```

3. **Run the server:**
   ```bash
   go run ./cmd/tachyon
   ```

The server will start and bind to the addresses specified in the configuration (defaults to `tcp://*:5555` for the ROUTER and `tcp://*:5556` for the PUB socket).

##  Architectural Overview

```
+-----------------------+
|   ZMQ API (Delivery)  |
| (Router, Pub/Sub)     |
+-----------+-----------+
            | (Envelope)
+-----------v-----------+
|  EventRouter (Usecase) |
+-----------+-----------+
            | (Topic)
+-----------v-----------+
| RouteStore (Repository)|
| (Adaptive Radix Tree) |
+-----------------------+
```

1.  **Delivery Layer (`zmq`):** Handles the raw network communication. It receives messages from clients, decompresses and decodes them, and wraps them in a `domain.Envelope`.
2.  **Usecase Layer (`usecase`):** Contains the core business logic. The `EventRouter` takes an `Envelope`, uses the `RouteStore` to determine the destination, and orchestrates the next steps.
3.  **Repository Layer (`repository`):** Provides an abstraction over the data storage. The `ART_RouteStore` implements the `RouteStore` interface using a high-performance radix tree to store and match routing keys.

This separation allows for easy testing and swapping of components. For example, the `RouteStore` could be backed by a different data structure or a distributed key-value store without changing the use case logic.
