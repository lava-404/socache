# Solana RPC Middleware 🚀

[![Rust](https://img.shields.io/badge/Rust-000000?style=flat\&logo=rust\&logoColor=white)](https://www.rust-lang.org/)
[![Tokio](https://img.shields.io/badge/Tokio-Async_Runtime-green)](https://tokio.rs/)
[![Axum](https://img.shields.io/badge/Axum-Web_Framework-blue)](https://github.com/tokio-rs/axum)
[![Solana](https://img.shields.io/badge/Solana-9945FF?style=flat\&logo=solana\&logoColor=white)](https://solana.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A high-performance Solana RPC middleware built in Rust that provides intelligent request routing, automatic failover, response caching, and WebSocket subscription multiplexing. The project sits between clients and upstream RPC providers, reducing infrastructure costs while improving reliability and performance.

## What It Does

Instead of sending requests directly to a single Solana RPC provider, clients communicate with this middleware.

The middleware intelligently:

* Distributes traffic across multiple RPC providers
* Automatically retries failed requests
* Caches repeated read operations
* Reduces RPC credit consumption
* Maintains a single upstream WebSocket subscription for multiple downstream clients
* Provides a unified endpoint regardless of upstream provider health

```text
Client
   │
   ▼
RPC Middleware
   │
   ├── Helius
   ├── Alchemy
   ├── QuickNode
   ├── Ankr
   └── Solana Mainnet RPC
```

## Motivation

Most Solana applications connect directly to a single RPC provider. While this works for small projects, it creates several problems:

* A single point of failure
* Expensive RPC credit usage
* Rate limiting issues
* Duplicate requests for identical data
* Redundant WebSocket subscriptions

This project solves those problems by introducing a middleware layer capable of routing, caching, and multiplexing requests before they reach upstream providers.

## Features

### HTTP JSON-RPC Proxy

The middleware exposes a standard JSON-RPC endpoint that accepts Solana RPC requests and forwards them to upstream providers.

Because the interface remains identical to Solana's native JSON-RPC API, existing wallets, SDKs, scripts, and applications can communicate with the middleware without modification.

Example request:

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "getBalance",
  "params": [
    "GwsPP9HHhCvEQeu3HTFzsVL6DEtnnYw4ALEtA3fMBC9Q"
  ]
}
```

### Round Robin Load Balancing

The middleware maintains a pool of upstream RPC providers.

Instead of sending every request to a single endpoint, requests are distributed using a Round Robin algorithm.

```text
Request 1 → Helius
Request 2 → Alchemy
Request 3 → QuickNode
Request 4 → Ankr
Request 5 → Mainnet Beta
Request 6 → Helius
```

This ensures traffic is spread evenly across all available providers.

### Automatic Failover & Retry Logic

If a provider becomes unavailable or starts rate limiting requests, the middleware automatically retries the request against another provider.

Retry conditions include:

```text
429 Too Many Requests
500 Internal Server Error
502 Bad Gateway
503 Service Unavailable
504 Gateway Timeout
```

Example:

```text
Client Request
      │
      ▼
   Alchemy
      │
      ├── 429 Error
      ▼
   QuickNode
      │
      └── Success
```

This allows the middleware to continue serving requests even when individual providers fail.

### Smart Read/Write Classification

The middleware distinguishes between read operations and write operations.

Read methods include:

```text
getBalance
getAccountInfo
getTransaction
getBlock
getSupply
getLatestBlockhash
```

Write methods include:

```text
sendTransaction
requestAirdrop
```

Only read operations are eligible for caching. Write operations always bypass the cache and are forwarded directly to an upstream provider.

### In-Memory Response Cache

To reduce duplicate requests and unnecessary RPC credit consumption, the middleware includes an in-memory cache powered by Moka.

When a read request arrives:

1. The request method and parameters are extracted.
2. A cache key is generated.
3. The cache is checked for an existing response.
4. If found, the cached response is returned immediately.
5. Otherwise the request is forwarded upstream.
6. The response is cached before being returned.

Example:

```text
getBalance(wallet123)
        │
        ▼
   Cache Miss
        │
        ▼
    RPC Call
        │
        ▼
 Store Response
```

Subsequent requests:

```text
getBalance(wallet123)
        │
        ▼
    Cache Hit
        │
        ▼
 Return Cached Data
```

### Request Hashing

Cache keys are generated from the RPC method and parameters.

Example:

```text
getBalance:["wallet123"]
```

The request string is hashed using BLAKE3 before being stored in the cache.

This creates compact, deterministic cache keys while avoiding large string allocations.

### Time-To-Live (TTL)

Cached responses automatically expire after a short period.

Current TTL:

```text
2 seconds
```

This reduces duplicate requests while ensuring blockchain data remains reasonably fresh.

## WebSocket Subscription Multiplexing

One of the largest inefficiencies in blockchain applications is opening duplicate WebSocket subscriptions.

Without a middleware layer:

```text
Client A ─┐
Client B ─┼──► accountSubscribe(wallet)
Client C ─┘
```

Three identical upstream subscriptions are created.

With multiplexing:

```text
Client A ─┐
Client B ─┼──► Middleware ───► One Subscription
Client C ─┘
```

The middleware creates only a single upstream subscription and shares updates among all interested clients.

This dramatically reduces upstream subscription counts and RPC usage.

## Subscription Architecture

The WebSocket layer maintains several internal mappings to track active subscriptions.

### Pending Requests

Maps outgoing RPC request IDs to account addresses.

```text
Request ID → Account
```

Example:

```text
1001 → wallet123
```

Used when correlating upstream subscription responses.

### Active Subscriptions

Maps Solana subscription IDs to account addresses.

```text
Subscription ID → Account
```

Example:

```text
45678 → wallet123
```

Used when processing account notifications.

### Client Registrations

Tracks which connected clients are interested in each account.

```text
Account → Connected Clients
```

Example:

```text
wallet123 → {Client A, Client B}
wallet456 → {Client C}
```

This enables targeted notification delivery instead of broadcasting every update to every client.

## Notification Flow

When an upstream account notification is received:

```text
Helius Notification
        │
        ▼
Subscription ID
        │
        ▼
Active Subscription Map
        │
        ▼
Account Address
        │
        ▼
Client Registry
        │
        ▼
Relevant Clients
        │
        ▼
Broadcast Update
```

Only clients subscribed to the affected account receive the update.

## Current Project Status

### Completed

* HTTP JSON-RPC proxy
* Multi-provider routing
* Round Robin load balancing
* Automatic failover and retry logic
* Read/write request classification
* Moka caching layer
* BLAKE3 request hashing
* Cache TTL support
* Upstream WebSocket connection
* Subscription state management
* WebSocket endpoint architecture

### In Progress

* End-to-end notification broadcasting
* Client disconnect cleanup
* Upstream unsubscribe handling
* Subscription lifecycle management

## Tech Stack

### Core

* Rust
* Tokio
* Axum

### Networking

* Reqwest
* Tokio Tungstenite

### Serialization

* Serde
* Serde JSON

### Caching

* Moka

### Hashing

* BLAKE3

### Concurrency

* RwLock
* Mutex
* AtomicUsize
* MPSC Channels

### Utilities

* UUID

## Future Improvements

* Weighted load balancing
* Health scoring for providers
* Prometheus metrics
* Distributed caching
* Request rate limiting
* Provider health monitoring
* Web dashboard
* Multi-region support
* Request analytics
* Subscription batching

## Running the Project

```bash
git clone <repository-url>

cd solana-rpc-middleware

cargo build

cargo run
```

The middleware starts on:

```text
http://localhost:3000
```

and exposes both HTTP JSON-RPC and WebSocket endpoints.

## License

MIT License

## Star This Repo

If this project helped you learn about Solana infrastructure, RPC routing, caching, or WebSocket multiplexing, consider giving it a star.

---

Built with Rust, caffeine, and an unreasonable amount of time spent arguing with ownership errors.
