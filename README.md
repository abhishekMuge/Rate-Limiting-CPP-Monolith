
# Rate-Limiting-CPP-Monolith

High-Performance C++ Rate Limiter Module.
A lightweight, thread-safe, low-latency Rate Limiter module written in modern C++ (C++17). Designed specifically for monolithic applications, this module provides an easy-to-integrate API to safeguard your endpoints, internal services, or heavy computations from traffic spikes.

## Features

- **Token Bucket Algorithm:** Uses an optimized, lazy-refill strategy that eliminates the need for expensive background replenishment threads.
- **Lock-Striping (Sharded Registry):** Distributes keys across multiple independent memory shards, drastically reducing mutex contention in multi-threaded environments.
- **Zero-Copy & Low Overhead:** Uses standard, highly optimized memory layout paradigms to ensure minimal impact on execution path latency.
- **Header/Source Decoupled Design:** Easily dropped into any existing HTTP framework (Crow, Pistache), gRPC service, or custom IPC system.

## Architecture Overview

Instead of protecting a single global registry map with one global lock, the keyspace is sharded using a hash function. Each shard maintains its own mutex lock, allowing threads accessing different keys to execute concurrently without waiting on each other.


```text

   [ Incoming Request: Key = "client_ip" ]
                     |
            [ Hash Function ]
                     |
       +-------------+-------------+
       |             |             |
   [Shard 0]     [Shard 1]     [Shard 2] ... [Shard N]
   (Lock 0)      (Lock 1)      (Lock 2)      (Lock N)
       |
 [TokenBucket] -> Allow / Deny

```

## Quick Start & Integration

### 1. Header Inclusion & Initialization

Initialize the `RateLimiterManager` globally or within your service container during the application's startup phase.

```cpp
#include "RateLimiterManager.h"

// Configuration: 16 shards, Max Capacity of 10 tokens, Refill 5 tokens/sec
auto limiter = std::make_unique<RateLimiterManager>(16, 10.0, 5.0);
```

### 2. Integrating with an HTTP/gRPC Middleware

Intercept incoming traffic or resource-heavy calls early in your processing pipeline by passing a unique key (e.g., Client IP, User ID, API Key):

```cpp
void handle_request(const HttpRequest& req, HttpResponse& res) {
    std::string client_key = req.get_client_ip();

    // Check if the request is permitted
    if (!limiter->allow(client_key)) {
        res.set_status(429); // Too Many Requests
        res.set_body("Rate limit exceeded. Please try again later.");
        return;
    }

    // Process the request normally...
    execute_business_logic();
}
```

## API Reference

### `RateLimiterManager`

- **`RateLimiterManager(size_t shards, double max_capacity, double refill_rate)`** Constructs the coordinator. Higher shard counts reduce lock contention in high-throughput environments.
- **`bool allow(const std::string& key)`** Evaluates consumption. Returns `true` if a token was successfully acquired; returns `false` if the bucket is exhausted.

### `TokenBucket`

- **`bool consume(double amount)`** Lazily updates the bucket state based on the elapsed wall-clock time and attempts to deduct the requested token amount.

## Requirements


- **Compiler:** GCC 8+, Clang 7+, or MSVC 2019+ (Requires C++17 support)
- **Build System:** CMake 3.12+
- **Standard Library:** Uses standard concurrency headers (`<mutex>`, `<chrono>`, `<thread>`). No external dependencies required.
