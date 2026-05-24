#pragma once

#include <iostream>
#include <vector>
#include <string>
#include <chrono>
#include <atomic>
#include <shared_mutex>
#include <unordered_map>


class TokenBucket {
private:
    // We pack both values into a struct so we can atomize the entire state transition
    struct BucketState {
        double tokens;
        uint64_t last_update_ns;
    };

    // Use std::atomic for hardware-level atomic updates via CAS loop
    std::atomic<BucketState> state_;
    
    const double capacity_;
    const double refill_rate_per_ns_;

public:
    TokenBucket(double capacity, double refill_rate_per_sec)
        : capacity_(capacity), 
          refill_rate_per_ns_(refill_rate_per_sec / 1e9) 
    {
        BucketState initial{capacity, get_current_time_ns()};
        state_.store(initial, std::memory_order_relaxed);
    }

    bool allow(double tokens_requested = 1.0) {
        BucketState current = state_.load(std::memory_order_relaxed);
        BucketState next;

        // CAS Loop: Attempt to lazily evaluate and consume tokens lock-free
        do {
            uint64_t now = get_current_time_ns();
            double elapsed_ns = 0.0;
            
            if (now > current.last_update_ns) {
                elapsed_ns = static_cast<double>(now - current.last_update_ns);
            }

            // 1. Calculate lazy replenishment
            double generated_tokens = elapsed_ns * refill_rate_per_ns_;
            double dynamic_tokens = std::min(capacity_, current.tokens + generated_tokens);

            // 2. Check if we have enough tokens
            if (dynamic_tokens < tokens_requested) {
                return false; // Throttled!
            }

            // 3. Prepare target state
            next.tokens = dynamic_tokens - tokens_requested;
            next.last_update_ns = now;

            // 4. Try to commit atomized state. If another thread beat us to it, 
            // 'current' is automatically refreshed with the new state, and we retry.
        } while (!state_.compare_exchange_weak(current, next, 
                                               std::memory_order_release, 
                                               std::memory_order_acquire));

        return true;
    }

    // Checks if the bucket has been idle longer than the expiry threshold.
    // Uses relaxed memory order since we are just inspecting the timestamp.
    bool is_stale(uint64_t now_ns, uint64_t expiry_threshold_ns) const {
        BucketState current = state_.load(std::memory_order_relaxed);
        if (now_ns < current.last_update_ns) return false;
        return (now_ns - current.last_update_ns) > expiry_threshold_ns;
    }

    // Double-checks that the bucket state hasn't changed since our map scan.
    // If it hasn't, we can safely erase it.
    bool confirm_evict(uint64_t expected_last_update_ns) {
        BucketState current = state_.load(std::memory_order_acquire);
        // If someone touched it in the split second between our check and our map lock, abort!
        return current.last_update_ns == expected_last_update_ns;
    }

private:
    static uint64_t get_current_time_ns() {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch()
        ).count();
    }
};