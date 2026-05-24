
#include "TokenBucket.h"

class ThreadSafeRateLimiter {
private:
    struct Shard {
        std::shared_mutex mutex;
        std::unordered_map<std::string, TokenBucket> buckets;
    };

    const size_t num_shards_;
    std::vector<Shard> shards_;
    const double capacity_;
    const double refill_rate_per_sec_;

    // Simple hash routing to find the correct shard index
    size_t get_shard_index(const std::string& key) const {
        return std::hash<std::string>{}(key) % num_shards_;
    }

    // Track the next shard to clean up so we rotate through them sequentially
    std::atomic<size_t> next_cleanup_shard_idx_{0};

    static uint64_t get_current_time_ns() {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch()
        ).count();
    }

public:
    ThreadSafeRateLimiter(size_t num_shards, double capacity, double refill_rate_per_sec)
        : num_shards_(num_shards), 
          shards_(num_shards), 
          capacity_(capacity), 
          refill_rate_per_sec_(refill_rate_per_sec) {}

    bool allow_request(const std::string& key) {
        size_t shard_idx = get_shard_index(key);
        Shard& shard = shards_[shard_idx];

        // 1. Fast Path: Shared Read Lock to see if the bucket already exists
        {
            std::shared_lock<std::shared_mutex> lock(shard.mutex);
            auto it = shard.buckets.find(key);
            if (it != shard.buckets.end()) {
                // The map structure is safe; internal bucket updates are atomic
                return it->second.allow(); 
            }
        }

        // 2. Slow Path: Exclusive Write Lock to insert a brand new user bucket
        {
            std::unique_lock<std::shared_mutex> lock(shard.mutex);
            
            // Double-checked locking pattern to ensure no one created it while upgrading lock
            auto [it, inserted] = shard.buckets.try_emplace(
                key, capacity_, refill_rate_per_sec_
            );
            
            return it->second.allow();
        }
    }
    // This method is called repeatedly by a single background thread
    void perform_incremental_cleanup(std::chrono::nanoseconds expiry_threshold) {
        size_t shard_idx = next_cleanup_shard_idx_.fetch_add(1, std::memory_order_relaxed) % num_shards_;
        Shard& shard = shards_[shard_idx];
        
        uint64_t now = get_current_time_ns();
        uint64_t threshold_ns = expiry_threshold.count();

        // Vector to collect keys that are candidates for eviction
        std::vector<std::pair<std::string, uint64_t>> eviction_candidates;

        // Phase 1: Shared/Read Lock to gather candidates without blocking requests
        {
            std::shared_lock<std::shared_mutex> read_lock(shard.mutex);
            for (const auto& [key, bucket] : shard.buckets) {
                if (bucket.is_stale(now, threshold_ns)) {
                    // Cache the key and the exact timestamp we observed
                    eviction_candidates.push_back({key, get_bucket_timestamp(bucket)});
                }
            }
        }

        if (eviction_candidates.empty()) return;

        // Phase 2: Exclusive/Write Lock to execute deletions
        // Use try_lock to completely avoid stalling the hot path if traffic is heavy right now
        std::unique_lock<std::shared_mutex> write_lock(shard.mutex, std::try_to_lock);
        if (!write_lock.owns_lock()) {
            return; // Shard is busy handling requests; skip it and let the next cycle grab it
        }

        for (const auto& [key, expected_ts] : eviction_candidates) {
            auto it = shard.buckets.find(key);
            if (it != shard.buckets.end()) {
                // CAS validation: Verify no request sneaked in and updated the bucket
                if (it->second.confirm_evict(expected_ts)) {
                    shard.buckets.erase(it);
                }
            }
        }
    }

private:
    // Helper to peek at the timestamp safely for the read phase
    static uint64_t get_bucket_timestamp(const TokenBucket& bucket) {
        // Accessing via standard interface or public layout
        // Assuming a friend setup or minor architectural visibility
        return bucket.get_current_state_ts_internal(); 
    }
};