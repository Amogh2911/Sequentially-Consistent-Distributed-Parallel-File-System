#pragma once

#include <cstdio>
#include <cstdlib>
#include <string>
#include <unordered_map>
#include <list>
#include <vector>
#include <mutex>
#include <chrono>
#include <iomanip> // For formatting time


#include "../pfs_proto/pfs_fileserver.grpc.pb.h"
#include <grpcpp/grpcpp.h>



#include "pfs_common/pfs_config.hpp" // Include to access configuration sizes

// Cache block structure
struct CacheBlock {
    std::string filename;
    size_t block_number;
    std::vector<char> data;
    bool dirty;                 // Indicates if the block has been modified
    bool valid;                 // Indicates if the block is valid
    int64_t last_access_time;   // For exact LRU
};

// Cache Manager Class
class CacheManager {
public:
    // Constructor
    CacheManager();

    // Enable or disable caching
    // void setCacheEnabled(bool enabled);
    bool isCacheEnabled() const;

    // Read from cache
    bool cacheLookUp(const std::string& filename, size_t block_number, std::vector<char>& data);

    bool isBlockDirty(const std::string& filename, size_t block_number);

    // Write to cache
    void writeBlock(const std::string& filename, size_t block_number, const std::vector<char>& data, bool is_dirty, size_t stripe_width, const std::vector<std::unique_ptr<pfsfile::FileServer::Stub>>& fileserver_stubs);

    // Invalidate blocks for a given range
    void invalidateBlocks(const std::string& filename, int64_t start_offset, int64_t end_offset, int stripe_width, const std::vector<std::unique_ptr<pfsfile::FileServer::Stub>>& fileserver_stubs);

    void writeThrough(const std::string& filename, int64_t start_offset, int64_t end_offset, int stripe_width, const std::vector<std::unique_ptr<pfsfile::FileServer::Stub>>& fileserver_stubs);


    // Remove all blocks for a file (e.g., on close)
    void removeFileBlocks(const std::string& filename, size_t stripe_width, const std::vector<std::unique_ptr<pfsfile::FileServer::Stub>>& fileserver_stubs);

    // Handle cache eviction
    void evictBlocks(size_t stripe_width, const std::vector<std::unique_ptr<pfsfile::FileServer::Stub>>& fileserver_stubs);

    void clearCache(size_t stripe_width, const std::vector<std::unique_ptr<pfsfile::FileServer::Stub>>& fileserver_stubs);
void printCache();

private:
    size_t max_blocks_;         // Maximum cache blocks (CLIENT_CACHE_BLOCKS)
    bool cache_enabled_;        // Cache on/off flag
    std::unordered_map<std::string, std::list<CacheBlock>::iterator> cache_map_; // Key: filename:block_number
    std::list<CacheBlock> cache_list_; // For LRU management
    std::mutex cache_mutex_; // Protects cache operations

    // Helper function to generate cache key
    std::string getCacheKey(const std::string& filename, size_t block_number);

    bool writeBlockToServer(const std::string& filename, size_t block_number, const std::string& block_data, size_t stripe_width, const std::vector<std::unique_ptr<pfsfile::FileServer::Stub>>& fileserver_stubs);


    // Calculate block number from byte offset
    size_t getBlockNumber(int64_t offset) const;
};

// Temporary function (can be removed once caching is fully implemented)
void cache_func_temp();

