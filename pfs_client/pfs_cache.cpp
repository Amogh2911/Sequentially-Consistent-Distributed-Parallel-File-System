
#include "pfs_cache.hpp"
#include "pfs_api.hpp"
extern struct pfs_execstat global_execstats;

// Constructor: Initializes the cache manager with the maximum number of blocks from config
CacheManager::CacheManager()
    : max_blocks_(CLIENT_CACHE_BLOCKS), cache_enabled_(true) {}


// Check if cache is enabled
bool CacheManager::isCacheEnabled() const {
    return cache_enabled_;
}


// Generate a unique key for each cache block
std::string CacheManager::getCacheKey(const std::string& filename, size_t block_number) {
    return filename + ":" + std::to_string(block_number);
}


// Read a block from the cache
bool CacheManager::cacheLookUp(const std::string& filename, size_t block_number, std::vector<char>& data) {
    std::lock_guard<std::mutex> lock(cache_mutex_);

    if (!cache_enabled_) {
        return false; // Cache is disabled, always return miss
    }

    std::string key = getCacheKey(filename, block_number);

    auto it = cache_map_.find(key);
    if (it == cache_map_.end()) {
        return false; // Cache miss
    }

    // Cache hit: Move the block to the front (most recently used)
    cache_list_.splice(cache_list_.begin(), cache_list_, it->second);

    data = it->second->data;
    it->second->last_access_time = std::chrono::steady_clock::now().time_since_epoch().count();
    return true; // Cache hit
}


// Read a block from the cache
bool CacheManager::isBlockDirty(const std::string& filename, size_t block_number) {
    std::lock_guard<std::mutex> lock(cache_mutex_);

    if (!cache_enabled_) {
        return false; // Cache is disabled, always return miss
    }

    std::string key = getCacheKey(filename, block_number);

    auto it = cache_map_.find(key);
    if (it == cache_map_.end()) {
        return false; // Cache miss
    }

    return it->second->dirty;
}

 // Helper function to write a block to the server
bool CacheManager::writeBlockToServer(const std::string& filename, size_t block_number, const std::string& block_data, size_t stripe_width, const std::vector<std::unique_ptr<pfsfile::FileServer::Stub>>& fileserver_stubs) {
    const size_t block_size = PFS_BLOCK_SIZE;
    const size_t stripe_blocks = STRIPE_BLOCKS;

    // Calculate the block properties
    size_t stripe_index = (block_number / stripe_blocks) % stripe_width;
    int stripe_number = block_number / (stripe_blocks * stripe_width);
    int block_within_stripe = block_number % stripe_blocks;

    // Calculate server-specific offset
    size_t server_start_offset = stripe_number * stripe_blocks * block_size + block_within_stripe * block_size;

    // Get the file server stub
    auto& file_stub = fileserver_stubs[stripe_index];

    // Prepare the WriteFileRequest
    pfsfile::WriteFileRequest write_request;
    write_request.set_filename(filename);
    write_request.set_start_offset(server_start_offset);
    write_request.set_data(block_data);
    write_request.set_num_bytes(block_size);

    // Perform the gRPC call
    pfsfile::WriteFileResponse write_response;
    grpc::ClientContext context;

    grpc::Status status = file_stub->WriteFile(&context, write_request, &write_response);

    if (!status.ok() || write_response.status() != 0) {
        std::cerr << "Error: Failed to write block " << block_number
                << " of file " << filename << " to server " << stripe_index
                << ". " << status.error_message() << std::endl;
        return false;
    }

    std::cout << "Successfully wrote block " << block_number
            << " of file " << filename << " to server " << stripe_index << std::endl;

    return true;
}

// Write a block to the cache
void CacheManager::writeBlock(const std::string& filename, size_t block_number, const std::vector<char>& data, bool is_dirty, size_t stripe_width, const std::vector<std::unique_ptr<pfsfile::FileServer::Stub>>& fileserver_stubs) {
    std::lock_guard<std::mutex> lock(cache_mutex_);

    if (!cache_enabled_) {
        return; // Cache is disabled, do not store data
    }

    std::string key = getCacheKey(filename, block_number);

    auto it = cache_map_.find(key);
    if (it != cache_map_.end()) {
        // Update existing block
        it->second->data = data;
        it->second->dirty = is_dirty || it->second->dirty; // Once dirty, always dirty until written back
        it->second->last_access_time = std::chrono::steady_clock::now().time_since_epoch().count();
        // Move to front
        cache_list_.splice(cache_list_.begin(), cache_list_, it->second);
    } else {
        // Add new block
        if (cache_list_.size() >= max_blocks_) {
            evictBlocks(stripe_width, fileserver_stubs);
        }
        CacheBlock new_block = {
            filename,
            block_number,
            data,
            is_dirty,
            true,
            std::chrono::steady_clock::now().time_since_epoch().count()
        };
        cache_list_.push_front(new_block);
        cache_map_[key] = cache_list_.begin();
    }
}

// Invalidate blocks within a specified range for a given file
void CacheManager::invalidateBlocks(const std::string& filename, int64_t start_offset, int64_t end_offset, int stripe_width, const std::vector<std::unique_ptr<pfsfile::FileServer::Stub>>& fileserver_stubs) {
    std::lock_guard<std::mutex> lock(cache_mutex_);

    if (!cache_enabled_) {
        return; // Cache is disabled, nothing to invalidate
    }

    size_t start_block = start_offset / PFS_BLOCK_SIZE;
    size_t end_block = end_offset / PFS_BLOCK_SIZE;

    for (size_t block_number = start_block; block_number <= end_block; ++block_number) {
        std::string key = getCacheKey(filename, block_number);
        auto it = cache_map_.find(key);
        if (it != cache_map_.end()) {
            if (it->second->dirty) {
                // Write back dirty block before invalidating
                std::cout<<"Invalidation Data before converting into string: ";
                for (char c : (std::vector<char>)it->second->data) {
                    std::cout << c << "";
                }
                std::cout << std::endl;

                std::string block_data_string(it->second->data.begin(), it->second->data.end());
                std::cout<<"Invalidation Data : "<<block_data_string<<std::endl;
                global_execstats.num_writebacks++;
                writeBlockToServer(filename, block_number, block_data_string, stripe_width, fileserver_stubs);
            }
            global_execstats.num_invalidations++;
            cache_list_.erase(it->second);
            cache_map_.erase(it);
        }
    }
}


// Invalidate blocks within a specified range for a given file
void CacheManager::writeThrough(const std::string& filename, int64_t start_offset, int64_t end_offset, int stripe_width, const std::vector<std::unique_ptr<pfsfile::FileServer::Stub>>& fileserver_stubs) {
    std::lock_guard<std::mutex> lock(cache_mutex_);

    if (!cache_enabled_) {
        return; // Cache is disabled, nothing to invalidate
    }

    size_t start_block = start_offset / PFS_BLOCK_SIZE;
    size_t end_block = end_offset / PFS_BLOCK_SIZE;

    for (size_t block_number = start_block; block_number <= end_block; ++block_number) {
        std::string key = getCacheKey(filename, block_number);
        auto it = cache_map_.find(key);
        if (it != cache_map_.end()) {
            if (it->second->dirty) {
                // Write back dirty block before invalidating
                std::cout<<"Write Through Data before converting into string: ";
                for (char c : (std::vector<char>)it->second->data) {
                    std::cout << c << "";
                }
                std::cout << std::endl;
                
                std::string block_data_string(it->second->data.begin(), it->second->data.end());
                std::cout<<"Write Through Data: "<<block_data_string<<std::endl;
                global_execstats.num_writebacks++;
                writeBlockToServer(filename, block_number, block_data_string, stripe_width, fileserver_stubs);
                it->second->dirty = false;
            }
        }
    }
}

// Remove all blocks associated with a file (e.g., when closing the file)
void CacheManager::removeFileBlocks(const std::string& filename, size_t stripe_width, const std::vector<std::unique_ptr<pfsfile::FileServer::Stub>>& fileserver_stubs) {
    std::lock_guard<std::mutex> lock(cache_mutex_);

    if (!cache_enabled_) {
        return; // Cache is disabled, nothing to remove
    }

    for (auto it = cache_list_.begin(); it != cache_list_.end();) {
        if (it->filename == filename) {
            if (it->dirty) {
                // Write back dirty block before removing
                std::string block_data_string(it->data.begin(), it->data.end());
                global_execstats.num_close_writebacks++;
                writeBlockToServer(filename, it->block_number, block_data_string, stripe_width, fileserver_stubs);

            }
            global_execstats.num_close_evictions++;
            cache_map_.erase(getCacheKey(it->filename, it->block_number));
            it = cache_list_.erase(it);
        } else {
            ++it;
        }
    }
}

// Evict least recently used blocks to maintain cache size
void CacheManager::evictBlocks(size_t stripe_width, const std::vector<std::unique_ptr<pfsfile::FileServer::Stub>>& fileserver_stubs) {
    while (cache_list_.size() >= max_blocks_) {
        CacheBlock& block = cache_list_.back();
        if (block.dirty) {
            // Write back dirty block before evicting
            std::string block_data_string(block.data.begin(), block.data.end());
            global_execstats.num_evictions++;
            writeBlockToServer(block.filename, block.block_number, block_data_string, stripe_width, fileserver_stubs);
        }
        cache_map_.erase(getCacheKey(block.filename, block.block_number));
        cache_list_.pop_back();
    }
}

// Clear all cache blocks, writing back any dirty blocks before removal
void CacheManager::clearCache(size_t stripe_width, const std::vector<std::unique_ptr<pfsfile::FileServer::Stub>>& fileserver_stubs) {
    std::lock_guard<std::mutex> lock(cache_mutex_);

    if (!cache_enabled_) {
        return; // Cache is disabled, nothing to clear
    }

    std::cout << "Clearing cache..." << std::endl;

    // Iterate over all cached blocks and write back dirty blocks
    for (auto it = cache_list_.begin(); it != cache_list_.end();) {
        if (it->dirty) {
            std::string block_data_string(it->data.begin(), it->data.end());
            std::cout << "Writing back dirty block: " << it->block_number << " of file " << it->filename << std::endl;
            writeBlockToServer(it->filename, it->block_number, block_data_string, stripe_width, fileserver_stubs);
        }
        // Remove block from cache
        cache_map_.erase(getCacheKey(it->filename, it->block_number));
        it = cache_list_.erase(it);
    }

    std::cout << "Cache cleared successfully." << std::endl;
}



// Function to print the current state of the cache
void CacheManager::printCache() {
    std::lock_guard<std::mutex> lock(cache_mutex_);

    if (!cache_enabled_) {
        std::cout << "Cache is currently disabled." << std::endl;
        return;
    }

    if (cache_list_.empty()) {
        std::cout << "Cache is empty." << std::endl;
        return;
    }

    std::cout << "Current Cache State:" << std::endl;
    std::cout << "=====================================================" << std::endl;
    std::cout << std::setw(20) << "Filename"
              << std::setw(15) << "Block Number"
              << std::setw(10) << "Dirty"
              << std::setw(20) << "Last Access Time" << std::endl;
    std::cout << "-----------------------------------------------------" << std::endl;

    for (const auto& block : cache_list_) {
        std::cout << std::setw(20) << block.filename
                  << std::setw(15) << block.block_number
                  << std::setw(10) << (block.dirty ? "Yes" : "No")
                  << std::setw(20) << block.last_access_time << std::endl;
    }

    std::cout << "=====================================================" << std::endl;
}



void cache_func_temp() {
    printf("%s: called.\n", __func__);
}
