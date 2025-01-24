#pragma once

#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <ctime>
#include <cstring>
#include <vector>
#include <list>
#include <unordered_map>
#include <mutex>
#include <condition_variable>
#include <semaphore>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <shared_mutex>

#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>

#include "pfs_common/pfs_common.hpp"

#include "pfs_proto/pfs_metaserver.pb.h"
#include "pfs_proto/pfs_metaserver.grpc.pb.h"

struct FileChunk {
    std::unordered_map<int, std::vector<std::pair<int64_t, int64_t>>> server_map;
};

struct pfs_filerecipe {
    int stripe_width;

    // Additional...
   std::unordered_map<int, std::vector<std::pair<int64_t, int64_t>>> server_map;
};

struct TokenInfo {
    int client_id;         // ID of the client holding the token
    uint64_t start_offset;  // Start of the locked byte range
    uint64_t end_offset;    // End of the locked byte range
    std::string token_type; // "read" or "write"
};


struct pfs_metadata {
    // Given metadata
    char filename[256];
    uint64_t file_size;
    time_t ctime;
    time_t mtime;
    struct pfs_filerecipe recipe;
    uint64_t mode;
    
    // Additional...
    // std::vector<TokenInfo> read_tokens;  // List of active read tokens
    // TokenInfo write_token; 

    // bool is_locked;      // Indicates if the file is currently locked for a read/write operation

};

struct FDInfo {
    std::string filename;
    int mode;  // 1 = read, 2 = read/write
};

struct ReadToken {
    int client_id;
    int64_t start_offset;
    int64_t end_offset;
    bool acked;
};


struct WriteToken {
    int client_id;
    int64_t start_offset;
    int64_t end_offset;
    bool acked;
};

struct Block {
    int read_tokens = 0;
    int write_tokens = 0;
    std::vector<int> clientsWithCache;
};

struct BlockAction {
    bool cacheRequester = false;
    std::vector<int> invalidate_clients;
    std::vector<int> write_through_clients;
};
