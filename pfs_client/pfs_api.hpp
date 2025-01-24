#pragma once

#include <cstdio>
#include <cstdint>
#include <cstdlib>
#include <cstdbool>
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

#include "pfs_common/pfs_config.hpp"
#include "pfs_common/pfs_common.hpp"


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
    pfs_filerecipe recipe;
    int mode;

    // Additional...
    // std::vector<TokenInfo> read_tokens;  // List of active read tokens
    // TokenInfo write_token; 

    // bool is_locked;      // Indicates if the file is currently locked for a read/write operation

};

struct file_metadata{
    char filename[256];
    int stripe_width;
    int mode;
    
};

struct pfs_execstat {
    long num_read_hits;
    long num_write_hits;
    long num_evictions;
    long num_writebacks;
    long num_invalidations;
    long num_close_writebacks;
    long num_close_evictions;
};


// struct ReadToken {
//     int client_id;
//     int64_t start_offset;
//     int64_t end_offset;
//     bool status;
//     std::mutex mutex;
//     std::condition_variable cv;
// };


// struct WriteToken {
//     int client_id;
//     int64_t start_offset;
//     int64_t end_offset;
//     bool status;
//     std::mutex mutex;
//     std::condition_variable ack_cv;
// };




struct ReadToken {
    int client_id;
    int64_t start_offset;
    int64_t end_offset;
    bool status;

    // Use smart pointers for mutex and condition_variable
    std::shared_ptr<std::mutex> mutex;
    std::shared_ptr<std::condition_variable> cv;

    ReadToken(int client_id_, int64_t start_, int64_t end_, bool status_)
        : client_id(client_id_), start_offset(start_), end_offset(end_), status(status_),
          mutex(std::make_shared<std::mutex>()), cv(std::make_shared<std::condition_variable>()) {}

//     // Copy constructor
//     ReadToken(const ReadToken& other)
//         : client_id(other.client_id), start_offset(other.start_offset),
//           end_offset(other.end_offset), status(other.status),
//           mutex(std::make_shared<std::mutex>(*other.mutex)),
//           cv(std::make_shared<std::condition_variable>()) {}

//     // Move constructor
//     ReadToken(ReadToken&& other) noexcept
//         : client_id(other.client_id), start_offset(other.start_offset),
//           end_offset(other.end_offset), status(other.status),
//           mutex(std::move(other.mutex)), cv(std::move(other.cv)) {}
};

struct WriteToken {
    int client_id;
    int64_t start_offset;
    int64_t end_offset;
    bool status;

    // Use smart pointers for mutex and condition_variable
    std::shared_ptr<std::mutex> mutex;
    std::shared_ptr<std::condition_variable> cv;

    WriteToken(int client_id_, int64_t start_, int64_t end_, bool status_)
        : client_id(client_id_), start_offset(start_), end_offset(end_), status(status_),
          mutex(std::make_shared<std::mutex>()), cv(std::make_shared<std::condition_variable>()) {}

//     // Copy constructor
//     WriteToken(const WriteToken& other)
//         : client_id(other.client_id), start_offset(other.start_offset),
//           end_offset(other.end_offset), status(other.status),
//           mutex(std::make_shared<std::mutex>(*other.mutex)),
//           ack_cv(std::make_shared<std::condition_variable>()) {}

//     // Move constructor
//     WriteToken(WriteToken&& other) noexcept
//         : client_id(other.client_id), start_offset(other.start_offset),
//           end_offset(other.end_offset), status(other.status),
//           mutex(std::move(other.mutex)), ack_cv(std::move(other.ack_cv)) {}
};



int pfs_initialize();
int pfs_finish(int client_id);
int pfs_create(const char *filename, int stripe_width);
int pfs_open(const char *filename, int mode);
int pfs_read(int fd, void *buf, size_t num_bytes, off_t offset);
int pfs_write(int fd, const void *buf, size_t num_bytes, off_t offset);
int pfs_close(int fd);
int pfs_delete(const char *filename);
int pfs_fstat(int fd, struct pfs_metadata *meta_data);
int pfs_execstat(struct pfs_execstat *execstat_data);
// int HealthCheck();
