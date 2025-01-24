#include "pfs_api.hpp"
#include "pfs_cache.hpp"
#include "pfs_metaserver/pfs_metaserver_api.hpp"
#include "pfs_fileserver/pfs_fileserver_api.hpp"

using grpc::Status;

#include <grpcpp/grpcpp.h>
#include "../pfs_proto/pfs_metaserver.grpc.pb.h"
#include "../pfs_proto/pfs_fileserver.grpc.pb.h"
#include <atomic>

struct pfs_execstat global_execstats={0};

class Client {
private:
    std::unique_ptr<pfsmeta::MetaServer::Stub> meta_server_stub;
    std::vector<std::unique_ptr<pfsfile::FileServer::Stub>> fileserver_stubs;
    std::unique_ptr<grpc::ClientReaderWriter<pfsmeta::ConnectRequest, pfsmeta::ConnectResponse>> connect_stream;
    std::mutex client_id_mutex;
    std::atomic<bool> shutdown_signal_{false};
    int client_id = -1;

    // Map to store file descriptor (FD) to metadata mapping on the client
    std::unordered_map<int, pfs_metadata> fd_to_metadata_map;
    std::unordered_map<std::string, pfs_metadata> filename_to_metadata_map;

    std::mutex fd_metadata_mutex;
    std::unordered_map<std::string, std::vector<int>> fd_map;
    std::mutex fd_mutex;


    std::unordered_map<std::string, std::vector<ReadToken>> read_token_table;
    std::unordered_map<std::string, std::vector<WriteToken>> write_token_table;

    std::mutex revoke_table_mutex; // To ensure thread-safe updates

    CacheManager cache;

    grpc::ClientContext& getConnectContext() {
        static grpc::ClientContext connect_context;
        return connect_context;
    }

    void UpdateReadTableAfterRevoking(std::vector<ReadToken>& ranges, int64_t start, int64_t end) {
        std::vector<ReadToken> updated_ranges;

        for (const auto& range : ranges) {
            // If no overlap, keep the range as is
            if (end < range.start_offset || start > range.end_offset) {
                updated_ranges.push_back(range);
            } else {
                // Handle overlap by splitting the range
                if (range.start_offset < start) {
                    ReadToken left_split = range;
                    left_split.end_offset = start - 1;
                    updated_ranges.push_back(std::move(left_split));
                }
                if (range.end_offset > end) {
                    ReadToken right_split = range;
                    right_split.start_offset = end + 1;
                    updated_ranges.push_back(std::move(right_split));
                }
            }
        }

        // Replace the old ranges with the updated ones
        ranges = std::move(updated_ranges);
    }

    void UpdateWriteTableAfterRevoking(std::vector<WriteToken>& ranges, int64_t start, int64_t end) {
        std::vector<WriteToken> updated_ranges;

        for (const auto& range : ranges) {
            // If no overlap, keep the range as is
            if (end < range.start_offset || start > range.end_offset) {
                updated_ranges.push_back(range);
            } else {
                // Handle overlap by splitting the range
                if (range.start_offset < start) {
                    WriteToken left_split = range;
                    left_split.end_offset = start - 1;
                    updated_ranges.push_back(std::move(left_split));
                }
                if (range.end_offset > end) {
                    WriteToken right_split = range;
                    right_split.start_offset = end + 1;
                    updated_ranges.push_back(std::move(right_split));
                }
            }
        }

        // Replace the old ranges with the updated ones
        ranges = std::move(updated_ranges);
    }


    void UpdateTableAfterRevoking(std::vector<std::pair<int64_t, int64_t>>& ranges, int64_t start, int64_t end) {
        std::vector<std::pair<int64_t, int64_t>> updated_ranges;

        for (const auto& range : ranges) {
            // If no overlap, keep the range as is
            if (end < range.first || start > range.second) {
                updated_ranges.push_back(range);
            } else {
                // Handle overlap by splitting the range
                if (range.first < start) {
                    updated_ranges.push_back({range.first, start - 1});
                }
                if (range.second > end) {
                    updated_ranges.push_back({end + 1, range.second});
                }
            }
        }

        // Replace the old ranges with the updated ones
        ranges = std::move(updated_ranges);
    }


    void removeLocalTokens(const std::string filename, int64_t start_offset, int64_t end_offset, const std::string &token_type) {

        if (token_type == "write") {
            // Update the write revoke table with proper overlap handling
            

            if (write_token_table.find(filename) != write_token_table.end()) {
                UpdateWriteTableAfterRevoking(write_token_table[filename], start_offset, end_offset);
            } else {
                std::cout << "Filename \"" << filename << "\" not found in write_token_table. No updates made." << std::endl;
            }


        } else if (token_type == "read") {
            // Update the read revoke table with proper overlap handling
            
            if (read_token_table.find(filename) != read_token_table.end()) {
                UpdateReadTableAfterRevoking(read_token_table[filename], start_offset, end_offset);
            } else {
                std::cout << "Filename \"" << filename << "\" not found in write_token_table. No updates made." << std::endl;
            }


        }
    }

    std::vector<int64_t> get_blocks_in_range(int64_t start_offset, int64_t end_offset) {

        int64_t start_block = start_offset / PFS_BLOCK_SIZE;
        int64_t end_block = end_offset / PFS_BLOCK_SIZE;
        std::vector<int64_t> blocks;
        for (int64_t b = start_block; b <= end_block; b++) {
            blocks.push_back(b);
        }
        return blocks;
    }

    void handleRevocation(const std::string &filename, int64_t start_offset, int64_t end_offset, const std::string &token_type, int stripe_width) {


        // Also remove or adjust any locally stored token info for these ranges if you store them client-side.
        std::lock_guard<std::mutex> lock(revoke_table_mutex);
        removeLocalTokens(filename, start_offset, end_offset, token_type);
    }


    // Function to check for overlaps
    bool NoOverlaps(const std::string& filename, int64_t start, int64_t end) {
        std::unique_lock<std::mutex> lock(revoke_table_mutex);

        // Check overlaps in read_token_table
        if (read_token_table.find(filename) != read_token_table.end()) {
            for (auto& token : read_token_table[filename]) {
                if (!(end < token.start_offset || start > token.end_offset)) {
                    std::cout << "Overlap with ReadToken: ["
                            << token.start_offset << ", " << token.end_offset << "]"
                            << " for client_id: " << token.client_id << std::endl;

                    if (!token.status) {
                        // Release the revoke_table_mutex lock while waiting
                        lock.unlock();
                        {
                            std::unique_lock<std::mutex> token_lock(*token.mutex);
                            token.cv->wait(token_lock, [&]() { return token.status; });
                        }
                        // Reacquire the revoke_table_mutex lock
                        lock.lock();
                    }
                }
            }
        }

        // Check overlaps in write_token_table
        if (write_token_table.find(filename) != write_token_table.end()) {
            for (auto& token : write_token_table[filename]) {
                if (!(end < token.start_offset || start > token.end_offset)) {
                    std::cout << "Overlap with WriteToken: ["
                            << token.start_offset << ", " << token.end_offset << "]"
                            << " for client_id: " << token.client_id << std::endl;

                    if (!token.status) {
                        // Release the revoke_table_mutex lock while waiting
                        lock.unlock();
                        {
                            std::unique_lock<std::mutex> token_lock(*token.mutex);
                            token.cv->wait(token_lock, [&]() { return token.status; });
                        }
                        // Reacquire the revoke_table_mutex lock
                        lock.lock();
                    }
                }
            }
        }

        std::cout << "No overlaps found for filename: " << filename
                << " in range [" << start << ", " << end << "]." << std::endl;
        return true;
    }


    void HandleResponse(const pfsmeta::ConnectResponse& response,
                        std::unique_ptr<grpc::ClientReaderWriter<pfsmeta::ConnectRequest, pfsmeta::ConnectResponse>>& stream) {

        std::string filename = response.filename();
        int64_t start_offset = response.start_offset();
        int64_t end_offset = response.end_offset();
        int requested_client_id = response.client_id();

        std::vector<int> fds = GetFDFromFilename(filename);

        int fd = fds[0];

        pfs_metadata metadata = GetMetadata(fd);
        int stripe_width = metadata.recipe.stripe_width;

        {

            if (response.revoke_request()) {
                std::string token_type = response.token_type();

                std::cout << "Received revocation request from MetaServer for fd: " << filename
                            << ", range: [" << start_offset << ", " << end_offset << "], type: " << token_type << std::endl;

            
                if(NoOverlaps(filename, start_offset, end_offset)) {
                    handleRevocation(filename, start_offset, end_offset, token_type, stripe_width);
                }


                pfsmeta::ConnectRequest ack_request;
                ack_request.set_message("Revoke acknowledged");
                ack_request.set_filename(filename);
                ack_request.set_start_offset(start_offset);
                ack_request.set_end_offset(end_offset);
                ack_request.set_token_type(token_type);
                ack_request.set_client_id(requested_client_id);

                if (!stream->Write(ack_request)) {
                    std::cerr << "Failed to send acknowledgment to MetaServer for revocation request." << std::endl;
                } else {
                    std::cout << "Acknowledgment sent to MetaServer for revocation request." << std::endl;
                }
            
            }
            else if(response.invalidate_request()){
                // Invalidation logic (non-overlapping token scenario)
                std::cout << "Received invalidation request for filename: " << filename 
                        << ", range [" << start_offset << ", " << end_offset << "]" << std::endl;

                cache.invalidateBlocks(filename, start_offset, end_offset, stripe_width, fileserver_stubs);

                // Not strictly required to ack invalidation, but if you want to:
                pfsmeta::ConnectRequest ack_request;
                ack_request.set_message("Invalidate acknowledged");
                ack_request.set_client_id(requested_client_id);
                if (!stream->Write(ack_request)) {
                    std::cerr << "Failed to send acknowledgment to MetaServer for invalidation request." << std::endl;
                } else {
                    std::cout << "Acknowledgment sent to MetaServer for invalidation request." << std::endl;
                }
            }

            else if(response.write_through_request()){
                // Write through logic (non-overlapping token scenario)
                std::cout << "Received Write through request for filename: " << filename 
                        << ", range [" << start_offset << ", " << end_offset << "]" << std::endl;
                
                cache.writeThrough(filename, start_offset, end_offset, stripe_width, fileserver_stubs);

                // Not strictly required to ack invalidation, but if you want to:
                pfsmeta::ConnectRequest ack_request;
                ack_request.set_message("Write through acknowledged");
                ack_request.set_client_id(requested_client_id);
                if (!stream->Write(ack_request)) {
                    std::cerr << "Failed to send acknowledgment to MetaServer for Write Through request." << std::endl;
                } else {
                    std::cout << "Acknowledgment sent to MetaServer for Write Through request." << std::endl;
                }

            }
        }
    }

    void StartStream(std::unique_ptr<grpc::ClientReaderWriter<pfsmeta::ConnectRequest, pfsmeta::ConnectResponse>>& stream) {
    std::thread([&, stream = stream.get()]() {
        while (!shutdown_signal_) {
            pfsmeta::ConnectResponse response;
            if (!stream->Read(&response)) {
                break; // Exit if stream ends from server side
            }
            HandleResponse(response, connect_stream);
        }
    }).detach();
}


public:
    Client() : cache() {};

    int getClientId() const {
        return client_id;
    }

    CacheManager& getCache() {
        return cache;
    }

    std::unique_ptr<pfsmeta::MetaServer::Stub>& getMetaServerStub() {
        return meta_server_stub;
    }

    bool isMetaServerStubInitialized() const {
        return meta_server_stub != nullptr;
    }

    void InitializeMetaSeverStub(const std::string& server_address) {
        meta_server_stub = pfsmeta::MetaServer::NewStub(grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials()));
    }

    std::unique_ptr<pfsfile::FileServer::Stub> InitializeFileSeverStub(const std::string& server_address) {
        return pfsfile::FileServer::NewStub(grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials()));
    }

    std::vector<std::unique_ptr<pfsfile::FileServer::Stub>>& getFileServerStubs() {
            return fileserver_stubs;
    }

    void AddReadToken(const std::string filename, int64_t start, int64_t end, bool status) {
        // std::lock_guard<std::mutex> lock(revoke_table_mutex);
        // read_token_table[filename].emplace_back(start, end);
        ReadToken token(client_id, start, end, status);


        // Lock the read_token_table and add the new token
        std::lock_guard<std::mutex> lock(revoke_table_mutex);
        read_token_table[filename].emplace_back(std::move(token));

        std::cout << "Added ReadToken for client_id: " << client_id
                << ", start_offset: " << start << ", end_offset: " << end
                << ", status: " << status << ", filename: " << filename << std::endl;

    }

    void AddWriteToken(const std::string filename, int64_t start, int64_t end, bool status) {

        WriteToken token(client_id, start, end, status);


        // Lock the write_token_table and add the new token
        std::lock_guard<std::mutex> lock(revoke_table_mutex);
        write_token_table[filename].emplace_back(std::move(token));

        std::cout << "Added WriteToken for client_id: " << client_id
                << ", start_offset: " << start << ", end_offset: " << end
                << ", status: " << status << ", filename: " << filename << std::endl;

    }

    void AddFileDescriptor(int fd, const std::string& filename) {
        std::lock_guard<std::mutex> lock(fd_mutex);
        fd_map[filename].push_back(fd);
    }

    std::vector<int> GetFDFromFilename(std::string filename) {
        std::vector<int> fds;
        std::lock_guard<std::mutex> lock(fd_mutex);
        auto it = fd_map.find(filename);
        if (it != fd_map.end()) {
            return it->second;
        }
        return fds;
    }

    bool ConnectToMetaserver() {
        if (!meta_server_stub) {
            std::cerr << "Error: meta_server_stub is null. Initialization failed." << std::endl;
            return false;
        }

        std::cout << "Starting ConnectToMetaserver stream." << std::endl;
        auto& connect_context = getConnectContext();

        connect_stream = meta_server_stub->ConnectToMetaserver(&connect_context);
        if (!connect_stream) {
            std::cerr << "Error: Failed to create ConnectToMetaserver stream." << std::endl;
            return false;
        }
        std::cout << "ConnectToMetaserver stream started successfully." << std::endl;

        pfsmeta::ConnectRequest initial_request;
        initial_request.set_message("Client connected");
        if (!connect_stream->Write(initial_request)) {
            std::cerr << "Error: Failed to send initial connection message." << std::endl;
            return false;
        }
        std::cout << "Sent initial connection message to MetaServer." << std::endl;

        pfsmeta::ConnectResponse initial_response;
        if (!connect_stream->Read(&initial_response)) {
            std::cerr << "Error: Failed to read initial response from MetaServer." << std::endl;
            return false;
        }
        {
            std::lock_guard<std::mutex> lock(client_id_mutex);
            client_id = initial_response.client_id();
        }
        connect_context.AddMetadata("client-id", std::to_string(client_id));
        std::cout << "Connected to MetaServer with client_id: " << client_id << std::endl;

        StartStream(connect_stream);
        return true;
    }

    bool CheckFileServerHealth(std::unique_ptr<pfsfile::FileServer::Stub>& file_stub) {
        grpc::ClientContext context;
        pfsfile::HealthCheckRequest request;
        pfsfile::HealthCheckResponse response;
        request.set_message("ping");

        grpc::Status status = file_stub->HealthCheck(&context, request, &response);
        if (status.ok() && response.message() == "pong") {
            std::cout << "FileServer is online." << std::endl;
            return true;
        } else {
            std::cerr << "FileServer health check failed: " << status.error_message() << std::endl;
            return false;
        }
    }

    bool InitializeServerStubs(const std::string& server_address, bool is_meta_server) {
        std::cout << "Creating channel to server at address: " << server_address << std::endl;

        if (is_meta_server) {
            InitializeMetaSeverStub(server_address);
            if (!meta_server_stub || !ConnectToMetaserver()) {
                std::cerr << "Error: MetaServer is not online." << std::endl;
                return false;
            }
            std::cout << "MetaServer stub initialized successfully." << std::endl;
        } else {
            std::unique_ptr<pfsfile::FileServer::Stub> fileserver_stub = InitializeFileSeverStub(server_address);
            if (!fileserver_stub || !CheckFileServerHealth(fileserver_stub)) {
                std::cerr << "Error: FileServer is not online." << std::endl;
                return false;
            }
            fileserver_stubs.push_back(std::move(fileserver_stub));
            std::cout << "FileServer stub initialized successfully." << std::endl;
        }
        return true;
    }

    void DisconnectFromMetaserver() {
        if (connect_stream) {
            // Gracefully end the stream from the client side if still active
            pfsmeta::ConnectRequest disconnect_request;
            disconnect_request.set_message("Client disconnecting");
            disconnect_request.set_client_id(client_id);

            connect_stream->Write(disconnect_request);
            connect_stream->WritesDone(); // Call once
            grpc::Status status = connect_stream->Finish(); // Call once
            if (status.ok()) {
                std::cout << "Disconnected from MetaServer successfully." << std::endl;
            } else {
                std::cerr << "Error during disconnection: " << status.error_message() << std::endl;
            }

            connect_stream.reset();
        }
    }



    void StopStream() { shutdown_signal_ = true; }

    // Function to cache metadata locally
    void addMetadata(int fd, const pfs_metadata& metadata) {
        std::lock_guard<std::mutex> lock(fd_metadata_mutex); // Ensure thread safety
        fd_to_metadata_map[fd] = metadata;
        std::cout << "Metadata cached for FD: " << fd << ", Filename: " << metadata.filename << std::endl;
    }

    pfs_metadata GetMetadata(int fd) {
        std::lock_guard<std::mutex> lock(fd_metadata_mutex); // Ensure thread safety
        if (fd_to_metadata_map.find(fd) != fd_to_metadata_map.end()) {
            return fd_to_metadata_map[fd];
        } else {
            throw std::runtime_error("Error: Metadata not found for FD: " + std::to_string(fd));
        }
    }

    void UpdateMetadata(int fd, int64_t offset, int64_t bytes_written) {
        std::lock_guard<std::mutex> lock(fd_metadata_mutex); // Ensure thread safety

        auto it = fd_to_metadata_map.find(fd);
        if (it != fd_to_metadata_map.end()) {
            pfs_metadata& metadata = it->second; // Get a reference to the metadata
            metadata.file_size = std::max(static_cast<int64_t>(metadata.file_size),  offset+bytes_written);
            metadata.mtime = static_cast<int64_t>(time(nullptr)); // Update modification time
        } else {
            throw std::runtime_error("Error: Metadata not found for FD: " + std::to_string(fd));
        }
    }



    // Merge overlapping or contiguous ranges for WriteToken
    void merge_write_token_ranges(std::vector<WriteToken>& tokens) {
        if (tokens.empty()) return;

        // Sort tokens based on start_offset
        std::sort(tokens.begin(), tokens.end(), [](const WriteToken& a, const WriteToken& b) {
            return a.start_offset < b.start_offset;
        });

        std::vector<WriteToken> merged_tokens;
        merged_tokens.push_back(tokens[0]);

        for (size_t i = 1; i < tokens.size(); ++i) {
            WriteToken& last = merged_tokens.back();
            if (tokens[i].start_offset <= last.end_offset + 1) {
                // Merge overlapping or contiguous ranges
                last.end_offset = std::max(last.end_offset, tokens[i].end_offset);
            } else {
                merged_tokens.push_back(tokens[i]);
            }
        }

        tokens = std::move(merged_tokens);
    }


    // Merge overlapping or contiguous ranges for ReadToken
    void merge_read_token_ranges(std::vector<ReadToken>& tokens) {
        if (tokens.empty()) return;

        // Sort tokens based on start_offset
        std::sort(tokens.begin(), tokens.end(), [](const ReadToken& a, const ReadToken& b) {
            return a.start_offset < b.start_offset;
        });

        std::vector<ReadToken> merged_tokens;
        merged_tokens.push_back(tokens[0]);

        for (size_t i = 1; i < tokens.size(); ++i) {
            ReadToken& last = merged_tokens.back();
            if (tokens[i].start_offset <= last.end_offset + 1) {
                // Merge overlapping or contiguous ranges
                last.end_offset = std::max(last.end_offset, tokens[i].end_offset);
            } else {
                merged_tokens.push_back(tokens[i]);
            }
        }

        tokens = std::move(merged_tokens);
    }


    std::vector<std::pair<int64_t, int64_t>> get_missing_ranges(
            const std::vector<std::pair<int64_t, int64_t>>& tokens,
            int64_t start_offset, int64_t end_offset) {
        std::vector<std::pair<int64_t, int64_t>> missing_ranges;

        // Sort tokens for consistency (if not already sorted)
        auto sorted_tokens = tokens;
        std::sort(sorted_tokens.begin(), sorted_tokens.end());

        int64_t current_start = start_offset;

        for (const auto& token : sorted_tokens) {
            if (current_start > end_offset) {
                break;  // Entire requested range is covered
            }

            if (token.second < current_start) {
                // Token is before the current range; ignore it
                continue;
            }

            if (token.first > current_start) {
                // There's a gap before the current token
                missing_ranges.emplace_back(current_start, std::min(end_offset, token.first - 1));
            }

            // Update the current start to the end of the current token + 1
            current_start = std::max(current_start, token.second + 1);
        }

        // If there's any range left uncovered at the end
        if (current_start <= end_offset) {
            missing_ranges.emplace_back(current_start, end_offset);
        }

        return missing_ranges;
    }

    std::vector<std::pair<int64_t, int64_t>> getMissingReadRanges(std::string filename, int64_t start_offset, int64_t end_offset) {
        std::lock_guard<std::mutex> req_lock(revoke_table_mutex);

        auto& read_tokens = read_token_table[filename];
        merge_read_token_ranges(read_tokens);

        std::vector<std::pair<int64_t, int64_t>> ranges;
        for (const auto& token : read_tokens) {
            ranges.emplace_back(token.start_offset, token.end_offset);
        }
          // Merge overlapping ranges
        return get_missing_ranges(ranges, start_offset, end_offset);
    }

    std::vector<std::pair<int64_t, int64_t>> getMissingWriteRanges(const std::string filename, int64_t start_offset, int64_t end_offset) {
        std::lock_guard<std::mutex> req_lock(revoke_table_mutex);

        auto& write_tokens = write_token_table[filename];
        merge_write_token_ranges(write_tokens);  // Merge overlapping ranges
        std::vector<std::pair<int64_t, int64_t>> ranges;
        for (const auto& token : write_tokens) {
            ranges.emplace_back(token.start_offset, token.end_offset);
        }
        return get_missing_ranges(ranges, start_offset, end_offset);
    }

    void updateWriteTokenStatusOnOverlap(int64_t start_offset,
                                int64_t end_offset,
                                std::vector<WriteToken>& tokens, bool status) {
        bool updated = false;

        // Check and update read tokens
        for (auto& token : tokens) {
            if (start_offset >= token.start_offset && end_offset <= token.end_offset) {
                token.status = status;
                std::cout << "Updated WriteToken ["
                        << token.start_offset << ", " << token.end_offset
                        << "] for client_id: " << token.client_id
                        << " to status: "<<status << std::endl;
                if(token.status){
                    std::lock_guard<std::mutex> token_lock(*token.mutex);
                    token.cv->notify_all(); // Notify all threads waiting on this token
                }
                updated = true;
            }
        }
    }

    void updateReadTokenStatusOnOverlap(int64_t start_offset,
                                int64_t end_offset,
                                std::vector<ReadToken>& tokens, bool status) {
        bool updated = false;

        // Check and update read tokens
        for (auto& token : tokens) {
            if (start_offset >= token.start_offset && end_offset <= token.end_offset) {
                token.status = status;
                std::cout << "Updated ReadToken ["
                        << token.start_offset << ", " << token.end_offset
                        << "] for client_id: " << token.client_id
                        << " to status: "<<status << std::endl;
                if(token.status){
                    std::lock_guard<std::mutex> token_lock(*token.mutex);
                    token.cv->notify_all(); // Notify all threads waiting on this token
                }
                updated = true;
            }
        }
    }

    void updateReadTokenStatusBeforeRead(const std::string filename, int start_offset, int end_offset) {
        std::lock_guard<std::mutex> req_lock(revoke_table_mutex);
        auto& read_tokens = read_token_table[filename];
        merge_read_token_ranges(read_tokens);  // Merge overlapping ranges
        updateReadTokenStatusOnOverlap(start_offset, end_offset, read_tokens, false);
    }

    void updateReadTokenStatusAfterRead(const std::string filename, int start_offset, int end_offset) {
        std::lock_guard<std::mutex> req_lock(revoke_table_mutex);
        auto& read_tokens = read_token_table[filename];
        updateReadTokenStatusOnOverlap(start_offset, end_offset, read_tokens, true);
    }

    void updateWriteTokenStatusBeforeWrite(const std::string filename, int start_offset, int end_offset) {
        std::lock_guard<std::mutex> req_lock(revoke_table_mutex);
        auto& write_tokens = write_token_table[filename];
        merge_write_token_ranges(write_tokens);  // Merge overlapping ranges
        updateWriteTokenStatusOnOverlap(start_offset, end_offset, write_tokens, false);
    }


    void updateWriteTokenStatusAfterWrite(const std::string filename, int start_offset, int end_offset) {
        std::lock_guard<std::mutex> req_lock(revoke_table_mutex);
        auto& write_tokens = write_token_table[filename];
        updateWriteTokenStatusOnOverlap(start_offset, end_offset, write_tokens, true);
    }

    std::unordered_set<size_t> FetchCacheableBlocks(const std::string filename, int64_t start_offset, int64_t end_offset, const std::string token_type, int client_id){
        // Fetch cachable blocks from MetaServer on a miss
        std::unordered_set<size_t> cacheable_blocks;
        grpc::ClientContext context;
        pfsmeta::CacheableBlocksRequest fetch_request;
        pfsmeta::CacheableBlocksResponse fetch_response;

        fetch_request.set_filename(filename);
        fetch_request.set_start_offset(start_offset);
        fetch_request.set_end_offset(end_offset);
        fetch_request.set_token_type(token_type);
        fetch_request.set_client_id(client_id);

        grpc::Status status = meta_server_stub->FetchCacheableBlocks(&context, fetch_request, &fetch_response);
        if (!status.ok() || fetch_response.status() != 0) {
            std::cerr << "Error: Failed to fetch cachable blocks from MetaServer. " << fetch_response.message() << "\n";
            return cacheable_blocks;
        }

        for (const auto& block : fetch_response.cacheable_blocks()) {
            cacheable_blocks.insert(block);
        }
        return cacheable_blocks;
    } 

    int sendGrantAck(const std::string filename, const std::pair<int64_t, int64_t>& range, bool isWrite){
        pfsmeta::ConnectRequest grant_request;
        grant_request.set_message("Grant acknowledged");
        grant_request.set_filename(filename);
        grant_request.set_start_offset(range.first);
        grant_request.set_end_offset(range.second);
        if(isWrite){
            grant_request.set_token_type("write");
        }
        else{
            grant_request.set_token_type("read");
        }
        grant_request.set_client_id(getClientId());

        if (!connect_stream->Write(grant_request)) {
            std::cerr << "Error: Failed to send Grant Acknowledged to MetaServer for range: ["
                      << range.first << ", " << range.second << "]." << std::endl;
            return -1;
        }
        return 0;
    }

    size_t readBlocks(const std::string &filename, size_t offset, size_t num_bytes, char *buf, int stripe_width, size_t file_size) {
        size_t block_size = PFS_BLOCK_SIZE;
        size_t remaining_bytes = num_bytes;
        size_t current_offset = offset;
        char *current_buf = buf;
        size_t total_bytes_read = 0;
        bool fetched_cacheable_blocks = false;
        std::unordered_set<size_t> cacheable_blocks;


        {
            std::lock_guard<std::mutex> lock(revoke_table_mutex);

            while (remaining_bytes > 0) {
                // Determine the block and stripe details
                size_t block_number = current_offset / block_size;
                int stripe_index = (block_number / STRIPE_BLOCKS) % stripe_width;
                int stripe_number = block_number / (STRIPE_BLOCKS * stripe_width);
                int block_within_stripe = block_number % STRIPE_BLOCKS;

                // Calculate the server start offset
                size_t server_start_offset = stripe_number * STRIPE_BLOCKS * block_size + block_within_stripe * block_size;

                // Calculate the block offset and bytes to read
                size_t block_offset = current_offset % block_size;
                size_t bytes_to_read = std::min(remaining_bytes, block_size - block_offset);

                if (remaining_bytes == bytes_to_read) {
                    bytes_to_read = std::min(remaining_bytes, file_size - current_offset);
                    std::cout << "++++++++++++++++++++++++++++++bytes_to_read: " << bytes_to_read
                            << "  remaining_bytes: " << remaining_bytes
                            << "   current_offset: " << current_offset << std::endl;
                }

                // if (current_offset + bytes_to_read > file_size) {
                //     bytes_to_read = file_size - current_offset;
                //     remaining_bytes = bytes_to_read;
                // }

                std::vector<char> block_data;
                // Check if the block is in the cache
                if (cache.isCacheEnabled() && cache.cacheLookUp(filename, block_number, block_data)) {
                    // Cache hit
                    global_execstats.num_read_hits++;
                    memcpy(current_buf, block_data.data() + block_offset, bytes_to_read);
                    std::cout << "Cache hit: Reading block " << block_number << " from cache.\n";
                } else {
                    if (!fetched_cacheable_blocks) {
                        cacheable_blocks = FetchCacheableBlocks(filename, offset, offset + num_bytes - 1, "read", getClientId());
                        fetched_cacheable_blocks = true;
                    }

                    // Prepare ReadFileRequest
                    pfsfile::ReadFileRequest read_request;
                    read_request.set_filename(filename);
                    read_request.set_start_offset(server_start_offset);
                    read_request.set_num_bytes(block_size); // Fetch the entire block

                    // Fetch data from the server
                    pfsfile::ReadFileResponse read_response;
                    grpc::ClientContext context;
                    auto &file_stub = fileserver_stubs[stripe_index];
                    grpc::Status status = file_stub->ReadFile(&context, read_request, &read_response);

                    if (!status.ok() || read_response.status() != 0) {
                        std::cerr << "Error: Failed to read from server " << stripe_index << ". "
                                << status.error_message() << std::endl;
                        return total_bytes_read; // Return bytes read so far
                    }

                    const std::string &block_data = read_response.data();

                    // Cache the block if it's in the cacheable list
                    if (cache.isCacheEnabled() && cacheable_blocks.find(block_number) != cacheable_blocks.end()) {
                        std::vector<char> data_to_cache(block_data.begin(), block_data.end());
                        cache.writeBlock(filename, block_number, data_to_cache, false, stripe_width, getFileServerStubs());
                    }

                    // Copy the relevant bytes to the buffer
                    memcpy(current_buf, block_data.c_str() + block_offset, bytes_to_read);
                }

                // Update counters
                current_buf += bytes_to_read;
                current_offset += bytes_to_read;
                remaining_bytes -= bytes_to_read;
                total_bytes_read += bytes_to_read;

                // Stop if EOF is reached
                if (current_offset >= file_size) {
                    return total_bytes_read; // EOF
                }
            }
        }

        return total_bytes_read;
    }

    size_t performBlockWrites(const std::string &filename, const char *buf, size_t num_bytes, size_t offset, int stripe_width, int mode) {

        const int block_size = PFS_BLOCK_SIZE;
        const int stripe_blocks = STRIPE_BLOCKS;

        size_t remaining_bytes = num_bytes;
        size_t current_offset = offset;
        const char *current_buf = buf;
        size_t total_bytes_written = 0;
        bool fetched_cacheable_blocks = false;
        std::unordered_set<size_t> cacheable_blocks;
        {
            std::lock_guard<std::mutex> lock(revoke_table_mutex);


            while (remaining_bytes > 0) {
                // Calculate the block number and its properties
                size_t block_number = current_offset / block_size;
                int stripe_index = (block_number / stripe_blocks) % stripe_width;

                // Calculate the server start offset independently
                int stripe_number = block_number / (stripe_blocks * stripe_width);
                int block_within_stripe = block_number % stripe_blocks;
                size_t server_start_offset = stripe_number * stripe_blocks * block_size + block_within_stripe * block_size;

                // Calculate the offset within the block
                size_t block_offset = current_offset % block_size;
                size_t bytes_to_write = std::min(remaining_bytes, block_size - block_offset);

                std::vector<char> block_data;

                if (cache.isCacheEnabled() && cache.cacheLookUp(filename, block_number, block_data)) {
                    // Cache hit: Update the block in the cache
                    global_execstats.num_write_hits++;
                    memcpy(&block_data[block_offset], current_buf, bytes_to_write);
                    cache.writeBlock(filename, block_number, block_data, true, stripe_width, getFileServerStubs());
                    std::cout << "Cache hit: Block " << block_number << " updated in cache.\n";
                } else {
                    if (!fetched_cacheable_blocks) {
                        std::cout<<"Fetching cacheable bloks"<<std::endl;
                        cacheable_blocks = FetchCacheableBlocks(filename, offset, offset + num_bytes - 1, "write", getClientId());
                        std::cout<<"Fetching cacheable bloks sucessfully"<<std::endl;

                        fetched_cacheable_blocks = true;
                    }

                    // Prepare ReadFileRequest
                    pfsfile::ReadFileRequest read_request;
                    read_request.set_filename(filename);
                    read_request.set_start_offset(server_start_offset);
                    read_request.set_num_bytes(block_size); // Fetch the entire block
                    read_request.set_mode(mode);

                    // Fetch data from the server
                    pfsfile::ReadFileResponse read_response;
                    grpc::ClientContext context;
                    auto &file_stub = fileserver_stubs[stripe_index];
                    grpc::Status status = file_stub->ReadFile(&context, read_request, &read_response);

                    if (!status.ok() || read_response.status() != 0) {
                        std::cerr << "Error: Failed to read from server " << stripe_index << ". "
                                << status.error_message() << std::endl;
                        return total_bytes_written; // Return bytes written so far
                    }

                    std::string block_data = read_response.data();

                    std::string writable_block_data = block_data;
                    if (block_data.empty()) {
                        writable_block_data = std::string(block_size, '\0');
                    }

                    // Copy the relevant bytes to the writable block
                    memcpy(&writable_block_data[block_offset], current_buf, bytes_to_write);

                    // Cache the block if it's in the cacheable list
                    if (cache.isCacheEnabled() && cacheable_blocks.find(block_number) != cacheable_blocks.end()) {
                        std::vector<char> data_to_cache(writable_block_data.begin(), writable_block_data.end());
                        cache.writeBlock(filename, block_number, data_to_cache, true, stripe_width, getFileServerStubs());
                        std::cout << "Cache miss: Block " << block_number << " cached after update.\n";
                    }
                    else{
                        // Optionally, perform server write here if needed
                        // Example code for writing to the server:
                        pfsfile::WriteFileRequest write_request;
                        write_request.set_filename(filename);
                        write_request.set_start_offset(server_start_offset);
                        write_request.set_data(writable_block_data);
                        write_request.set_num_bytes(block_size);

                        pfsfile::WriteFileResponse write_response;
                        grpc::ClientContext write_context;
                        grpc::Status write_status = file_stub->WriteFile(&write_context, write_request, &write_response);

                        if (!write_status.ok() || write_response.status() != 0) {
                            std::cerr << "Error: Write to FileServer failed.\n";
                            return total_bytes_written;
                        }
                    }
                }

                // Adjust for the next iteration
                current_buf += bytes_to_write;
                current_offset += bytes_to_write;
                remaining_bytes -= bytes_to_write;
                total_bytes_written += bytes_to_write;
            }
        }

        std::cout<<"Bytes Writen: "<<total_bytes_written<<" filename: "<<filename<< " offset: "<<offset<< " num_bytes: "<< num_bytes<<std::endl;;

        return total_bytes_written;
    }

    int close(int fd){
        std::lock_guard<std::mutex> fd_lock(fd_mutex);
        std::lock_guard<std::mutex> metadata_lock(fd_metadata_mutex);
        std::lock_guard<std::mutex> revoke_lock(revoke_table_mutex);

        // Check if the fd exists in the fd_to_metadata_map
        if (fd_to_metadata_map.find(fd) == fd_to_metadata_map.end()) {
            std::cout << "File descriptor not found in metadata map." << std::endl;
            return -1;
        }

        // Get metadata associated with this fd
        pfs_metadata& metadata = fd_to_metadata_map.at(fd);
        std::string filename = metadata.filename;

        // Fetch the stripe width from the metadata
        int stripe_width = metadata.recipe.stripe_width;

        // On metaserver side: Delete tokens and update block client cache
        pfsmeta::DeleteClientTokensRequest request;
        request.set_client_id(client_id);
        request.set_filename(filename);
        request.set_fd(fd);

        pfsmeta::DeleteClientTokensResponse response;
        grpc::ClientContext context;
        grpc::Status status = meta_server_stub->DeleteTokensForClient(&context, request, &response);

        if (!status.ok()) {
            std::cerr << "Error in DeleteTokensForClient RPC: " << status.error_message() << std::endl;
            return -1;
        }

        if (response.success() != 0) {
            std::cerr << "Error in DeleteTokensForClient: " << response.message() << std::endl;
            return -1;
        }

        // On client side: Remove tokens
        if (read_token_table.find(filename) != read_token_table.end()) {
            read_token_table.erase(filename);
        }
        if (write_token_table.find(filename) != write_token_table.end()) {
            write_token_table.erase(filename);
        }

        cache.printCache();
        // Invalidate cache for the file and pass stripe width
        cache.removeFileBlocks(filename, stripe_width, fileserver_stubs);

        // Update mappings
        fd_to_metadata_map.erase(fd);
        fd_map[filename].erase(
            std::remove(fd_map[filename].begin(), fd_map[filename].end(), fd),
            fd_map[filename].end());

        if (fd_map[filename].empty()) {
            fd_map.erase(filename);
        }

        // Add metadata to filename_to_metadata_map
        filename_to_metadata_map[filename] = metadata;

        return 0; // Success
    }

    int deleteFile(const char* filename) {

        if (!filename || std::string(filename).empty()) {
            std::cerr << "Error: Invalid filename provided to deleteFile." << std::endl;
            return -1;
        }
        std::cout<<"Deleting file : "<<filename<<std::endl;;

        std::lock_guard<std::mutex> metadata_lock(fd_metadata_mutex);

        if (filename_to_metadata_map.find(filename) == filename_to_metadata_map.end()) {
            std::cerr << "File descriptor not found in metadata map." << std::endl;
            return -1;
        }

        // Get metadata associated with this filename
        pfs_metadata& metadata = filename_to_metadata_map.at(filename);
        std::cout<<"Deleting file : "<<filename<< " found metadata"<<std::endl;


        // Fetch the stripe width and server map from the metadata
        int stripe_width = metadata.recipe.stripe_width;

        // Prepare the CheckClientsWithFile request
        pfsmeta::CheckClientsWithFileRequest request;
        request.set_filename(filename);
        request.set_client_id(client_id); // Assuming client_id is a global or class member variable

        // Response object
        pfsmeta::CheckClientsWithFileResponse response;
        grpc::ClientContext context;

        // Call the CheckClientsWithFile RPC
        grpc::Status status = meta_server_stub->CheckClientsWithFile(&context, request, &response);

        if (!status.ok()) {
            std::cerr << "Error in CheckClientsWithFile RPC: " << status.error_message() << std::endl;
            return -1;
        }

        if (response.is_open()) {
            std::cerr << "File cannot be deleted: other clients still have it open." << std::endl;
            return -1;
        }

        std::cout<<"Deleting file : "<<filename<< " no client has opened this file"<<std::endl;


        // Send delete requests only to servers in the stripe
        for (int stripe_index = 0; stripe_index < stripe_width; stripe_index++) {

            // Prepare the delete file request
            pfsfile::DeleteFileRequest fs_request;
            fs_request.set_filename(filename);

            // Response object
            pfsfile::DeleteFileResponse fs_response;
            grpc::ClientContext fs_context;

            // Call the DeleteFile RPC on the specific server
            grpc::Status fs_status = fileserver_stubs[stripe_index]->DeleteFile(&fs_context, fs_request, &fs_response);

            if (!fs_status.ok() || fs_response.status() != 0) {
                std::cerr << "Error deleting file on file server ID " << stripe_index 
                          << ": " << fs_response.message() << std::endl;
                return -1;
            }
        }

        // Remove the metadata from filename_to_metadata_map
        if (filename_to_metadata_map.find(filename) != filename_to_metadata_map.end()) {
            filename_to_metadata_map.erase(filename);
        }

        std::cerr << "File deleted successfully." << std::endl;

        return 0; // Success
    }

    void FinishClient() {

        // Clean up internal data structures
        {
            std::lock_guard<std::mutex> fd_lock(fd_mutex);
            fd_map.clear();
        }

        {
            std::lock_guard<std::mutex> meta_lock(fd_metadata_mutex);
            fd_to_metadata_map.clear();
            filename_to_metadata_map.clear();
        }

        {
            std::lock_guard<std::mutex> revoke_lock(revoke_table_mutex);
            read_token_table.clear();
            write_token_table.clear();
        }

        // Clear file server stubs
        fileserver_stubs.clear();

        // Stop any ongoing streams
        StopStream();
    }


};



// Singleton function to access Client
Client& GetClient() {
    static Client instance;
    return instance;
}


int pfs_initialize() {

    Client& client = GetClient();
    // Read pfs_list.txt
    std::ifstream pfs_list("../pfs_list.txt");
    if (!pfs_list.is_open()) {
        std::cerr << "Error: Cannot open pfs_list.txt." << std::endl;
        return -1;
    }
    // Check if all servers (NUM_FILE_SERVERS + 1) are online
    std::string line;
    std::getline(pfs_list, line);
    // Connect with metaserver using gRPC
    if (!client.InitializeServerStubs(line, true)) {
        pfs_list.close();
        std::cerr << "Error: Failed to connect to MetaServer." << std::endl;
        return -1;
    }

    metaserver_api_temp();
   
    // Connect with all fileservers (NUM_FILE_SERVERS) using gRPC
    for (int i = 0; i < NUM_FILE_SERVERS; ++i) {
        std::getline(pfs_list, line);
        if (!client.InitializeServerStubs(line, false)) {
            pfs_list.close();
            std::cerr << "Error: Failed to connect to Fileserver "<< i << std::endl;
            return -1;
        }

        fileserver_api_temp();
    }
    pfs_list.close();
    std::cout<<"client_id1: "<<client.getClientId()<<std::endl;

    return client.getClientId();
}

int pfs_finish(int client_id) {
    Client& client = GetClient();

    if (client_id != client.getClientId()) {
        std::cerr << "Error: Provided client_id does not match the initialized client ID." << std::endl;
        return -1;
    }

    // Disconnect from MetaServer
    try {
        client.DisconnectFromMetaserver();
    } catch (const std::exception& e) {
        std::cerr << "Error during MetaServer disconnection: " << e.what() << std::endl;
        // Continue with cleanup even if disconnection fails
    }

    // Clean up internal data structures
    client.FinishClient();

    std::cout << "PFS client finalized successfully." << std::endl;

    return 0;
}


int pfs_create(const char *filename, int stripe_width) {
    Client& client = GetClient();
    if (!client.isMetaServerStubInitialized()) {
        std::cerr << "Error: MetaServer stub is not initialized." << std::endl;
        return -1;
    }

    std::cout << "Requesting to create file: " << filename << " with stripe width: " << stripe_width << std::endl;

    pfsmeta::CreateFileRequest create_request;
    create_request.set_filename(filename);
    create_request.set_stripe_width(stripe_width);

    pfsmeta::CreateFileResponse create_response;
    grpc::ClientContext context;

    grpc::Status status = client.getMetaServerStub()->CreateFile(&context, create_request, &create_response);

    if (!status.ok()) {
        std::cerr << "Error: Failed to create file " << filename << ". " << status.error_message() << std::endl;
        return -1;
    }

    if (create_response.status() != 0) {
        std::cerr << "MetaServer error: " << create_response.message() << std::endl;
        return -1;
    }

    std::cout << "File created successfully: " << filename << std::endl;
    return 0;
}


int pfs_open(const char *filename, int mode) {
    Client& client = GetClient();
    if (!client.isMetaServerStubInitialized()) {
        std::cerr << "Error: MetaServer stub is not initialized." << std::endl;
        return -1;
    }

    std::cout << "Requesting to open file: " << filename << " with mode: " << mode << std::endl;

    std::vector<int> fds = client.GetFDFromFilename(filename);
    if(!fds.empty()){
        std::cout << "Requested is already opened by this client"<< std::endl;
        return -1;
    }

    pfsmeta::OpenFileRequest open_request;
    open_request.set_filename(filename);
    open_request.set_mode(mode);

    pfsmeta::OpenFileResponse open_response;
    grpc::ClientContext context;

    grpc::Status status = client.getMetaServerStub()->OpenFile(&context, open_request, &open_response);

    if (!status.ok()) {
        std::cerr << "Error: Failed to open file " << filename << ". " << status.error_message() << std::endl;
        return -1;
    }

    if (open_response.status() != 0) {
        std::cerr << "MetaServer error: " << open_response.message() << std::endl;
        return -1;
    }

    int fd = open_response.fd();
    std::cout << "File opened successfully. FD: " << fd << std::endl;

    client.AddFileDescriptor(fd, filename);

    // Cache metadata locally for future requests
    pfs_metadata metadata;
    strncpy(metadata.filename, open_response.metadata().filename().c_str(), sizeof(metadata.filename));
    metadata.file_size = open_response.metadata().file_size();
    metadata.ctime = open_response.metadata().ctime();
    metadata.mtime = open_response.metadata().mtime();
    metadata.recipe.stripe_width = open_response.metadata().recipe().stripe_width();
    metadata.mode = open_response.metadata().mode();

    std::cout<<"stripe_width: "<<metadata.recipe.stripe_width<<std::endl;

    client.addMetadata(fd, metadata);

    return fd;
}

int pfs_read(int fd, void *buf, size_t num_bytes, off_t offset) {
    // ...
    Client& client = GetClient();

    // Check client cache
    // cache_func_temp();
    if (client.getClientId() == -1) {
        std::cerr << "Error: Client is not initialized. Invalid client_id." << std::endl;
        return -1;
    }

    if(fd<0){
        std::cerr << "Error: Invalid file descriptor " << fd << "." << std::endl;
        return -1;
    }

    pfs_metadata meta_data = client.GetMetadata(fd);
    if(meta_data.mode!=1 && meta_data.mode!=2){
        std::cerr << "Error: File descriptor " << fd << " is not opened in read-write/ read mode." << std::endl;
        return -1;
    }

    std::string filename = meta_data.filename;
    // try {
    //     filename = client.GetFilenameFromFD(fd);
    // } catch (const std::exception& e) {
    //     std::cerr << e.what() << std::endl;
    //     return -1;
    // }

    // Step 2: Define the start and end offsets for the read request
    int64_t start_offset = offset;
    int64_t end_offset = offset + num_bytes - 1;
    std::vector<std::pair<int64_t, int64_t>> missing_ranges = client.getMissingReadRanges(filename, start_offset, end_offset);

    pfs_metadata metadata; 
    pfs_fstat(fd, &metadata);

    size_t file_size = metadata.file_size;
        std::cout<<"Before File size: "<<file_size<<" filename: "<<filename<<std::endl;

    if(!missing_ranges.empty()){
        for (const auto& range : missing_ranges) {
            // Create the TokenRequest message
            pfsmeta::TokenRequest token_request;
            token_request.set_fd(fd);
            // token_request.set_client_id(client_id);
            token_request.set_start_offset(range.first);
            token_request.set_end_offset(range.second);
            token_request.set_token_type("read");
            token_request.set_offset(static_cast<size_t>(offset));

            // Prepare the response
            pfsmeta::TokenResponse token_response;
            grpc::ClientContext context;

            std::cout << "Requesting Read Token for fd: " << fd
                    << " from " << range.first << " to " << range.second << std::endl;

            // Add client_id to metadata
            context.AddMetadata("client-id", std::to_string(client.getClientId()));
            grpc::Status status = client.getMetaServerStub()->RequestToken(&context, token_request, &token_response);

            
            if (!status.ok() || token_response.status() != 0) {
                std::cerr << "Read Token request denied: " << token_response.message() << std::endl;
                return -1;
            }

            client.AddReadToken(filename, range.first, range.second, false);
            std::cout << "sending ack for Read Token for fd: " << fd
                    << " from " << range.first << " to " << range.second << std::endl;
            if(client.sendGrantAck(filename, range, false) != 0){
                std::cout << "Failed to send Read Token granted ack for fd: " << fd << " from " << start_offset << " to " << end_offset << std::endl;
            }
            file_size = token_response.file_size();
            std::cout<<"File size: "<<file_size<<" filename: "<<filename<<std::endl;;

            std::cout << "Read Token granted for fd: " << fd << " from " << start_offset << " to " << end_offset << std::endl;
        }
        client.updateReadTokenStatusBeforeRead(filename, start_offset, end_offset);
    }

    
    int stripe_width = meta_data.recipe.stripe_width;

    int bytes_read = client.readBlocks(filename, offset, num_bytes, reinterpret_cast<char *>(buf), stripe_width, file_size);
    client.updateReadTokenStatusAfterRead(filename, start_offset, end_offset);
    std::cout<<"Bytes Read: "<<bytes_read<<" filename: "<<filename<< " offset: "<<offset<< " num_bytes: "<< num_bytes<<std::endl;;


    return bytes_read;

}


int pfs_write(int fd, const void *buf, size_t num_bytes, off_t offset) {
    // ...
    Client& client = GetClient();
    // cache_func_temp();

     if (client.getClientId() == -1) {
        std::cerr << "Error: Client is not initialized. Invalid client_id." << std::endl;
        return -1;
    }

    if(fd<0){
        std::cerr << "Error: Invalid file descriptor " << fd << "." << std::endl;
        return -1;
    }

    pfs_metadata metadata = client.GetMetadata(fd);
    if(metadata.mode!=2){
        std::cerr << "Error: File descriptor " << fd << " is not opened in read-write mode." << std::endl;
        return -1;
    }

    std::string filename = metadata.filename;
    size_t file_size = metadata.file_size;

    // Step 2: Define the start and end offsets for the read request
    int64_t start_offset = offset;
    int64_t end_offset = offset + num_bytes - 1;
    std::vector<std::pair<int64_t, int64_t>> missing_ranges = client.getMissingWriteRanges(filename, start_offset, end_offset);

    if(!missing_ranges.empty()){
        for (const auto& range : missing_ranges) {
            // Create the TokenRequest message
            pfsmeta::TokenRequest token_request;
            token_request.set_fd(fd);
            // token_request.set_client_id(client_id);
            token_request.set_start_offset(range.first);
            token_request.set_end_offset(range.second);
            token_request.set_token_type("write");
            token_request.set_offset(static_cast<size_t>(offset));

            // Prepare the response
            pfsmeta::TokenResponse token_response;
            grpc::ClientContext context;

            std::cout << "Requesting Write Token for fd: " << fd
                    << " from " << range.first << " to " << range.second << std::endl;

            // Add client_id to metadata
            context.AddMetadata("client-id", std::to_string(client.getClientId()));
            grpc::Status status = client.getMetaServerStub()->RequestToken(&context, token_request, &token_response);

            
            if (!status.ok() || token_response.status() != 0) {
                std::cerr << "Write Token request denied: " << token_response.message() << std::endl;
                return -1;
            }

            client.AddWriteToken(filename, range.first, range.second, false);
            client.AddReadToken(filename, range.first, range.second, true);
            if(client.sendGrantAck(filename, range, true) != 0){
                std::cout << "Failed to send Write Token granted ack for fd: " << fd << " from " << start_offset << " to " << end_offset << std::endl;
            }

            file_size = token_response.file_size();
            std::cout << "Write Token granted for fd: " << fd << " from " << start_offset << " to " << end_offset << std::endl;
        }

        client.updateWriteTokenStatusBeforeWrite(filename, start_offset, end_offset);

    }

    // size_t file_size = meta_data.file_size;
    int stripe_width = metadata.recipe.stripe_width;



    // Perform block writes
    size_t bytes_written = client.performBlockWrites(filename, reinterpret_cast<const char *>(buf), num_bytes, offset, stripe_width, metadata.mode);

    if (bytes_written < num_bytes) {
        std::cerr << "Error: Failed to write all bytes. Only " << bytes_written << " bytes written.\n";
        return -1;
    }

    client.UpdateMetadata(fd, offset, bytes_written);
    metadata.file_size = std::max(file_size, offset+bytes_written);

    // Update metadata on the MetaServer
    
    pfsmeta::UpdateMetadataRequest update_request;
    update_request.set_filename(metadata.filename);
    update_request.set_file_size(metadata.file_size);

    pfsmeta::UpdateMetadataResponse update_response;
    grpc::ClientContext update_context;

    grpc::Status update_status = client.getMetaServerStub()->UpdateMetadata(&update_context, update_request, &update_response);

    if (!update_status.ok() || update_response.status() != 0) {
        std::cerr << "Error: Failed to update metadata on MetaServer. " << update_response.message() << std::endl;
        return -1;
    }

    client.updateWriteTokenStatusAfterWrite(filename, start_offset, end_offset);

    return num_bytes;
}

int pfs_close(int fd) {

    Client& client = GetClient();
    // cache_func_temp();

     if (client.getClientId() == -1) {
        std::cerr << "Error: Client is not initialized. Invalid client_id." << std::endl;
        return -1;
    }

    if(fd<0){
        std::cerr << "Error: Invalid file descriptor " << fd << "." << std::endl;
        return -1;
    }

    return client.close(fd);
}


int pfs_delete(const char *filename) {

    Client& client = GetClient();
    // cache_func_temp();

     if (client.getClientId() == -1) {
        std::cerr << "Error: Client is not initialized. Invalid client_id." << std::endl;
        return -1;
    }

    return client.deleteFile(filename);
}

int pfs_fstat(int fd, struct pfs_metadata *meta_data) {
    Client& client = GetClient();

    // Check if the file descriptor is valid
    if (fd < 0) {
        std::cerr << ": Invalid file descriptor " << fd << "." << std::endl;
        return -1;
    }

    // Prepare the FstatRequest
    pfsmeta::FstatRequest fstat_request;
    fstat_request.set_fd(fd);

    // Response for the RPC
    pfsmeta::FstatResponse fstat_response;
    grpc::ClientContext context;

    // Perform the RPC
    grpc::Status status = client.getMetaServerStub()->Fstat(&context, fstat_request, &fstat_response);

    // Check RPC status
    if (!status.ok()) {
        std::cerr << "Error: Failed to fetch metadata for FD " << fd << ". " << status.error_message() << std::endl;
        return -1;
    }

    // Check response status
    if (fstat_response.status() != 0) {
        std::cerr << "MetaServer error: " << fstat_response.message() << std::endl;
        return -1;
    }

    // Populate the metadata struct
    const auto& metadata_response = fstat_response.metadata();

    // Copy filename
    strncpy(meta_data->filename, metadata_response.filename().c_str(), sizeof(meta_data->filename));
    std::cout << "Filename: " << meta_data->filename << "\n";

    // Copy file size
    meta_data->file_size = metadata_response.file_size();
    std::cout << "File Size: " << meta_data->file_size << " bytes\n";

    // Copy creation time
    meta_data->ctime = metadata_response.ctime();
    std::cout << "Creation Time: " << std::ctime(&meta_data->ctime);

    // Copy modified time
    meta_data->mtime = metadata_response.mtime();
    std::cout << "Modified Time: " << std::ctime(&meta_data->mtime);

    // Copy file mode
    meta_data->mode = metadata_response.mode();
    std::cout << "Mode: " << (meta_data->mode == 1 ? "Read" : "Read/Write") << "\n";

    // Populate and print the recipe
    meta_data->recipe.stripe_width = metadata_response.recipe().stripe_width();
    std::cout << "Stripe Width: " << meta_data->recipe.stripe_width << "\n";


    std::cout << "Metadata fetched successfully for FD: " << fd << std::endl;
    return 0;
}


int pfs_execstat(struct pfs_execstat *execstat_data) {

    execstat_data->num_read_hits = global_execstats.num_read_hits;
    execstat_data->num_write_hits = global_execstats.num_write_hits;
    execstat_data->num_evictions = global_execstats.num_evictions;
    execstat_data->num_writebacks = global_execstats.num_writebacks;
    execstat_data->num_invalidations = global_execstats.num_invalidations;
    execstat_data->num_close_writebacks = global_execstats.num_close_writebacks;
    execstat_data->num_close_evictions = global_execstats.num_close_evictions;
    
    return 0;
}
