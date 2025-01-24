#include "pfs_metaserver.hpp"
#include <grpcpp/grpcpp.h>
#include <random>

using grpc::Status;
using grpc::ServerContext;
using grpc::ServerReaderWriter;
using grpc::ServerBuilder;
using grpc::Server;

using pfsmeta::OpenFileRequest;
using pfsmeta::OpenFileResponse;
using pfsmeta::CreateFileRequest;
using pfsmeta::CreateFileResponse;
using pfsmeta::ReadFileRequest;
using pfsmeta::ReadFileResponse;
using pfsmeta::TokenRequest;
using pfsmeta::TokenResponse;
using pfsmeta::ConnectRequest;
using pfsmeta::ConnectResponse;
using pfsmeta::CacheableBlocksRequest;
using pfsmeta::CacheableBlocksResponse;

using namespace std;

class MetaServerImpl final : public pfsmeta::MetaServer::Service {
private:

    std::random_device rd;
    std::mt19937 random_engine;
    std::uniform_int_distribution<int> fd_distribution;
    std::uniform_int_distribution<int> client_id_distribution;

    std::unordered_map<int, grpc::ServerReaderWriter<pfsmeta::ConnectResponse, pfsmeta::ConnectRequest>*> client_id_to_stream;
    std::unordered_map<grpc::ServerReaderWriter<pfsmeta::ConnectResponse, pfsmeta::ConnectRequest>*, int> stream_to_client_id;
    std::unordered_map<std::string, pfs_metadata> filename_to_metadata;
    std::unordered_map<int, FDInfo> fd_map;
    std::mutex client_map_mutex;
    std::mutex metadata_mutex;
    std::mutex fd_mutex;

    std::unordered_map<std::string, vector<ReadToken>> read_token_table;
    std::unordered_map<std::string, vector<WriteToken>> write_token_table;
    std::unordered_map<int, std::mutex> client_stream_mutexes;
    std::unordered_map<std::string, std::mutex> filename_to_metadata_mutexes;


    // Data structures for ACK tracking
    std::unordered_map<int, std::unordered_map<int, int>> pending_acks; // fd -> client_id -> count
    std::mutex ack_mutex;
    std::condition_variable ack_cv;

    std::unordered_map<int, std::atomic<int>> pending_ack_counts; // Track pending acks per client ID
    std::unordered_map<int, std::condition_variable> client_acks_cond_vars; // Condition variables per client
    std::unordered_map<int, std::condition_variable> client_token_cond_vars; // Condition variables per client

    std::unordered_map<int, std::mutex> client_acks_mutexes; // Mutexes for each client
    std::unordered_map<int, std::mutex> client_token_mutexes; // Mutexes for each client
    std::mutex token_table_mutex;     // Protects read_token_table and write_token_table

    std::unordered_map<std::string, std::unordered_map<int, Block>> block_to_clients_map;
    std::mutex blocks_table_mutex; 
    
public:

    MetaServerImpl() : random_engine(rd()), fd_distribution(10000, 99999), client_id_distribution(1000, 9999) {}

    int getClientIdFromContext(const grpc::ServerContext* context) {
        auto metadata = context->client_metadata();
        auto it = metadata.find("client-id");
        if (it != metadata.end()) {
            return std::stoi(std::string(it->second.data(), it->second.size()));
        }
        return -1; // Return -1 if client_id is not found
    }

    // Function to generate a unique random file descriptor
    int generate_unique_fd() {
        
        int fd;
        do {
            fd = fd_distribution(random_engine);
        } while (fd_map.find(fd) != fd_map.end());
        return fd;
    }

    int generate_unique_client_id() {
        
        int client_id;
        do {
            client_id = client_id_distribution(random_engine);
        } while (client_id_to_stream.find(client_id) != client_id_to_stream.end());
        return client_id;
    }

    Status HealthCheck(ServerContext* context,
                                    const pfsmeta::HealthCheckRequest* request,
                                    pfsmeta::HealthCheckResponse* response) {
        std::string client_message = request->message();
        std::cout << "Received HealthCheck request: " << client_message << std::endl;

        // Respond with "pong"
        response->set_message("pong");
        std::cout << "Responded with HealthCheck: pong" << std::endl;
        return Status::OK;
    }

    void handle_acknowledgment(int client_id) {
        auto& ack_count = pending_ack_counts[client_id];
        if (ack_count.fetch_sub(1) == 1) {
            // Notify the client condition variable when all acks are received
            std::lock_guard<std::mutex> lock(client_acks_mutexes[client_id]);
            std::cout<< "Receive ack pending_ack_counts for client: "<<client_id<<" is: "<<ack_count<<std::endl; 
            client_acks_cond_vars[client_id].notify_all();
        }
        std::cout<< "Receive ack pending_ack_counts for client: "<<client_id<<" is: "<<ack_count<<std::endl; 

    }

    void initialize_client_resources(int client_id) {
        // Lock a global mutex to ensure thread-safe map population
        static std::mutex global_init_mutex;
        std::lock_guard<std::mutex> lock(global_init_mutex);

        // Check and initialize resources for the client ID if not already present
        if (client_acks_mutexes.find(client_id) == client_acks_mutexes.end()) {
            client_acks_mutexes[client_id]; // Constructs a new mutex
            client_token_mutexes[client_id]; // Constructs a new mutex
            client_acks_cond_vars[client_id]; // Constructs a new condition variable
            client_token_cond_vars[client_id]; // Constructs a new condition variable
            pending_ack_counts[client_id] = 0; // Initialize atomic counter
            client_stream_mutexes[client_id];
        }
    }

    void cleanup_client_resources(int client_id) {
        static std::mutex global_cleanup_mutex;
        std::lock_guard<std::mutex> lock(global_cleanup_mutex);

        client_acks_mutexes.erase(client_id);
        client_token_mutexes.erase(client_id);
        client_acks_cond_vars.erase(client_id);
        client_token_cond_vars.erase(client_id);
        pending_ack_counts.erase(client_id);
        client_stream_mutexes.erase(client_id);
    }



    Status ConnectToMetaserver(ServerContext* context,
                                     ServerReaderWriter<pfsmeta::ConnectResponse, pfsmeta::ConnectRequest>* stream) override {
        

        int client_id;
        {
            std::lock_guard<std::mutex> lock(client_map_mutex);
            // Generate a unique client ID
            client_id = generate_unique_client_id();
            client_id_to_stream[client_id] = stream;
            stream_to_client_id[stream] = client_id;
            std::cout << "Client connected with client_id: " << client_id << ". Total clients: " << client_id_to_stream.size() << std::endl;
        }

        initialize_client_resources(client_id);

        pfsmeta::ConnectRequest request;
        while (stream->Read(&request)) {
            std::string client_message = request.message();
            std::cout << "Received message from client: " << client_message << std::endl;
            if(client_message=="Client connected"){
            // Respond to the client indicating the server is alive
                pfsmeta::ConnectResponse response;
                response.set_message("MetaServer is alive");
                response.set_client_id(client_id);
                if (!stream->Write(response)) {
                    std::cerr << "Failed to respond to client." << std::endl;
                    break;
                }
            }
            if (client_message == "Client disconnecting") {
                break;
            }
            else if(client_message=="Revoke acknowledged" || client_message == "Write through acknowledged" 
                                    || client_message == "Invalidate acknowledged"){
                std::cout << client_message << " received from client " << stream_to_client_id[stream] << "      "<< request.client_id()<<std::endl;
                handle_acknowledgment(request.client_id());

            }
            else if (client_message == "Grant acknowledged") {
                int64_t gstart = request.start_offset();
                int64_t gend = request.end_offset();
                std::string fname = request.filename();
                std::string ttype = request.token_type();
                int cid = request.client_id();

                {
                    std::lock_guard<std::mutex> tt_lock(token_table_mutex);
                    if (ttype == "read") {
                        for (auto &token : read_token_table[fname]) {
                            if (token.client_id == cid && token.start_offset == gstart && token.end_offset == gend) {
                                token.acked = true;
                                std::lock_guard<std::mutex> c_lock(client_token_mutexes[cid]);
                                client_token_cond_vars[cid].notify_all(); // Wake any waiters
                                break;
                            }
                        }
                    } else if (ttype == "write") {
                        for (auto &token : write_token_table[fname]) {
                            if (token.client_id == cid && token.start_offset == gstart && token.end_offset == gend) {
                                token.acked = true;
                                std::lock_guard<std::mutex> c_lock(client_token_mutexes[cid]);
                                client_token_cond_vars[cid].notify_all(); // Wake any waiters
                                break;
                            }
                        }
                        for (auto &token : read_token_table[fname]) {
                            if (token.client_id == cid && token.start_offset == gstart && token.end_offset == gend) {
                                token.acked = true;
                                std::lock_guard<std::mutex> c_lock(client_token_mutexes[cid]);
                                client_token_cond_vars[cid].notify_all(); // Wake any waiters
                                break;
                            }
                        }
                    }
                }

                std::cout << "Grant acknowledged from client " << cid << " for [" << gstart << ", " << gend << "]." << std::endl;
            }

        }
        // Client disconnected, remove the stream from the list
        {
            std::lock_guard<std::mutex> lock(client_map_mutex);
            client_id_to_stream.erase(client_id);
            stream_to_client_id.erase(stream);
            std::cout << "Client with client_id " << client_id << " disconnected. Total clients: " << client_id_to_stream.size() << std::endl;
        }

        return Status::OK;
    }

    // Handler for CreateFile
    Status CreateFile(ServerContext* context, const CreateFileRequest* request,
                      CreateFileResponse* response) override {
        std::string filename = request->filename();
        int stripe_width = request->stripe_width();
        
        {
            std::lock_guard<std::mutex> lock(metadata_mutex);

            // Check for duplicate filename or invalid stripe width
            if (filename_to_metadata.find(filename) != filename_to_metadata.end()) {
                response->set_status(-1);
                response->set_message("File already exists.");
                return Status::OK;
            }

            if (stripe_width > NUM_FILE_SERVERS) {
                response->set_status(-1);
                response->set_message("Invalid stripe width.");
                return Status::OK;
            }

            // Initialize file metadata
            pfs_metadata metadata;
            strncpy(metadata.filename, filename.c_str(), sizeof(metadata.filename));
            metadata.file_size = 0;
            metadata.ctime = std::time(nullptr);
            metadata.mtime = metadata.ctime;
            metadata.recipe.stripe_width = stripe_width;

            // Store metadata
            filename_to_metadata[filename] = metadata;
            filename_to_metadata_mutexes[filename];
        }

        response->set_status(0);
        response->set_message("File created successfully.");

        std::cout << "File created: " << filename << std::endl;
        return Status::OK;
    }

    // Handler for OpenFile
    Status OpenFile(ServerContext* context, const OpenFileRequest* request,
                    OpenFileResponse* response) override {
        std::string filename = request->filename();
        int mode = request->mode();

        std::unordered_map<std::string, pfs_metadata>::iterator it;
        {
            // Lock to access metadata
            std::lock_guard<std::mutex> lock(metadata_mutex);
            // Check if the file exists
            it = filename_to_metadata.find(filename);
            if (it == filename_to_metadata.end()) {
                response->set_status(-1);
                response->set_message("File not found.");
                return Status::OK;
            }
        }

        pfs_metadata& metadata = it->second;

        if (mode == 2 && metadata.mode == 1) { // Trying to open a read-only file in read/write mode
            response->set_status(-1);
            response->set_message("File is already opened in read-only mode.");
            return Status::OK;
        }

        int fd;
        {
            std::lock_guard<std::mutex> lock(fd_mutex);
            // Generate a unique random FD
            fd = generate_unique_fd();
            // Store FD information in a map
            fd_map[fd] = {filename, mode};
        }
        metadata.mode = mode;

        response->set_status(0);
        response->set_message("File opened successfully.");
        response->set_fd(fd);

        auto* metadata_response = response->mutable_metadata();
        metadata_response->set_filename(metadata.filename);
        metadata_response->set_file_size(metadata.file_size);
        metadata_response->set_ctime(metadata.ctime);
        metadata_response->set_mtime(metadata.mtime);
        metadata_response->set_mode(mode);

        auto* recipe_response = metadata_response->mutable_recipe();
        recipe_response->set_stripe_width(metadata.recipe.stripe_width);

        std::cout << "File '" << filename << "' opened with FD: " << fd << ", Mode: " << mode << std::endl;
        return Status::OK;
    }


    bool send_revoke_request(const std::string filename, int client_id, const WriteToken& token) {
        std::cout << "Sending revoke request to client " << token.client_id
                << " for fd: " << filename << ", range: [" << token.start_offset << ", " << token.end_offset << "], type: write"
                << std::endl;

        std::unordered_map<int, grpc::ServerReaderWriter<pfsmeta::ConnectResponse, pfsmeta::ConnectRequest>*>::iterator client_stream_it;
        {
            std::lock_guard<std::mutex> lock(client_map_mutex);
            client_stream_it = client_id_to_stream.find(token.client_id);
            if (client_stream_it == client_id_to_stream.end()) {
                std::cerr << "Error: No stream found for client " << token.client_id << std::endl;
                return false;
            }
        }

        std::lock_guard<std::mutex> lock(client_stream_mutexes[token.client_id]);
        auto* client_stream = client_stream_it->second;

        ConnectResponse revoke_request;
        revoke_request.set_message("Revoke request");
        // revoke_request.set_fd(fd);
        revoke_request.set_client_id(client_id);
        revoke_request.set_start_offset(token.start_offset);
        revoke_request.set_end_offset(token.end_offset);
        revoke_request.set_token_type("write");
        revoke_request.set_revoke_request(true);
        revoke_request.set_filename(filename);

        if (!client_stream->Write(revoke_request)) {
            std::cerr << "Failed to send revoke request to client " << token.client_id << std::endl;
            return false;
        }

        return true;
    }

    bool send_revoke_request(const std::string filename, int client_id, const ReadToken& token) {

        std::cout << "Sending revoke request to client " << token.client_id
                << " for fd: " << filename << ", range: [" << token.start_offset << ", " << token.end_offset << "], type: write"
                << std::endl;
                
        std::unordered_map<int, grpc::ServerReaderWriter<pfsmeta::ConnectResponse, pfsmeta::ConnectRequest>*>::iterator client_stream_it;

        {
            std::lock_guard<std::mutex> lock(client_map_mutex);
            client_stream_it = client_id_to_stream.find(token.client_id);
            if (client_stream_it == client_id_to_stream.end()) {
                std::cerr << "Error: No stream found for client " << token.client_id << std::endl;
                return false;
            }
        }

        std::lock_guard<std::mutex> lock(client_stream_mutexes[token.client_id]);

        auto* client_stream = client_stream_it->second;
        pfsmeta::ConnectResponse revoke_request;
        revoke_request.set_message("Revoke request");
        // revoke_request.set_fd(fd);
        revoke_request.set_client_id(client_id);
        revoke_request.set_start_offset(token.start_offset);
        revoke_request.set_end_offset(token.end_offset);
        revoke_request.set_token_type("read");
        revoke_request.set_revoke_request(true);
        revoke_request.set_filename(filename);
        

        if (!client_stream->Write(revoke_request)) {
            std::cerr << "Failed to send revoke request to client " << token.client_id << std::endl;
            return false;
        }

        return true;
    }


    std::vector<WriteToken> find_overlapping_write_tokens(const std::vector<WriteToken>& tokens, int64_t start, int64_t end) {
        std::vector<WriteToken> overlapping_tokens;
        for (const auto& token : tokens) {
            if (!(end < token.start_offset || start > token.end_offset)) {
                overlapping_tokens.push_back({
                    token.client_id,
                    std::max(start, token.start_offset),
                    std::min(end, token.end_offset),
                    token.acked
                });
            }
        }
        return overlapping_tokens;
    }

    std::vector<ReadToken> find_overlapping_read_tokens(const std::vector<ReadToken>& tokens, int64_t start, int64_t end) {
        std::vector<ReadToken> overlapping_tokens;
        for (const auto& token : tokens) {
            if (!(end < token.start_offset || start > token.end_offset)) {
                overlapping_tokens.push_back({
                    token.client_id,
                    std::max(start, token.start_offset),
                    std::min(end, token.end_offset),
                    token.acked
                });
            }
        }
        return overlapping_tokens;
    }

    std::vector<ReadToken> find_overlapping_read_tokens_waiting_for_acks(const std::vector<ReadToken>& tokens, int64_t start, int64_t end) {
        std::vector<ReadToken> overlapping_tokens;

        for (const auto& token : tokens) {
            // Check for overlap and that the token is not acknowledged
            if (!(end < token.start_offset || start > token.end_offset) && !token.acked) {
                overlapping_tokens.push_back({
                    token.client_id,
                    std::max(start, token.start_offset),
                    std::min(end, token.end_offset),
                    token.acked
                });
            }
        }

        return overlapping_tokens;
    }


    void update_write_token_table(std::vector<WriteToken>& tokens, const WriteToken& token_to_remove) {
        std::vector<WriteToken> updated_tokens;
        for (auto it = tokens.begin(); it != tokens.end();) {
            if (it->end_offset < token_to_remove.start_offset || it->start_offset > token_to_remove.end_offset) {
                // No overlap, keep the token as is
                ++it;
            } else {
                // Overlap exists, need to adjust or remove the token
                if (it->start_offset < token_to_remove.start_offset) {
                    // Left non-overlapping part
                    updated_tokens.push_back({
                        it->client_id,
                        it->start_offset,
                        token_to_remove.start_offset - 1,
                        true
                    });
                }
                if (it->end_offset > token_to_remove.end_offset) {
                    // Right non-overlapping part
                    updated_tokens.push_back({
                        it->client_id,
                        token_to_remove.end_offset + 1,
                        it->end_offset,
                        true
                    });
                }
                // Remove the current token
                it = tokens.erase(it);
            }
        }
        // Add updated tokens
        tokens.insert(tokens.end(), updated_tokens.begin(), updated_tokens.end());
    }

    void update_read_token_table(std::vector<ReadToken>& tokens, const ReadToken& token_to_remove) {
        std::vector<ReadToken> updated_tokens;
        for (auto it = tokens.begin(); it != tokens.end();) {
            if (it->end_offset < token_to_remove.start_offset || it->start_offset > token_to_remove.end_offset) {
                // No overlap, keep the token as is
                ++it;
            } else {
                // Overlap exists, need to adjust or remove the token
                if (it->start_offset < token_to_remove.start_offset) {
                    // Left non-overlapping part
                    updated_tokens.push_back({
                        it->client_id,
                        it->start_offset,
                        token_to_remove.start_offset - 1,
                        true
                    });
                }
                if (it->end_offset > token_to_remove.end_offset) {
                    // Right non-overlapping part
                    updated_tokens.push_back({
                        it->client_id,
                        token_to_remove.end_offset + 1,
                        it->end_offset,
                        true
                    });
                }
                // Remove the current token
                it = tokens.erase(it);
            }
        }
        // Add updated tokens
        tokens.insert(tokens.end(), updated_tokens.begin(), updated_tokens.end());
    }

    bool revoke_overlapping_tokens(const std::string filename, int client_id, const std::vector<WriteToken>& overlapping_write_tokens, 
                               const std::vector<ReadToken>& overlapping_read_tokens) {
        for (size_t i = 0; i < overlapping_write_tokens.size(); i++) {
            auto& global_token = overlapping_write_tokens[i]; // non-const reference

            // Increment the pending ack count for this client ID
            pending_ack_counts[client_id].fetch_add(1);

            std::cout << "write revoke_overlapping_tokens before send_revoke_request, client_id: " << global_token.client_id << ", range: " << global_token.start_offset << "-" << global_token.end_offset << std::endl;

            if (!send_revoke_request(filename, client_id, global_token)) {
                pending_ack_counts[client_id].fetch_sub(1);
                return false;
            }
            std::cout << "write revoke_overlapping_tokens after send_revoke_request, client_id: " << global_token.client_id << ", range: " << global_token.start_offset << "-" << global_token.end_offset << std::endl;
            std::cout<< "pending_ack_counts for client: "<<client_id<<" is: "<<pending_ack_counts[client_id]<<std::endl; 

        }

        for (size_t i = 0; i < overlapping_read_tokens.size(); i++) {
            auto& global_token = overlapping_read_tokens[i]; // non-const reference
        
            // Increment the pending ack count for this client ID
            pending_ack_counts[client_id].fetch_add(1);

            std::cout << "read revoke_overlapping_tokens before send_revoke_request, client_id: " << global_token.client_id << ", range: " << global_token.start_offset << "-" << global_token.end_offset << std::endl;


            if (!send_revoke_request(filename, client_id, global_token)) {
                pending_ack_counts[client_id].fetch_sub(1);
                return false;
            }

            std::cout << "read revoke_overlapping_tokens after send_revoke_request, client_id: " << global_token.client_id << ", range: " << global_token.start_offset << "-" << global_token.end_offset <<std::endl;
            std::cout<< "pending_ack_counts for client: "<<client_id<<" is: "<<pending_ack_counts[client_id]<<std::endl; 


        }

        return true;
    }

    std::vector<int64_t> calculate_block_numbers(const std::string &filename, int64_t start_offset, int64_t end_offset) {

        int64_t start_block = start_offset / PFS_BLOCK_SIZE;
        int64_t end_block = end_offset / PFS_BLOCK_SIZE;
        std::vector<int64_t> blocks;
        for (int64_t b = start_block; b <= end_block; b++) {
            blocks.push_back(b);
        }
        return blocks;
    }

    // Update block counts from token tables
    void updateBlockCountsFromTokenTables(const std::string &filename) {
        // Clear counts first
        for (auto &blk_pair : block_to_clients_map[filename]) {
            blk_pair.second.read_tokens = 0;
            blk_pair.second.write_tokens = 0;
        }

        // Count read tokens
        for (auto &rtoken : read_token_table[filename]) {
            // if (!rtoken.acked) continue;
            auto blocks = calculate_block_numbers(filename, rtoken.start_offset, rtoken.end_offset);
            for (auto blk_num : blocks) {
                block_to_clients_map[filename][blk_num].read_tokens++;
            }
        }

        // Count write tokens
        for (auto &wtoken : write_token_table[filename]) {
            // if (!wtoken.acked) continue;
            auto blocks = calculate_block_numbers(filename, wtoken.start_offset, wtoken.end_offset);
            for (auto blk_num : blocks) {
                block_to_clients_map[filename][blk_num].write_tokens++;
            }
        }
    }


    // Determine invalidation and write-through per-block
    std::unordered_map<int, BlockAction> determineInvalidationsAndWriteThrough(const std::string &filename, int client_id, const std::string &token_type, int64_t start_offset, int64_t end_offset) {
        std::unordered_map<int, BlockAction> block_actions;
        auto blocks = calculate_block_numbers(filename, start_offset, end_offset);

        for (auto blk_num : blocks) {
            BlockAction action;
            auto &blk = block_to_clients_map[filename][blk_num];

            // Gather current cache holders
            // token_type decides scenario
            if (token_type == "write") {
                if (blk.write_tokens == 0 && blk.read_tokens == 0) {
                    // First write token on this block
                    action.cacheRequester = true;
                } else if (blk.write_tokens > 0) {
                    // Another writer existed
                    // Need write-through from old writer(s), then invalidate them
                    for (int cid : blk.clientsWithCache) {
                        if (cid != client_id) {
                           
                            action.invalidate_clients.push_back(cid);
                        }
                    }
                    blk.clientsWithCache.clear();
                    action.cacheRequester = true; 
                } else if (blk.write_tokens == 0 && blk.read_tokens > 0) {
              
                    for (int cid : blk.clientsWithCache) {
                        if (cid != client_id) {
                            action.invalidate_clients.push_back(cid);
                        }
                    }
                    blk.clientsWithCache.clear();
                    action.cacheRequester = true;
                }
            } else { // "read"
                    if (blk.write_tokens > 0) {
                    
                    for (int cid : blk.clientsWithCache) {
                        if (cid != client_id) {
                            action.write_through_clients.push_back(cid);
                        }
                    }
                    action.cacheRequester = false;
                }
                else if(blk.write_tokens == 0 && blk.read_tokens >= 1){
                    for (int cid : blk.clientsWithCache) {
                        if (cid != client_id) {
                            action.write_through_clients.push_back(cid);
                        }
                    }
                    action.cacheRequester = true;

                }

            }

            block_actions[blk_num] = action;
        }
        return block_actions;
    }


    // Add requester to block caches where allowed
    void addRequesterToSelectedBlocks(const std::string &filename, int client_id, const std::unordered_map<int, BlockAction> &block_actions) {
        for (auto &pair : block_actions) {
            int blk_num = pair.first;
            auto &action = pair.second;
            if (action.cacheRequester) {
                auto &blk = block_to_clients_map[filename][blk_num];
                if (std::find(blk.clientsWithCache.begin(), blk.clientsWithCache.end(), client_id) == blk.clientsWithCache.end()) {
                    blk.clientsWithCache.push_back(client_id);
                }
            }
        }
    }

    // Once all invalidation/write-through requests are sent and counters incremented, we wait for acks
    void waitForRequesterAcks(int client_id) {
        std::unique_lock<std::mutex> lock(client_acks_mutexes[client_id]);
        client_acks_cond_vars[client_id].wait(lock, [&]() {
            return pending_ack_counts[client_id] == 0;
        });
    }

    // Stub functions to send requests (replace with actual logic)
    bool sendInvalidationRequest(int target_client_id, int req_client_id, const std::string &filename, int64_t start_offset, int64_t end_offset) {
        // Construct and send an invalidation message to target_client_id
        // Increment pending_ack_counts for the requesting client will be done by caller

        std::unordered_map<int, grpc::ServerReaderWriter<pfsmeta::ConnectResponse, pfsmeta::ConnectRequest>*>::iterator client_stream_it;

        {
            std::lock_guard<std::mutex> lock(client_map_mutex);
            client_stream_it = client_id_to_stream.find(target_client_id);
            if (client_stream_it == client_id_to_stream.end()) {
                std::cerr << "Error: No stream found for client " << target_client_id << std::endl;
                return false;
            }
        }

        std::cout << "Sending Invalidate request to client " << target_client_id
                << " for fd: " << filename << ", range: [" << start_offset << ", " << end_offset << "], type: write"
                << std::endl;

        std::lock_guard<std::mutex> lock(client_stream_mutexes[target_client_id]);

        auto* client_stream = client_stream_it->second;
        pfsmeta::ConnectResponse invalidate_request;
        invalidate_request.set_message("Invalidate Request");
        // revoke_request.set_fd(fd);
        invalidate_request.set_client_id(req_client_id);
        invalidate_request.set_start_offset(start_offset);
        invalidate_request.set_end_offset(end_offset);
        invalidate_request.set_filename(filename);
        invalidate_request.set_invalidate_request(true);
        

        if (!client_stream->Write(invalidate_request)) {
            std::cerr << "Failed to send revoke request to client " << target_client_id << std::endl;
            return false;
        }

        return true;
    }

    bool sendWriteThroughRequest(int target_client_id, int req_client_id, const std::string &filename, int64_t start_offset, int64_t end_offset) {
        // Construct and send a write-through request to target_client_id
        // Increment pending_ack_counts for the requesting client will be done by caller

        std::unordered_map<int, grpc::ServerReaderWriter<pfsmeta::ConnectResponse, pfsmeta::ConnectRequest>*>::iterator client_stream_it;

        {
            std::lock_guard<std::mutex> lock(client_map_mutex);
            client_stream_it = client_id_to_stream.find(target_client_id);
            if (client_stream_it == client_id_to_stream.end()) {
                std::cerr << "Error: No stream found for client " << target_client_id << std::endl;
                return false;
            }
        }

        std::cout << "Sending Write Through request to client " << target_client_id
                << " for fd: " << filename << ", range: [" << start_offset << ", " << end_offset << "], type: write"
                << std::endl;

        std::lock_guard<std::mutex> lock(client_stream_mutexes[target_client_id]);

        auto* client_stream = client_stream_it->second;
        pfsmeta::ConnectResponse write_through_request;
        write_through_request.set_message("Write_through Request");
        // revoke_request.set_fd(fd);
        write_through_request.set_client_id(req_client_id);
        write_through_request.set_start_offset(start_offset);
        write_through_request.set_end_offset(end_offset);
        write_through_request.set_filename(filename);
        write_through_request.set_write_through_request(true);
        

        if (!client_stream->Write(write_through_request)) {
            std::cerr << "Failed to send revoke request to client " << target_client_id << std::endl;
            return false;
        }

        return true;
    }


    void processCacheInvalidations(const std::string &filename, int client_id, const std::string &token_type, int64_t start_offset, int64_t end_offset) {
        
        int total_requests = 0;
        std::unordered_map<int, BlockAction> block_actions;

        {
            std::lock_guard<std::mutex> lock(blocks_table_mutex);
            // Determine invalidations/write-through per block
            block_actions = determineInvalidationsAndWriteThrough(filename, client_id, token_type, start_offset, end_offset);
            

            // Send requests and track acks
            for (auto &pair : block_actions) {
                auto &action = pair.second;

                // Write-through
                for (int cid : action.write_through_clients) {
                    int64_t start_offset = pair.first * PFS_BLOCK_SIZE;
                    int64_t end_offset = ((pair.first + 1) * PFS_BLOCK_SIZE) - 1;
                    pending_ack_counts[client_id].fetch_add(1);
                    if(!sendWriteThroughRequest(cid, client_id, filename, start_offset, end_offset)){
                        pending_ack_counts[client_id].fetch_sub(1);;
                    }
                    total_requests++;
                    std::cout<< "Sending request pending_ack_counts for client: "<<client_id<<" is: "<<total_requests<<std::endl; 

                }

                // Invalidation
                for (int cid : action.invalidate_clients) {
                    int64_t start_offset = pair.first * PFS_BLOCK_SIZE;
                    int64_t end_offset = ((pair.first + 1) * PFS_BLOCK_SIZE) - 1;
                    pending_ack_counts[client_id].fetch_add(1);
                    if(!sendInvalidationRequest(cid, client_id, filename, start_offset, end_offset)){
                        pending_ack_counts[client_id].fetch_sub(1);
                    }
                    total_requests++;
                    std::cout<< "Sending request pending_ack_counts for client: "<<client_id<<" is: "<<total_requests<<std::endl; 

                }
            }
        }

        // Wait for all acks from these operations
        if (total_requests > 0) {
            std::cout<<"waiting for Invalidation acks"<<std::endl;
            waitForRequesterAcks(client_id);
            std::cout<<"woke up after geeting all Invalidation acks"<<std::endl;
        }


        {
            std::lock_guard<std::mutex> lock(blocks_table_mutex);
            // After all acks, add requester to cache where allowed
            addRequesterToSelectedBlocks(filename, client_id, block_actions);
        }
    }

    std::vector<WriteToken*> find_overlapping_write_tokens_wait(std::vector<WriteToken>& tokens, int64_t start, int64_t end) {
        std::vector<WriteToken*> overlapping;
        for (auto &token : tokens) {
            if (!(end < token.start_offset || start > token.end_offset) && !token.acked) {
                overlapping.push_back(&token);
            }
        }
        return overlapping;
    }

    std::vector<ReadToken*> find_overlapping_read_tokens_wait(std::vector<ReadToken>& tokens, int64_t start, int64_t end) {
        std::vector<ReadToken*> overlapping;
        for (auto &token : tokens) {
            if (!(end < token.start_offset || start > token.end_offset) && !token.acked) {
                overlapping.push_back(&token);
            }
        }
        return overlapping;
    }


     // Handler for RequestToken (manages read/write tokens)
    Status RequestToken(ServerContext* context, const TokenRequest* request, TokenResponse* response) override {
        int client_id = getClientIdFromContext(context);
        if (client_id == -1) {
            std::cerr << "Error: Client ID not found in metadata." << std::endl;
            response->set_status(-1);
            response->set_message("Client ID not found.");
            return Status::OK;
        }
        pending_ack_counts[client_id] = 0;
        
        int fd = request->fd();
        int64_t start_offset = request->start_offset();
        int64_t end_offset = request->end_offset();
        std::string token_type = request->token_type();
        int64_t offset = request->offset();
        std::string filename;
        {
            std::lock_guard<std::mutex> lock(fd_mutex);
            
            auto it = fd_map.find(fd);
            if (it != fd_map.end()) {
                filename = it->second.filename;
            } else {
                std::cout << "File descriptor " << fd << " not found in map." << std::endl;
            }
        }

        pfs_metadata metadata;
        {
            // Lock to access metadata
            std::lock_guard<std::mutex> lock(metadata_mutex);
            // Check if the file exists
            std::unordered_map<std::string, pfs_metadata>::iterator it = filename_to_metadata.find(filename);
            if (it == filename_to_metadata.end()) {
                response->set_status(-1);
                response->set_message("File not found.");
                return Status::OK;
            }
            metadata = it->second;
        }

        std::cout << "Token request received for fd: " << fd << ", client_id: " << client_id << ", range: " << start_offset << "-" << end_offset << ", type: " << token_type << std::endl;

        // Validate offset
        if (token_type == "read" && offset >= static_cast<int64_t>(metadata.file_size)) {
            response->set_status(-1);
            response->set_message("Error: Offset " + std::to_string(offset) + 
                                " is beyond file size " + std::to_string(metadata.file_size) + ".");
            return grpc::Status::OK;
        }

        std::vector<int64_t> no_cache_blocks;

        if(token_type != "read" && token_type != "write") {
            response->set_status(-1);
            response->set_message("Invalid token type requested. Must be 'read' or 'write'.");
            return Status::OK;
        }

        vector<WriteToken> write_overlapping_tokens;
        vector<ReadToken> read_overlapping_tokens;


        {
            std::unique_lock<std::mutex> overlap_lock(token_table_mutex);
            auto& write_tokens = write_token_table[filename];
            auto& read_tokens = read_token_table[filename];

            // These now return pointers to the actual tokens in the tables
            std::vector<WriteToken*> write_overlapping_tokens = find_overlapping_write_tokens_wait(write_tokens, start_offset, end_offset);
            std::vector<ReadToken*> read_overlapping_tokens = find_overlapping_read_tokens_wait(read_tokens, start_offset, end_offset);

            // Wait on write overlapping tokens
            for (auto* token_ptr : write_overlapping_tokens) {
                if (!token_ptr->acked) {
                    overlap_lock.unlock();

                    std::unique_lock<std::mutex> lock(client_token_mutexes[token_ptr->client_id]);
                    std::cout << "client id: " << client_id
                            << " write revoke_overlapping_tokens waiting for, client_id: " << token_ptr->client_id
                            << ", range: " << token_ptr->start_offset << "-" << token_ptr->end_offset << std::endl;

                    while (!token_ptr->acked) {
                        client_token_cond_vars[token_ptr->client_id].wait(lock);
                    }

                    overlap_lock.lock();
                }
            }

            // Wait on read overlapping tokens
            for (auto* token_ptr : read_overlapping_tokens) {
                if (!token_ptr->acked) {
                    overlap_lock.unlock();

                    std::unique_lock<std::mutex> lock(client_token_mutexes[token_ptr->client_id]);
                    std::cout << "client id: " << client_id
                            << " write revoke_overlapping_tokens waiting for, client_id: " << token_ptr->client_id
                            << ", range: " << token_ptr->start_offset << "-" << token_ptr->end_offset << std::endl;

                    while (!token_ptr->acked) {
                        client_token_cond_vars[token_ptr->client_id].wait(lock);
                    }

                    overlap_lock.lock();
                }
            }
        }


            
        if(token_type == "read"){
            std::lock_guard<std::mutex> lock(token_table_mutex);
            auto& write_tokens = write_token_table[filename];
            auto& read_tokens = read_token_table[filename];
            write_overlapping_tokens = find_overlapping_write_tokens(write_tokens, start_offset, end_offset);

            if(!write_overlapping_tokens.empty()){
                for (const auto& token : write_overlapping_tokens) {
                    update_write_token_table(write_tokens, token);
                }
                
                read_token_table[filename].push_back({client_id, start_offset, end_offset, false});

            }
            else{

                vector<ReadToken> read_overlapping_tokens_waiting_for_acks = 
                        find_overlapping_read_tokens_waiting_for_acks(read_tokens, start_offset, end_offset);

                for (size_t i = 0; i < read_overlapping_tokens_waiting_for_acks.size(); i++) {
                    auto& global_token = read_overlapping_tokens_waiting_for_acks[i]; // non-const reference
                    if (!global_token.acked) {
                        std::unique_lock<std::mutex> lock(client_token_mutexes[global_token.client_id]);
                        std::cout <<"client id: "<<client_id<< "read waiting for, client_id: " << global_token.client_id << ", range: " << global_token.start_offset << "-" << global_token.end_offset << std::endl;
                        client_token_cond_vars[global_token.client_id].wait(lock, [&]() {
                            return global_token.acked;
                        });
                    }
                }
                // Grant read token
                read_token_table[filename].push_back({client_id, start_offset, end_offset, true});
                
                {
                    // Update block_to_clients_map counts from token tables
                    std::lock_guard<std::mutex> lock(blocks_table_mutex);
                    updateBlockCountsFromTokenTables(filename);
                }

                processCacheInvalidations(filename, client_id, token_type, start_offset, end_offset);
                std::cout << "After updating Blocks in ------Token request received for fd: " << fd << ", client_id: " << client_id << ", range: " << start_offset << "-" << end_offset << ", type: " << token_type << std::endl;


                response->set_status(0);
                response->set_message("Read Token granted.");
                response->set_file_size(metadata.file_size);
                return Status::OK;
            }                
        }
        else if(token_type == "write"){
            std::lock_guard<std::mutex> lock(token_table_mutex);
            auto& write_tokens = write_token_table[filename];
            write_overlapping_tokens = find_overlapping_write_tokens(write_tokens, start_offset, end_offset);
            auto& read_tokens = read_token_table[filename];
            read_overlapping_tokens = find_overlapping_read_tokens(read_tokens, start_offset, end_offset);

            if(write_overlapping_tokens.empty() && read_overlapping_tokens.empty()){

                // Grant the write token after successful revocation
                write_token_table[filename].push_back({client_id, start_offset, end_offset, true});
                read_token_table[filename].push_back({client_id, start_offset, end_offset, true});
                
                {
                    // Update block_to_clients_map counts from token tables
                    std::lock_guard<std::mutex> lock(blocks_table_mutex);
                    updateBlockCountsFromTokenTables(filename);
                }


                processCacheInvalidations(filename, client_id, token_type, start_offset, end_offset);
                std::cout << "After updating Blocks in ------Token request received for fd: " << fd << ", client_id: " << client_id << ", range: " << start_offset << "-" << end_offset << ", type: " << token_type << std::endl;


                response->set_status(0);
                response->set_message("Write Token granted.");
                response->set_file_size(metadata.file_size);
                return Status::OK;
            }
            else{

                if(!write_overlapping_tokens.empty()){
                    for (const auto& token : write_overlapping_tokens) {
                        update_write_token_table(write_tokens, token);
                    }
                }
                if(!read_overlapping_tokens.empty()){
                    for (const auto& token : read_overlapping_tokens) {
                        update_read_token_table(read_tokens, token);
                    }
                }
                write_token_table[filename].push_back({client_id, start_offset, end_offset, false});
                read_token_table[filename].push_back({client_id, start_offset, end_offset, false});

            }
        }

        std::cout<<"client: "<<client_id<<" before revoke_overlapping_tokens call"<<std::endl;

        if (!revoke_overlapping_tokens(filename, client_id, write_overlapping_tokens, read_overlapping_tokens)) {
            response->set_status(-1);
            response->set_message("Failed to revoke overlapping tokens.");
            return Status::OK;
        }

        // Wait for all acknowledgments for the requesting client's tokens
        {
            std::unique_lock<std::mutex> client_lock(client_acks_mutexes[client_id]);
            client_acks_cond_vars[client_id].wait(client_lock, [&]() {
                return pending_ack_counts[client_id] == 0; // Wait until all acks are received
            });
        }

        std::cout<<"client: "<<client_id<<" thread is woke up"<<std::endl;
        
        {
            // Update block_to_clients_map counts from token tables
            std::lock_guard<std::mutex> lock(token_table_mutex);
            std::lock_guard<std::mutex> block_lock(blocks_table_mutex);
            updateBlockCountsFromTokenTables(filename);
        }

        std::cout<<"Starting: Invalidations for cache"<<std::endl;
        processCacheInvalidations(filename, client_id, token_type, start_offset, end_offset);

        response->set_status(0);
        response->set_message("Token granted after resolving conflicts.");

        {
            // Lock to access metadata
            std::lock_guard<std::mutex> lock(metadata_mutex);
            // Check if the file exists
            std::unordered_map<std::string, pfs_metadata>::iterator it = filename_to_metadata.find(filename);
            if (it == filename_to_metadata.end()) {
                response->set_status(-1);
                response->set_message("File not found.");
                return Status::OK;
            }
            metadata = it->second;
        }
        response->set_file_size(metadata.file_size);

        // {
        //     std::unique_lock<std::mutex> lock(client_token_mutexes[client_id]);
        //     client_token_cond_vars[client_id].notify_all();
        // }

        return Status::OK;

    }
    

    grpc::Status FetchCacheableBlocks(ServerContext* context,
                                  const CacheableBlocksRequest* request,
                                  CacheableBlocksResponse* response) {
        const std::string& filename = request->filename();
        int64_t start_offset = request->start_offset();
        int64_t end_offset = request->end_offset();
        const std::string& token_type = request->token_type();
        int req_client_id = request->client_id();

        std::cout<<"reached FetchCacheableBlocks call in server by clientId: "<<req_client_id<< " for filename: "<<filename<<" [ "<<start_offset<<", "<<end_offset<<"]   token type: "<<token_type<<std::endl;
        
        
        std::unique_lock<std::mutex> block_map_lock(blocks_table_mutex);
        // Check if file exists in block_to_clients_map
        if (block_to_clients_map.find(filename) == block_to_clients_map.end()) {
            return grpc::Status(grpc::StatusCode::NOT_FOUND, "File not found in block_to_clients_map");
        }

        std::cout<<"Found block map FetchCacheableBlocks call in server by clientId: "<<req_client_id<< " for filename: "<<filename<<" [ "<<start_offset<<", "<<end_offset<<"]   token type: "<<token_type<<std::endl;

        // Get the block map for the file
        std::unordered_map<int, Block>& block_map = block_to_clients_map[filename];

        // Get the block numbers in the requested range
        auto block_nums = calculate_block_numbers(filename, start_offset, end_offset);

        // Determine cacheable blocks
        for (int64_t block_num : block_nums) {
            if (block_map.find(block_num) == block_map.end()) {
                continue; // Skip blocks not in the map
            }

            
            // Determine cacheability based on token type
            if (token_type == "write") {
                block_map_lock.unlock();
                processCacheInvalidations(filename, req_client_id, token_type, start_offset, end_offset);
                Block& blk = block_map[block_num];

                // Write is cacheable if no other clients hold the block in cache
                if ((blk.clientsWithCache.empty() || blk.clientsWithCache[0] == req_client_id)) {
                    response->add_cacheable_blocks(block_num);
                }
                block_map_lock.lock();
            } else if (token_type == "read") {
                Block& blk = block_map[block_num];
                if (blk.write_tokens == 0) {
                    auto it = std::find(blk.clientsWithCache.begin(), blk.clientsWithCache.end(), req_client_id);
                    bool client_not_in_cache = (it == blk.clientsWithCache.end());

                    if (client_not_in_cache) {
                        blk.clientsWithCache.push_back(req_client_id);
                        response->add_cacheable_blocks(block_num);
                    } else {
                        response->add_cacheable_blocks(block_num);
                    }
                }
            }
        }

        // Populate response
        response->set_status(0);
        response->set_message("Cacheable blocks calculated successfully.");
        return grpc::Status::OK;
    }



    Status Fstat(ServerContext* context, const pfsmeta::FstatRequest* request,
                             pfsmeta::FstatResponse* response) {
        int fd = request->fd();
        std::string filename;
        {
            std::lock_guard<std::mutex> lock(fd_mutex);
            // Find the file associated with the given FD
            auto fd_it = fd_map.find(fd);
            if (fd_it == fd_map.end()) {
                response->set_status(-1);
                response->set_message("Invalid file descriptor.");
                return Status::OK;
            }
            else{
                filename = fd_it->second.filename;
            }
        }

         
        std::unordered_map<std::string, pfs_metadata>::iterator it;
        {
            // Lock to access metadata
            std::lock_guard<std::mutex> lock(metadata_mutex);
            // Check if the file exists in metadata
            it = filename_to_metadata.find(filename);
            if (it == filename_to_metadata.end()) {
                response->set_status(-1);
                response->set_message("File metadata not found.");
                return Status::OK;
            }
        }

        

        const pfs_metadata& metadata = it->second;

        // Populate the response
        response->set_status(0);
        response->set_message("Metadata fetched successfully.");
        auto* metadata_response = response->mutable_metadata();
        metadata_response->set_filename(metadata.filename);
        metadata_response->set_file_size(metadata.file_size);
        metadata_response->set_ctime(metadata.ctime);
        metadata_response->set_mtime(metadata.mtime);
        metadata_response->set_mode(metadata.mode);
        
        auto* recipe_response = metadata_response->mutable_recipe();
        recipe_response->set_stripe_width(metadata.recipe.stripe_width);

        std::cout << "Metadata sent for FD: " << fd << ", Filename: " << filename << std::endl;
        return Status::OK;
    }

    // UpdateMetadata implementation
    grpc::Status UpdateMetadata(grpc::ServerContext* context,
                                 const pfsmeta::UpdateMetadataRequest* request,
                                 pfsmeta::UpdateMetadataResponse* response) override {
        const std::string& filename = request->filename();
        int64_t file_size = request->file_size();
        int64_t mtime = request->mtime();

        std::cout << "Received UpdateMetadata request for file: " << filename << std::endl;

        pfs_metadata& metadata = [&]() -> pfs_metadata& {
            std::lock_guard<std::mutex> lock(metadata_mutex);

            auto it = filename_to_metadata.find(filename);
            if (it == filename_to_metadata.end()) {
                std::cerr << "Error: File not found in metadata: " << filename << std::endl;
                response->set_status(-1);
                response->set_message("File not found.");
                throw grpc::Status::OK;
            }
            return it->second;
        }();

        {
            std::lock_guard<std::mutex> file_lock(filename_to_metadata_mutexes[filename]);
            // Update file metadata
            metadata.file_size = file_size;
            
            std::cout << "Metadata updated successfully for file: " << filename << std::endl;
        }

        // Send success response
        response->set_status(0);
        response->set_message("Metadata updated successfully.");
        return grpc::Status::OK;
    }

    grpc::Status DeleteTokensForClient(grpc::ServerContext* context, 
                                const pfsmeta::DeleteClientTokensRequest* request, 
                                pfsmeta::DeleteClientTokensResponse* response) {

        std::cout<< "Inside deleteClient Tokens" <<std::endl;

        int client_id = request->client_id();
        std::string filename = request->filename();
        int fileFD = request->fd();

            // Scoped lock for thread safety
            std::lock_guard<std::mutex> lock(filename_to_metadata_mutexes[filename]);

            std::lock_guard<std::mutex> block_map_lock(blocks_table_mutex);
            std::lock_guard<std::mutex> metadata_lock(metadata_mutex);
            std::lock_guard<std::mutex> token_table_lock(token_table_mutex);

            // Remove tokens from read and write token tables
            read_token_table[filename].erase(
                std::remove_if(read_token_table[filename].begin(), read_token_table[filename].end(),
                            [client_id](const ReadToken& token) { return token.client_id == client_id; }),
                read_token_table[filename].end()
            );

            write_token_table[filename].erase(
                std::remove_if(write_token_table[filename].begin(), write_token_table[filename].end(),
                            [client_id](const WriteToken& token) { return token.client_id == client_id; }),
                write_token_table[filename].end()
            );

            updateBlockCountsFromTokenTables(filename);

            std::cout<< " after updating blocks pfs_close for client id: "<<client_id<<"  filename: " <<filename<<std::endl;


           auto file_it = block_to_clients_map.find(filename);
            if (file_it != block_to_clients_map.end()) {
                // Iterate over all blocks for this file
                for (auto& [block_num, block] : file_it->second) {
                    // Remove the client ID from the clientsWithCache vector
                    block.clientsWithCache.erase(
                        std::remove(block.clientsWithCache.begin(), block.clientsWithCache.end(), client_id),
                        block.clientsWithCache.end()
                    );
                }
            }

            // Update the file's metadata with the current time
            auto metadata_it = filename_to_metadata.find(filename);
            if (metadata_it != filename_to_metadata.end()) {
                pfs_metadata& metadata = metadata_it->second;
                metadata.mtime = static_cast<int64_t>(std::time(nullptr)); // Update modified time
                std::cout << "Updated mtime for filename: " << filename << " to " << metadata.mtime << std::endl;
            } else {
                std::cerr << "Warning: Metadata for filename " << filename << " not found!" << std::endl;
            }



            // Remove file descriptor entries for this client
            auto fd_it = fd_map.find(fileFD);
            if (fd_it != fd_map.end()) {
                fd_map.erase(fd_it);
                std::cout << "Removed FD: " << fileFD << ", Filename: " << fd_it->second.filename << std::endl;
            } else {
                std::cerr << "FD: " << fileFD << " not found in fd_map." << std::endl;
            }

            response->set_success(0);
            return grpc::Status::OK;
    }

    grpc::Status CheckClientsWithFile(grpc::ServerContext* context, 
                           const pfsmeta::CheckClientsWithFileRequest* request, 
                           pfsmeta::CheckClientsWithFileResponse* response) {
        std::string filename = request->filename();
        // int requesting_client_id = request->client_id();

        try {
            std::lock_guard<std::mutex> lock(metadata_mutex);

            // Check if any file descriptors are open for this file
            bool is_open = std::any_of(fd_map.begin(), fd_map.end(),
                                    [&filename](const auto& fd_pair) {
                                        return fd_pair.second.filename == filename;
                                    });

            // Check token tables if no file descriptors are active
            if (!is_open) {
                auto read_token_it = read_token_table.find(filename);
                auto write_token_it = write_token_table.find(filename);

                is_open = (read_token_it != read_token_table.end() && !read_token_it->second.empty()) ||
                        (write_token_it != write_token_table.end() && !write_token_it->second.empty());
            }

            // If no active clients, remove metadata
            if (!is_open) {
                filename_to_metadata.erase(filename);
            }

            response->set_is_open(is_open);
            return grpc::Status::OK;

        } catch (const std::exception& e) {
            std::cerr << "Error in CheckFileOpen: " << e.what() << std::endl;
            return grpc::Status(grpc::StatusCode::INTERNAL, "Failed to check if file is open");
        }
    }
};



void RunServer(const std::string line) {
    std::string server_address(line);
    MetaServerImpl service;

    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Metadata Server listening on " << server_address << std::endl;
    server->Wait();
}


int main(int argc, char *argv[]) {
    printf("%s:%s: PFS meta server start! Hostname: %s, IP: %s\n", __FILE__,
           __func__, getMyHostname().c_str(), getMyIP().c_str());

    // Parse pfs_list.txt
    std::ifstream pfs_list("../pfs_list.txt");
    if (!pfs_list.is_open()) {
        fprintf(stderr, "%s: can't open pfs_list.txt file.\n", __func__);
        exit(EXIT_FAILURE);
    }

    std::string line;
    std::getline(pfs_list, line);
    if (line.substr(0, line.find(':')) != getMyHostname()) {
        fprintf(stderr, "%s: hostname not on the first line of pfs_list.txt.\n",
                __func__);
        exit(EXIT_FAILURE);
    }
    pfs_list.close();
    std::string listen_port = line.substr(line.find(':') + 1);

    // Run the PFS metadata server and listen to requests
    printf("%s: Launching PFS metadata server on %s, with listen port %s...\n",
           __func__, getMyHostname().c_str(), listen_port.c_str());

    // Do something...
    RunServer(line);
    
    printf("%s:%s: PFS meta server done!\n", __FILE__, __func__);
    return 0;
}