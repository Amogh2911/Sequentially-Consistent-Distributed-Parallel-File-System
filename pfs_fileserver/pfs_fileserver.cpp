#include "pfs_fileserver.hpp"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

std::string path = "/scratch/";
class FileServerImpl final : public pfsfile::FileServer::Service {
public:

    // HealthCheck implementation
    Status HealthCheck(ServerContext* context,
                                    const pfsfile::HealthCheckRequest* request,
                                    pfsfile::HealthCheckResponse* response) {
        std::string client_message = request->message();
        std::cout << "Received HealthCheck request: " << client_message << std::endl;

        // Respond with "pong"
        response->set_message("pong");
        std::cout << "Responded with HealthCheck: pong" << std::endl;
        return Status::OK;
    }


    Status WriteFile(ServerContext* context,
                 const pfsfile::WriteFileRequest* request,
                 pfsfile::WriteFileResponse* response) override {
        // Construct full file path
        const std::string full_path = path + request->filename();
        int64_t start_offset = request->start_offset();
        const std::string& data = request->data();
        int64_t num_bytes = request->num_bytes();

        std::cout << "Write request for file: " << full_path
                << " at offset: " << start_offset
                << " with data size: " << data.size() << std::endl;

        // Open the file in read-write mode, create if not exists
        std::fstream file(full_path, std::ios::in | std::ios::out | std::ios::binary);
        if (!file.is_open()) {
            // Try to create the file if it doesn't exist
            file.open(full_path, std::ios::out | std::ios::binary);
            file.close();

            // Reopen in read-write mode
            file.open(full_path, std::ios::in | std::ios::out | std::ios::binary);
            if (!file.is_open()) {
                std::cerr << "Error: Failed to open or create file: " << full_path << std::endl;
                response->set_status(-1);
                response->set_message("Failed to open or create file.");
                return Status::OK;
            }
        }

        // Seek to the start offset
        file.seekp(start_offset, std::ios::beg);
        if (!file) {
            std::cerr << "Error: Failed to seek to offset " << start_offset << " in file: " << full_path << std::endl;
            file.close();
            response->set_status(-1);
            response->set_message("Failed to seek to offset.");
            return Status::OK;
        }

        // Write the data
        file.write(data.c_str(), num_bytes);
        if (!file) {
            std::cerr << "Error: Failed to write all data to file: " << full_path << std::endl;
            file.close();
            response->set_status(-1);
            response->set_message("Failed to write all data.");
            return Status::OK;
        }

        file.close();

        std::cout << "Successfully wrote " << num_bytes << " bytes to file: " << full_path << std::endl;

        // Set response
        response->set_status(0);
        response->set_message("Write successful.");
        response->set_bytes_written(num_bytes);

        return Status::OK;
    }

    
    // ReadFile implementation
    Status ReadFile(ServerContext* context,
                    const pfsfile::ReadFileRequest* request,
                    pfsfile::ReadFileResponse* response) override {
        // Construct the full file path
        const std::string full_path = path + request->filename();
        int64_t start_offset = request->start_offset();
        int64_t num_bytes = request->num_bytes();
        int32_t mode = request->mode();

        std::cout << "Read request for file: " << full_path
                  << " at offset: " << start_offset
                  << " with num_bytes: " << num_bytes << std::endl;

        // // Open the file in read mode
        // std::ifstream file(full_path, std::ios::binary | std::ios::in);
        // if (!file.is_open()) {
        //     std::cerr << "Error: Failed to open file: " << full_path << std::endl;
        //     response->set_status(-1);
        //     response->set_message("Failed to open file.");
        //     return Status::OK;
        // }

        std::cout<<"Mode: "<< mode<<std::endl;
        // Open the file in read-write mode, create if not exists
        std::fstream file(full_path, std::ios::in | std::ios::binary);
        if (!file.is_open()) {
            if(mode == 1){
                std::cerr << "Error: Failed to open file: " << full_path << std::endl;
                response->set_status(-1);
                response->set_message("Failed to open file.");
                return Status::OK;
            }
            else{
                // Try to create the file if it doesn't exist
                file.open(full_path, std::ios::out | std::ios::binary);
                file.close();

                // Reopen in read-write mode
                file.open(full_path, std::ios::in | std::ios::binary);
                if (!file.is_open()) {
                    std::cerr << "Error: Failed to open or create file: " << full_path << std::endl;
                    response->set_status(-1);
                    response->set_message("Failed to open or create file.");
                    return Status::OK;
                }
            }
        }

        // Seek to the start offset
        file.seekg(start_offset, std::ios::beg);
        if (!file) {
            std::cerr << "Error: Failed to seek to offset " << start_offset << " in file: " << full_path << std::endl;
            file.close();
            response->set_status(-1);
            response->set_message("Failed to seek to offset.");
            return Status::OK;
        }

        // Read the requested block
        std::string data(num_bytes, '\0');
        file.read(&data[0], num_bytes);
        std::streamsize bytes_read = file.gcount();

        // if (bytes_read == 0) {
        //     std::cerr << "Error: End of file reached for file: " << full_path << std::endl;
        //     file.close();
        //     response->set_status(-1);
        //     response->set_message("End of file reached.");
        //     return Status::OK;
        // }

        // Close the file
        file.close();

        std::cout << "Successfully read " << bytes_read << " bytes from file: " << full_path << std::endl;

        // Populate the response
        response->set_status(0);
        response->set_message("Read successful.");
        response->set_data(data.substr(0, bytes_read)); // Send only the bytes read

        return Status::OK;
    }

    // Status DeleteFile(ServerContext* context,
    //               const pfsfile::DeleteFileRequest* request,
    //               pfsfile::DeleteFileResponse* response) override {
    //     // Construct the full file path
    //     const std::string full_path = path + request->filename();

    //     std::cout << "Delete request for file: " << full_path << std::endl;

    //     // Try to remove the file
    //     if (std::remove(full_path.c_str()) == 0) {
    //         std::cout << "Successfully deleted file: " << full_path << std::endl;

    //         // Populate response
    //         response->set_status(0); // Success
    //         response->set_message("File successfully deleted.");
    //     } else {
    //         std::cerr << "Error: Failed to delete file: " << full_path << std::endl;

    //         // Populate response with error
    //         response->set_status(-1); // Failure
    //         response->set_message("Failed to delete file. File may not exist or permission denied.");
    //     }

    //     return Status::OK;
    // }

    Status DeleteFile(ServerContext* context,
                  const pfsfile::DeleteFileRequest* request,
                  pfsfile::DeleteFileResponse* response) override {
        // Construct the full file path
        const std::string full_path = path + request->filename();

        std::cout << "Delete request for file: " << full_path << std::endl;

        // Check if the file exists
        if (std::filesystem::exists(full_path)) {
            // Try to remove the file
            if (std::remove(full_path.c_str()) == 0) {
                std::cout << "Successfully deleted file: " << full_path << std::endl;

                // Populate response
                response->set_status(0); // Success
                response->set_message("File successfully deleted.");
            } else {
                std::cerr << "Error: Failed to delete file: " << full_path << std::endl;

                // Populate response with error
                response->set_status(-1); // Failure
                response->set_message("Failed to delete file. Permission denied.");
            }
        } else {
            std::cout << "File not found: " << full_path << ". Nothing to delete." << std::endl;

            // Populate response indicating there's nothing to delete
            response->set_status(0); // Success (since there's no error in this case)
            response->set_message("File does not exist. Nothing to delete.");
        }

        return Status::OK;
    }


};


void RunFileServer(const std::string server_address){

    FileServerImpl service;
    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "FileServer listening on " << server_address << std::endl;

    server->Wait();
}

int main(int argc, char *argv[]) {
    printf("%s:%s: PFS file server start! Hostname: %s, IP: %s\n",
           __FILE__, __func__, getMyHostname().c_str(), getMyIP().c_str());

    // Parse pfs_list.txt
    std::ifstream pfs_list("../pfs_list.txt");
    if (!pfs_list.is_open()) {
        fprintf(stderr, "%s: can't open pfs_list.txt file.\n", __func__);
        exit(EXIT_FAILURE);
    }

    bool found = false;
    std::string line;
    std::getline(pfs_list, line); // First line is the meta server
    while (std::getline(pfs_list, line)) {
        if (line.substr(0, line.find(':')) == getMyHostname()) {
            found = true;
            break;
        }
    }
    if (!found) {
        fprintf(stderr, "%s: hostname not found in pfs_list.txt.\n", __func__);
        exit(EXIT_FAILURE);
    }
    pfs_list.close();
    std::string listen_port = line.substr(line.find(':') + 1);

    // Run the PFS fileserver and listen to requests
    printf("%s: Launching PFS file server on %s, with listen port %s...\n",
           __func__, getMyHostname().c_str(), listen_port.c_str());

    // Do something...

    RunFileServer(line);

    
    printf("%s:%s: PFS file server done!\n", __FILE__, __func__);
    return 0;
}
