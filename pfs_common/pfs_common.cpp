#include "pfs_common.hpp"

// Get the current node's hostname
std::string getMyHostname() {
    char hostname[255] = {0};
    gethostname(hostname, sizeof(hostname));

    std::string ret(hostname);
    return ret;
}

// Get the current node's IP address
std::string getMyIP() {
    struct hostent *host_entry = gethostbyname(getMyHostname().c_str());
    char *temp = inet_ntoa(*((struct in_addr *)host_entry->h_addr_list[0]));

    std::string ret(temp);
    return ret;
}
