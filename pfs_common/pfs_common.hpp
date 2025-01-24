#pragma once

#include <ctime>
#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <sys/types.h>

#include <unistd.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <string>
#include <iostream>
#include <fstream>
#include <vector>

#include "pfs_config.hpp"

std::string getMyHostname();
std::string getMyIP();
