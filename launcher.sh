#!/bin/bash

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
RESET='\033[0m'

execute_server() {
    filename="pfs_list.txt"
    current_hostname=$(hostname)
    file_line_number=0
    match_found=false

    while IFS= read -r line; do
        ((file_line_number++))
        host=$(echo "$line" | cut -d':' -f1)
        port=$(echo "$line" | cut -d':' -f2)

        if [[ "$host" == "$current_hostname" && "$file_line_number" -eq 1 ]]; then
            match_found=true
            echo -e "${GREEN}Launching the metaserver...${RESET}"
            pushd pfs_metaserver > /dev/null
            ./pfs_metaserver
            popd > /dev/null
            break
        elif [[ "$host" == "$current_hostname" ]]; then
            match_found=true
            echo -e "${GREEN}Launching the fileserver...${RESET}"
            pushd pfs_fileserver > /dev/null
            ./pfs_fileserver
            popd > /dev/null
            break
        fi
    done < "$filename"

    if ! $match_found; then
        echo "My hostname $current_hostname is not in the pfs_list.txt file."
        exit 1
    fi
}

if [[ "$#" -eq 0 ]]; then
    echo -e "${RED}No argument provided. Exiting the script.${RESET}"
    exit 1
fi

if [[ "$1" == "0" ]]; then
    execute_server
    exit 0
fi
