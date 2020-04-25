#pragma once

#include <cstring>
#include <fstream>
#include <sstream>
#include <vector>
#include <unordered_map>
#include <cstdio>
#include <iostream>

/**
 * Parse a comma deliminated string
 * Citation: https://www.tutorialspoint.com/parsing-a-comma-delimited-std-string-in-cplusplus
 * @param input string to be split
 * @param delim character to be split on
 * @return a vector containing all the strings
 */
inline std::vector<std::string> split(std::string input, char delim) {
    std::vector<std::string> result;
    std::stringstream input_stream(input);

    while (input_stream.good()) {
        std::string word;
        getline(input_stream, word, delim);
        result.push_back(word);
    }

    return result;
}


/**
 * Citations:
 * 1. https://www.tutorialspoint.com/parsing-a-comma-delimited-std-string-in-cplusplus
 */

/* CS6210_TASK: Create your data structure here for storing spec from the config file */
struct MapReduceSpec {
    int num_workers; // n_workers
    int num_outputs; // n_output_files
    int map_size; // map_kilobytes
    std::string output_dir; // output_dir
    std::string user_id;  // user_id
    std::vector <std::string> worker_addrs; // worker_ipaddr_ports
    std::vector <std::string> input_files; // input_files
};


/* CS6210_TASK: Populate MapReduceSpec data structure with the specification from the config file */
/**
 * Citation: https://stackoverflow.com/questions/347949/how-to-convert-a-stdstring-to-const-char-or-char
 * Citation: https://stackoverflow.com/questions/10732010/format-s-expects-argument-of-type-char
 *
 * @param config_filename
 * @param mr_spec
 * @return
 */
inline bool read_mr_spec_from_config_file(
        const std::string &config_filename,
        MapReduceSpec &mr_spec
) {

    std::ifstream config_file(config_filename);
    std::string property;
    std::unordered_map <std::string, std::string> config_map;

    char key[256];
    char value[256];

    // read
    while (std::getline(config_file, property)) {
        sscanf(property.c_str(), "%[_,a-z,A-Z]=%s", key, value);
        config_map[key] = value;
    }
    config_file.close();

    // set fields
    mr_spec.num_workers = atoi(config_map["n_workers"].c_str());
    mr_spec.num_outputs = atoi(config_map["n_output_files"].c_str());
    mr_spec.map_size = atoi(config_map["map_kilobytes"].c_str());
    mr_spec.output_dir = config_map["output_dir"];
    mr_spec.user_id = config_map["user_id"];
    mr_spec.worker_addrs = std::move(split(config_map["worker_ipaddr_ports"], ','));
    mr_spec.input_files = std::move(split(config_map["input_files"], ','));

    return true;
}


/* CS6210_TASK: validate the specification read from the config file */
/**
 * Citation: https://github.com/Overv/VulkanTutorial/issues/10
 * @param mr_spec
 * @return
 */
inline bool validate_mr_spec(const MapReduceSpec &mr_spec) {
    bool worker_pass = mr_spec.num_workers > 0 && (mr_spec.num_workers == mr_spec.worker_addrs.size());
    bool output_pass = mr_spec.map_size > 0 && mr_spec.num_outputs > 0;
    bool output_dir_pass = !mr_spec.output_dir.empty();
    bool user_id_pass = !mr_spec.user_id.empty();

    bool input_files_pass = mr_spec.input_files.size() > 0;
    if (input_files_pass) {
        for (std::string file_name: mr_spec.input_files) {
            std::ifstream input_file(file_name);
            input_files_pass &= input_file.is_open();
            input_file.close();
        }
    }

    // validate worker address port
    bool worker_addr_pass = mr_spec.worker_addrs.size() > 0;
    if (worker_addr_pass) {
        char host[50], port[50];
        for (std::string worker_addr : mr_spec.worker_addrs) {
            sscanf(worker_addr.c_str(), "%[A-Z,a-z]:%[0-9]", host, port);
            worker_addr_pass &= (strncmp(host, "localhost", 9) == 0);
            worker_addr_pass &= (0 < atoi(port) && atoi(port) <= 65535);
        }
    }

    return worker_pass && output_pass && output_dir_pass && user_id_pass && input_files_pass && worker_addr_pass;
}
