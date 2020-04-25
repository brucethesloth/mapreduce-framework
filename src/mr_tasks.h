#pragma once

#include <string>
#include <iostream>
#include <fstream>
#include <vector>

const std::string KV_SEP = " ";

/* CS6210_TASK Implement this data structureas per your implementation.
		You will need this when your worker is running the map task*/
struct BaseMapperInternal {

    /* DON'T change this function's signature */
    BaseMapperInternal();

    /* DON'T change this function's signature */
    void emit(const std::string &key, const std::string &val);

    /* NOW you can add below, data members and member functions as per the need of your implementation*/
    void initialize(std::string, int, int);

    std::vector <std::string> get_output_files();

    std::vector <std::string> output_files;
};


/* CS6210_TASK Implement this function */
inline BaseMapperInternal::BaseMapperInternal() {}


/* CS6210_TASK Implement this function */
inline void BaseMapperInternal::emit(const std::string &key, const std::string &val) {
    std::size_t hash_value = std::hash < std::string > {}(key);
    std::size_t bucket = hash_value % output_files.size();
//    std::cout << "key: " << key << " hash: " << hash_value << " bucket: " << bucket << std::endl;
    std::ofstream output(output_files.at(bucket), std::ios::app);
    output << key << KV_SEP << val << std::endl;
//    std::cout << key << KV_SEP << val << std::endl;
    output.close();
}

inline std::vector <std::string> BaseMapperInternal::get_output_files() {
    return output_files;
}

inline void BaseMapperInternal::initialize(std::string output_dir, int shard_id, int num_outputs) {

    for (int i = 0; i < num_outputs; i++) {
        std::string file_name = output_dir + "/interim_" + std::to_string(shard_id) + "_part_" + std::to_string(i);
        std::remove(file_name.c_str());
        std::fstream output(file_name, std::ios::app);
        output.close();
        output_files.push_back(file_name);
    }
}

/*-----------------------------------------------------------------------------------------------*/


/* CS6210_TASK Implement this data structure as per your implementation.
		You will need this when your worker is running the reduce task*/
struct BaseReducerInternal {

    /* DON'T change this function's signature */
    BaseReducerInternal();

    /* DON'T change this function's signature */
    void emit(const std::string &key, const std::string &val);

    /* NOW you can add below, data members and member functions as per the need of your implementation*/
    void initialize(std::string, int);

    std::vector <std::string> get_output_files();

    std::vector <std::string> output_files;
};


/* CS6210_TASK Implement this function */
inline BaseReducerInternal::BaseReducerInternal() {

}

inline void BaseReducerInternal::initialize(std::string output_dir, int num_outputs) {
    for (int i = 0; i < num_outputs; i++) {
        std::string file_name = output_dir + "/final_" +  std::to_string(i);
//        std::remove(file_name.c_str());
        std::fstream output(file_name, std::ios::app);
        output.close();
        output_files.push_back(file_name);
    }
}

/* CS6210_TASK Implement this function */
inline void BaseReducerInternal::emit(const std::string &key, const std::string &val) {
//    std::cout << "Dummy emit by BaseReducerInternal: " << key << KV_SEP << val << std::endl;
    std::size_t hash_value = std::hash < std::string > {}(key);
    std::size_t bucket = hash_value % output_files.size();
//    std::cout << "key: " << key << " hash: " << hash_value << " bucket: " << bucket << std::endl;
    std::ofstream output(output_files.at(bucket), std::ios::app);
    output << key << KV_SEP << val << std::endl;
//    std::cout << key << KV_SEP << val << std::endl;
    output.close();
}

inline std::vector <std::string> BaseReducerInternal::get_output_files() {
    return output_files;
}
