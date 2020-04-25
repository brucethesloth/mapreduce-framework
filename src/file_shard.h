#pragma once

#include <vector>
#include <cstring>
#include <cmath>
#include <fstream>
#include <iostream>
#include "mapreduce_spec.h"
#include <algorithm>
#include <vector>

const int KB = 1024;

/**
 * Citation: https://www.geeksforgeeks.org/set-position-with-seekg-in-cpp-language-file-handling/
 *
 */

struct FileSegment {
    std::string file_name;
    std::streampos begin;
    std::streampos end;
};

/* CS6210_TASK: Create your own data structure here, where you can hold information about file splits,
     that your master would use for its own bookkeeping and to convey the tasks to the workers for mapping */
struct FileShard {
    int sid;
    std::vector<FileSegment *> segments;
};

inline void print_segment(FileSegment *segment) {
    std::cout << segment->file_name
              << "(" << segment->begin << " , " << segment->end << ")"
              << std::endl;
}

inline void print_shard(FileShard *shard) {

    std::cout << "Start ----- " << shard->sid << " -----" << std::endl;
    for (auto &segment : shard->segments) {
        print_segment(segment);
    }
    std::cout << "----- " << shard->sid << " ----- End" << std::endl;

};

inline FileSegment *create_file_segment(std::string file_name, std::streampos begin, std::streampos end) {
    FileSegment *segment = new FileSegment();

    segment->file_name = file_name;
    segment->begin = begin;
    segment->end = end;

    return segment;
}

inline FileShard *create_file_shard(int sid) {
    FileShard *shard = new FileShard();
    shard->sid = sid;
    return shard;
}

inline void append_segments(FileShard *shard, std::vector<FileSegment *> segments) {
    shard->segments = segments;
}


/**
 * Citation: http://www.cplusplus.com/doc/tutorial/files/
 * @param file_name
 * @return
 */
inline size_t get_input_file_size(const char *file_name) {
    std::streampos begin, end;
    std::ifstream file(file_name, std::ios::binary);

    begin = file.tellg();
    file.seekg(0, std::ios::end);
    end = file.tellg();
    file.close();

    return end - begin;
}

/**
 *
 * @param mr_spec
 * @return
 */
inline size_t get_total_input_size(const MapReduceSpec &mr_spec) {

    size_t total_size;

    for (std::string file_name : mr_spec.input_files) {
        total_size += get_input_file_size(file_name.c_str());
    }

    return total_size;
}

/* CS6210_TASK: Create fileshards from the list of input files, map_kilobytes etc. using mr_spec you populated  */
/**
 * Citation: https://stackoverflow.com/questions/25475384/when-and-why-do-i-need-to-use-cin-ignore-in-c
 * @param mr_spec
 * @param file_shards
 * @return
 */

inline bool shard_files(const MapReduceSpec &mr_spec, std::vector <FileShard> &file_shards) {
    int shard_capacity = mr_spec.map_size * KB;
    int shard_id = 1;
    int file_size, remainder_file_size;

    FileShard *current_file_shard = create_file_shard(shard_id);
    FileSegment *segment = nullptr;
    std::vector<FileSegment *> segments;
    int space_on_shard = shard_capacity;

    for (std::string input_file_name : mr_spec.input_files) {
        file_size = get_input_file_size(input_file_name.c_str());
        remainder_file_size = file_size;
        std::ifstream input(input_file_name, std::ios::binary);

        // fill current shard if the we still have some room
        if (space_on_shard > 0) {
            if (space_on_shard > file_size) {
                // our space can include the entire file
                segment = create_file_segment(input_file_name, 0, file_size);
                segments.push_back(segment);
                space_on_shard -= file_size;
            } else {
                // has room, but can only fill partial
                input.seekg(0, std::ios::beg);
                int start = input.tellg();
                input.seekg(space_on_shard, std::ios::cur);
                input.ignore(shard_capacity, '\n');
                int end = input.tellg();

                segment = create_file_segment(input_file_name, start, end - 1);
                segments.push_back(segment);

                remainder_file_size -= space_on_shard;

                append_segments(current_file_shard, segments);
                file_shards.push_back(*current_file_shard);

                segments.clear();
                space_on_shard = 0;
            }
        }

        // fill current file
        if (space_on_shard == 0) {
            int additional_shard = remainder_file_size / shard_capacity;
            int offset = remainder_file_size % shard_capacity;

            // first get the start and end
            int start = input.tellg();
            input.seekg( shard_capacity, std::ios::cur );
            int end = input.tellg();

            if (end >= file_size) {
                end = file_size - 1;
            }

            // if we need additional shards, we traverse it
            for (int i = 0; i < additional_shard; i++) {
                current_file_shard = create_file_shard(++shard_id);
                segment = create_file_segment(input_file_name, start, end);
                segments.push_back(segment);
                append_segments(current_file_shard, segments);
                file_shards.push_back(*current_file_shard);
                segments.clear();

                start = end + 1;
                end = std::min(file_size, start + shard_capacity);
            }

            // otherwise, just create a segment
            if (offset > 0) {
                current_file_shard = create_file_shard(++shard_id);
                segment = create_file_segment(input_file_name, start, end);
                segments.push_back(segment);

                space_on_shard = shard_capacity - offset;
            }
        }

        input.close();
    }

    if (segments.size() > 0) {
        append_segments(current_file_shard, segments);
        file_shards.push_back(*current_file_shard);
        segments.clear();
    }

    return true;
}
