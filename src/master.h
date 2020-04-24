#pragma once

#include <iostream>
#include <thread>
#include <unordered_map>
#include <mutex>
#include <condition_variable>
#include <vector>
#include <queue>
#include <chrono>
#include <string>
#include <thread>
#include <grpc++/grpc++.h>
#include "masterworker.grpc.pb.h"
#include "mapreduce_spec.h"
#include "file_shard.h"


// GRPC Stuff
using grpc::Channel;
using grpc::CompletionQueue;
using grpc::ClientContext;
using grpc::Status;
using grpc::ClientAsyncResponseReader;
using masterworker::ShardSegment;
using masterworker::ShardInfo;
using masterworker::WorkerService;
using masterworker::MapRequest;
using masterworker::MapReply;

const int TIMEOUT_MAX = 32;
const int INITIAL_TIME_OUT = 1;
const int BACK_OFF_FACTOR = 2;

struct MapTask {
    int timeout;
    MapRequest request;
};

struct Worker {
    std::string addr;
    std::unique_ptr <WorkerService::Stub> stub;
};

inline ShardInfo to_protobuf_shard(FileShard *shard) {
    ShardInfo protobuf_shard;

    for (FileSegment *segment : shard->segments) {

        ShardSegment *protobuf_segment = protobuf_shard.add_segments();
        protobuf_segment->set_filename(segment->file_name);
        protobuf_segment->set_begin(segment->begin);
        protobuf_segment->set_end(segment->end);
    }

    return protobuf_shard;
}

inline FileShard *to_file_shard(ShardInfo *protobuf_shard) {

    FileShard *file_shard;

    for (ShardSegment protobuf_segment : protobuf_shard->segments()) {
        FileSegment *file_segment;
        file_segment->file_name = protobuf_segment.filename();
        file_segment->begin = protobuf_segment.begin();
        file_segment->end = protobuf_segment.end();
        file_shard->segments.push_back(file_segment);
    }

    return file_shard;
}


/* CS6210_TASK: Handle all the bookkeeping that Master is supposed to do.
	This is probably the biggest task for this project, will test your understanding of map reduce */
class Master {

public:
    /* DON'T change the function signature of this constructor */
    Master(const MapReduceSpec &, const std::vector <FileShard> &);

    /* DON'T change this function's signature */
    bool run();

private:
    int M; // number of mapper
    int R; // number of reducers
    /* NOW you can add below, data members and member functions as per the need of your implementation*/
    std::queue <std::string> interim_file_queue;
    std::mutex interim_file_lock;
    std::condition_variable interim_cv;

    // map task management
    std::queue<MapTask *> map_task_queue;
    std::mutex map_task_lock;
    std::condition_variable map_cv;

    // worker management
    std::map <std::string, std::unique_ptr<WorkerService::Stub>> worker_stubs;
    std::queue <std::string> worker_queue;
    std::mutex worker_lock;
    std::condition_variable worker_cv;

    // GRPC Stuff
    CompletionQueue cq;

    // Map Stuff
    struct AsyncMapCall {
        int timeout;
        std::string worker_addr;
        MapRequest request;
        MapReply reply;
        ClientContext context;
        Status status;
        std::unique_ptr <ClientAsyncResponseReader<MapReply>> response_reader;
    };

    void MapPhase();

    void CallMap(std::string worker_addr, MapTask *task);

    void AsyncCompleteMap();
};

/* CS6210_TASK: This is all the information your master will get from the framework.
	You can populate your other class data members here if you want */
Master::Master(const MapReduceSpec &mr_spec, const std::vector <FileShard> &file_shards) {
    M = file_shards.size();

    // create map tasks
    for (FileShard file_shard : file_shards) {

        ShardInfo shard = to_protobuf_shard(&file_shard);
        MapRequest *request = new MapRequest;
        request->set_user_id(mr_spec.user_id);
        request->set_allocated_shard(&shard);

        MapTask *task = new MapTask;
        task->timeout = INITIAL_TIME_OUT;
        task->request = *request;

        map_task_queue.push(task);
    }

    // initialize worker resources
    for (std::string worker_addr : mr_spec.worker_addrs) {
        std::shared_ptr <Channel> channel =
                grpc::CreateChannel(worker_addr, grpc::InsecureChannelCredentials());
        std::unique_ptr <WorkerService::Stub> stub(WorkerService::NewStub(channel));
        worker_stubs.insert(std::pair < std::string,
                            std::unique_ptr < WorkerService::Stub > > (worker_addr, std::move(stub)));
        worker_queue.push(worker_addr);
    }
}

/* CS6210_TASK: Here you go. once this function is called you will complete whole map reduce task and return true if succeeded */
bool Master::run() {
    std::cout << "Map Phase Started! " << std::endl;
    MapPhase();

    std::cout << "Map Phase Complete!" << std::endl;
    return true;
}

void Master::MapPhase() {

    while (interim_file_queue.size() < M) {
        std::cout << "Interim File Size: " << interim_file_queue.size() << " M: " << M << std::endl;

        // 1. Get a Task
        std::unique_lock<std::mutex> task_guard( map_task_lock );
        while( map_task_queue.size() == 0 ) {
            map_cv.wait(task_guard, [this] { return map_task_queue.size() > 0; });
        }
        MapTask *task = map_task_queue.front();
        map_task_queue.pop();
        map_cv.notify_one();
        task_guard.unlock();
        std::cout << "Got Task for [shard " << task->request.shard().id() << "]" << std::endl;

        // 2. Get a worker
        std::unique_lock<std::mutex> worker_guard( worker_lock );
        while( worker_queue.size() == 0 ) {
            worker_cv.wait( worker_guard, [this] { return worker_queue.size() > 0; } );
        }
        std::string worker_addr = worker_queue.front();
        worker_queue.pop();
        worker_cv.notify_one();
        worker_guard.unlock();
        std::cout << "Got Worker from [ " << worker_addr << "]" << std::endl;

        // 3. Spawn a Listener Routine
        std::thread map_listener = std::thread(&Master::AsyncCompleteMap, this);
        map_listener.detach();

        // 4. do async map call
        CallMap( worker_addr, task );

    }
}


void Master::CallMap(std::string worker_addr, MapTask *task) {
    std::cout << "Call Map Started!" << std::endl;
    // fill in call data: timeout, worker_addr, protobuf_shard are extra stuff
    AsyncMapCall *map_call = new AsyncMapCall;
    map_call->timeout = task->timeout;
    map_call->worker_addr = worker_addr;
    map_call->request = task->request;
    std::unique_ptr <WorkerService::Stub> &stub_ = worker_stubs.at(worker_addr);
    map_call->response_reader =
            stub_->PrepareAsyncDoMap(&map_call->context, task->request, &cq);

    // set deadline
    std::chrono::system_clock::time_point deadline =
            std::chrono::system_clock::now() + std::chrono::seconds(task->timeout);

    map_call->context.set_deadline(deadline);

    // make the call!
    map_call->response_reader->StartCall();
    map_call->response_reader->Finish(&map_call->reply, &map_call->status, (void *) map_call);
}

void Master::AsyncCompleteMap() {
    std::cout << "Listener Thread Started!" << std::endl;
    void *got_tag;
    bool ok = false;
    std::string worker_addr;

    while (cq.Next(&got_tag, &ok)) {
        AsyncMapCall *call = static_cast<AsyncMapCall *>(got_tag);

        GPR_ASSERT(ok);

        if (!call->status.ok()) {
            std::cout << "RPC failed. Error Code: " << call->status.error_code() << std::endl;
            // remake map task with new back off
            int current_timeout = call->timeout;
            int new_timeout = current_timeout;
            if (call->status.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED) {
                // remake map task with new back off
                new_timeout = current_timeout < TIMEOUT_MAX ? BACK_OFF_FACTOR * current_timeout : current_timeout;
            }

            MapTask *new_task = new MapTask;
            new_task->timeout = new_timeout;
            new_task->request = call->request;
            std::cout << "Retrying for request shard [" << new_task->request.shard().id() << "] " << std::endl;

            // queue the new attempt -- wait until the task queue is empty
            std::unique_lock <std::mutex> map_task_guard(map_task_lock);
            while (map_task_queue.size() > 0) {
                map_cv.wait(map_task_guard, [this] { return map_task_queue.size() == 0; });
            }
            map_task_queue.push(new_task);
            map_cv.notify_one();
            map_task_guard.unlock();
        } else {
            std::cout << "RPC Completed Successfully. "
                      << "Reply: [ " << call->reply.worker_addr() << "," << call->reply.filename() << " ]"
                      << std::endl;

            // queue reply to interim file
            std::unique_lock <std::mutex> interim_files_guard(interim_file_lock);
            interim_file_queue.push(call->reply.filename());
            interim_files_guard.unlock();

            std::cout << "Pushed Intrim Files back!" << std::endl;
        }

        std::cout << "Pushing Worker Back!" << std::endl;
        // put worker back - wait until all workers are gone
        std::unique_lock <std::mutex> worker_queue_guard(worker_lock);
        worker_queue.push(call->worker_addr);
        worker_cv.notify_one();
        worker_queue_guard.unlock();

        std::cout << "Pushed Worker Back!" << std::endl;

        delete call;
    }
}
