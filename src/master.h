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

enum TaskStatus {
    PENDING, RUNNING, COMPLETED
};
enum WorkerStatus {
    BUSY, AVAILABLE
};

struct MapTask {
    int shard_id;
    TaskStatus status;
    MapRequest request;
};

struct WorkerInfo {
    std::string addr;
    int timeout;
    WorkerStatus status;
};

inline ShardInfo to_protobuf_shard(FileShard *shard) {
    ShardInfo protobuf_shard;

    for (FileSegment *segment : shard->segments) {

        ShardSegment *protobuf_segment = protobuf_shard.add_segments();
        protobuf_segment->set_file_name(segment->file_name);
        protobuf_segment->set_begin(segment->begin);
        protobuf_segment->set_end(segment->end);
    }

    return protobuf_shard;
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
    /* NOW you can add below, data members and member functions as per the need of your implementation*/
    int M; // number of mapper
    int R; // number of reducers

    // map task management
    std::vector<MapTask *> map_task_tracker;

    // worker management
    std::map <std::string, std::unique_ptr<WorkerService::Stub>> worker_stubs;
    std::vector<WorkerInfo *> worker_tracker;

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

    void CallMap(WorkerInfo *worker_info, MapTask *task);
    void AsyncCompleteMap();

    bool shouldDoMapWork();
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
        request->set_num_outputs(mr_spec.num_outputs);
        request->set_output_dir(mr_spec.output_dir);
        request->set_allocated_shard(&shard);

        MapTask *task = new MapTask;
        task->shard_id = shard.id();
        task->request = *request;
        task->status = PENDING;

        map_task_tracker.push_back(task);
    }

    // initialize worker resources
    for (std::string worker_addr : mr_spec.worker_addrs) {
        std::shared_ptr <Channel> channel =
                grpc::CreateChannel(worker_addr, grpc::InsecureChannelCredentials());
        std::unique_ptr <WorkerService::Stub> stub(WorkerService::NewStub(channel));

        std::cout << "Initiating worker resource for [" << worker_addr << "]" << std::endl;
        worker_stubs.insert(std::pair < std::string,
                            std::unique_ptr < WorkerService::Stub > > (worker_addr, std::move(stub)));

        WorkerInfo *worker_info = new WorkerInfo;

        worker_info->timeout = INITIAL_TIME_OUT;
        worker_info->addr = worker_addr;
        worker_info->status = AVAILABLE;

        worker_tracker.push_back(worker_info);

        std::cout << "Worker Resource initiated for [" << worker_addr << "]" << std::endl;
    }
}

/* CS6210_TASK: Here you go. once this function is called you will complete whole map reduce task and return true if succeeded */
bool Master::run() {
    MapPhase();

    return true;
}

void Master::MapPhase() {
    std::cout << "Map Phase Started! " << std::endl;

    // 1. Spawn a listener routine to reap the results of the future map calls
    std::thread map_listener(&Master::AsyncCompleteMap, this);

    // 2. Do Map work
    while (shouldDoMapWork()) {

        //2.1 Pick a Task
        int t = -1;
        for (int i = 0; i < map_task_tracker.size(); i++) {
            if (map_task_tracker.at(i)->status == PENDING) {
                map_task_tracker.at(i)->status = RUNNING;
                t = i;
                break;
            }
        }

        if (t != -1) {
            // 2.2 Pick a worker
            std::cout << "t: " << t << std::endl;
            std::cout << "Work needs to be done for shard [" << map_task_tracker.at(t)->shard_id << "]." << std::endl;

            int w = -1;

            for (int j = 0; j < worker_tracker.size(); j++) {
                if (worker_tracker.at(j)->status == AVAILABLE) {
                    worker_tracker.at(j)->status = BUSY;
                    w = j;
                    break;
                }
            }

            if (w != -1) {
                std::cout << "Got Worker from [" << worker_tracker.at(w)->addr << "]" << std::endl;

                // 2.3 Call Map
                std::thread async_callMap(&Master::CallMap, this, worker_tracker.at(w), map_task_tracker.at(t));
                async_callMap.detach();

                std::cout << "Dispatched job for shard [" << map_task_tracker.at(t)->shard_id
                          << "] to worker [" << worker_tracker.at(w)->addr << "]" << std::endl;
            }

            std::cout << "No worker is available at the moment for shard [" << t << "]" << std::endl;
        } else {
            std::cout << "All tasks are either running or finished" << std::endl;
        }
    }

    map_listener.join();

    std::cout << "Map Phase Complete!" << std::endl;
}


void Master::CallMap(WorkerInfo *worker_info, MapTask *task) {
    std::cout << "Shard [" << task->shard_id
              << "] is being handled by Worker [" << worker_info->addr
              << "]." << std::endl;

    // fill in call data: timeout, worker_addr, protobuf_shard are extra stuff
    AsyncMapCall *map_call = new AsyncMapCall;
    map_call->timeout = worker_info->timeout;
    map_call->worker_addr = worker_info->addr;
    map_call->request = task->request;
    std::unique_ptr <WorkerService::Stub> &stub_ = worker_stubs.at(worker_info->addr);
    map_call->response_reader =
            stub_->PrepareAsyncDoMap(&map_call->context, task->request, &cq);

    // set deadline
    std::chrono::system_clock::time_point deadline =
            std::chrono::system_clock::now() + std::chrono::seconds(worker_info->timeout);

    map_call->context.set_deadline(deadline);

    // make the call!
    map_call->response_reader->StartCall();
    map_call->response_reader->Finish(&map_call->reply, &map_call->status, (void *) map_call);
}

void Master::AsyncCompleteMap() {
    std::cout << "Listener Thread Started!" << std::endl;
    void *got_tag;
    bool ok = false;

    while (shouldDoMapWork()) {
        cq.Next(&got_tag, &ok);

        AsyncMapCall *call = static_cast<AsyncMapCall *>(got_tag);

        GPR_ASSERT(ok);

        // updating task status
        std::cout << "Updating Status for shard [" << call->request.shard().id() << "]" << std::endl;
        for (MapTask *t : map_task_tracker) {
            if (t->shard_id == call->request.shard().id()) {
                if (!call->status.ok()) {
                    std::cout << "Shard [" << call->request.shard().id() << "] failed!" << std::endl;
                    t->status = PENDING;
                } else {
                    std::cout << "Shard [" << call->request.shard().id() << "] completed!" << std::endl;
                    t->status = COMPLETED;
                }

                break;
            }
        }

        // updating worker status
        int new_timeout;
        std::cout << "Releasing Worker [" << call->worker_addr << "] Back!" << std::endl;
        for (WorkerInfo *worker_info : worker_tracker) {
            if (std::strcmp( worker_info->addr.c_str(), call->worker_addr.c_str() ) == 0) {

                if (!call->status.ok()) {
                    new_timeout = call->timeout < TIMEOUT_MAX ? BACK_OFF_FACTOR * call->timeout : call->timeout;
                } else {
                    new_timeout = call->timeout > INITIAL_TIME_OUT ? (call->timeout / BACK_OFF_FACTOR) : call->timeout;
                }

                worker_info->timeout = new_timeout;
                worker_info->status = AVAILABLE;
                break;
            }
        }
        std::cout << "Worker [" << call->worker_addr << "] released!" << std::endl;

        delete call;
    }
}

bool Master::shouldDoMapWork() {
    for (MapTask *task : map_task_tracker) {

        if (task->status != COMPLETED) {
            return true;
        }
    }

    std::cout << "Work Done !" << std::endl;
    return false;
}
