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
using masterworker::ReduceRequest;
using masterworker::ReduceReply;

const int TIMEOUT_MAX = 4;
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

struct ReduceTask {
    int reduce_id;
    TaskStatus status;
    ReduceRequest request;
};

struct WorkerInfo {
    std::string addr;
    int timeout;
    WorkerStatus status;
};

inline ShardInfo to_protobuf_shard(FileShard *shard) {
    ShardInfo protobuf_shard;
    protobuf_shard.set_shard_id(shard->sid);

    for (FileSegment *segment : shard->segments) {

        ShardSegment *protobuf_segment = protobuf_shard.add_segments();
        protobuf_segment->set_file_name(segment->file_name);
        protobuf_segment->set_begin(segment->begin);
        protobuf_segment->set_end(segment->end);
    }

    return protobuf_shard;
}

inline void print_map_tracker(std::vector<MapTask *> tasks) {
    for (MapTask *task : tasks) {
        std::string status = task->status == PENDING ? "P" : task->status == RUNNING ? "R" : "C";
        std::cout << "Task:"
                  << " shard: " << task->shard_id
                  << " status: " << status << std::endl;
    }
}

inline void print_reduce_tracker(std::vector<ReduceTask *> tasks) {
    for (ReduceTask *task : tasks) {
        std::string status = task->status == PENDING ? "P" : task->status == RUNNING ? "R" : "C";
        std::cout << "Task:"
                  << " part: " << task->reduce_id
                  << " status: " << status
                  << std::endl;

    }
}

inline void print_worker_tracker( std::vector<WorkerInfo *> worker_tracker) {
    for (WorkerInfo *info : worker_tracker) {
        std::string status = info->status == BUSY ? "Busy" : "Available";
        std::cout << "Worker: " << info->addr
                    << " status: " << status
                    << " timeout: " << info->timeout
                    << std::endl;
    }
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
    MapReduceSpec mr_spec;
    std::vector <FileShard> file_shards;

    // tasks management
    std::vector<MapTask *> map_task_tracker;
    std::vector<ReduceTask *> reduce_task_tracker;

    // worker management
    std::map <std::string, std::unique_ptr<WorkerService::Stub>> worker_stubs;
    std::vector<WorkerInfo *> worker_tracker;

    // GRPC Stuff
    CompletionQueue cq;

    struct AsyncMapCall {
        int timeout;
        std::string worker_addr;
        MapRequest request;
        MapReply reply;
        ClientContext context;
        Status status;
        std::unique_ptr <ClientAsyncResponseReader<MapReply>> response_reader;
    };

    struct AsyncReduceCall {
        int timeout;
        std::string worker_addr;
        ReduceRequest request;
        ReduceReply reply;
        ClientContext context;
        Status status;
        std::unique_ptr <ClientAsyncResponseReader<ReduceReply>> response_reader;
    };

    // Worker Functions
    void WorkerSetup();
    void SetWorkerAvailable(std::string, int);
    WorkerInfo *GetAvailableWorker();

    // Map Functions
    void PrepareMapPhase();

    void MapPhase();

    void CallMap(MapTask *task, WorkerInfo *worker_info);

    void AsyncCompleteMap();

    bool ShouldDoMapWork();

    // Reduce Functions
    void PrepareReducePhase();

    void ReducePhase();

    void CallReduce(ReduceTask *, WorkerInfo *);

    void AsyncCompleteReduce();

    bool ShouldDoReduceWork();
};

/* CS6210_TASK: This is all the information your master will get from the framework.
	You can populate your other class data members here if you want */
Master::Master(const MapReduceSpec &mr_spec, const std::vector <FileShard> &file_shards) {
    this->mr_spec = mr_spec;
    this->file_shards = std::move(file_shards);

    WorkerSetup();
    PrepareMapPhase();
    PrepareReducePhase();
}

/* CS6210_TASK: Here you go. once this function is called you will complete whole map reduce task and return true if succeeded */
bool Master::run() {
    MapPhase();
    ReducePhase();
    std::cout << "Map Reduce Done..." <<  std::endl;

    return true;
}

void Master::WorkerSetup() {
    // initialize worker resources
    for (std::string worker_addr : mr_spec.worker_addrs) {
        std::shared_ptr <Channel> channel =
                grpc::CreateChannel(worker_addr, grpc::InsecureChannelCredentials());
        std::unique_ptr <WorkerService::Stub> stub(WorkerService::NewStub(channel));

        worker_stubs.insert(std::pair < std::string,
                            std::unique_ptr < WorkerService::Stub > > (worker_addr, std::move(stub)));

        WorkerInfo *worker_info = new WorkerInfo;

        worker_info->timeout = INITIAL_TIME_OUT;
        worker_info->addr = worker_addr;
        worker_info->status = AVAILABLE;

        worker_tracker.emplace_back(worker_info);

    }
}

void Master::SetWorkerAvailable(std::string worker_addr, int new_timeout) {
    for (WorkerInfo *worker_info : worker_tracker) {
        if (std::strcmp(worker_info->addr.c_str(), worker_addr.c_str()) == 0) {
            worker_info->timeout = new_timeout;
            worker_info->status = AVAILABLE;
            break;
        }
    }
}

WorkerInfo *Master::GetAvailableWorker() {
    WorkerInfo *available_worker = nullptr;

    for (WorkerInfo *it : worker_tracker) {
        if (it->status == AVAILABLE) {
            available_worker = it;
        }

        break;
    }

    return available_worker;
}

void Master::PrepareMapPhase() {
    // create map tasks
    for (FileShard file_shard : file_shards) {

        ShardInfo shard = to_protobuf_shard(&file_shard);
        MapRequest *request = new MapRequest;
        request->set_user_id(mr_spec.user_id);
        request->set_num_outputs(mr_spec.num_outputs);
        request->set_output_dir(mr_spec.output_dir);
        request->set_allocated_shard(&shard);

        MapTask *task = new MapTask;
        task->shard_id = file_shard.sid;
        task->request = *request;
        task->status = PENDING;

        map_task_tracker.emplace_back(task);
    }

    print_map_tracker(map_task_tracker);
}

void Master::MapPhase() {
    std::cout << "Map Phase Started!" << std::endl;

    // 1. Spawn a map listener routine to reap the results of the future map calls
    std::thread map_listener(&Master::AsyncCompleteMap, this);

    // 2. Do Map work
    while (ShouldDoMapWork()) {

        MapTask *task = nullptr;

        // 2.1 Pick a task
        for (MapTask *it: map_task_tracker) {
            if (it->status == PENDING) {
                task = it;
                break;
            }
        }

        if (task != nullptr) {
            // 2.2 Pick a worker
            WorkerInfo *worker = GetAvailableWorker();

            if (worker != nullptr) {
                task->status = RUNNING;
                worker->status = BUSY;

                // 2.3 Call Map
                std::thread async_callMap(&Master::CallMap, this, task, worker);
                async_callMap.detach();

                std::cout << "Dispatched job for shard [" << task->shard_id
                          << "] to worker [" << worker->addr << "]" << std::endl;
            }
        }
    }

    map_listener.join();

    std::cout << "Map Phase Complete!" << std::endl;
}

void Master::CallMap(MapTask *task, WorkerInfo *worker_info) {
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
    std::cout << "Map Listener Thread Started!" << std::endl;
    void *got_tag;
    bool ok = false;

    while (ShouldDoMapWork()) {
        cq.Next(&got_tag, &ok);
        AsyncMapCall *call = static_cast<AsyncMapCall *>(got_tag);

        GPR_ASSERT(ok);

        // updating map task status
        int shard_id = call->request.shard().shard_id();
        TaskStatus task_status;
        int new_timeout;

        if (!call->status.ok()) {
            std::cout << "map [" << shard_id << "] failed!" << std::endl;
            task_status = PENDING;
            new_timeout = call->timeout < TIMEOUT_MAX ? BACK_OFF_FACTOR * call->timeout : call->timeout;
        } else {
            std::cout << "map [" << shard_id << "] completed!" << std::endl;
            task_status = COMPLETED;
            for (int i = 0; i < call->reply.file_names().size(); i++) {
                std::string interim_filename = call->reply.file_names(i);
                reduce_task_tracker.at(i)->request.add_file_names(interim_filename);
            }
            new_timeout = call->timeout > INITIAL_TIME_OUT ? (call->timeout / BACK_OFF_FACTOR) : call->timeout;
        }

        map_task_tracker.at(shard_id - 1)->status = task_status;
        SetWorkerAvailable(call->worker_addr, new_timeout);
        std::cout << "worker [" << call->worker_addr << "] released!" << std::endl;

        delete call;
    }
}

bool Master::ShouldDoMapWork() {
    for (MapTask *task : map_task_tracker) {

        if (task->status != COMPLETED) {
            return true;
        }
    }

    return false;
}

void Master::ReducePhase() {
    std::cout << "Reduce Phase Started!" << std::endl;

    // 1. Spawn a reduce listener routine to reap the results of the future reduce calls
    std::thread reduce_listener(&Master::AsyncCompleteReduce, this);

    // 2. Do Reduce work
    while (ShouldDoReduceWork()) {
        ReduceTask *task = nullptr;

        // 2.1 Pick a task
        for (ReduceTask *it: reduce_task_tracker) {
            if (it->status == PENDING) {
                task = it;
                break;
            }
        }

        if (task != nullptr) {
            // 2.2 Pick a worker
            WorkerInfo *worker = GetAvailableWorker();

            if (worker != nullptr) {
                task->status = RUNNING;
                worker->status = BUSY;

                // 2.3 Call Reduce
                std::thread async_callReduce(&Master::CallReduce, this, task, worker);
                async_callReduce.detach();

                std::cout << "Dispatched job for reduce [" << task->reduce_id
                          << "] to worker [" << worker->addr << "]" << std::endl;
            }
        }
    }

    reduce_listener.join();

    std::cout << "Reduce Phase Complete!" << std::endl;
}

void Master::PrepareReducePhase() {
    // create reduce tasks
    for (int i = 0; i < mr_spec.num_outputs; i++) {
        ReduceTask *task = new ReduceTask;

        ReduceRequest *request = new ReduceRequest;
        request->set_reduce_id(i);
        request->set_user_id(mr_spec.user_id);
        request->set_num_outputs(mr_spec.num_outputs);
        request->set_output_dir(mr_spec.output_dir);

        task->status = PENDING;
        task->reduce_id = i;
        task->request = *request;

        reduce_task_tracker.emplace_back(task);
    }
}

void Master::CallReduce(ReduceTask *task, WorkerInfo *worker_info) {
    AsyncReduceCall *reduce_call = new AsyncReduceCall;
    reduce_call->timeout = worker_info->timeout;
    reduce_call->worker_addr = worker_info->addr;
    reduce_call->request = task->request;
    std::unique_ptr <WorkerService::Stub> &stub_ = worker_stubs.at(worker_info->addr);
    reduce_call->response_reader =
            stub_->PrepareAsyncDoReduce(&reduce_call->context, task->request, &cq);

    std::chrono::system_clock::time_point deadline =
            std::chrono::system_clock::now() + std::chrono::seconds(worker_info->timeout);

    reduce_call->context.set_deadline(deadline);

    // make the call!
    reduce_call->response_reader->StartCall();
    reduce_call->response_reader->Finish(&reduce_call->reply, &reduce_call->status, (void *) reduce_call);

}

void Master::AsyncCompleteReduce() {
    std::cout << "Reduce Listener Thread Started!" << std::endl;
    void *got_tag;
    bool ok = false;

    while (ShouldDoReduceWork()) {
        cq.Next(&got_tag, &ok);
        AsyncReduceCall *call = static_cast<AsyncReduceCall *>(got_tag);

        GPR_ASSERT(ok);

        // updating reduce task status
        int reduce_id = call->request.reduce_id();
        TaskStatus task_status;
        int new_timeout;

        if (!call->status.ok()) {
            std::cout << "reduce [" << reduce_id << "] failed!" << std::endl;
            task_status = PENDING;
            new_timeout = call->timeout < TIMEOUT_MAX ? BACK_OFF_FACTOR * call->timeout : call->timeout;
        } else {
            std::cout << "reduce [" << reduce_id << "] completed!" << std::endl;
            task_status = COMPLETED;
            new_timeout = call->timeout > INITIAL_TIME_OUT ? (call->timeout / BACK_OFF_FACTOR) : call->timeout;
        }

        reduce_task_tracker.at(reduce_id)->status = task_status;
        SetWorkerAvailable(call->worker_addr, new_timeout);
        std::cout << "reduce worker [" << call->worker_addr << "] released!" << std::endl;

        delete call;
    }
}

bool Master::ShouldDoReduceWork() {
    for (ReduceTask *task: reduce_task_tracker) {
        if (task->status != COMPLETED) {
            return true;
        }
    }

    return false;
}
