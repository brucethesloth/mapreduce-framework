#pragma once

#include <string>
#include <fstream>
#include <iostream>
#include <grpc++/grpc++.h>
#include <grpcpp/health_check_service_interface.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <mr_task_factory.h>
#include "masterworker.grpc.pb.h"
#include "mr_tasks.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using masterworker::ShardInfo;
using masterworker::ShardSegment;
using masterworker::MapRequest;
using masterworker::MapReply;
using masterworker::ReduceReply;
using masterworker::WorkerService;

extern std::shared_ptr<BaseMapper> get_mapper_from_task_factory(const std::string& user_id);
extern std::shared_ptr<BaseReducer> get_reducer_from_task_factory(const std::string& user_id);

/* CS6210_TASK: Handle all the task a Worker is supposed to do.
	This is a big task for this project, will test your understanding of map reduce */
class Worker : public WorkerService::Service {

	public:
		/* DON'T change the function signature of this constructor */
		Worker(std::string ip_addr_port);

		/* DON'T change this function's signature */
		bool run();

	private:
		/* NOW you can add below, data members and member functions as per the need of your implementation*/
		std::string worker_addr;

		Status DoMap(ServerContext* context, const MapRequest* request, MapReply* reply);
        Status DoReduce(ServerContext* context, const MapReply* request, ReduceReply* reply);
};


/* CS6210_TASK: ip_addr_port is the only information you get when started.
	You can populate your other class data members here if you want */
Worker::Worker(std::string worker_addr) {
    this->worker_addr = worker_addr;
}

Status Worker::DoMap(ServerContext *context, const MapRequest *request, MapReply *reply) {
    // todo: implement me!
//    std::cout << "worker.run(), I 'm not ready yet" <<std::endl;
    auto mapper = get_mapper_from_task_factory(request->user_id());
    ShardInfo shard = request->shard();

    // debug
//    std::cout << "Worker got fileshard info!\n";
//    for (ShardSegment segment : shard.segments()) {
//        std::string file_name = segment.file_name();
//        int begin = segment.begin();
//        int end = segment.end();
//
//        // debug
//        std::cout << "filename: " << file_name << ", start: " << begin << ", end: " << end << std::endl;
//
//        std::ifstream input(file_name.c_str(), std::ios::binary);
//        input.seekg(begin, std::ios::beg);
//        std::string content;
//
//        while (input.tellg() < end && std::getline(input, content)) {
//            mapper->map(content);
//        }
//    }

    // reply
    reply->set_worker_addr(worker_addr);
    reply->set_filename("worker_" + worker_addr);

    // write to file
    std::cout << "work for Shard [" << request->shard().shard_id() << "] is completed by worker [" << worker_addr << "]" << std::endl;
    return Status::OK;
}

Status Worker::DoReduce(ServerContext *context, const MapReply *request, ReduceReply *reply) {
    // todo: implement me!
}

/* CS6210_TASK: Here you go. once this function is called your woker's job is to keep looking for new tasks
	from Master, complete when given one and again keep looking for the next one.
	Note that you have the access to BaseMapper's member BaseMapperInternal impl_ and
	BaseReduer's member BaseReducerInternal impl_ directly,
	so you can manipulate them however you want when running map/reduce tasks*/
bool Worker::run() {
    grpc::EnableDefaultHealthCheckService(true);
    grpc::reflection::InitProtoReflectionServerBuilderPlugin();
    ServerBuilder builder;
    // Listen on the given address without any authentication mechanism.
    builder.AddListeningPort(worker_addr, grpc::InsecureServerCredentials());
    // Register "service" as the instance through which we'll communicate with
    // clients. In this case it corresponds to an *synchronous* service.
    builder.RegisterService(this);
    // Finally assemble the server.
    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << worker_addr << std::endl;

    // Wait for the server to shutdown. Note that some other thread must be
    // responsible for shutting down the server for this call to ever return.
    server->Wait();
}
