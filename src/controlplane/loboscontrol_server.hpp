#pragma once
#include <iostream>
#include <memory>
#include <string>
#include <thread>

#include "loboscontrol.pb.h"
#include "loboscontrol.grpc.pb.h"
#include <grpcpp/grpcpp.h>

class ControlPlaneServer {
public:
    void start(const std::string& address);
    void stop();
private:
    std::thread grpc_thread_;
    std::unique_ptr<grpc::Server> server_;
};