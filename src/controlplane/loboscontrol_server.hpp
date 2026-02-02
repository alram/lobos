#pragma once
#include <iostream>
#include <memory>
#include <string>
#include <thread>

#include "loboscontrol.pb.h"
#include "loboscontrol.grpc.pb.h"
#include <grpcpp/grpcpp.h>

#include "../store/store.hpp"
#include "../common/common.hpp"

class ControlPlaneImpl final : public loboscontrol::ControlPlane::Service {
public:
    explicit ControlPlaneImpl(ControlPlane* cp) : cp_(cp) {}
    grpc::Status AddUser(grpc::ServerContext* ctx, 
                        const loboscontrol::AuthParams* auth_params,
                        loboscontrol::UserReply* reply) override;
    grpc::Status ListAllUsers(grpc::ServerContext* ctx, 
                        const loboscontrol::Filters* filters, 
                        loboscontrol::ListAllUsersReply* reply) override;
private:
    ControlPlane* cp_;
};

class ControlPlane {
public:
    ControlPlane(Store& store, std::unordered_map<std::string, std::string>& s3_users) 
    : store_(store) 
    , s3_users_(s3_users) {};

    std::optional<User> add_user(User u);
    std::vector<User> list_all_users(std::string filter);
private:
    Store& store_;
    std::unordered_map<std::string, std::string>& s3_users_;
};

class ControlPlaneServer {
public:
    void start(const std::string& address, ControlPlane* cp);
    void stop();
private:
    std::thread grpc_thread_;
    std::unique_ptr<grpc::Server> server_;
};
