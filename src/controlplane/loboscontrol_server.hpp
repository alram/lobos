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

class ControlPlane {
public:
    ControlPlane(Store& store, std::unordered_map<std::string, std::string>& s3_users) 
    : store_(store) 
    , s3_users_(s3_users) {};

    std::pair<grpc::StatusCode, std::optional<User>> add_user(User u);
    std::vector<User> list_all_users(std::string filter);
    grpc::StatusCode rm_user(std::string name, bool force);
    std::pair<grpc::StatusCode, std::optional<User>> add_key(User u);
    grpc::StatusCode rm_key(std::string user, std::string key);
private:
    Store& store_;
    std::unordered_map<std::string, std::string>& s3_users_;
    std::unordered_map<std::string, std::vector<User>> users_;
};

class ControlPlaneServer {
public:
    void start(const std::string& address, ControlPlane* cp);
    void stop();
private:
    std::thread grpc_thread_;
    std::unique_ptr<grpc::Server> server_;
};

class ControlPlaneImpl final : public loboscontrol::ControlPlane::Service {
public:
    explicit ControlPlaneImpl(ControlPlane* cp) : cp_(cp) {}
    grpc::Status AddUser(grpc::ServerContext* ctx, 
                        const loboscontrol::User* user,
                        loboscontrol::UserReply* reply) override;
    grpc::Status ListAllUsers(grpc::ServerContext* ctx, 
                        const loboscontrol::Filters* filters, 
                        loboscontrol::ListAllUsersReply* reply) override;
    grpc::Status RmUser(grpc::ServerContext* ctx, 
                        const loboscontrol::RmUserParams* user,
                        google::protobuf::BoolValue* reply) override;
    grpc::Status AddKey(grpc::ServerContext* ctx,
                        const loboscontrol::User *user,
                        loboscontrol::UserReply* reply) override;
    grpc::Status RmKey(grpc::ServerContext* ctx,
                        const loboscontrol::User* user,
                        google::protobuf::BoolValue* reply) override;
private:
    ControlPlane* cp_;
};
