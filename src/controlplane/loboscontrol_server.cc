#include "loboscontrol_server.hpp"

class ControlPlaneImpl final : public loboscontrol::ControlPlane::Service {

    grpc::Status AddUser(grpc::ServerContext* ctx, 
                        const loboscontrol::AuthParams* auth_params,
                        loboscontrol::UserReply* reply) override {
        std::string msg = "Added user with key: " + auth_params->key();
        reply->set_message(msg);
        return grpc::Status::OK;
    }

    grpc::Status ListAllUsers(grpc::ServerContext* ctx, 
                        const loboscontrol::Filters* filters, 
                        loboscontrol::ListAllUsersReply* reply) override {
        reply->add_message()->set_message("msg1");
        reply->add_message()->set_message("msg2");
        return grpc::Status::OK;
    }
};

void ControlPlaneServer::start(const std::string& address) {
    grpc_thread_ = std::thread([this, address] {
        ControlPlaneImpl service;
        grpc::ServerBuilder builder;
        builder.AddListeningPort(address, grpc::InsecureServerCredentials());
        builder.RegisterService(&service);

        auto server = builder.BuildAndStart();
        std::cout << "Control plane listening on " << address << std::endl;
        server_ = std::move(server);
        server_->Wait();
    });
}

void ControlPlaneServer::stop() {
    std::cout << "Shutting down control plane" << std::endl;
    if (server_)
        server_->Shutdown();
    if (grpc_thread_.joinable())
        grpc_thread_.join();
}