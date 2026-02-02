#include <random>
#include "loboscontrol_server.hpp"

grpc::Status ControlPlaneImpl::AddUser(grpc::ServerContext* ctx, 
                    const loboscontrol::User* user,
                    loboscontrol::UserReply* reply) {

    if (user->name() == "") {
        return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "name is not specified");
    }

    User u {
        user->name(),
        user->key(),
        user->secret(),
        user->backend(),
    };
    auto user_ret = cp_->add_user(u);
    if (!user_ret)
        return grpc::Status(grpc::StatusCode::UNKNOWN, "couldn't add user");

    auto* p = reply->mutable_user();
    p->set_name(user_ret->name);
    p->set_key(user_ret->key);
    p->set_secret(user_ret->secret);
    p->set_backend(user_ret->backend);
    
    return grpc::Status::OK;
}

grpc::Status ControlPlaneImpl::ListAllUsers(grpc::ServerContext* ctx, 
                    const loboscontrol::Filters* filters, 
                    loboscontrol::ListAllUsersReply* reply) {

    // TODO filters
    auto users = cp_->list_all_users("");
    for (const User& u : users) {
        auto* user = reply->add_users();
        auto* p = user->mutable_user();
        p->set_name(u.name);
        p->set_key(u.key);
        p->set_secret(u.secret);
        p->set_backend(u.backend);
    }

    return grpc::Status::OK;
}

void ControlPlaneServer::start(const std::string& address, ControlPlane* cp) {
    grpc_thread_ = std::thread([this, cp, address] {
        ControlPlaneImpl service(cp);
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

void random_gen(int len, const std::string allowed_chars, std::string& s) {
    std::random_device rd;
    std::mt19937 mt(rd());
    std::uniform_int_distribution<> distribution(0, allowed_chars.size() - 1);

    for (size_t i = 0; i < len; i++)
        s += allowed_chars[distribution(mt)];
}

std::string generate_acces_key() {
    const std::string allowed_chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    std::string access_key = "LB";
    random_gen(18, allowed_chars, access_key);
    return access_key;
}

std::string generate_secret(){
    const std::string allowed_chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz+";
    std::string secret;
    random_gen(40, allowed_chars, secret);
    return secret;
}

std::optional<User> ControlPlane::add_user(User u) {
    if (u.backend.empty())
        u.backend = "lobos";
    // TODO if u.backend is not lobos nor empty
    // compare against existing backends and throw
    // if not exists
    if (u.backend != "lobos")
        return {};

    if (u.key.empty())
        u.key = generate_acces_key();

    if (u.secret.empty())
        u.secret = generate_secret();

    auto r = store_.metadata_add_user(u);
    if (r < 0) {
        return {};
    }

    s3_users_.insert(std::pair<std::string, std::string>(u.key, u.secret));

    return u;
}

std::vector<User> ControlPlane::list_all_users(std::string filter) {
    return store_.metadata_list_users(filter);
}