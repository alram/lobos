#include "loboscontrol_server.hpp"
#include "../common/common.hpp"

grpc::Status add_error_h(grpc::StatusCode sc) {
    switch (sc) {
        case grpc::StatusCode::ALREADY_EXISTS:
            return grpc::Status(grpc::StatusCode::ALREADY_EXISTS, "user already exists");
        case grpc::StatusCode::UNIMPLEMENTED:
            return grpc::Status(grpc::StatusCode::UNIMPLEMENTED, "only lobos backend is accepted");
        case grpc::StatusCode::INTERNAL:
            return grpc::Status(grpc::StatusCode::INTERNAL, "could not write user information to disk");
        case grpc::StatusCode::NOT_FOUND:
            return grpc::Status(grpc::StatusCode::NOT_FOUND, "user does not exist");
        case grpc::StatusCode::INVALID_ARGUMENT:
            return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "the specified key is already in use by this user");
        default:
            return grpc::Status(grpc::StatusCode::UNKNOWN, "could not add user but we do not know why");
    }
}

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
    auto resp = cp_->add_user(u);
    if (resp.first != grpc::StatusCode::OK) {
        return add_error_h(resp.first);
    }

    auto* p = reply->mutable_user();
    p->set_name(resp.second->name);
    p->set_key(resp.second->key);
    p->set_secret(resp.second->secret);
    p->set_backend(resp.second->backend);
    
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

grpc::Status ControlPlaneImpl::RmUser(grpc::ServerContext* ctx, 
                    const loboscontrol::RmUserParams* user,
                    google::protobuf::BoolValue* reply) {

    auto resp = cp_->rm_user(user->name(), user->force());
    if (resp != grpc::StatusCode::OK) {
        reply->set_value(false);
        switch (resp) {
            case grpc::StatusCode::NOT_FOUND:
                return grpc::Status(grpc::StatusCode::NOT_FOUND, "user does not exist");
            case grpc::StatusCode::PERMISSION_DENIED:
                return grpc::Status(grpc::StatusCode::PERMISSION_DENIED, "user has existing keys");
            case grpc::StatusCode::INTERNAL:
                return grpc::Status(grpc::StatusCode::INTERNAL, "could not delete user information from disk");
            default:
                return grpc::Status(grpc::StatusCode::UNKNOWN, "could not add user but we do not know why");
        }
    }
    reply->set_value(true);
    return grpc::Status::OK;
}
grpc::Status ControlPlaneImpl::AddKey(grpc::ServerContext* ctx,
                    const loboscontrol::User *user,
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

    auto resp = cp_->add_key(u);
    if (resp.first != grpc::StatusCode::OK)
        return add_error_h(resp.first);

    auto* p = reply->mutable_user();
    p->set_name(resp.second->name);
    p->set_key(resp.second->key);
    p->set_secret(resp.second->secret);
    p->set_backend(resp.second->backend);

    return grpc::Status::OK;
}

grpc::Status ControlPlaneImpl::RmKey(grpc::ServerContext* ctx,
                    const loboscontrol::User* user,
                    google::protobuf::BoolValue* reply) {

    if (user->name().empty() || user->key().empty()) {
        reply->set_value(false);
        return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "name and key are required");
    }
    auto resp = cp_->rm_key(user->name(), user->key());
    if (resp != grpc::StatusCode::OK) {
        reply->set_value(false);
        return grpc::Status(grpc::StatusCode::INTERNAL, "could not delete key on disk");
    }

    reply->set_value(true);
    return grpc::Status::OK;

};

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

std::pair<grpc::StatusCode, std::optional<User>> ControlPlane::add_user(User u) {

    if (users_.contains(u.name))
        return std::pair<grpc::StatusCode, std::optional<User>>(grpc::StatusCode::ALREADY_EXISTS, {});

    if (u.backend.empty())
        u.backend = "lobos";
    // TODO if u.backend is not lobos nor empty
    // compare against existing backends and throw
    // if not exists
    if (u.backend != "lobos")
        return std::pair<grpc::StatusCode, std::optional<User>>(grpc::StatusCode::UNIMPLEMENTED, {});

    if (u.key.empty())
        u.key = generate_acces_key();

    if (u.secret.empty())
        u.secret = generate_secret();

    auto r = store_.metadata_add_user(u);
    if (r < 0)
        return std::pair<grpc::StatusCode, std::optional<User>>(grpc::StatusCode::INTERNAL, {});

    users_[u.name].emplace_back(u);
    s3_users_.insert(std::pair<std::string, std::string>(u.key, u.secret));

    return std::pair<grpc::StatusCode, std::optional<User>>(grpc::StatusCode::OK, u);
}

std::vector<User> ControlPlane::list_all_users(std::string filter) {
    // This method is also called at boot by the server instance 
    // we take that opp to cache the data in an unordered map
    auto on_disk_users = store_.metadata_list_users(filter);
    if (users_.size() == 0) {
        for (User& u : on_disk_users) {
            users_[u.name].emplace_back(u);
        }
    }

    return on_disk_users;
}

grpc::StatusCode ControlPlane::rm_user(std::string name, bool force) {
    if (!users_.contains(name))
        return grpc::StatusCode::NOT_FOUND;

    if (users_[name].size() > 0 && force == false) 
        return grpc::StatusCode::PERMISSION_DENIED;
    
    // remove the user from disk 
    bool success = store_.metadata_remove_user(name);
    if (!success)
        return grpc::StatusCode::INTERNAL;
    // remove from actives keys
    for (const User& u : users_[name]) {
        s3_users_.erase(u.key);
    }
    // remove from cached struct
    users_.erase(name);

    return grpc::StatusCode::OK;
}
std::pair<grpc::StatusCode, std::optional<User>> ControlPlane::add_key(User u) {
    if (!users_.contains(u.name))
        return std::pair<grpc::StatusCode, std::optional<User>>(grpc::StatusCode::NOT_FOUND, {});

    if (u.backend.empty())
        u.backend = "lobos";
    // TODO if u.backend is not lobos nor empty
    // compare against existing backends and throw
    // if not exists
    if (u.backend != "lobos")
        return std::pair<grpc::StatusCode, std::optional<User>>(grpc::StatusCode::UNIMPLEMENTED, {});

    if (u.key.empty())
        u.key = generate_acces_key();
    for (const auto& it : users_[u.name]) {
        if (it.key == u.key)
            return std::pair<grpc::StatusCode, std::optional<User>>(grpc::StatusCode::INVALID_ARGUMENT, {});
    }

    if (u.secret.empty())
        u.secret = generate_secret();

    auto rc = store_.metadata_add_key(u);
    if (rc)
        return std::pair<grpc::StatusCode, std::optional<User>>(grpc::StatusCode::INTERNAL, {});

    users_[u.name].emplace_back(u);
    s3_users_.insert(std::pair<std::string, std::string>(u.key, u.secret));

    return std::pair<grpc::StatusCode, std::optional<User>>(grpc::StatusCode::OK, u);
}

grpc::StatusCode ControlPlane::rm_key(std::string user, std::string key) {
    bool key_exists = false;
    User u;
    for (const auto& it : users_[user]) {
        if (it.key == key) {
            key_exists = true;
            u = it;
            break;
        }
    }
    if (!key_exists)
        return grpc::StatusCode::NOT_FOUND;
    
    auto deleted = store_.metadata_rm_key(user, u);
    if (!deleted)
        return grpc::StatusCode::INTERNAL;

    s3_users_.erase(key);
    std::erase_if(users_[user], [key](User u) { return u.key == key; });

    return grpc::StatusCode::OK;
}