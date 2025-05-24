#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <chrono>

#include <grpcpp/grpcpp.h>
#include "transaction.pb.h"
#include "transaction.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerWriter;
using grpc::ServerReaderWriter;
using grpc::Status;

class TransactionServerImpl final : public transaction::TransactionService::Service {
    Status SendTransaction (ServerContext* context, const transaction::Transaction* request,
                    transaction::Result* reply) override {
        std::cout << "Thread " << std::this_thread::get_id() <<
        " recebeu transação de id " << request->id_transacao() << ": " <<
        request->id_usuario_pagador() << " | " << request->id_usuario_recebedor() << " | " <<
        request->id_regiao() << " | " <<request->modalidade_pagamento() << " | " <<
        request->data_horario() << " | " << request->valor_transacao() << " R$" << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(1));
        reply->set_ok(true);
        return Status::OK;
    }
};

void RunServer() {
    std::string server_address("0.0.0.0:50051");
    TransactionServerImpl service;

    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << server_address << std::endl;

    server->Wait();
}

int main() {
    RunServer();
    return 0;
}
