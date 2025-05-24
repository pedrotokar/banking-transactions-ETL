// ===========Versão com threadpool gerenciada pelog RPC===============
// #include <iostream>
// #include <memory>
// #include <string>
// #include <thread>
// #include <chrono>
//
// #include <grpcpp/grpcpp.h>
// #include "transaction.pb.h"
// #include "transaction.grpc.pb.h"
//
// using grpc::Server;
// using grpc::ServerBuilder;
// using grpc::ServerContext;
// using grpc::ServerReader;
// using grpc::ServerWriter;
// using grpc::ServerReaderWriter;
// using grpc::Status;
//
// using transaction::Transaction;
// using transaction::Result;
// using transaction::TransactionService;
//
// class TransactionServerImpl final : public TransactionService::Service {
// public:
//     Status SendTransaction (ServerContext* context, const Transaction* request,
//                     Result* reply) override {
//=============SOBREESCREVER ESSE MÉTODO PRA COLOCAR A NOSSA LÓGICA :D===================
//
//         std::cout << "Thread " << std::this_thread::get_id() <<
//         " recebeu transação de id " << request->id_transacao() << ": " <<
//         request->id_usuario_pagador() << " | " << request->id_usuario_recebedor() << " | " <<
//         request->id_regiao() << " | " <<request->modalidade_pagamento() << " | " <<
//         request->data_horario() << " | " << request->valor_transacao() << " R$" << std::endl;
//
//         std::this_thread::sleep_for(std::chrono::seconds(1));
//         reply->set_ok(true);
//         return Status::OK;
//     }
// };
//
// void RunServer() {
//     std::string server_address("0.0.0.0:50051");
//     TransactionServerImpl service;
//
//     ServerBuilder builder;
//     builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
//     builder.RegisterService(&service);
//
//     std::unique_ptr<Server> server(builder.BuildAndStart());
//     std::cout << "Server listening on " << server_address << std::endl;
//
//     server->Wait();
// }
//
// int main() {
//     RunServer();
//     return 0;
// }

// ===========Versão com uma única thread gerenciada na mão===============
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <chrono>
#include <cassert>

#include <grpcpp/grpcpp.h>
#include "transaction.pb.h"
#include "transaction.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using grpc::ServerCompletionQueue;

using transaction::Transaction;
using transaction::Result;
using transaction::TransactionService;

class CallData {
public:
    CallData(TransactionService::AsyncService* service, ServerCompletionQueue* cq)
    : service_(service), cq_(cq), responder_(&ctx_), status_(CREATE) {
        Proceed();
    }

    void Proceed() {
        if (status_ == CREATE) {
            status_ = PROCESS;
            service_->RequestSendTransaction(&ctx_, &request_, &responder_, cq_, cq_, this);
        } else if (status_ == PROCESS) {
            new CallData(service_, cq_);

            //=============SOBREESCREVER ESSE PEDAÇO PRA COLOCAR A NOSSA LÓGICA :D===================
            std::cout << "Thread " << std::this_thread::get_id() <<
            " recebeu transação de id " << request_.id_transacao() << ": " <<
            request_.id_usuario_pagador() << " | " << request_.id_usuario_recebedor() << " | " <<
            request_.id_regiao() << " | " << request_.modalidade_pagamento() << " | " <<
            request_.data_horario() << " | " << request_.valor_transacao() << " R$" << std::endl;

            std::this_thread::sleep_for(std::chrono::seconds(1));

            reply_.set_ok(true);

            status_ = FINISH;
            responder_.Finish(reply_, Status::OK, this);
        } else {
            assert(status_ == FINISH);
            delete this;
        }
    }

private:
    TransactionService::AsyncService* service_;
    ServerCompletionQueue* cq_;
    ServerContext ctx_;

    Transaction request_;
    Result reply_;

    grpc::ServerAsyncResponseWriter<Result> responder_;

    enum CallStatus { CREATE, PROCESS, FINISH };
    CallStatus status_;
};

class TransactionServer final {
public:
    ~TransactionServer() {
        server_->Shutdown();
        cq_->Shutdown();
        void* tag;
        bool ok;
        while (cq_->Next(&tag, &ok)) {}
    }

    void Run() {
        std::string server_address("0.0.0.0:50051");

        ServerBuilder builder;

        builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());

        builder.RegisterService(&service_);

        cq_ = builder.AddCompletionQueue();

        server_ = builder.BuildAndStart();
        std::cout << "Server listening on " << server_address << std::endl;


        HandleRpcs();
    }

private:
    void HandleRpcs() {
        new CallData(&service_, cq_.get());
        void* tag;
        bool ok;
        while (true) {
            if (!cq_->Next(&tag, &ok)) {
                std::cerr << "Completion queue shut down. Exiting HandleRpcs." << std::endl;
                break;
            }

            if (ok) {
                static_cast<CallData*>(tag)->Proceed();
            } else {
                std::cerr << "Operation failed or server shutting down for tag: " << tag << std::endl;
            }
        }
    }

    std::unique_ptr<ServerCompletionQueue> cq_;
    TransactionService::AsyncService service_;
    std::unique_ptr<Server> server_;
};

int main() {
    TransactionServer server;
    server.Run();
    return 0;
}
