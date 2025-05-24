cmake_minimum_required(VERSION 3.14)
project(transaction_server)

find_package(gRPC REQUIRED)

add_executable(transaction_server
  src/grpc_server/transaction_server.cpp
  proto/transaction.pb.cc
  proto/transaction.grpc.pb.cc

)

target_include_directories(transaction_server PRIVATE
  proto
)

target_link_libraries(transaction_server
  gRPC::grpc++
  protobuf::libprotobuf
)
