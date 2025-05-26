cmake_minimum_required(VERSION 3.14)
project(transaction_server)

find_package(gRPC REQUIRED)
find_package(SQLite3 REQUIRED)

file(GLOB DATAFRAME "src/dataframe/*.cpp")
file(GLOB DATAREPOSITORY "src/datarepository/*.cpp")
file(GLOB SRCFILES "src/*.cpp")

add_executable(transaction_server
  src/grpc_server/transaction_server.cpp
  proto/transaction.pb.cc
  proto/transaction.grpc.pb.cc
  ${DATAFRAME}
  ${DATAREPOSITORY}
  ${SRCFILES}
)

target_include_directories(transaction_server PRIVATE
  proto
  include
)

target_link_libraries(transaction_server
  gRPC::grpc++
  protobuf::libprotobuf
  ${SQLite3_LIBRARIES} # ADICIONADO: Linka com a biblioteca SQLite3 encontrada
)
