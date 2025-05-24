.DEFAULT_GOAL := all

SRC_DIR = src
INCLUDE_DIR = include
MODULES_DIRS = dataframe datarepository
DRIVER_DIR = drivers
SERVER_DIR = grpc_server
PROTOS_DIR = proto

CXX = g++
CXXFLAGS = -Wall -std=c++20 -I $(INCLUDE_DIR)
CXXFLAGS_SERVER = -Wall -std=c++20 -I $(INCLUDE_DIR) -I $(PROTOS_DIR)

SRC_TESTS = $(wildcard $(SRC_DIR)/*.cpp $(foreach dir, $(MODULES_DIRS), $(wildcard $(SRC_DIR)/$(dir)/*.cpp)) $(SRC_DIR)/$(DRIVER_DIR)/test.cpp)
SRC_BANK = $(wildcard $(SRC_DIR)/*.cpp $(foreach dir, $(MODULES_DIRS), $(wildcard $(SRC_DIR)/$(dir)/*.cpp)) $(SRC_DIR)/$(DRIVER_DIR)/bank.cpp)
SRC_SERVER = $(wildcard $(SRC_DIR)/*.cpp $(foreach dir, $(MODULES_DIRS), $(wildcard $(SRC_DIR)/$(dir)/*.cpp)) $(PROTOS_DIR)/*.cc $(SRC_DIR)/$(SERVER_DIR)/transaction_server.cpp)

.PHONY: build help run clean

## all: build run ## Build and run the project

buildtests: $(SRC_TESTS) ## Build the project
	$(CXX) $(CXXFLAGS) -o tests $(SRC_TESTS) -lsqlite3

runtests: ## Run the project
	./tests

test: buildtests runtests

buildbank: $(SRC_BANK) ## Build the project
	$(CXX) $(CXXFLAGS) -o bankETL $(SRC_BANK) -lsqlite3

runbank:
	./bankETL

bank: buildbank runbank

buildserver: $(SRC_BANK) ## Build the project
	$(CXX) $(CXXFLAGS_SERVER) -o bankServer $(SRC_SERVER) -lsqlite3

runserver:
	./bankServer

server: buildserver runserver

help: ## Show this help
	@./scripts/help.sh $(MAKEFILE_LIST)

clean: ## Clean created files
	rm -f $(wildcard ./*.exe) $(wildcard ./*out*)

csvgen:
	python extras/csv_creator/make_mock.py

csvtest:
	python extras/csv_creator/test_mock.py
