.DEFAULT_GOAL := help

SRC_DIR = src
INCLUDE_DIR = include

CXX = g++
CXXFLAGS = -Wall -I $(INCLUDE_DIR)

SRC = $(wildcard $(SRC_DIR)/*.cpp)

.PHONY: build help run clean

all: build run ## Build and run the project

build: $(SRC) ## Build the project
	@echo "Building project..."
	$(CXX) $(CXXFLAGS) -o out $(SRC)

run: ## Run the project
	./out

help: ## Show this help
	@./scripts/help.sh $(MAKEFILE_LIST)

clean: ## Clean created files
	rm -f $(wildcard ./*.exe) $(wildcard ./*out*)