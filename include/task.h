#ifndef TASK_H
#define TASK_H

#include <vector>
#include <mutex>
#include <utility>
#include <memory>
#include <thread>
#include <queue>
#include <condition_variable>
#include <atomic>
#include "dataframe.h"
#include "datarepository.h"
#include "types.h"

struct OutputSpec {};

class Task : public std::enable_shared_from_this<Task>{
public:
    virtual ~Task() = default;
    //Interfaces para adicionar saídas e relacionamentos
    void addNext(std::shared_ptr<Task> nextTask);
    void addPrevious(std::shared_ptr<Task> previousTask, std::vector<bool> splitDFs);
    void addOutput(DataFrame* outputDF);
    //Getters das saídas e relacionamentos
    const std::vector<std::shared_ptr<Task>>& getNextTasks();
    const std::vector<std::pair<std::shared_ptr<Task>, std::vector<bool>>>& getPreviousTasks();
    const std::vector<DataFrame*>& getOutputs();
    //Função comum para todos os blocos do etl que deverá executar eles.
    virtual void execute(int numThreads = 1) {};

protected:
    //Vetores com as saídas e relacionamentos
    std::vector<std::shared_ptr<Task>> nextTasks;
    std::vector<DataFrame*> outputDFs;
    std::vector<std::pair<std::shared_ptr<Task>, std::vector<bool>>> previousTasks;
};

class Transformer : public Task {
public:
    virtual ~Transformer() = default;
    //Função virtual que qualquer transformer deve implementar
    virtual void transform(std::vector<DataFrame*>& outputs,
                           const std::vector<std::pair<std::vector<int>, DataFrame*>>& inputs) = 0;

    //Implementação específica do transformer para o execute
    void execute(int numThreads = 1) override;

private:
    void executeWithThreading(int numThreads);
};


class Extractor : public Task {
public:
    Extractor(): bufferMutex(), dfMutex(), cv(), endProduction(false) {};
    virtual ~Extractor() = default;
    void extract(int numThreads);
    void addOutput(DataFrame* outputDF);
    void addRepo(FileRepository* repo){ repository = repo;};

    //Implementação específica do extractor para o execute
    void execute(int numThreads = 1);
private:
    FileRepository* repository;
    DataFrame* dfOutput;

    std::queue<StrRow> buffer;
    std::mutex bufferMutex;
    std::mutex dfMutex;
    std::condition_variable cv;
    std::atomic<bool> endProduction;

    void producer();
    void consumer();
};

class Loader : public Task {
public:
    Loader(): bufferMutex(), repoMutex(), cv(), endProduction(false) {};
    virtual ~Loader() = default;
    void createRepo(int numThreads);
    void updateRepo(int numThreads);
    void addRows(int numThreads);

    void addRepo(FileRepository* repo){ repository = repo;};

    //Implementação específica do loader para o execute
    void execute(int numThreads = 1);
private:
    FileRepository* repository;
    DataFrame* dfInput;
    void getInput();

    std::queue<StrRow> buffer;
    std::mutex bufferMutex;
    std::mutex repoMutex;
    std::condition_variable cv;
    std::atomic<bool> endProduction;

    void producer();
    void consumer();
};

#endif
