#ifndef TASK_H
#define TASK_H

#include <vector>
#include <array>
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

using DataFrameWithIndexes = std::pair<std::vector<int>, std::shared_ptr<DataFrame>>;

class Task : public std::enable_shared_from_this<Task>{
public:
    virtual ~Task() = default;
    //Interfaces para adicionar saídas e relacionamentos
    void addNext(std::shared_ptr<Task> nextTask, std::vector<bool> splitDFs);
    void addPrevious(std::shared_ptr<Task> previousTask, std::vector<bool> splitDFs);
    void addOutput(std::shared_ptr<DataFrame> outputDF);
    //Getters das saídas e relacionamentos
    const std::vector<std::shared_ptr<Task>>& getNextTasks();
    const std::vector<std::pair<std::shared_ptr<Task>, std::vector<bool>>>& getPreviousTasks();
    const std::vector<std::shared_ptr<DataFrame>>& getOutputs();
    //Funções para gerenciar as pendencias antes de executar a task
    void incrementExecutedPreviousTasks();
    const bool checkPreviousTasks() const;

    //----------Métodos abstratos---------//

    //método abstrato comum para todos os blocos do etl que deverá executar eles.
    //o primeiro deve simplesmente executar sem mexer em nenhuma interface de threading
    virtual void executeMonoThread() {};
    //e o segundo deverá enfileirar threads para trabalhar e retornar elas
    virtual std::vector<std::thread> executeMultiThread(int numThreads = 1) = 0;

    //método abstrato para gerenciar o uso dos dataframes de saída da task
    virtual void decreaseConsumingCounter() {};

    //método abstrato que faz devidas limpezas após o final do funcionamento do bloco
    virtual void finishExecution() {};

protected:
    //Vetores com as saídas e relacionamentos
    std::vector<std::shared_ptr<Task>> nextTasks;
    std::vector<std::shared_ptr<DataFrame>> outputDFs;
    std::vector<std::pair<std::shared_ptr<Task>, std::vector<bool>>> previousTasks;
    //Contador de tasks a seguir usando os dataframes de saída
    int tasksConsumingOutput = 0;
    //Contador de tasks anteriores pendentes de execução
    size_t cntExecutedPreviousTasks = 0;

};

class Transformer : public Task {
public:
    Transformer(): consumingCounterMutex() {};
    virtual ~Transformer() = default;

    //Função virtual que qualquer transformer deve implementar - no caso, o usuário
    virtual void transform(std::vector<std::shared_ptr<DataFrame>>& outputs,
                           const std::vector<DataFrameWithIndexes>& inputs) {};

    //Implementação específica do transformer para o executes
    void executeMonoThread() override;
    std::vector<std::thread> executeMultiThread(int numThreads = 1) override;

    //Implementação específica para os métodos de pós execução e contagem
    void decreaseConsumingCounter() override;
    void finishExecution() override;

private:
    //Métodos privados para facilitar o gerenciamento do que fazer
    std::vector<std::thread> executeWithThreading(int numThreads);

protected:
    std::mutex consumingCounterMutex;
};


class Extractor : public Task {
public:
    Extractor(): buffer(), bufferMutex(), dfMutex(), consumingCounterMutex(), cv(), endProduction(false) {};
    virtual ~Extractor() = default;

    void addOutput(std::shared_ptr<DataFrame> outputDF);
    void addRepo(DataRepository* repo){ repository = repo;};

    //Implementação específica do extractor para o execute
    virtual void executeMonoThread() override;
    virtual std::vector<std::thread> executeMultiThread(int numThreads = 1) override;

    //Implementação específica para os métodos de pós execução e contagem
    void decreaseConsumingCounter() override;
    void finishExecution() override;

private:
    DataRepository* repository;
    std::shared_ptr<DataFrame> dfOutput;

    std::queue<std::string> buffer;
    size_t maxBufferSize;
    std::mutex bufferMutex;
    std::mutex dfMutex;
    std::mutex consumingCounterMutex;
    std::condition_variable cv;
    std::atomic<bool> endProduction;

    void producer();
    void consumer();
};

class ExtractorFile : public Extractor {
public:
    virtual ~ExtractorFile() = default;

private:
    FileRepository* repository;
};

class ExtractorSQLite : public Extractor {
public:
    virtual ~ExtractorSQLite() = default;
    
private:
    SQLiteRepository* repository;
};


class Loader : public Task {
public:
    Loader(): repoMutex(), inputIndex(0) {};
    virtual ~Loader() = default;

    void addRepo(DataRepository* repo){ repository = repo;};

    //Implementação específica do loader para o execute
    virtual void executeMonoThread() override;
    virtual std::vector<std::thread> executeMultiThread(int numThreads = 1) override;

    //Implementação específica para os métodos de pós execução e contagem
    void finishExecution() override;

private:
    DataRepository* repository;
    std::vector<DataFrameWithIndexes> getInput(int numThreads);

    std::mutex repoMutex;
    int inputIndex;

    void addRows(DataFrameWithIndexes pair);
    void updateRepo(int numThreads);
};

class LoaderFile : public Loader {
public: 
    virtual ~LoaderFile() = default;

private:
    FileRepository* repository;
};
    
class LoaderSQLite : public Loader {
public: 
    virtual ~LoaderSQLite() = default;

private:
    SQLiteRepository* repository;
};

#endif
