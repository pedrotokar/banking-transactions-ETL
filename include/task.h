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
    void blockParallel(){blockMultiThreading = true;};
    //Getters das saídas e relacionamentos
    const std::vector<std::shared_ptr<Task>>& getNextTasks();
    const std::vector<std::pair<std::shared_ptr<Task>, std::vector<bool>>>& getPreviousTasks();
    const std::vector<std::shared_ptr<DataFrame>>& getOutputs();
    const bool canBeParallel();
    //Funções para gerenciar as pendencias antes de executar a task
    void incrementExecutedPreviousTasks();
    const bool checkPreviousTasks() const;

    //----------Métodos abstratos---------//

    //método abstrato comum para todos os blocos do etl que deverá executar eles.
    //o primeiro deve simplesmente executar sem mexer em nenhuma interface de threading
    virtual void executeMonoThread() {};
    //e o segundo deverá enfileirar threads para trabalhar e retornar elas
    virtual std::vector<std::thread> executeMultiThread(int numThreads, std::vector<std::atomic<bool>>& completedThreads,
                                                        std::condition_variable& orchestratorCv, std::mutex& orchestratorMutex) = 0;
    //método abstrato para gerenciar o uso dos dataframes de saída da task (chamado por tasks posteriores)
    virtual void decreaseConsumingCounter() {};
    //método abstrato que faz devidas limpezas após o final do funcionamento do bloco
    virtual void finishExecution() {};

    // Heurística para decidir o número de threads
    // void setWeight(int w);
    // int getWeight() const;
    
    void setMaxThreadsProportion(double pThreadsNew); // 0 < p <= 1
    double getMaxThreadsProportion() const;
    
    // void setAuxOrquestrador(int val);
    // void incrementAuxOrquestrador();
    // int getAuxOrquestrador() const;
    
    void setTaskName(const std::string name);
    std::string getTaskName() const;

    // void setLevel(int newLevel);
    // int getLevel() const;

    void setBaseWeight(int newBaseWeight);
    int getBaseWeight() const;
protected:
    //Vetores com as saídas e relacionamentos
    std::vector<std::shared_ptr<Task>> nextTasks;
    std::vector<std::shared_ptr<DataFrame>> outputDFs;
    std::vector<std::pair<std::shared_ptr<Task>, std::vector<bool>>> previousTasks;
    bool blockMultiThreading = false;
    //Contador de tasks a seguir usando os dataframes de saída
    int tasksConsumingOutput = 0;
    //Contador de tasks anteriores pendentes de execução
    size_t cntExecutedPreviousTasks = 0;

    //Heurista para decidir numero de threads
    double pThreads = 0.5;
    // int weight = 0;
    // int auxOrquestrador = 0;
    std::string taskName = "";
    // int taskLevel = 0;
    int baseWeight = 1;

    //Função auxiliar para retornar uma thread executando a operação versão monothread do bloco
    void executeMonoThreadSpecial(std::vector<std::atomic<bool>>& completedList, int tIndex,
                                  std::condition_variable& orchestratorCv, std::mutex& orchestratorMutex);
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
    std::vector<std::thread> executeMultiThread(int numThreads, std::vector<std::atomic<bool>>& completedThreads,
                                                std::condition_variable& orchestratorCv, std::mutex& orchestratorMutex) override;

    //Implementação específica para os métodos de pós execução e contagem
    void decreaseConsumingCounter() override;
    void finishExecution() override;

private:
    //Método privado para facilitar o gerenciamento do que fazer
    std::vector<std::thread> executeWithThreading(int numThreads, std::vector<std::atomic<bool>>& completedList,
                                                  std::condition_variable& orchestratorCv, std::mutex& orchestratorMutex);
    //Wrapper que chama a função definida do usuário e faz limpezas de thread depois
    void transformThread(std::vector<std::shared_ptr<DataFrame>>& outputs,
                         const std::vector<DataFrameWithIndexes>& inputs,
                         std::vector<std::atomic<bool>>& completedList, int tIndex,
                         std::condition_variable& orchestratorCv, std::mutex& orchestratorMutex);

protected:
    std::mutex consumingCounterMutex;
};


class Extractor : public Task {
public:
    Extractor(): buffer(), bufferMutex(), dfMutex(), consumingCounterMutex(), cv(), endProduction(false) {};
    virtual ~Extractor() = default;
    //addOUtput especial pro extractor
    void addOutput(std::shared_ptr<DataFrame> outputDF);
    //Setter específico do extractor
    void addRepo(DataRepository* repo){ repository = repo;};

    //Implementação específica do extractor para o execute
    virtual void executeMonoThread() override;
    virtual std::vector<std::thread> executeMultiThread(int numThreads, std::vector<std::atomic<bool>>& completedThreads,
                                                        std::condition_variable& orchestratorCv, std::mutex& orchestratorMutex) override;

    //Implementação específica para os métodos de pós execução e contagem
    void decreaseConsumingCounter() override;
    void finishExecution() override;

protected:
    std::shared_ptr<DataFrame> dfOutput;
private:
    DataRepository* repository;
    std::queue<std::string> buffer;
    size_t maxBufferSize;
    std::mutex bufferMutex;
    std::mutex dfMutex;
    std::mutex consumingCounterMutex;
    std::condition_variable cv;
    std::atomic<bool> endProduction;
    //Funções para execução com multithreading
    void producer(std::vector<std::atomic<bool>>& completedList, int tIndex,
                  std::condition_variable& orchestratorCv, std::mutex& orchestratorMutex);
    void consumer(std::vector<std::atomic<bool>>& completedList, int tIndex,
                  std::condition_variable& orchestratorCv, std::mutex& orchestratorMutex);
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

class ExtractorMemory : public Extractor {
public:
    virtual ~ExtractorMemory() = default;
private:
    MemoryRepository* repository;
};

class ExtractorNoop : public Extractor {
public:
    virtual ~ExtractorNoop() = default;

    void addOutput(std::shared_ptr<DataFrame> outputDF);

    virtual void executeMonoThread() override;
    virtual std::vector<std::thread> executeMultiThread(int numThreads, std::vector<std::atomic<bool>>& completedThreads,
                                                        std::condition_variable& orchestratorCv, std::mutex& orchestratorMutex) override;

private:
    MemoryRepository* repository;
};

class Loader : public Task {
public:
    Loader(): repoMutex(), inputIndex(0), clearRepo(true) {};
    Loader(int inputDFIndex, bool clearRepository): repoMutex(), inputIndex(inputDFIndex), clearRepo(clearRepository) {};
    virtual ~Loader() = default;

    //Setter específico do loader
    void addRepo(DataRepository* repo){ repository = repo;};

    //Implementação específica do loader para o execute
    virtual void executeMonoThread() override;
    virtual std::vector<std::thread> executeMultiThread(int numThreads, std::vector<std::atomic<bool>>& completedThreads,
                                                        std::condition_variable& orchestratorCv, std::mutex& orchestratorMutex) override;

    //Implementação específica para os métodos de pós execução e contagem
    void finishExecution() override;

protected:
    DataRepository* repository;
    std::vector<DataFrameWithIndexes> getInput(int numThreads);

    std::mutex repoMutex;
    int inputIndex;
    bool clearRepo;

    void addRows(DataFrameWithIndexes pair, std::vector<std::atomic<bool>>& completedList, int tIndex,
                 std::condition_variable& orchestratorCv, std::mutex& orchestratorMutex);

};

class LoaderFile : public Loader {
public: 
    LoaderFile(int inputDFIndex, bool clearRepository)
        : Loader(inputDFIndex, clearRepository){};
    virtual ~LoaderFile() = default;

private:
    std::mutex repoMutex;
    int inputIndex;
    bool clearRepo;
    FileRepository* repository;
};
    
class LoaderSQLite : public Loader {
public: 
    LoaderSQLite(int inputDFIndex, bool clearRepository)
        : Loader(inputDFIndex, clearRepository){};
    virtual ~LoaderSQLite() = default;

private:
    std::mutex repoMutex;
    int inputIndex;
    bool clearRepo;
    SQLiteRepository* repository;
};

class LoaderMemory : public Loader {
public: 
    LoaderMemory(int inputDFIndex, bool clearRepository)
        : Loader(inputDFIndex, clearRepository){};
    virtual ~LoaderMemory() = default;

private:
    std::mutex repoMutex;
    int inputIndex;
    bool clearRepo;
    MemoryRepository* repository;
};

#endif
