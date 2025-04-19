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
    virtual void execute() {};
    //Funções para verificar se a task possui pendencias
    void incrementExecutedPreviousTasks();
    const bool checkPreviousTasks() const;
protected:
    //Vetores com as saídas e relacionamentos
    std::vector<std::shared_ptr<Task>> nextTasks;
    std::vector<DataFrame*> outputDFs;
    std::vector<std::pair<std::shared_ptr<Task>, std::vector<bool>>> previousTasks;
    size_t cntExecutedPreviousTasks=0;
};

class Transformer : public Task {
public:
    virtual ~Transformer() = default;
    //Função virtual que qualquer transformer deve implementar
    virtual void transform(std::vector<DataFrame*>& outputs,
                           const std::vector<std::pair<std::vector<int>, DataFrame*>>& inputs) = 0;
    //Função virtual que será varia entre transformer, extractor e loader
    void execute() override;
};

class Extractor : public Task {
public:
    virtual ~Extractor() = default;
    void extract(DataFrame* & output, FileRepository* & repository);

    void addRepo(FileRepository* repo){ repository = repo;};

    void execute();
private:
    FileRepository* repository;
};

class Loader : public Task {
public:
    virtual ~Loader() = default;
    void createRepo(DataFrame* & dfInput, FileRepository* & repository);
    void actualizeRepo(DataFrame* & dfInput, FileRepository* & repository);
    void addRows(DataFrame* & dfInput, FileRepository* & repository);

    void addRepo(FileRepository* repo){ repository = repo;};

    void execute();
private:
    FileRepository* repository;
};

#endif
