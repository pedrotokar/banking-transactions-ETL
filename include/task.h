#ifndef TASK_H
#define TASK_H

#include <vector>
#include <mutex>
#include <utility>
#include <memory>
#include "dataframe.h"

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
    //Função virtual que será varia entre transformer, extractor e loader
    void execute() override;
};

#endif
