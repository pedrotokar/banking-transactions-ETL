#ifndef TASK_H
#define TASK_H

#include <vector>
#include <mutex>
#include <utility>
#include <memory>
#include "dataframe.h"

struct OutputSpec {};

class Task {
public:
    virtual ~Task() = default;
    void addNext(std::shared_ptr<Task> nextTask);
    void addOutput(DataFrame* outputDF);

protected:
    std::vector<std::shared_ptr<Task>> nextTasks;
    std::vector<DataFrame*> outputDFs;
};

class Transformer : public Task {
public:
    virtual ~Transformer() = default;
    virtual void transform(std::vector<DataFrame*>& outputs,
                           const std::vector<std::pair<std::vector<int>, DataFrame*>>& inputs) = 0;
    void execute(const std::vector<std::pair<std::vector<int>, DataFrame*>>& inputs);

};

#endif
