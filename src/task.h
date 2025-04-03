#ifndef TASK_H
#define TASK_H

#include <vector>
#include <mutex>
#include <utility>
#include <memory>
#include "series.h"

struct OutputSpec {};

class Task {
public:
    virtual ~Task() = default;
    void addNext(std::shared_ptr<Task> nextTask);
    void addOutput(const OutputSpec& spec);

protected:
    std::vector<std::shared_ptr<Task>> nextTasks;
    std::vector<OutputSpec> outputSpecs;
};

class Transformer : public Task {
public:
    virtual ~Transformer() = default;
    virtual void transform(std::vector<DataFrame*>& outputs,
                           const std::vector<std::pair<std::vector<int>, DataFrame*>>& inputs) = 0;
};

#endif
