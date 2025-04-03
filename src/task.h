#ifndef TASK_H
#define TASK_H
#include <vector>
#include <mutex>
#include <utility>
#include <memory>
#include "series.h"


struct output_spec {};

class Task {
public:
    virtual ~Task() = default;
    void add_next(std::shared_ptr<Task> next_task);
    void add_output(const output_spec& spec);

protected:
    std::vector<std::shared_ptr<Task>> next_tasks;
    std::vector<output_spec> output_specs;
};

class Transformer : public Task {
public:
    virtual ~Transformer() = default;
    virtual void transform(std::vector<DataFrame*>& outputs,
                           const std::vector<std::pair<std::vector<int>, DataFrame*>>& inputs) = 0;
};

#endif
