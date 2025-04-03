#include "task.h"

void Task::add_next(std::shared_ptr<Task> next_task) {
    next_tasks.push_back(next_task);
}

void Task::add_output(const output_spec& spec) {
    output_specs.push_back(spec);
}
