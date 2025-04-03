#include "task.h"

void Task::addNext(std::shared_ptr<Task> nextTask) {
    nextTasks.push_back(nextTask);
}

void Task::addOutput(const OutputSpec& spec) {
    outputSpecs.push_back(spec);
}
