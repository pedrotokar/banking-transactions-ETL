#include "task.h"
#include "dataframe.h"

void Task::addNext(std::shared_ptr<Task> nextTask) {
    nextTasks.push_back(nextTask);
}

void Task::addOutput(DataFrame* spec) {
    outputDFs.push_back(spec);
}

void Transformer::execute(const std::vector<std::pair<std::vector<int>, DataFrame*>>& inputs){
    std::cout << "calling transform" << std::endl;
    transform(outputDFs, inputs);
    std::cout << "called transform" << std::endl;
}
