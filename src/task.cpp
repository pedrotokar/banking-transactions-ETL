#include "task.h"
#include "dataframe.h"

void Task::addNext(std::shared_ptr<Task> nextTask) {
    nextTasks.push_back(nextTask);
    std::vector<bool> splitDFs;
    for(size_t i = 0; i < outputDFs.size(); i++){
        splitDFs.push_back(true);
    }
    nextTask->addPrevious(shared_from_this(), splitDFs);
}

void Task::addPrevious(std::shared_ptr<Task> previousTask, std::vector<bool> splitDFs){
    if(previousTask->getOutputs().size() != splitDFs.size()){
        throw "The number of elements in the vector splitDFs doesnt match the number of dataframes that the task outputs";
    }
    auto pair = make_pair(previousTask, splitDFs);
    previousTasks.push_back(pair);
}

void Task::addOutput(DataFrame* spec) {
    outputDFs.push_back(spec);
}

const std::vector<std::shared_ptr<Task>>& Task::getNextTasks(){
    return nextTasks;
}

const std::vector<std::pair<std::shared_ptr<Task>, std::vector<bool>>>& Task::getPreviousTasks(){
    return previousTasks;
}

const std::vector<DataFrame*>& Task::getOutputs(){
    return outputDFs;
}

void Transformer::execute(){
    std::vector<std::pair<std::vector<int>, DataFrame*>> inputs;
    for (auto previousTask : previousTasks){
        size_t dataFrameCounter = previousTask.first->getOutputs().size();
        for (size_t i = 0; i < dataFrameCounter; i++){
            auto dataFrame = previousTask.first->getOutputs().at(i);
            //Primeiro constrói o vetor index para cada dataframe e depois faz e dá push no pair.
            //Como tem só uma thread, só pega o indice todo do dataframe
            std::vector<int> indexes;
            for (size_t j = 0; j < dataFrame->size(); j++){
                indexes.push_back(j);
            }
            auto pair = std::make_pair(indexes, dataFrame);
            inputs.push_back(pair);
        }
    }
    std::cout << "calling transform" << std::endl;
    transform(outputDFs, inputs);
    std::cout << "called transform" << std::endl;
}

void Transformer::executeWithThreading(int numThreads = 2){
    std::vector<std::vector<std::pair<std::vector<int>, DataFrame*>>> threadInputs;
    for (int i = 0; i < numThreads; i++){
        std::vector<std::pair<std::vector<int>, DataFrame*>> inputs;
        threadInputs.push_back(inputs);
    }
    for (auto previousTask : previousTasks){
        size_t dataFrameCounter = previousTask.first->getOutputs().size();
        for (size_t i = 0; i < dataFrameCounter; i++){
            auto dataFrame = previousTask.first->getOutputs().at(i);
            bool shouldSplit = previousTask.second.at(i);
            if(shouldSplit){
                int blockSize = dataFrame->size()/numThreads;
                for (int tIndex = 0; tIndex < numThreads - 1; tIndex++){
                    std::vector<int> indexes;
                    size_t startingPoint = tIndex * blockSize;
                    size_t endingPoint = (tIndex + 1) * blockSize;
                    for (size_t j = startingPoint; j < endingPoint; j++){
                        indexes.push_back(j);
                    }
                    auto pair = std::make_pair(indexes, dataFrame);
                    threadInputs.at(tIndex).push_back(pair);
                }
                std::vector<int> indexes;
                size_t startingPoint = (numThreads - 1) * blockSize;
                size_t endingPoint = dataFrame->size();
                for (size_t j = startingPoint; j < endingPoint; j++){
                    indexes.push_back(j);
                }
                auto pair = std::make_pair(indexes, dataFrame);
                threadInputs.at(numThreads - 1).push_back(pair);
            } else {
                std::vector<int> indexes;
                for (size_t j = 0; j < dataFrame->size(); j++){
                    indexes.push_back(j);
                }
                for (int tIndex = 0; tIndex < numThreads; tIndex++){
                    auto pair = std::make_pair(indexes, dataFrame);
                    threadInputs.at(tIndex).push_back(pair);
                }
                //inputs.push_back(pair);
            }
        }
    }
    for(int tIndex = 0; tIndex < numThreads; tIndex++){
        std::cout << "Aqui eu chamo a thread " << tIndex << std::endl;
        transform(outputDFs, threadInputs.at(tIndex));
    }
}
