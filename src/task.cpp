#include "task.h"
#include "dataframe.h"
#include "datarepository.h"
#include <memory>
#include <vector>

//Definições das interfaces para adicionar saídas e relacionamentos
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

//Getters
const std::vector<std::shared_ptr<Task>>& Task::getNextTasks(){
    return nextTasks;
}

const std::vector<std::pair<std::shared_ptr<Task>, std::vector<bool>>>& Task::getPreviousTasks(){
    return previousTasks;
}

const std::vector<DataFrame*>& Task::getOutputs(){
    return outputDFs;
}

//Sobrescreve o método abstrato execute com o que a transformers precisam fazer
void Transformer::execute(){
    std::vector<std::pair<std::vector<int>, DataFrame*>> inputs;
    for (auto previousTask : previousTasks){
        size_t dataFrameCounter = previousTask.first->getOutputs().size();
        for (size_t i = 0; i < dataFrameCounter; i++){
            auto dataFrame = previousTask.first->getOutputs().at(i);
            //bool shouldSplit = previousTask.second.at(i);
            //Primeiro constrói index para cada dataframe e depois faz e dá push no pair. Atualmente só faz uma array de índices mas isso irá mudar.
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

void Extractor::extract(DataFrame* & output, FileRepository* & repository) {   
    while (true) {
        DataRow row = repository->getRow();

        if (!repository->hasNext()) break;

        auto parsedRow = repository->parseRow(row);
        output->addRow(parsedRow);
    };
};

void Extractor::execute(){
    DataFrame* outDF = outputDFs.at(0);
    extract(outDF, repository);
    std::cout << outDF->toString() << std::endl;
};