#include "task.h"
#include "dataframe.h"
#include "datarepository.h"
#include <memory>
#include <vector>
#include <thread>
#include <chrono>
#include <atomic>
#include <condition_variable>

//TODO: melhorar isso daqui
std::vector<int> getRangeVector(size_t size, size_t numDivisions, size_t dIndex){
    std::vector<int> indexes;
    int blockSize = size/numDivisions;
    size_t startingPoint = dIndex * blockSize;
    size_t endingPoint;
    if(dIndex != numDivisions - 1){
        endingPoint = (dIndex + 1) * blockSize;
    } else{
        endingPoint = size;
    }
    for (size_t j = startingPoint; j < endingPoint; j++){
        indexes.push_back(j);
    }
    return indexes;
}
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
    auto start = std::chrono::high_resolution_clock::now();
    std::cout << "calling transform" << std::endl;
    transform(outputDFs, inputs);
    std::cout << "called transform" << std::endl;
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> elapsed = end - start;
    std::cout << "--- Time elapsed in transformer : " << elapsed.count() << "ms" << std::endl;
}


void Transformer::executeWithThreading(int numThreads = 2){
    std::vector<
        std::vector<std::pair<std::vector<int>, DataFrame*>>
    > threadInputs; //Vai ser um vector do que seria a entrada pra cada df. Vou fazer uns typdef aqui depois pq tá feio kk
    for (int i = 0; i < numThreads; i++){
        std::vector<std::pair<std::vector<int>, DataFrame*>> inputs;
        threadInputs.push_back(inputs); //Input para cada thread. Começa vazio
    }
    for (auto previousTask : previousTasks){ //Roda as tasks anteriores
        size_t dataFrameCounter = previousTask.first->getOutputs().size();
        for (size_t i = 0; i < dataFrameCounter; i++){ //Roda cada df que pode sair da task anterior. Se só passar a ser um fixo por task, esse for iria de base
            auto dataFrame = previousTask.first->getOutputs().at(i);

            bool shouldSplit = previousTask.second.at(i);
            if(shouldSplit){
                for (int tIndex = 0; tIndex < numThreads; tIndex++){
                    std::vector<int> indexes = getRangeVector(dataFrame->size(), numThreads, tIndex);
                    auto pair = std::make_pair(indexes, dataFrame);
                    threadInputs.at(tIndex).push_back(pair);
                }
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
    auto start = std::chrono::high_resolution_clock::now();
//    std::mutex transformerMutex;
    std::vector<std::thread> transformCalls;
    transformCalls.reserve(numThreads);

    for(int tIndex = 0; tIndex < numThreads; tIndex++){
        std::cout << "Aqui eu enfilero a thread " << tIndex << std::endl;
        transformCalls.emplace_back(&Transformer::transform, this, ref(outputDFs), threadInputs.at(tIndex));
//        transform(outputDFs, threadInputs.at(tIndex));
    }
    for(auto& transformCall: transformCalls){
        if(transformCall.joinable()){
            transformCall.join();
        }
    }
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> elapsed = end - start;
    std::cout << "--- Time elapsed in transformer : " << elapsed.count() << "ms" << std::endl;
}

void Extractor::extract(DataFrame* & output, FileRepository* & repository) {   
    while (true) {
        DataRow row = repository->getRow();

        if (!repository->hasNext()) break;

        auto parsedRow = repository->parseRow(row);
        output->addRow(parsedRow);
    };

    repository->close();
};

void Extractor::execute(){
    auto start = std::chrono::high_resolution_clock::now();
    DataFrame* outDF = outputDFs.at(0);
    extract(outDF, repository);
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> elapsed = end - start;
    std::cout << "--- Time elapsed in extractor : " << elapsed.count() << "ms" << std::endl;
};

void Loader::createRepo(DataFrame* & dfInput, FileRepository* & repository) {   
    repository->clear();
    StrRow header = dfInput->getHeader();
    repository->appendHeader(header);
    addRows(dfInput, repository);
};

void Loader::actualizeRepo(DataFrame* & dfInput, FileRepository* & repository) {   
    return;    
};

void Loader::addRows(DataFrame* & dfInput, FileRepository* & repository) {   
    for (size_t i = 0; i < dfInput->size(); i++)
    {
        std::vector<std::string> row = dfInput->getRow(i);
        repository->appendRow(row);
    }
    repository->close();
};


void Loader::execute(){
    auto start = std::chrono::high_resolution_clock::now();

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
    DataFrame* dfInput = inputs[0].second;

    createRepo(dfInput, repository);
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> elapsed = end - start;
    std::cout << "--- Time elapsed in loader : " << elapsed.count() << "ms" << std::endl;
};
