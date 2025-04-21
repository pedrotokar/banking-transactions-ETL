#include "task.h"
#include "dataframe.h"
#include "datarepository.h"
#include <memory>
#include <stdexcept>
#include <vector>
#include <thread>
#include <chrono>
#include <atomic>
#include <condition_variable>
#include <iostream>

//TODO: melhorar isso daqui
//Função auxiliar para não poluir a execute do transformer
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

// ###############################################################################################
// ###############################################################################################
// Métodos da classe Task

//Definições das interfaces para adicionar saídas e relacionamentos - comum a todas as tasks
void Task::addNext(std::shared_ptr<Task> nextTask, std::vector<bool> splitDFs) {
    nextTask->addPrevious(shared_from_this(), splitDFs);

    nextTasks.push_back(nextTask);
    tasksConsumingOutput = nextTasks.size();
}

//Todas as tasks precisam saber suas anteriores e próximas, por isso esse método também é necessário
void Task::addPrevious(std::shared_ptr<Task> previousTask, std::vector<bool> splitDFs){
    if(previousTask->getOutputs().size() != splitDFs.size()){
        throw std::runtime_error("The number of elements in the vector splitDFs doesnt match the number of dataframes that the task outputs.");
    }
    auto pair = make_pair(previousTask, splitDFs);
    previousTasks.push_back(pair);
}

void Task::addOutput(std::shared_ptr<DataFrame> modelDF) {
    outputDFs.push_back(modelDF->emptyCopy());
}

void Task::incrementExecutedPreviousTasks(){
    cntExecutedPreviousTasks++;
}

//Getters
const std::vector<std::shared_ptr<Task>>& Task::getNextTasks(){
    return nextTasks;
}

const std::vector<std::pair<std::shared_ptr<Task>, std::vector<bool>>>& Task::getPreviousTasks(){
    return previousTasks;
}

const std::vector<std::shared_ptr<DataFrame>>& Task::getOutputs(){
    return outputDFs;
}

const bool Task::checkPreviousTasks() const {
    return cntExecutedPreviousTasks == previousTasks.size();
}


// ###############################################################################################
// ###############################################################################################
// Metodos da classe transformer

//Coloca a lógica específica do transformador para limpar seus DFs de saída
void Transformer::decreaseConsumingCounter(){
    std::unique_lock<std::mutex> lock(consumingCounterMutex);
    tasksConsumingOutput--;
    if(tasksConsumingOutput == 0){
        for(size_t i = 0; i < outputDFs.size(); i++){
            //std::cout << "Limpando dataframes agora que as saídas já consumiram " << outputDFs[i].use_count() << std::endl;
            outputDFs[i] = outputDFs[i]->emptyCopy();
        }
        tasksConsumingOutput = nextTasks.size();
    }
}

std::vector<std::thread> Transformer::executeMultiThread(int numThreads){
    std::vector<std::thread> runningThreads;
    if(numThreads == 1){
        runningThreads.emplace_back(&Transformer::executeMonoThread, this);
    }
    else{
        runningThreads = executeWithThreading(numThreads);
    }
    return runningThreads;
}

void Transformer::executeMonoThread(){

    std::vector<DataFrameWithIndexes> inputs;//A entrada da função transform

    //Constrói essa entrada de acordo com os dataframes anteriores
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
    transform(outputDFs, inputs);
}

//Função separada da executeMultiThread para não poluir ela
std::vector<std::thread> Transformer::executeWithThreading(int numThreads){
    //Um vector contendo as entradas que serão passadas para cada thread
    std::vector<std::vector<DataFrameWithIndexes>> threadInputs;
    for (int i = 0; i < numThreads; i++){
        std::vector<DataFrameWithIndexes> inputs;
        threadInputs.push_back(inputs); //Input para cada thread. Começa vazio
    }
    //Constrói  as entradas de acordo com os dataframes anteriores
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
            }
        }
    }
    std::vector<std::thread> threadList;
    threadList.reserve(numThreads);
    for(int tIndex = 0; tIndex < numThreads; tIndex++){
        //Cada thread executa o equivalente a transform(outputDFs, threadInputs.at(tIndex));
        threadList.emplace_back(&Transformer::transform, this, ref(outputDFs), threadInputs.at(tIndex));
    }
    return threadList;
}

void Transformer::finishExecution(){
    //Limpeza pós execução
    for (auto previousTask: previousTasks){
        previousTask.first->decreaseConsumingCounter();
    }
    cntExecutedPreviousTasks = 0;
}

// ###############################################################################################
// ###############################################################################################
// Metodos da classe Extractor

void Extractor::addOutput(std::shared_ptr<DataFrame> modelDF) {
    outputDFs.push_back(modelDF->emptyCopy());
    dfOutput = outputDFs.at(0);
}

void Extractor::decreaseConsumingCounter(){
    std::unique_lock<std::mutex> lock(consumingCounterMutex);
    tasksConsumingOutput--;
    if(tasksConsumingOutput == 0){
        for(size_t i = 0; i < outputDFs.size(); i++){
            //std::cout << "Limpando dataframes agora que as saídas já consumiram " << outputDFs[i].use_count() << std::endl;
            outputDFs[i] = outputDFs[i]->emptyCopy();
            dfOutput = outputDFs[i];
        }
        tasksConsumingOutput = nextTasks.size();
    }
}

void Extractor::executeMonoThread(){
    std::cout << "Executando extrator sem paralelizar" << std::endl;
    // Percorre toda a base de dados
    while (true) {
        // Pega cada linha
        DataRow row = repository->getRow();

        // Verifica se já percorreu a base completamente
        if (!repository->hasNext()) break;

        // Processa cada linha
        StrRow parsedRow = repository->parseRow(row);
        // Adicina a linha processada no DF de output
        dfOutput->addRow(parsedRow);
    };
}

std::vector<std::thread> Extractor::executeMultiThread(int numThreads){
    std::vector<std::thread> runningThreads;
    if(numThreads == 1 || numThreads == 2){
        runningThreads.emplace_back(&Extractor::executeMonoThread, this);
    }
    else{
        std::cout << "Executando extrator com " << numThreads - 1<< " de consumidor" << std::endl;
        maxBufferSize = numThreads * numThreads;

        runningThreads.emplace_back(&Extractor::producer, this);
        for (int i = 0; i < numThreads - 1; ++i) {
            runningThreads.emplace_back(&Extractor::consumer, this);
        }
    }
    return runningThreads;
}

void Extractor::producer() {
    while (true) {
        // Pega um batch de linhas da base de dados
        std::string rows = repository->getBatch();

        // Mutex para caso o buffer se encha
        std::unique_lock<std::mutex> lock(bufferMutex);
        cv.wait(lock, [this] { return buffer.size() < maxBufferSize; });

        // Adiciona o batch de linhas ao buffer
        buffer.push(rows);

        // Verifica se terminou
        if (!repository->hasNext()) break;

        // Libera o mutex
        lock.unlock();

        // Notifica aos consumidores
        cv.notify_all();
    };
    // Fecha o arquivo e informa que encerrou a produção
    repository->close();
    endProduction = true;

    // Notifica aos consumidores que encerrou a produção
    cv.notify_all();
};

void Extractor::consumer() {
    while (true) {
        std::unique_lock<std::mutex> lock(bufferMutex);

        // Aguarda até que o buffer não esteja vazio enquanto há produção
        cv.wait(lock, [this] { return !buffer.empty() || endProduction; });

        // Verifica se terminou o serviço
        if (buffer.empty() && endProduction) break;

        // Pega o primeira linha no buffer
        std::string rows = buffer.front();
        buffer.pop();

        // Converte para string as linhas do repostitório
        std::vector<StrRow> parsedRows = repository->parseBatch(rows);

        // Libera o mutex antes de acessar dfOutput
        lock.unlock();

        // Adiciona o dado processado ao dfOutput
        {
            std::lock_guard<std::mutex> dfLock(dfMutex);
            for (StrRow parsedRow : parsedRows)
                dfOutput->addRow(parsedRow);
        }
        cv.notify_all();
    }
}

void Extractor::finishExecution(){
    repository->close();
    endProduction = false;
    cntExecutedPreviousTasks = 0;
}

// ###############################################################################################
// ###############################################################################################
// Metodos da classe Loader

std::vector<DataFrameWithIndexes> Loader::getInput(int numThreads) {
    std::vector<DataFrameWithIndexes> inputs;
    std::shared_ptr<DataFrame> dfInput = previousTasks[0].first->getOutputs().at(inputIndex);
    if (numThreads > 1) {
        for (int i = 0; i < numThreads; i++){
            std::vector<int> indexes = getRangeVector(dfInput->size(), numThreads, i);
            auto pair = std::make_pair(indexes, dfInput);
            inputs.push_back(pair);
        }
    } 
    else {
        //Primeiro constrói index para cada dataframe e depois faz e dá push no pair. Atualmente só faz uma array de índices mas isso irá mudar.
        std::vector<int> indexes;
        for (size_t j = 0; j < dfInput->size(); j++){
            indexes.push_back(j);
        }
        auto pair = std::make_pair(indexes, dfInput);
        inputs.push_back(pair);
    }
    return inputs;
}

void Loader::executeMonoThread(){
    if(true){ //Tenho só que colocar linha por cima?
        std::vector<DataFrameWithIndexes> inputs = getInput(1);
        if(true){ //Tenho que apagar o repositório?
            repository->clear();
            StrRow header = inputs.at(0).second->getHeader();
            repository->appendHeader(header);
        }
        std::shared_ptr<DataFrame> dfInput = inputs[0].second;
        for (auto i: inputs[0].first) {
            // Pega cada linha do DF
            std::vector<std::string> row = dfInput->getRow(i);
            // Adiciona a linha ao repositório
            repository->appendRow(row);
        }
    } else {
        //Aqui vai entrar a lógica SINGLETHREADED para atualizar as linhas.
        std::cout << "guilherme" << std::endl;
    }
}

std::vector<std::thread> Loader::executeMultiThread(int numThreads){
//    numThreads += 2;
    std::vector<std::thread> runningThreads;
    if(numThreads == 1){
        runningThreads.emplace_back(&Loader::executeMonoThread, this);
    }
    else{
        if(true){ //Tenho que só colocar a linha por cima?
            std::vector<DataFrameWithIndexes> inputs = getInput(numThreads);
            if(true){ //Tenho que apagar o repositório?
                repository->clear();
                StrRow header = inputs.at(0).second->getHeader();
                repository->appendHeader(header);
            }
            for (int i = 0; i < numThreads; i++) {
                runningThreads.emplace_back(&Loader::addRows, this, inputs[i]);
            }
        } else {
            //Aqui vai entrar a lógica MULTITHREADED para atualizar linhas. Pra gerar as threads tem que usar o vetor runningThreads
            std::cout << "guilherme" << std::endl;
        }
    }
    return runningThreads;
}

void Loader::updateRepo(int numThreads) {   
    return;    
};

void Loader::addRows(DataFrameWithIndexes pair) {
    std::shared_ptr<DataFrame> dfInput = pair.second;
    std::vector<StrRow> rows;
    for (int i: pair.first) {
        // Pega cada linha do DF
        StrRow row = dfInput->getRow(i);
        // Adiciona as linhas ao vetor de linhas
        rows.push_back(row);
    }
    {
        std::lock_guard<std::mutex> lock(repoMutex);
        for (StrRow row : rows)
            repository->appendRow(row);
    }
};

void Loader::finishExecution() {
    repository->close();
    for (auto previousTask: previousTasks){
        previousTask.first->decreaseConsumingCounter();
    }
    cntExecutedPreviousTasks = 0;
}

// void Loader::producer() {
//     int i = 0;
//     int inputSize = dfInput->size();
//     while (true) {
//         // Verifica se terminou
//         if (i == inputSize) break;
//
//         // Pega cada linha do DF
//         std::vector<std::string> row = dfInput->getRow(i);
//         i++;
//
//         // Aguarda caso o buffer esteja cheio
//         std::unique_lock<std::mutex> lock(bufferMutex);
//         cv.wait(lock, [this] { return buffer.size() < maxBufferSize; });
//         // Adiciona a linha ao buffer
//         buffer.push(row);
//
//         // Notifica aos consumidores
//         cv.notify_all();
//    };

// void Loader::consumer() {
//     while (true) {
//         std::unique_lock<std::mutex> lock(bufferMutex);
//
//         // Aguarda até que o buffer não esteja vazio enquanto há produção
//         cv.wait(lock, [this] { return !buffer.empty() || endProduction; });
//
//         // Verifica se terminou o serviço
//         if (buffer.empty() && endProduction) break;
//
//         // Pega o primeira linha no buffer
//         StrRow row = buffer.front();
//         buffer.pop();
//
//         // Libera o mutex antes de acessar o repositório
//         lock.unlock();
//
//         // Adiciona o dado processado ao repositório
//         {
//             std::lock_guard<std::mutex> dfLock(repoMutex);
//             repository->appendRow(row);
//         }
//         cv.notify_all();
//     }
// }
