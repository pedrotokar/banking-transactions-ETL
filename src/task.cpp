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

const bool Task::canBeParallel(){
    return !blockMultiThreading;
}

void Task::setWeight(int w) {
    weight = w;
}
int Task::getWeight() const {
    return weight;
}
void Task::setRecomendedThreadsNum(size_t numThreads) {
    recomendedThreadsNum = numThreads;
}
size_t Task::getRecomendedThreadsNum() const {
    return recomendedThreadsNum;
}

void Task::setAuxOrquestrador(int val) {
    auxOrquestrador = val;
}

void Task::incrementAuxOrquestrador() {
    auxOrquestrador++;
}

int Task::getAuxOrquestrador() const {
    return auxOrquestrador;
}

void Task::setTaskName(const std::string name) {
    taskName = name;
}
std::string Task::getTaskName() const {
    return taskName;
}

void Task::setLevel(int newLevel) {
    taskLevel = newLevel;
}

int Task::getLevel() const {
    return taskLevel;
}

void Task::executeMonoThreadSpecial(std::vector<std::atomic<bool>>& completedList, int tIndex, std::condition_variable& orchestratorCv, std::mutex& orchestratorMutex){
    executeMonoThread();
    completedList[tIndex].store(true, std::memory_order_release);
    {
        std::lock_guard<std::mutex> lk(orchestratorMutex);
        orchestratorCv.notify_one();
    }
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

void Transformer::transformThread(std::vector<std::shared_ptr<DataFrame>>& outputs,
                     const std::vector<DataFrameWithIndexes>& inputs,
                     std::vector<std::atomic<bool>>& completedList, int tIndex,
                     std::condition_variable& orchestratorCv, std::mutex& orchestratorMutex){
    // std::cout << tIndex << " " << completedList->size() << std::endl;
    transform(outputs, inputs);
    completedList[tIndex].store(true, std::memory_order_release);
    {
        std::lock_guard<std::mutex> lk(orchestratorMutex);
        orchestratorCv.notify_one();
    }
}

std::vector<std::thread> Transformer::executeMultiThread(int numThreads, std::vector<std::atomic<bool>>& completedThreads,
                                                         std::condition_variable& orchestratorCv, std::mutex& orchestratorMutex){
    std::vector<std::thread> runningThreads;
    if(numThreads == 1){
        runningThreads.emplace_back(&Transformer::executeMonoThreadSpecial, this, ref(completedThreads), 0, ref(orchestratorCv), ref(orchestratorMutex));
    }
    else{
        runningThreads = executeWithThreading(numThreads, completedThreads,  orchestratorCv, orchestratorMutex);
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
std::vector<std::thread> Transformer::executeWithThreading(int numThreads, std::vector<std::atomic<bool>>& completedList,
                                                           std::condition_variable& orchestratorCv, std::mutex& orchestratorMutex){
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
//    threadList.reserve(numThreads);
    for(int tIndex = 0; tIndex < numThreads; tIndex++){
        //Cada thread executa o equivalente a transform(outputDFs, threadInputs.at(tIndex));
        threadList.emplace_back(&Transformer::transformThread, this, ref(outputDFs), threadInputs.at(tIndex), ref(completedList), tIndex, ref(orchestratorCv), ref(orchestratorMutex));
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

std::vector<std::thread> Extractor::executeMultiThread(int numThreads, std::vector<std::atomic<bool>>& completedThreads,
                                                       std::condition_variable& orchestratorCv, std::mutex& orchestratorMutex){
    std::vector<std::thread> runningThreads;
    if(numThreads == 1){
        runningThreads.emplace_back(&Extractor::executeMonoThreadSpecial, this, ref(completedThreads), 0, ref(orchestratorCv), ref(orchestratorMutex));
    }
    else{
        std::cout << "Executando extrator com " << numThreads << " threads" << std::endl;
        maxBufferSize = numThreads * numThreads;

        runningThreads.emplace_back(&Extractor::producer, this, ref(completedThreads), 0,  ref(orchestratorCv), ref(orchestratorMutex));
        for (int i = 0; i < numThreads - 1; ++i) {
            runningThreads.emplace_back(&Extractor::consumer, this, ref(completedThreads), i + 1, ref(orchestratorCv), ref(orchestratorMutex));
        }
    }
    return runningThreads;
}

void Extractor::producer(std::vector<std::atomic<bool>>& completedList, int tIndex,
                         std::condition_variable& orchestratorCv, std::mutex& orchestratorMutex) {
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
    completedList[tIndex].store(true, std::memory_order_release);
    {
        std::lock_guard<std::mutex> lk(orchestratorMutex);
        orchestratorCv.notify_one();
    }
};

void Extractor::consumer(std::vector<std::atomic<bool>>& completedList, int tIndex,
                         std::condition_variable& orchestratorCv, std::mutex& orchestratorMutex) {
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
    completedList[tIndex].store(true, std::memory_order_release);
    {
        std::lock_guard<std::mutex> lk(orchestratorMutex);
        orchestratorCv.notify_one();
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
    repository->open();
    std::cout << "Executando loader sem paralelizar" << std::endl;
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

std::vector<std::thread> Loader::executeMultiThread(int numThreads, std::vector<std::atomic<bool>>& completedThreads,
                                                    std::condition_variable& orchestratorCv, std::mutex& orchestratorMutex){

    std::vector<std::thread> runningThreads;
    if(numThreads == 1){
        runningThreads.emplace_back(&Loader::executeMonoThreadSpecial, this, ref(completedThreads), 0, ref(orchestratorCv), ref(orchestratorMutex));
    }
    else{
        repository->open();
        std::cout << "Executando loader com " << numThreads << " threads" << std::endl;
        if(true){ //Tenho que só colocar a linha por cima?
            std::vector<DataFrameWithIndexes> inputs = getInput(numThreads);
            if(true){ //Tenho que apagar o repositório?
                repository->clear();
                StrRow header = inputs.at(0).second->getHeader();
                repository->appendHeader(header);
            }
            for (int i = 0; i < numThreads; i++) {
                runningThreads.emplace_back(&Loader::addRows, this, inputs[i], ref(completedThreads), i, ref(orchestratorCv), ref(orchestratorMutex));
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

void Loader::addRows(DataFrameWithIndexes pair, std::vector<std::atomic<bool>>& completedList, int tIndex, std::condition_variable& orchestratorCv, std::mutex& orchestratorMutex) {
    std::shared_ptr<DataFrame> dfInput = pair.second;
    std::vector<StrRow> rows;
    for (int i: pair.first) {
        // Pega cada linha do DF
        StrRow row = dfInput->getRow(i);
        // Adiciona as linhas ao vetor de linhas
        rows.push_back(row);
    }

    std::string batchRows = repository->serializeBatch(rows);

    {
        std::lock_guard<std::mutex> lock(repoMutex);
        repository->appendStr(batchRows);
    }
    completedList[tIndex].store(true, std::memory_order_release);
    {
        std::lock_guard<std::mutex> lk(orchestratorMutex);
        orchestratorCv.notify_one();
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
