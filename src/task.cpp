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

void Task::resetExecutedPreviousTasks(){
    cntExecutedPreviousTasks = 0;
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

//Sobrescreve o método abstrato execute com o que a transformers precisam fazer
void Transformer::execute(int numThreads){
    if(numThreads == 1){
        std::vector<DataFrameWithIndexes> inputs;
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
        // std::cout << "calling transform" << std::endl;
        transform(outputDFs, inputs);
        // std::cout << "called transform" << std::endl;
        auto end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double, std::milli> elapsed = end - start;
        std::cout << "--- Time elapsed in transformer : " << elapsed.count() << "ms" << std::endl;
    }
    else{
        executeWithThreading(numThreads);
    }

    //Limpeza pós execução
    for (auto previousTask: previousTasks){
        previousTask.first->decreaseConsumingCounter();
    }
}

//Versão multithread do execute - divide corretamente os índices para cada thread saber em que parte do DF deve operar sobre.
void Transformer::executeWithThreading(int numThreads){
    //Um vector contendo as entradas que serão passadas para cada thread
    std::vector<std::vector<DataFrameWithIndexes>> threadInputs;
    for (int i = 0; i < numThreads; i++){
        std::vector<DataFrameWithIndexes> inputs;
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
            }
        }
    }
    auto start = std::chrono::high_resolution_clock::now();

    std::vector<std::thread> threadList;
    threadList.reserve(numThreads);
    for(int tIndex = 0; tIndex < numThreads; tIndex++){
        //Cada thread executa o equivalente a transform(outputDFs, threadInputs.at(tIndex));
        threadList.emplace_back(&Transformer::transform, this, ref(outputDFs), threadInputs.at(tIndex));
    }
    //Espera threads terminarem
    for(auto& workingThread: threadList){
        if(workingThread.joinable()){
            workingThread.join();
        }
    }

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> elapsed = end - start;
    std::cout << "--- Time elapsed in transformer : " << elapsed.count() << "ms" << std::endl;
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

void Extractor::extract(int numThreads) {
    // Multithread
    if (numThreads > 1) {
        std::cout << "Executando extrator com " << numThreads << " de consumidor" << std::endl;
        maxBufferSize = numThreads * numThreads;
    
        std::thread threadProducer(&Extractor::producer, this);

        std::vector<std::thread> consumers;
        for (int i = 0; i < numThreads; ++i) {
            consumers.emplace_back(&Extractor::consumer, this);
        }    

        if (threadProducer.joinable()) threadProducer.join();
        for (auto& threadConsumer : consumers) {
            if (threadConsumer.joinable()) threadConsumer.join();
        }
    } 
    // Uma única thread
    else {  
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
    };
    repository->close();
};

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


void Extractor::execute(int numThreads){
    numThreads++;
    auto start = std::chrono::high_resolution_clock::now();
    extract(numThreads);
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> elapsed = end - start;
    std::cout << "--- Time elapsed in extractor : " << elapsed.count() << "ms" << std::endl;

    endProduction = false;
};

// ###############################################################################################
// ###############################################################################################
// Metodos da classe Loader

void Loader::getInput() {
    std::vector<DataFrameWithIndexes> inputs;
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
    dfInput = inputs[0].second;
}

void Loader::createRepo(int numThreads) {   
    repository->clear();

    StrRow header = dfInput->getHeader();
    repository->appendHeader(header);

    addRows(numThreads);
};

void Loader::updateRepo(int numThreads) {   
    return;    
};

void Loader::addRows(int numThreads) {  
    // Multithread
    if (numThreads > 1) {
        std::thread threadProducer(&Loader::producer, this);
        maxBufferSize = numThreads * numThreads;

        std::vector<std::thread> consumers;
        for (int i = 0; i < numThreads; ++i) {
            consumers.emplace_back(&Loader::consumer, this);
        }    

        if (threadProducer.joinable()) threadProducer.join();
        for (auto& threadConsumer : consumers) {
            if (threadConsumer.joinable()) threadConsumer.join();
        }
    } 
    // Uma única thread
    else {  
        // Percorre por todo DF de input
        for (size_t i = 0; i < dfInput->size(); i++) {
            // Pega cada linha do DF
            std::vector<std::string> row = dfInput->getRow(i);
            // Adiciona a linha ao repositório
            repository->appendRow(row);
        }
    }
    repository->close();
};

void Loader::producer() {   
    int i = 0;
    int inputSize = dfInput->size();
    while (true) {
        // Verifica se terminou
        if (i == inputSize) break;

        // Pega cada linha do DF   
        std::vector<std::string> row = dfInput->getRow(i);
        i++;

        // Aguarda caso o buffer esteja cheio
        std::unique_lock<std::mutex> lock(bufferMutex);
        cv.wait(lock, [this] { return buffer.size() < maxBufferSize; });
        // Adiciona a linha ao buffer
        buffer.push(row);

        // Notifica aos consumidores
        cv.notify_all();
    };
    // Assinala aaos consumidores que encerrou a produção
    endProduction = true;
    cv.notify_all();
};

void Loader::consumer() {   
    while (true) {
        std::unique_lock<std::mutex> lock(bufferMutex);

        // Aguarda até que o buffer não esteja vazio enquanto há produção
        cv.wait(lock, [this] { return !buffer.empty() || endProduction; });

        // Verifica se terminou o serviço
        if (buffer.empty() && endProduction) break;

        // Pega o primeira linha no buffer
        StrRow row = buffer.front();
        buffer.pop();

        // Libera o mutex antes de acessar o repositório
        lock.unlock(); 

        // Adiciona o dado processado ao repositório
        {
            std::lock_guard<std::mutex> dfLock(repoMutex);
            repository->appendRow(row);
        }
        cv.notify_all();
    }
}

void Loader::execute(int numThreads){
    auto start = std::chrono::high_resolution_clock::now();

    getInput();
    createRepo(numThreads);
    dfInput.reset();

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> elapsed = end - start;
    std::cout << "--- Time elapsed in loader : " << elapsed.count() << "ms" << std::endl;

    //Limpeza pós execução
    for (auto previousTask: previousTasks){
        previousTask.first->decreaseConsumingCounter();
    }
};
