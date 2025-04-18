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
    auto start = std::chrono::high_resolution_clock::now();
    //testeTrigger2();
    transform(outputDFs, inputs);
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> elapsed = end - start;
    std::cout << "--- Time elapsed in transformer : " << elapsed.count() << "ms" << std::endl;
//    std::cout << "calling transform" << std::endl;
//    std::cout << "called transform" << std::endl;
}

void Extractor::addOutput(DataFrame* spec) {
    outputDFs.push_back(spec);
    dfOutput = outputDFs.at(0);
}

void Extractor::extract(int numThreads) {  
    std::thread threadProducer(&Extractor::producer, this);

    std::vector<std::thread> consumers;
    for (int i = 0; i < numThreads; ++i) {
        consumers.emplace_back(&Extractor::consumer, this);
    }    

    if (threadProducer.joinable()) threadProducer.join();
    for (auto& threadConsumer : consumers) {
        if (threadConsumer.joinable()) threadConsumer.join();
    }
};

void Extractor::producer() {   
    auto maxBufferSize = buffer.size() + 10000;
    while (true) {
        // Pega cada linha da base de dados
        DataRow row = repository->getRow();

        // Verifica se terminou
        if (!repository->hasNext()) break;
   
        // Converte para string a linha do repostitório
        StrRow parsedRow = repository->parseRow(row);
     
        std::unique_lock<std::mutex> lock(bufferMutex);
        cv.wait(lock, [this, &maxBufferSize] { return buffer.size() < maxBufferSize; });

        // Adiciona a linha ao buffer
        buffer.push(parsedRow);

        // Notifica aos consumidores
        cv.notify_all();
    };
    // cv.notify_all();
    // Fecha o arquivo e informa que encerrou a produção
    repository->close();
    endProduction = true;
};

void Extractor::consumer() {   
    while (true) {
        std::unique_lock<std::mutex> lock(bufferMutex);

        // Aguarda até que o buffer não esteja vazio enquanto há produção
        cv.wait(lock, [this] { return !buffer.empty() || endProduction; });
        // Verifica se terminou o serviço
        if (buffer.empty() && endProduction) break;
        // Pega o primeira linha no buffer
        StrRow parsedRow = buffer.front();
        buffer.pop();
        
        // Libera o mutex antes de acessar dfOutput
        lock.unlock(); 

        // Adiciona o dado processado ao dfOutput
        {
            std::lock_guard<std::mutex> dfLock(dfMutex);
            dfOutput->addRow(parsedRow);
        }
        cv.notify_all();
    }
}

void Extractor::execute(){
    auto start = std::chrono::high_resolution_clock::now();
    extract(1);
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> elapsed = end - start;
    std::cout << "--- Time elapsed in extractor : " << elapsed.count() << "ms" << std::endl;
};

void Loader::getInput() {
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
    dfInput = inputs[0].second;
}

void Loader::createRepo(int numThreads) {   
    repository->clear();
    StrRow header = dfInput->getHeader();
    repository->appendHeader(header);
    addRows(numThreads);
};

void Loader::actualizeRepo(int numThreads) {   
    return;    
};

/*void Loader::addRows() {   
    for (size_t i = 0; i < dfInput->size(); i++)
    {
        std::vector<std::string> row = dfInput->getRow(i);
        repository->appendRow(row);
    }
    repository->close();
};*/

void Loader::addRows(int numThreads) {  
    std::thread threadProducer(&Loader::producer, this);

    std::vector<std::thread> consumers;
    for (int i = 0; i < numThreads; ++i) {
        consumers.emplace_back(&Loader::consumer, this);
    }    

    if (threadProducer.joinable()) threadProducer.join();
    for (auto& threadConsumer : consumers) {
        if (threadConsumer.joinable()) threadConsumer.join();
    }
    repository->close();
};

void Loader::producer() {   
    int i = 0;
    int inputSize = dfInput->size();
    auto maxBufferSize = buffer.size() + 10000;
    while (true) {
        // Verifica se terminou
        if (i == inputSize) break;

        // Pega cada linha do DF   
        std::vector<std::string> row = dfInput->getRow(i);
        i++;

        // Adiciona a linha ao buffer
        
        std::unique_lock<std::mutex> lock(bufferMutex);
        cv.wait(lock, [this, &maxBufferSize] { return buffer.size() < maxBufferSize; });
        buffer.push(row);
        // Notifica aos consumidores
        cv.notify_all();
    };
    // cv.notify_all();

    endProduction = true;
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

void Loader::execute(){
    auto start = std::chrono::high_resolution_clock::now();

    getInput();
    createRepo(1);

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> elapsed = end - start;
    std::cout << "--- Time elapsed in loader : " << elapsed.count() << "ms" << std::endl;
};
