#include "trigger.hpp"
#include <queue>
#include <thread>
#include <chrono>
#include <atomic>
#include <condition_variable>
#include <iostream>
#include <map>
#include <string>

// ##################################################################################################
// ##################################################################################################
// Implementação dos métodos de Trigger
Trigger::~Trigger() = default; // Destrutor padrão

void Trigger::setExtractors(const std::vector<std::shared_ptr<Task>>& vExtractors) {
    this->vExtractors = vExtractors;
}

void Trigger::addExtractor(std::shared_ptr<Task> extractor) {
    vExtractors.push_back(extractor);
}

void Trigger::clearExtractors() {
    vExtractors.clear();
}

void Trigger::orchestratePipelineMonoThread() {
    std::queue<std::shared_ptr<Task>> tasksQueue;

    // Adiciona os extratores à fila de tarefas
    for (const auto& extractor : vExtractors) {
        tasksQueue.push(extractor);
    }
    
    std::cout << "Iniciando execução da pipeline...\n";
    while (!tasksQueue.empty()) {
        auto task = tasksQueue.front();
        tasksQueue.pop();

        // Executa a tarefa
        std::cout << "(1)Tamanho do nextTasks da task atual: " << task->getNextTasks().size() << std::endl;
        task->executeMonoThread();
        task->finishExecution();
        std::cout << "(2)Tamanho do nextTasks da task atual: " << task->getNextTasks().size() << std::endl;

        // Adiciona as próximas tarefas à fila
        const auto& nextTasks = task->getNextTasks();
        for (const auto& nextTask : nextTasks) {
            nextTask->incrementExecutedPreviousTasks();

            // Se todas as tarefas anteriores foram executadas, adiciona a próxima tarefa à fila
            if(nextTask->checkPreviousTasks()) {
                tasksQueue.push(nextTask);  
            }
        }
    }
    std::cout << "Pipeline concluída.\n";
}
/*
void Trigger::orchestratePipelineMultiThread(int numThreads) {
    std::queue<std::shared_ptr<Task>> tasksQueue;
    std::mutex queueMutex;
    std::condition_variable cv;
    std::atomic<int> pendingTasks{0};

    {
        std::lock_guard<std::mutex> lock(queueMutex);
        for (const auto& extractor : vExtractors) {
            tasksQueue.push(extractor);
            pendingTasks++;
        }
    }

    auto worker = [&]() {
        while (true) {
            std::shared_ptr<Task> currentTask;
            {
                std::unique_lock<std::mutex> lock(queueMutex);
                cv.wait(lock, [&]() { 
                    return !tasksQueue.empty() || pendingTasks.load() == 0; 
                });

                if (tasksQueue.empty() && pendingTasks.load() == 0) {
                    return;
                }

                currentTask = tasksQueue.front();
                tasksQueue.pop();
            }

            int numThreadsCalling = 4;
            std::vector<int>* completedThreads = new std::vector<int>(numThreadsCalling, 0);
            std::vector<std::thread> threadList = currentTask->executeMultiThread(numThreadsCalling, (*completedThreads));
            //Rodando esse vetor você pode saber quando uma thread completou (e se o vetor for todo de verdadeiros, se o bloco completou)
            //Não to com energia pra fazer um sistema que verifique por isso, mas com certeza é possível agora

            std::vector<int> completed(numThreadsCalling, 0);
            int runningCount = numThreadsCalling;
            while(runningCount != 0){
                for(int i = 0; i < numThreadsCalling; i++){
                    if(completed[i] == 0 && (*completedThreads)[i] == 1){
                        std::cout << "thread " << i << " terminou" << std::endl;
                        runningCount--;
                        threadList[i].join();
                        completed[i] = true;
                    }
                }
            }

            // for(int i = 0; i < numThreadsCalling; i++) std::cout << completedThreads.at(i) << " "; std::cout << std::endl;
            // for(auto& thread: threadList){
            //     if(thread.joinable()){
            //         thread.join();
            //     }
            //     for(int i = 0; i < numThreadsCalling; i++) std::cout << completedThreads.at(i) << " "; std::cout << std::endl;
            // }
            // for(int i = 0; i < numThreadsCalling; i++) std::cout << completedThreads.at(i) << " "; std::cout << std::endl;

            currentTask->finishExecution();
            {
                std::lock_guard<std::mutex> lock(queueMutex);
                for (const auto& nextTask : currentTask->getNextTasks()) {
                    nextTask->incrementExecutedPreviousTasks();
                    if(nextTask->checkPreviousTasks()) {
                        tasksQueue.push(nextTask);
                        pendingTasks++;
                    }
                }
            }

            pendingTasks--;
            cv.notify_all();
        }
    };

    std::vector<std::thread> workers;
    for (int i = 0; i < numThreads; ++i) {
        workers.emplace_back(worker);
    }

    for (auto& workerThread : workers) {
        if (workerThread.joinable()) {
            workerThread.join();
        }
    }
}*/

struct taskNode {
    std::shared_ptr<Task> task;
    int level = 0;
    int cpWeight = 0;
    int sumWeight = 0;
    int auxOrquestrador = 0;
    int numChild = 0;

    taskNode() = default;
    taskNode(std::shared_ptr<Task> task) : task(task) {};
};

bool Trigger::calculateThreadsDistribution(int numThreads) {
    std::cout << "Calculando a distribuição ideal de threads...\n";

    if(vExtractors.empty()){
        std::cout << "Nenhum extrator foi adicionado ao Trigger.\n";
        return 0;  
    }
    else if(vExtractors[0]->getWeight() != 0){
        std::cout << "A distribuição de threads já foi calculada.\n";
        return 0;
    }

    std::map<std::string, taskNode> taskMap;
    std::queue<std::string> tasksQueue, tasksQueueReverse;

    // Adiciona os extratores à fila de tarefas
    for (const auto& extractor : vExtractors) {
        extractor->setWeight(1);                   // retirar isso depois
        taskMap[extractor->getTaskName()] = taskNode(extractor);
        tasksQueue.push(extractor->getTaskName());
    }

    std::cout << "Iniciando queue 1" << std::endl;
    while (!tasksQueue.empty()) {
        auto& crrTaskNode = taskMap[tasksQueue.front()];
        tasksQueue.pop();
        
        if(crrTaskNode.auxOrquestrador == 1) continue;

        std::cout << "Tarefa atual: " << crrTaskNode.task->getTaskName() << std::endl;
        // Marca a task como visitada
        crrTaskNode.auxOrquestrador = 1;

        const auto& nextTasks = crrTaskNode.task->getNextTasks();

        // Adiciona as próximas tarefas à fila
        if(!nextTasks.empty()){
            for (const auto& nextTask : nextTasks) {
                std::string nextTaskName = nextTask->getTaskName();
                // Se a task não foi visitada, adiciona à fila
                if(taskMap.find(nextTaskName) == taskMap.end()){
                    taskMap[nextTaskName] = taskNode(nextTask); 
                    taskMap[nextTaskName].level = crrTaskNode.level + 1;
                    tasksQueue.push(nextTaskName);
                }
            }
        }
        // Se não tiver próxima tarefa então a task é um Loader
        else{
            tasksQueueReverse.push(crrTaskNode.task->getTaskName());
        }
    }

    std::cout << "Iniciando queue 2" << std::endl;
    while(!tasksQueueReverse.empty()){
        auto& crrNodeTask = taskMap[tasksQueueReverse.front()];
        tasksQueueReverse.pop();

        std::cout << "Tarefa atual: " << crrNodeTask.task->getTaskName() << std::endl;

        int crrBaseWeight = crrNodeTask.task->getBaseWeight(); // TODO: calcular essa valor com a estimativa do peso da task;

        const auto& nextTasks = crrNodeTask.task->getNextTasks();

        // Caso base: Loader
        if(nextTasks.empty()){ 
            crrNodeTask.cpWeight  = crrBaseWeight;
            crrNodeTask.sumWeight = crrBaseWeight;
        }
        else{
            int cpChild = 0, sumChild=0;
            for (const auto& nextTask : nextTasks) {
                auto& child = taskMap[nextTask->getTaskName()];
                
                cpChild   = std::max(cpChild, child.cpWeight);
                sumChild += child.sumWeight;
                crrNodeTask.numChild += 1 + child.numChild;
            }

            crrNodeTask.cpWeight   = cpChild;
            crrNodeTask.sumWeight  += sumChild;
        }

        std::cout << "cpWeight:  " << crrNodeTask.cpWeight
              << "    sumWeight: " << crrNodeTask.sumWeight
              << "    Level: "    << crrNodeTask.level
              << std::endl << std::endl;

        const auto& previousTasks = crrNodeTask.task->getPreviousTasks();

        if(!previousTasks.empty()) {
            for(const auto& pair: previousTasks){
                std::string prevTaskName = pair.first->getTaskName();
                auto& prevTaskNode = taskMap[prevTaskName];

                prevTaskNode.auxOrquestrador++;
                
                if(prevTaskNode.cpWeight != 0) continue;

                const auto& nextTasksPrevTask = prevTaskNode.task->getNextTasks();
                if((int)nextTasksPrevTask.size() + 1 == prevTaskNode.auxOrquestrador){
                    tasksQueueReverse.push(prevTaskName);
                }
            }
        }
    }

    std::cout << "Cálculo concluído.\n";
    return 1;
}

// struct TaskComparator {
//     bool operator()(const std::shared_ptr<Task>& a, const std::shared_ptr<Task>& b) {
//         return (a->getWeight() > b->getWeight()) || (a->getWeight() == b->getWeight() && a->getLevel() < b->getLevel());
//     }
// };
/*
void Trigger::orchestratePipelineMultiThread2(int numThreads) {
    std::priority_queue<std::shared_ptr<Task>, std::vector<std::shared_ptr<Task>>, TaskComparator> tasksQueue;
    std::mutex queueMutex;
    std::condition_variable cv;
    std::atomic<int> pendingTasks{0};
    
    std::cout << "Chamando calculateThreadsDistribution..." << std::endl;
    calculateThreadsDistribution(numThreads);

    {
        std::lock_guard<std::mutex> lock(queueMutex);
        for (const auto& extractor : vExtractors) {
            tasksQueue.push(extractor);
            pendingTasks++;
        }
    }

    auto worker = [&]() {
        while (true) {
            std::shared_ptr<Task> currentTask;
            {
                std::unique_lock<std::mutex> lock(queueMutex);
                cv.wait(lock, [&]() { 
                    return !tasksQueue.empty() || pendingTasks.load() == 0; 
                });

                if (tasksQueue.empty() && pendingTasks.load() == 0) {
                    return;
                }

                currentTask = tasksQueue.top();
                tasksQueue.pop();
            }
            int numThreadsCalling = 5;
            std::vector<int> completedThreads(numThreadsCalling, 0);
            std::vector<std::thread> threadList = currentTask->executeMultiThread(numThreadsCalling, completedThreads);
            for(auto& thread: threadList){
                //std::cout << pair.second.get_id() << std::endl;
                if(thread.joinable()){
                    thread.join();
                }
            }
            currentTask->finishExecution();

            {
                std::lock_guard<std::mutex> lock(queueMutex);
                for (const auto& nextTask : currentTask->getNextTasks()) {
                    nextTask->incrementExecutedPreviousTasks();
                    if(nextTask->checkPreviousTasks()) {
                        tasksQueue.push(nextTask);
                        pendingTasks++;
                    }
                }
            }

            pendingTasks--;
            cv.notify_all();
        }
    };

    std::vector<std::thread> workers;
    for (int i = 0; i < numThreads; ++i) {
        workers.emplace_back(worker);
    }

    for (auto& workerThread : workers) {
        if (workerThread.joinable()) {
            workerThread.join();
        }
    }
}*/

struct ExecGroup {
    std::shared_ptr<Task> task;
    std::shared_ptr<std::vector<std::atomic<bool>>> flags;
    std::vector<std::thread> threads;
    std::vector<bool> joined;
};

void Trigger::orchestratePipelineMultiThread3(int maxThreads) {
    calculateThreadsDistribution(maxThreads);

    std::queue<std::shared_ptr<Task>> tasksQueue;
    for (auto& extr : vExtractors) 
        tasksQueue.push(extr);

    std::vector<ExecGroup> activeGroups;

    int usedThreads = 0;

    // mutex + condvar para orquestrador dormir/acordar
    std::mutex orchestratorMutex;
    std::condition_variable orchestratorCv;

    while (!tasksQueue.empty() || !activeGroups.empty()) {
        // Disparar tarefas quando houver threads disponíveis
        while (!tasksQueue.empty() && usedThreads < maxThreads) {
            auto crrTask = tasksQueue.front();
            tasksQueue.pop();

            // Decide quantas threads essa Task quer usar
            int crrTaskThreadsNum;
            if(crrTask->canBeParallel()) {
                crrTaskThreadsNum = std::min(1, maxThreads - usedThreads);
            } 
            else {
                crrTaskThreadsNum = 1;
            }

            if(crrTaskThreadsNum <= 0) {
                std::cout << "Número de threads inválido para a tarefa: " << crrTask->getTaskName() << std::endl;
                continue;
            }

            auto flags = std::make_shared<std::vector<std::atomic<bool>>>(crrTaskThreadsNum);
            for (auto &f : *flags) f.store(false, std::memory_order_relaxed);

            // o que o executeMultiThread deve fazer
            // cria as threads e as coloca em execução
            // cada thread vai executar a tarefa e, ao final, vai
            // sinaliza conclusão
            // (*flags)[i].store(true, std::memory_order_release);
            // notificar o orquestrador
            // {
            //    std::lock_guard<std::mutex> lk(orchestratorMutex);
            //    orchestratorCv.notify_one();
            // }

            auto threadsList = crrTask->executeMultiThread(crrTaskThreadsNum, (*flags), orchestratorCv, orchestratorMutex);
            std::vector<bool> crrJoined(crrTaskThreadsNum, false);

            // registra o grupo ativo
            activeGroups.push_back(
                ExecGroup{crrTask, flags, std::move(threadsList), std::move(crrJoined)}
            );

            usedThreads += crrTaskThreadsNum;
        }

        for (auto it = activeGroups.begin(); it != activeGroups.end(); ) {
            auto& group = *it;

            // percorre cada sub‑thread do grupo
            for (size_t i = 0; i < group.threads.size(); ++i) {
                // se terminou e ainda não chamou join()
                if (!group.joined[i] && group.flags->at(i).load(std::memory_order_acquire)) {
                    if (group.threads[i].joinable()) group.threads[i].join();

                    usedThreads--;          // libera 1 slot
                    group.joined[i] = true; // marca como joined
                }
            }

            bool allJoined = true;
            for (auto joined_ith : group.joined) {
                if (!joined_ith) {
                    allJoined = false;
                    break;
                }
            }

            if (allJoined) {
                // finaliza a Task
                group.task->finishExecution();

                // enfileira nextTasks (leva em conta dependências)
                for (auto& nxt : group.task->getNextTasks()) {
                    nxt->incrementExecutedPreviousTasks();
                    if (nxt->checkPreviousTasks())
                        tasksQueue.push(nxt);
                }

                // remove do vector de grupos ativos
                it = activeGroups.erase(it);
            } 
            else {
                ++it;
            }
        }

        // Aguarda notificação
        if (tasksQueue.empty() && !activeGroups.empty()) {
            std::unique_lock<std::mutex> lock(orchestratorMutex);
            // acorda sempre que:
            //  - chegar nova Task em tasksQueue, ou
            //  - algum grupo terminar e notificar
            orchestratorCv.wait(lock, [&](){
                bool anyThreadFinished = false;
                for (auto& group : activeGroups) {
                    for (size_t i = 0; i < group.threads.size(); ++i) {
                        if (!group.joined[i] && group.flags->at(i).load(std::memory_order_acquire)) {
                            anyThreadFinished = true;
                            break;
                        }
                    }
                    if (anyThreadFinished) break;
                }
                
                return !tasksQueue.empty() || anyThreadFinished;
            });
        }
    }
}


// ##################################################################################################
// ##################################################################################################
// Implementação de RequestTrigger
void RequestTrigger::start(int numThreads) {
    if(numThreads > 1) {
        std::cout << "Executando a pipeline com " << numThreads << " threads.\n";
        orchestratePipelineMultiThread3(numThreads);
    } 

    else {
        std::cout << "Executando a pipeline em uma única thread.\n";
        orchestratePipelineMonoThread();
    }
}

// ##################################################################################################
// ##################################################################################################
// Implementação de TimerTrigger
TimerTrigger::TimerTrigger(unsigned int intervaloMs)
    : intervalo(intervaloMs), stopFlag(false) {}

TimerTrigger::~TimerTrigger() {
    stop();
}

void TimerTrigger::start(int numThreads) {
    stopFlag = false;
    timerThread = std::thread([this]() {
        while (!stopFlag) {
            orchestratePipelineMonoThread();
            std::this_thread::sleep_for(std::chrono::milliseconds(intervalo));
        }
    });
}

void TimerTrigger::stop() {
    stopFlag = true;
    if (timerThread.joinable()) {
        timerThread.join();
    }
}
