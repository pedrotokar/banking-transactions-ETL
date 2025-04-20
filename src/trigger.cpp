#include "trigger.hpp"
#include <queue>
#include <thread>
#include <chrono>
#include <atomic>
#include <condition_variable>
#include <iostream>

// Destrutor padrão
Trigger::~Trigger() = default;

// Implementação dos métodos de Trigger
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

            std::vector<std::thread> threadList = currentTask->executeMultiThread();
            for(auto& workingThread: threadList){
                if(workingThread.joinable()){
                    workingThread.join();
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
}

// Implementação de RequestTrigger
void RequestTrigger::start(int numThreads) {
    if(numThreads > 1) {
        std::cout << "Executando a pipeline com " << numThreads << " threads.\n";
        orchestratePipelineMultiThread(numThreads);
    } 
    else {
        std::cout << "Executando a pipeline em uma única thread.\n";
        orchestratePipelineMonoThread();
    }
}

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
