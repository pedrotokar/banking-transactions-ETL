#ifndef TRIGGER_HPP
#define TRIGGER_HPP

#include <vector>
#include <queue>
#include <thread>
#include <chrono>
#include <atomic>
#include <condition_variable>
#include <memory>
#include <iostream>
#include "task.h"  // Inclui a definição de Task e Transformer

// Classe abstrata Trigger
class Trigger {
public:
    virtual ~Trigger() = default;
    
    // Define os extratores (ou tasks de início) da pipeline.
    // Estes serão os pontos de entrada para a execução.
    void setExtractors(const std::vector<std::shared_ptr<Task>>& vExtractors) {
        this->vExtractors = vExtractors;
    }
    void addExtractor(std::shared_ptr<Task> extractor) {
        vExtractors.push_back(extractor);
    }
    void clearExtractors() {
        vExtractors.clear();
    }
    
    // Método virtual para iniciar o trigger
    virtual void start(int numThreads) = 0;
    
protected:
    // Vetor de tasks que serão os pontos de partida da pipeline
    std::vector<std::shared_ptr<Task>> vExtractors;

    // Método interno responsável por orquestrar a execução da pipeline
    void orchestratePipelineMonoThread() {
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
            task->execute();
            std::cout << "(2)Tamanho do nextTasks da task atual: " << task->getNextTasks().size() << std::endl;

            // Adiciona as próximas tarefas à fila
            const auto& nextTasks = task->getNextTasks();
            for (const auto& nextTask : nextTasks) {
                tasksQueue.push(nextTask);
            }
        }
        std::cout << "Pipeline concluída.\n";
    }

    // Método interno responsável por orquestrar a execução da pipeline em múltiplas threads.
    // numThreads define o número fixo de threads que serão utilizadas.
    void orchestratePipelineMultiThread(int numThreads) {
        std::queue<std::shared_ptr<Task>> tasksQueue;
        std::mutex queueMutex;
        std::condition_variable cv;
        // Contador de tasks pendentes: inclui tasks na fila e em processamento.
        std::atomic<int> pendingTasks{0};

        // Insere os extratores na fila e atualiza o contador de tasks pendentes.
        {
            std::lock_guard<std::mutex> lock(queueMutex);
            for (const auto& extractor : vExtractors) {
                tasksQueue.push(extractor);
                pendingTasks++;
            }
        }

        // Função que define o comportamento dos worker threads.
        auto worker = [&]() {
            while (true) {
                std::shared_ptr<Task> currentTask;
                {
                    // Aguarda até que haja uma task na fila ou até que não haja nenhuma task pendente.
                    std::unique_lock<std::mutex> lock(queueMutex);
                    cv.wait(lock, [&]() { 
                        return !tasksQueue.empty() || pendingTasks.load() == 0; 
                    });

                    // Se a fila estiver vazia e não houver tasks pendentes, finaliza o worker.
                    if (tasksQueue.empty() && pendingTasks.load() == 0) {
                        return;
                    }

                    // Retira uma task da fila.
                    currentTask = tasksQueue.front();
                    tasksQueue.pop();
                }

                // Executa a task fora da região crítica.
                currentTask->execute();

                // Após a execução, adiciona as próximas tasks (se houver) na fila.
                {
                    std::lock_guard<std::mutex> lock(queueMutex);
                    for (const auto& nextTask : currentTask->getNextTasks()) {
                        tasksQueue.push(nextTask);
                        pendingTasks++;
                    }
                }

                // Marca a task atual como concluída e notifica os outros threads.
                pendingTasks--;
                cv.notify_all();
            }
        };

        // Cria e inicia o número fixo de threads de trabalho.
        std::vector<std::thread> workers;
        for (int i = 0; i < numThreads; ++i) {
            workers.emplace_back(worker);
        }

        // Aguarda que todos os threads finalizem.
        for (auto& workerThread : workers) {
            if (workerThread.joinable()) {
                workerThread.join();
            }
        }
    }

};

// Trigger que executa a pipeline apenas uma vez
class RequestTrigger : public Trigger {
public:
    void start(int numThreads=1) override {
        // Executa a pipeline uma única vez
        if(numThreads > 1) {
            std::cout << "Executando a pipeline com " << numThreads << " threads.\n";
            orchestratePipelineMultiThread(numThreads);
        } 
        else {
            std::cout << "Executando a pipeline em uma única thread.\n";
            orchestratePipelineMonoThread();
        }
    }
};

// Trigger que executa a pipeline repetidamente a cada intervalo definido
class TimerTrigger : public Trigger {
public:
    // O construtor recebe o intervalo em milissegundos
    TimerTrigger(unsigned int intervaloMs)
        : intervalo(intervaloMs), stopFlag(false) {}

    ~TimerTrigger() {
        stop();
    }
    
    void start(int numThreads=1) override {
        stopFlag = false;
        // Cria uma thread que roda o trigger em loop até que seja solicitado parar
        timerThread = std::thread([this]() {
            while (!stopFlag) {
                orchestratePipelineMonoThread();
                std::this_thread::sleep_for(std::chrono::milliseconds(intervalo));
            }
        });
    }
    
    // Permite parar a execução do TimerTrigger
    void stop() {
        stopFlag = true;
        if (timerThread.joinable()) {
            timerThread.join();
        }
    }
    
private:
    unsigned int intervalo;          // Intervalo entre execuções (em milissegundos)
    std::atomic<bool> stopFlag;      // Sinaliza quando deve parar o loop
    std::thread timerThread;         // Thread responsável pela execução periódica
};

#endif // TRIGGER_HPP