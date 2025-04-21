#ifndef TRIGGER_HPP
#define TRIGGER_HPP

#include <vector>
#include <map>
#include <string>
#include <memory>
#include <atomic>
#include <thread>
#include "task.h"  // Inclui a definição de Task e Transformer

struct taskNode {
    std::shared_ptr<Task> task;
    int level = 0;
    int cpWeight = 0;
    int sumWeight = 0;
    double finalWeight = 0.0;
    int auxOrquestrador = 0;
    int numChild = 0;

    taskNode() = default;
    taskNode(std::shared_ptr<Task> task) : task(task) {};
};

// Classe abstrata Trigger
class Trigger {
public:
    virtual ~Trigger();
    
    // Define os extratores (ou tasks de início) da pipeline.
    // Estes serão os pontos de entrada para a execução.
    void setExtractors(const std::vector<std::shared_ptr<Task>>& vExtractors);
    void addExtractor(std::shared_ptr<Task> extractor);
    void clearExtractors();
    
    // Método virtual para iniciar o trigger
    virtual void start(int numThreads) = 0;
    
    // Méto para alterar os parâmetros alpha e beta
    // Esses parâmetros são usados para calcular a distribuição de threads
    double getAlpha() const;
    void setAlpha(double alpha);
protected:
    // Vetor de tasks que serão os pontos de partida da pipeline
    std::vector<std::shared_ptr<Task>> vExtractors;
    // Mapa de tarefas com seus respectivos nós
    std::map<std::string, taskNode> taskMap;
    // Variáveis para controle da heurística de distribuição de threads
    double alpha=2.0/3.0;

    // Métodos internos para orquestração da pipeline
    void orchestratePipelineMonoThread();
    void orchestratePipelineMultiThread(int numThreads);
    // Novos métodos para orquestração com múltiplas threads
    void orchestratePipelineMultiThread2(int numThreads);
    void orchestratePipelineMultiThread3(int numThreads);
    bool calculateThreadsDistribution(int numThreads);
};

// Trigger que executa a pipeline apenas uma vez
class RequestTrigger : public Trigger {
public:
    void start(int numThreads = 1) override;
};

// Trigger que executa a pipeline repetidamente a cada intervalo definido
class TimerTrigger : public Trigger {
public:
    // O construtor recebe o intervalo em milissegundos
    TimerTrigger(unsigned int intervaloMs);
    ~TimerTrigger();
    
    void start(int numThreads = 1) override;
    void stop();
    
private:
    unsigned int intervalo;          // Intervalo entre execuções (em milissegundos)
    std::atomic<bool> stopFlag;      // Sinaliza quando deve parar o loop
    std::thread timerThread;         // Thread responsável pela execução periódica
};

#endif // TRIGGER_HPP