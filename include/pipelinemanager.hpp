#ifndef PIPELINEMANAGER_HPP
#define PIPELINEMANAGER_HPP

#include <vector>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <memory>

#include "types.h" // Para DataBatch, VarRow, etc.
#include "dataframe.h" // Para DataFrame
#include "trigger.hpp" // Para IPipelineExecutor

class PipelineManager {
public:
    // Construtor:
    // - pipeline_trigger: Uma referência (ou ponteiro inteligente) para a
    //                      implementação concreta da pipeline.
    // - empty_dataframe: Um dataframe vazio contendo a estrutura que será utilizada para acumular os
    //                    dados recebidos antes de serem enviados para a pipeline executar.
    // - df_trigger_size: Número mínimo de linhas no dataframe agregado
    //                    para considerar disparar a pipeline (exemplo de critério).

    PipelineManager(ServerTrigger& pipeline_trigger, 
                    DataFrame empty_dataframe, 
                    size_t pipelineNumThreads=8, 
                    size_t df_trigger_size=1);
    ~PipelineManager();

    // Impede cópia e atribuição para simplificar o gerenciamento da thread e mutexes.
    PipelineManager(const PipelineManager&) = delete;
    PipelineManager& operator=(const PipelineManager&) = delete;
    PipelineManager(PipelineManager&&) = delete;
    PipelineManager& operator=(PipelineManager&&) = delete;

    // Método para o servidor gRPC submeter um batch.
    // Este é o principal ponto de entrada de dados no PipelineManager. É thread-safe.
    void submitDataBatch(DataBatch batch);

    // Inicia a thread de processamento interna do PipelineManager.
    void start();

    // Sinaliza para a thread de processamento interna para parar e aguarda sua finalização.
    void stop();

private:
    // Loop principal executado pela thread interna do PipelineManager.
    void processingLoop();

    ServerTrigger& pipeline_trigger; // Referência ao trigger da pipeline concreta

    std::queue<DataBatch> batchs_queue; // Fila para os batches que chegam
    std::mutex queue_mutex;             // Mutex para proteger batchs_queue
    std::condition_variable queue_cv;   // CV para sinalizar novos dados na fila

    DataFrame waiting_dataframe, running_dataframe; // Onde os batches são acumulados

    std::thread worker_thread;      // A thread que executa processingLoop()
    std::thread orchestratorThread; // A thread que executa a pipeline (orquestrador)

    std::atomic<bool> running{false};  // Sinalizador para controlar o loop da thread

    size_t df_trigger_size; // Critério para disparar a pipeline
    size_t pipelineNumThreads; // Número de threads que a pipeline pode usar
};

#endif // PIPELINEMANAGER_HPP