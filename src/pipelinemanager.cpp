// PipelineManager.cpp
#include "pipelinemanager.hpp"
#include <iostream> // Para logs/debug, remova se não for necessário

// Construtor
PipelineManager::PipelineManager(ServerTrigger& trigger, DataFrame empty_df_template, size_t df_trigger_size)
    : pipeline_trigger(trigger),        // Inicializa a referência
      df_trigger_size(df_trigger_size), // Inicializa o tamanho do trigger
      running(false) {                  // Atomic bool inicializado como false
    
    if(empty_df_template.size() > 0) {
        std::cerr << "PipelineManager: Error - empty_df_template should be empty." << std::endl;
        throw std::invalid_argument("empty_df_template is not empty.");
    }

    waiting_dataframe = empty_df_template;
    running_dataframe = empty_df_template;
    std::cout << "PipelineManager: Constructed." << std::endl;
}

// Destrutor
PipelineManager::~PipelineManager() {
    if (running.load()) {
        stop(); // Garante que a thread pare e seja juntada
    }
    std::cout << "PipelineManager: Destroyed." << std::endl;
}

// Método para o servidor gRPC (ou outro produtor) submeter um batch.
void PipelineManager::submitDataBatch(DataBatch batch) {
    if (!running.load()) {
        std::cout << "PipelineManager: Not running, cannot submit data batch." << std::endl;
        return;
    }
    std::cout << "PipelineManager: Submitting data batch." << std::endl;
    {
        std::lock_guard<std::mutex> lock(queue_mutex);
        batchs_queue.push(std::move(batch));
    }
    queue_cv.notify_one(); // Notifica a worker_thread que há novos dados
    std::cout << "PipelineManager: batchs_queue.size() = " << batchs_queue.size() << std::endl;
}

// Inicia a thread de processamento interna do PipelineManager.
void PipelineManager::start() {
    if (running.load()) {
        std::cout << "PipelineManager: Already running." << std::endl;
        return;
    }
    running.store(true);
    worker_thread = std::thread(&PipelineManager::processingLoop, this);
    std::cout << "PipelineManager: Worker thread started." << std::endl;
}

// Sinaliza para a thread de processamento interna para parar e aguarda sua finalização.
void PipelineManager::stop() {
    if (!running.load()) {
        std::cout << "PipelineManager: Already stopped." << std::endl;
        return;
    }
    running.store(false);
    queue_cv.notify_one(); // Acorda a thread se estiver esperando na CV para que ela possa verificar 'running'

    if (worker_thread.joinable()) {
        worker_thread.join();
    }
    if (orchestratorThread.joinable()) {
        orchestratorThread.join(); // Garante que a thread da pipeline também seja juntada
    }
    std::cout << "PipelineManager: Worker thread stopped and joined." << std::endl;
}

// Loop principal executado pela thread interna do PipelineManager.
void PipelineManager::processingLoop() {
    // Mantém uma cópia local do template vazio para resetar o waiting_dataframe.
    DataFrame local_empty_template = waiting_dataframe; // Captura o estado inicial (estrutura)

    while (running.load()) {
        // std::cout << "PipelineManager: processingLoop loop" << std::endl;
        DataBatch current_data_batch;
        {
            std::unique_lock<std::mutex> lock(queue_mutex);
            // Espera até que a fila não esteja vazia OU o PipelineManager não esteja mais rodando
            queue_cv.wait(lock, [this] {
                return !batchs_queue.empty() || !running.load();
            });

            if (!running.load() && batchs_queue.empty()) {
                // PipelineManager parando e fila vazia, sair do loop
                break;
            }

            if (batchs_queue.empty()) {
                continue;
            }

            current_data_batch = std::move(batchs_queue.front());
            batchs_queue.pop();
        }

        // Adiciona os dados do batch recebido ao waiting_dataframe
        if (!current_data_batch.empty()) {
            std::cout << "PipelineManager: Adding batch of size " << current_data_batch.size() << std::endl;
            std::cout << "PipelineManager: waiting_dataframe.size()" << waiting_dataframe.size() << std::endl;
            for (const auto& row : current_data_batch) {
                waiting_dataframe.addRow(row);
            }
            std::cout << "PipelineManager: waiting_dataframe.size()" << waiting_dataframe.size() << std::endl;
        }

        // Lógica para decidir quando disparar a pipeline
        bool flag_trigger_pipeline = !pipeline_trigger.isBusy() && 
                                      waiting_dataframe.size() > 0 && 
                                      waiting_dataframe.size() > df_trigger_size;
        
        if (flag_trigger_pipeline) {
            if(orchestratorThread.joinable()) {
                std::cout << "PipelineManager: Waiting for orchestrator thread to finish." << std::endl;
                orchestratorThread.join();
            }

            // Prepara os dados para a pipeline.
            std::swap(waiting_dataframe, running_dataframe);

            waiting_dataframe = local_empty_template; // Reseta waiting_dataframe para a estrutura vazia.

            std::cout << "PipelineManager: Triggering pipeline with " << running_dataframe.size() << " rows." << std::endl;
            auto start = std::chrono::high_resolution_clock::now();
            orchestratorThread = pipeline_trigger.start(8, std::make_shared<DataFrame>(running_dataframe)); // TODO: alterar 8 para o numero de threads escolhido
            auto end = std::chrono::high_resolution_clock::now();
            std::chrono::duration<double, std::milli> elapsed = end - start;
            std::cout << "Tempo de execução da pipeline: " << elapsed.count() << " milissegundos.\n";
            // Após a execução (se síncrona), running_dataframe pode ser explicitamente limpo
            // ou simplesmente será sobrescrito na próxima troca.
            // Se for assíncrono, certifique-se de que running_dataframe não seja modificado
            // até que a pipeline termine de usá-lo (isBusy() deve cobrir isso).
            // Se 'executePipeline' for síncrona, o 'isBusy()' deve se tornar 'true'
            // no início de 'executePipeline' e 'false' no final.
        }
    }
    std::cout << "PipelineManager: Processing loop finished." << std::endl;
}
