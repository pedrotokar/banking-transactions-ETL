// PipelineManager.cpp
#include "pipelinemanager.hpp"
#include <iostream> // Para logs/debug, remova se não for necessário

// Construtor
PipelineManager::PipelineManager(ServerTrigger& trigger, DataFrame empty_df_template, size_t df_trigger_size)
    : pipeline_trigger(trigger), // Inicializa a referência
      df_trigger_size(df_trigger_size), // Inicializa o tamanho do trigger
      running(false) { // Atomic bool inicializado como false
    // Armazena o template do dataframe vazio para resetar o waiting_dataframe
    // e para inicializar o running_dataframe com a estrutura correta.
    // Isso assume que DataFrame tem um construtor de cópia ou operador de atribuição.
    waiting_dataframe = empty_df_template;
    running_dataframe = empty_df_template; // Para garantir que tenha a estrutura correta ao ser usado pela primeira vez
    // std::cout << "PipelineManager: Constructed." << std::endl;
}

// Destrutor
PipelineManager::~PipelineManager() {
    if (running.load()) {
        stop(); // Garante que a thread pare e seja juntada
    }
    // std::cout << "PipelineManager: Destroyed." << std::endl;
}

// Método para o servidor gRPC (ou outro produtor) submeter um batch.
void PipelineManager::submitDataBatch(DataBatch batch) {
    if (!running.load()) {
        // Opcional: Lidar com submissão de dados após o PipelineManager ter sido parado
        // std::cerr << "PipelineManager: Warning! Data submitted after stop(): batch of " << batch.size() << " rows discarded." << std::endl;
        return;
    }
    {
        std::lock_guard<std::mutex> lock(queue_mutex);
        batchs_queue.push(std::move(batch)); // Usa std::move para eficiência
    }
    queue_cv.notify_one(); // Notifica a worker_thread que há novos dados
}

// Inicia a thread de processamento interna do PipelineManager.
void PipelineManager::start() {
    if (running.load()) {
        // std::cout << "PipelineManager: Already running." << std::endl;
        return;
    }
    running.store(true);
    worker_thread = std::thread(&PipelineManager::processingLoop, this);
    // std::cout << "PipelineManager: Worker thread started." << std::endl;
}

// Sinaliza para a thread de processamento interna para parar e aguarda sua finalização.
void PipelineManager::stop() {
    if (!running.load()) {
        // std::cout << "PipelineManager: Already stopped." << std::endl;
        return;
    }
    running.store(false);
    queue_cv.notify_one(); // Acorda a thread se estiver esperando na CV para que ela possa verificar 'running'

    if (worker_thread.joinable()) {
        worker_thread.join();
    }
    // std::cout << "PipelineManager: Worker thread stopped and joined." << std::endl;
}

// Loop principal executado pela thread interna do PipelineManager.
void PipelineManager::processingLoop() {
    // Mantém uma cópia local do template vazio para resetar o waiting_dataframe.
    // É crucial que o 'empty_dataframe' passado ao construtor seja de fato
    // um template válido e vazio.
    DataFrame local_empty_template = waiting_dataframe; // Captura o estado inicial (estrutura)

    while (running.load()) {
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
                // PipelineManager rodando, mas fila vazia (pode acontecer se notificado para parar e running ainda é true)
                // ou em caso de spurious wakeup (embora o predicado da wait deva cobrir isso).
                continue;
            }

            current_data_batch = std::move(batchs_queue.front());
            batchs_queue.pop();
        } // Mutex da fila (queue_mutex) é liberado aqui

        // Adiciona os dados do batch recebido ao waiting_dataframe
        if (!current_data_batch.empty()) {
            // Assumindo que DataFrame tem um método para adicionar linhas.
            // Se houver um método mais eficiente como `appendBatch(const DataBatch&)`, use-o.
            for (const auto& row : current_data_batch) { // Supondo que DataBatch é iterável e contém VarRow
                waiting_dataframe.addRow(row);
            }
            // std::cout << "PipelineManager: Added " << current_data_batch.size() << " rows. Waiting DF size: " << waiting_dataframe.rowCount() << std::endl;
        }

        // Lógica para decidir quando disparar a pipeline
        bool should_trigger_pipeline = false;

        // Verifica se a pipeline pode ser acionada
        if (!pipeline_trigger.isBusy() && waiting_dataframe.size() > 0 && waiting_dataframe.size() > df_trigger_size) {
            if (!running.load()) {
                should_trigger_pipeline = true;
            }
        }

        if (should_trigger_pipeline) {
            // Prepara os dados para a pipeline.
            std::swap(waiting_dataframe, running_dataframe);

            waiting_dataframe = local_empty_template; // Reseta waiting_dataframe para a estrutura vazia.

            // std::cout << "PipelineManager: Triggering pipeline with " << running_dataframe.rowCount() << " rows." << std::endl;
            pipeline_trigger.start(8, running_dataframe); // TODO: alterar 8 para o numero de threads escolhido
            // Após a execução (se síncrona), running_dataframe pode ser explicitamente limpo
            // ou simplesmente será sobrescrito na próxima troca.
            // Se for assíncrono, certifique-se de que running_dataframe não seja modificado
            // até que a pipeline termine de usá-lo (isBusy() deve cobrir isso).
            // Se 'executePipeline' for síncrona, o 'isBusy()' deve se tornar 'true'
            // no início de 'executePipeline' e 'false' no final.
        }
    }
    // std::cout << "PipelineManager: Processing loop finished." << std::endl;

    // Processamento final de quaisquer dados restantes na fila se o manager foi parado
    // e o loop principal terminou antes de processar tudo.
    // (Alternativamente, a lógica de !running.load() dentro do loop já tenta tratar isso)
    if (!batchs_queue.empty() && !pipeline_trigger.isBusy()) {
        // std::cout << "PipelineManager: Processing remaining batches after loop..." << std::endl;
        // Esta lógica pode ser complexa e precisa garantir que a pipeline seja chamada
        // se houver dados, mesmo que 'running' seja false.
        // Por simplicidade, o loop principal tenta esvaziar a fila antes de sair se running=false.
        // Se a pipeline estiver ocupada, esses dados restantes podem não ser processados.
        // Considere se essa situação precisa de tratamento mais robusto.
    }
}