#include "dataframe.h"
#include "datarepository.h"
#include "task.h"
#include "trigger.hpp"

#include <iostream>
#include <vector>
#include <any>
#include <unordered_map>


using namespace std;

class SumAgeTransformerTestInterface : public Transformer {
public:
    void transform(std::vector<DataFrame*>& outputs,
                   const std::vector<std::pair<std::vector<int>, DataFrame*>>& inputs) override
    {
        if (inputs.empty()) {
            std::cout << "[SumAgeTransformer] Nenhum DataFrame na entrada.\n";
            return;
        }

        DataFrame* df = inputs[0].second;
        if (!df) {
            std::cout << "[SumAgeTransformer] DataFrame nulo.\n";
            return;
        }

        // Procura a coluna "age"
        int ageColIndex = -1;
        for (size_t i = 0; i < 3; i++) {
            auto col = df->getColumn(i);
            if (col->getIdentifier() == "age") {
                ageColIndex = static_cast<int>(i);
                break;
            }
        }

        if (ageColIndex < 0) {
            std::cout << "[SumAgeTransformer] Coluna 'age' não encontrada.\n";
            return;
        }

        // Soma todos os valores int na coluna "age"
        int sum = 0;
        auto ageCol = df->getColumn(ageColIndex);
        for (auto i : inputs.at(0).first) {
            sum += std::stoi(ageCol->getValue(i));
        }

        //Adiciona ao DataFrame de saída a soma
        DataFrame* outDf = outputs.at(0);
//        std::cout << typeid(typeof(*(outDf->columns.at(0)))).name() << std::endl;
        std::vector<any> row {sum};
        outDf->addRow(row);

        std::cout << "[SumAgeTransformer] Soma da coluna 'age': " << sum << "\n";
    }
};

class SumAgeTransformerTestInterface_nosense : public Transformer {
    public:
        void transform(std::vector<DataFrame*>& outputs,
                        const std::vector<std::pair<std::vector<int>, DataFrame*>>& inputs) override
        {
            if (inputs.empty()) {
                std::cout << "[SumAgeTransformerTestInterface_nosense] Nenhum DataFrame na entrada.\n";
                return;
            }

            DataFrame* df = inputs[0].second;
            if (!df) {
                std::cout << "[SumAgeTransformerTestInterface_nosense] DataFrame nulo.\n";
                return;
            }

            // Procura a coluna "age"
            int ageColIndex = -1;
            for (size_t i = 0; i < 3; i++) {
                auto col = df->getColumn(i);
                if (col->getIdentifier() == "age") {
                    ageColIndex = static_cast<int>(i);
                    break;
                }
            }

            if (ageColIndex < 0) {
                std::cout << "[SumAgeTransformerTestInterface_nosense] Coluna 'age' não encontrada.\n";
                return;
            }

            // Soma todos os f(age_i) = #{x: x<= 1e6 e x primo e x=k*age_i-1 pra k natural} para cada i na coluna "age"
            int sum = 0;
            auto ageCol = df->getColumn(ageColIndex);
            for (size_t i = 0; i < ageCol->size(); i++) {
                int aux = std::stoi(ageCol->getValue(i));

                for(int j = aux; j < (int)1e6; j+=aux) {
                    bool prime = true;

                    for(int k = 2; k*k <= (j-1); k++) {
                        if((j-1) % k == 0) {
                            prime = false;
                            break;
                        }
                    }

                    if (prime) {
                        sum += 1;
                    }
                }
            }

            //Adiciona ao DataFrame de saída a soma
            DataFrame* outDf = outputs.at(0);
    //        std::cout << typeid(typeof(*(outDf->columns.at(0)))).name() << std::endl;
            std::vector<any> row {sum};
            outDf->addRow(row);

            std::cout << "[SumAgeTransformer] Soma da coluna 'age': " << sum << "\n";
        }
    };

class DuplicateDFTransformer : public Transformer {
public:
    void transform(std::vector<DataFrame*>& outputs,
                   const std::vector<std::pair<std::vector<int>, DataFrame*>>& inputs) override
    {
        if (inputs.empty()) {
            std::cout << "[DuplicateDFTransformer] Nenhum DataFrame na entrada.\n";
            return;
        }

        DataFrame* df = inputs[0].second;
        if (!df) {
            std::cout << "[DuplicateDFTransformer] DataFrame nulo.\n";
            return;
        }

        //if (salaryColIndex < 0) {
        //    std::cout << "[DoubleSalaryTransformer] Coluna 'salary' não encontrada.\n";
        //    return;
        //}
        auto outDf = outputs.at(0);
        for (auto index : inputs[0].first) {
            std::vector<std::any> row;
            auto age = df->getElement<int>(index, 0); row.push_back(age);
            auto name = df->getElement<std::string>(index, 1); row.push_back(name);
            auto salary = df->getElement<double>(index, 2); row.push_back(salary);
            auto extraInt = df->getElement<int>(index, 3); row.push_back(extraInt);
            outDf->addRow(row);
            outDf->addRow(row);
        }

        std::cout << "[DuplicateDFTransformer] Concluído: dataframe foi dobrado\n";
    }
};

void teste2() {
    auto ageColumn = std::make_shared<Column<int>>("age", 0, 0);
    ageColumn->addValue(25);
    ageColumn->addValue(30);
    ageColumn->addValue(45);

    auto nameColumn = std::make_shared<Column<std::string>>("name", 1, "");
    nameColumn->addValue("Ana");
    nameColumn->addValue("Bruno");
    nameColumn->addValue("Carla");

        auto salaryColumn = std::make_shared<Column<double>>("salary", 2, -1);
    salaryColumn->addValue(5000.42);
    salaryColumn->addValue(7500.42);
    salaryColumn->addValue(9000.42);

    auto extraIntColumn = std::make_shared<Column<int>>("codigo", 3, -100);
    extraIntColumn->addValue(101);
    extraIntColumn->addValue(102);
    extraIntColumn->addValue(103);
    extraIntColumn->addValue(103);

    DataFrame df;
    df.addColumn(ageColumn);
    df.addColumn(nameColumn);
    df.addColumn(salaryColumn);

    cout << "DataFrame before adding a new row:\n" << df.toString() << endl;


    vector<any> row {42, nullptr, 5000.0};

    df.addRow(row);

    cout << "DataFrame after adding a new row:\n" << df.toString() << endl;

    df.addColumn(extraIntColumn);
    cout << "DataFrame after adding a new column:\n" << df.toString() << endl;

    vector<any> row2 {42, string("John"), 5000.0, nullptr};

    df.addRow(row2);
    cout << "DataFrame after adding a new row:\n" << df.toString() << endl;

    cout << "Testando função de adicionar row de strings com conversão automatica" << endl;
    // A operação tem que ser por linha pra garantir que o registros estejam corretos na hora de paralelizar
    // Se paralelizar pela leitura da coluna pode acontecer da ordem não ser correta
    // Vai evitar criar um mutex pra cada coluna
    // Assumo que o maior gargalo seja quebrar as linhas no separador
    // Assumo que os tipos da coluna são 100% consistentes

    vector<string> row3 {"42", "John", "5000.0", "123"};
    df.addRow(row3);
    cout << "DataFrame after adding a new row with strings:\n" << df.toString() << endl;
}

DataFrame* buildDFteste3() {
    //Dataframe inicial para testes
    DataFrame* df = new DataFrame();
    
    auto ageColumn = std::make_shared<Column<int>>("age", 0, 0);
    auto nameColumn = std::make_shared<Column<std::string>>("name", 1, "");
    auto salaryColumn = std::make_shared<Column<double>>("salary", 2, -1);
    auto extraIntColumn = std::make_shared<Column<int>>("codigo", 3, -100);

    df->addColumn(ageColumn);
    df->addColumn(nameColumn);
    df->addColumn(salaryColumn);
    df->addColumn(extraIntColumn);

    return df;
}

void teste3() {
    DataFrame* df = buildDFteste3();

    FileRepository* repository = new FileRepository("data/teste_1.csv", ",", true);

    auto e0 = std::make_shared<Extractor>();

    e0->addOutput(df);
    e0->addRepo(repository);

    e0->execute();

    DataFrame* dfOut0 = buildDFteste3();

    DataFrame* dfOut1 = buildDFteste3();

    DataFrame* dfOut2 = new DataFrame();
    auto ageSumColumn = std::make_shared<Column<int>>("ageSum", 0, -1);
    dfOut2->addColumn(ageSumColumn);

    cout << "Initial dataframe:\n" << df->toString() << endl;

    cout << "Output dataframe t1:\n" << dfOut1->toString() << endl;

    cout << "Output dataframe t2:\n" << dfOut2->toString() << endl;

    //t1 e t2 são dois tratadores para teste. t0 é um mock, representadno uma etapa já completada da pipeline
    auto t0 = std::make_shared<DuplicateDFTransformer>();
    auto t1 = std::make_shared<DuplicateDFTransformer>();
    auto t2 = std::make_shared<SumAgeTransformerTestInterface>();
    
    //Antes de fazer as conexões tenho que add output em todos. O usuário que faz isso.
    t0->addOutput(dfOut0);
    t1->addOutput(dfOut1);
    t2->addOutput(dfOut2);

    e0->addNext(t0);
    cout << "Tamanho do nextTasks do e0: " << e0->getNextTasks().size() << endl;
    cout << "Tamanho do previousTasks do t0: " << t0->getPreviousTasks().size() << endl;
    //T0 é um mock - representa um tratador que já foi completo anteriormente
    //Após adicionar as especificações de output, o usuário deve usar addNext pra construir o grafo. Talvez eu troque pro addPrevious, mas a ideia segue sendo a mesma.s
    t0->addNext(t1);
    cout << "Tamanho do nextTasks do t1: " << t1->getNextTasks().size() << endl;
    cout << "Tamanho do previousTasks do t2: " << t2->getPreviousTasks().size() << endl;
    t1->addNext(t2);

    auto l0 = std::make_shared<Loader>();
    t1->addNext(l0);

    cout << "Tamanho do nextTasks do t1: " << t1->getNextTasks().size() << endl;
    cout << "Tamanho do previousTasks do t2: " << t2->getPreviousTasks().size() << endl;

    //Com isso pronto, em teoria o orquestrador, depois de navegar na pipeline e etc, só teria que chamar esse execute pra um tratador cujos anteriores estivessem completos já.
    //Esse inputs é inutil agora, só não mudei a assinatura da função ainda. No futuro deve ser trocado por algo do tipo "threadCount" ou coisa assim
    t0->execute();
    cout << "Output dataframe t0 after operation:\n" << dfOut0->toString() << endl;
    
    t1->execute();
    cout << "Output dataframe t1 after operation:\n" << dfOut1->toString() << endl;
    
    // FileRepository* new_repository = new FileRepository("data/result_1_teste_3.csv", ",", true);
    l0->addRepo(repository);
    l0->execute();

    t2->execute();
    cout << "Output dataframe t2 after operation:\n" << dfOut2->toString() << endl;
}

void teste4() {
    DataFrame df;
    // ===== Agora podemos adicionar colunas de forma muito mais simples!
    // ==================================================================

    // df.addColumn(std::make_shared<Column<std::string>>("STR", 0, ""));
    // df.addColumn(std::make_shared<Column<int>>("INT", 1, -1));
    // df.addColumn(std::make_shared<Column<double>>("DOUBLE", 2, -1.0));

    // df.addColumn<std::string> ("STR", 0);
    // df.addColumn<int>         ("INT", 1);
    // df.addColumn<double>      ("DOUBLE", 2, -1.0);

    df.addColumn<std::string> ("STR"); // não preciso passar a posição, usa nulo padrão global definido em types.h
    df.addColumn<int>         ("INT", 1, -1); // mas se quiser passar a posição, precisa passar o valor padrão ;(
    df.addColumn<double>      ("DOUBLE", -1, -100); // se quiser nulo custom e pos automatica tem que colocar pos = -1 aqui

    cout << df.getColumn("DOUBLE")->getPosition() << endl;

    FileRepository repo("data/teste_repo.csv", ",", false);
    string line;
    while (true) {
        auto line = repo.getRow();
        if (!repo.hasNext()) { break; }
        df.addRow(repo.parseRow(line));
    }

    cout << df.toString() << endl;

    auto col = df.getColumn("DOUBLE"); // busca a coluna pelo seu identificador

    cout << "Coluna 0: " << col->toString() << endl;

    Column<int> colInt("INT", -1, -1);

    cout << "Coluna 1: " << colInt.toString() << endl;

    auto df_cp = df.emptyCopy(); // faz uma copia vazia do dataframe, com as colunas definidas
    cout << "Df copiado vazio: " << endl;
    cout << df_cp->toString() << endl;
    cout << df_cp->getColumn("DOUBLE")->toString() << endl;

    auto df_cp2 = df.emptyCopy({"STR", "INT"}); // faz uma copia vazia do dataframe, de colunas especificas
    cout << "Df copiado parcial vazio: " << endl;
    cout << df_cp2->toString() << endl;
}

void testeTrigger(){
    DataFrame* df = buildDFteste3();

    DataFrame* dfOut1 = new DataFrame();
    auto ageColumn2 =  std::make_shared<Column<int>>("age", 0, 0);
    auto nameColumn2 = std::make_shared<Column<std::string>>("name", 1, "");
    auto salaryColumn2 = std::make_shared<Column<double>>("salary", 2, -1);
    auto extraIntColumn2 = std::make_shared<Column<int>>("codigo", 3, -100);
    dfOut1->addColumn(ageColumn2);
    dfOut1->addColumn(nameColumn2);
    dfOut1->addColumn(salaryColumn2);
    dfOut1->addColumn(extraIntColumn2);

    DataFrame* dfOut2 = new DataFrame();
    auto ageSumColumn = std::make_shared<Column<int>>("ageSum", 0, -1);
    dfOut2->addColumn(ageSumColumn);

    cout << "Initial dataframe:\n" << df->toString() << endl;

    cout << "Output dataframe t1:\n" << dfOut1->toString() << endl;

    cout << "Output dataframe t2:\n" << dfOut2->toString() << endl;

    //t1 e t2 são dois tratadores para teste. t0 é um mock, representadno uma etapa já completada da pipeline
    auto t0 = std::make_shared<DuplicateDFTransformer>();
    auto t1 = std::make_shared<DuplicateDFTransformer>();
    auto t2 = std::make_shared<SumAgeTransformerTestInterface>();

    //Antes de fazer as conexões tenho que add output em todos. O usuário que faz isso.
    t0->addOutput(df);
    t1->addOutput(dfOut1);
    t2->addOutput(dfOut2);

    //T0 é um mock - representa um tratador que já foi completo anteriormente
    //Após adicionar as especificações de output, o usuário deve usar addNext pra construir o grafo. Talvez eu troque pro addPrevious, mas a ideia segue sendo a mesma.s
    t0->addNext(t1);
    cout << "Tamanho do nextTasks do t1: " << t1->getNextTasks().size() << endl;
    cout << "Tamanho do previousTasks do t2: " << t2->getPreviousTasks().size() << endl;
    t1->addNext(t2);
    cout << "Tamanho do nextTasks do t1: " << t1->getNextTasks().size() << endl;
    cout << "Tamanho do previousTasks do t2: " << t2->getPreviousTasks().size() << endl;
    
    RequestTrigger trigger;
    trigger.addExtractor(t0);

    std::cout << "Startando trigger...\n";
    trigger.start(1);
    std::cout << "Trigger finalizado.\n";

    cout << "Output dataframe t0 after operation:\n" << df->toString() << endl;
    cout << "Output dataframe t1 after operation:\n" << dfOut1->toString() << endl;
    cout << "Output dataframe t2 after operation:\n" << dfOut2->toString() << endl;
}

void testeTrigger2(){
    DataFrame* df = buildDFteste3();

    DataFrame* dfOut1 = new DataFrame();
    auto ageColumn2 =  std::make_shared<Column<int>>("age", 0, 0);
    auto nameColumn2 = std::make_shared<Column<std::string>>("name", 1, "");
    auto salaryColumn2 = std::make_shared<Column<double>>("salary", 2, -1);
    auto extraIntColumn2 = std::make_shared<Column<int>>("codigo", 3, -100);
    dfOut1->addColumn(ageColumn2);
    dfOut1->addColumn(nameColumn2);
    dfOut1->addColumn(salaryColumn2);
    dfOut1->addColumn(extraIntColumn2);

    DataFrame* dfOut2 = new DataFrame();
    auto ageSumColumn = std::make_shared<Column<int>>("ageSum", 0, -1);
    dfOut2->addColumn(ageSumColumn);

    DataFrame* dfOut3 = new DataFrame();
    auto ageSumColumn2 = std::make_shared<Column<int>>("ageSum", 0, -1);
    dfOut3->addColumn(ageSumColumn2);

    cout << "Initial dataframe:\n" << df->toString() << endl;

    cout << "Output dataframe t1:\n" << dfOut1->toString() << endl;

    cout << "Output dataframe t2:\n" << dfOut2->toString() << endl;

    //t1 e t2 são dois tratadores para teste. t0 é um mock, representadno uma etapa já completada da pipeline
    auto t0 = std::make_shared<DuplicateDFTransformer>();
    auto t1 = std::make_shared<DuplicateDFTransformer>();
    auto t2 = std::make_shared<SumAgeTransformerTestInterface_nosense>();
    auto t3 = std::make_shared<SumAgeTransformerTestInterface_nosense>();

    //Antes de fazer as conexões tenho que add output em todos. O usuário que faz isso.
    t0->addOutput(df);
    t1->addOutput(dfOut1);
    t2->addOutput(dfOut2);
    t3->addOutput(dfOut3);

    //T0 é um mock - representa um tratador que já foi completo anteriormente
    //Após adicionar as especificações de output, o usuário deve usar addNext pra construir o grafo. Talvez eu troque pro addPrevious, mas a ideia segue sendo a mesma.s
    t0->addNext(t1);
    cout << "Tamanho do nextTasks do t1: " << t1->getNextTasks().size() << endl;
    cout << "Tamanho do previousTasks do t2: " << t2->getPreviousTasks().size() << endl;
    t1->addNext(t2);
    t1->addNext(t3);
    cout << "Tamanho do nextTasks do t1: " << t1->getNextTasks().size() << endl;
    cout << "Tamanho do previousTasks do t2: " << t2->getPreviousTasks().size() << endl;
    cout << "Tamanho do previousTasks do t3: " << t3->getPreviousTasks().size() << endl;
    
    RequestTrigger trigger;
    trigger.addExtractor(t0);
  
    std::cout << "Startando trigger...\n";
    trigger.start(2);
    std::cout << "Trigger finalizado.\n";

    cout << "Output dataframe t0 after operation:\n" << df->toString() << endl;
    cout << "Output dataframe t1 after operation:\n" << dfOut1->toString() << endl;
    cout << "Output dataframe t2 after operation:\n" << dfOut2->toString() << endl;
    cout << "Output dataframe t3 after operation:\n" << dfOut3->toString() << endl;

}

class FilterDFTransformer : public Transformer {
private:
    std::vector<std::string> filterStrings;
    std::mutex filterMutex;
public:
    FilterDFTransformer(std::vector<std::string> filter = {}): filterStrings(filter), filterMutex() {};
    void transform(std::vector<DataFrame*>& outputs,
                   const std::vector<std::pair<std::vector<int>, DataFrame*>>& inputs) override
    {
        if (inputs.empty()) {
            std::cout << "[filterDFTransformer] Nenhum DataFrame na entrada.\n";
            return;
        }

        DataFrame* df = inputs[0].second;
        if (!df) {
            std::cout << "[FilterDFTransformer] DataFrame nulo.\n";
            return;
        }

        std::cout << "[FilterDFTransformer] Tamanho do dataframe de entrada: " << df->size() << " | ";
        std::cout << "Tamanho da array de índices: " << inputs[0].first.size() << std::endl;

        auto outDf = outputs.at(0);
        for (auto index : inputs[0].first) {
            for (auto compareString: filterStrings){
                if(df->getElement<std::string>(index, 0) == compareString){
                    std::vector<std::any> row;
                    auto posicao = df->getElement<std::string>(index, 0); row.push_back(posicao);
                    auto idade = df->getElement<int>(index, 1); row.push_back(idade);
                    auto ano = df->getElement<int>(index, 2); row.push_back(ano);
                    auto salario = df->getElement<double>(index, 3); row.push_back(salario);
                    {
                        std::unique_lock<std::mutex> lock(filterMutex);
                        outDf->addRow(row);
                    }
                    break;
                }
            }
        }
        std::cout << "[FilterDFTransformer] Concluído: dataframe foi filtrado. Tamanho de saída: " << outDf->size() << std::endl;
    }
};

class AgeSumTransformer : public Transformer {
private:
    std::mutex sumMutex;
public:
    AgeSumTransformer(): sumMutex() {};
    void transform(std::vector<DataFrame*>& outputs,
                   const std::vector<std::pair<std::vector<int>, DataFrame*>>& inputs) override
    {
        if (inputs.empty()) {
            std::cout << "[AgeSumTransformer] Nenhum DataFrame na entrada.\n";
            return;
        }

        DataFrame* df = inputs[0].second;
        if (!df) {
            std::cout << "[AgeSumTransformer] DataFrame nulo.\n";
            return;
        }

        std::cout << "[AgeSumTransformer] Tamanho do dataframe de entrada: " << df->size() << " | ";
        std::cout << "Tamanho da array de índices: " << inputs[0].first.size() << std::endl;

        std::unordered_map<std::string, int> sum;

        int repeatTimes = 20;
        for (int i = 0; i < repeatTimes; i++){
            for (auto index : inputs[0].first) {
                auto categoria = df->getElement<std::string>(index, 0);
                auto idade = df->getElement<int>(index, 1);
                sum[categoria] += idade/repeatTimes;
            }
        }

        auto outDf = outputs.at(0);

        for (const auto& [chave, valor] : sum){
            {
                std::unique_lock<std::mutex> lock(sumMutex);
                bool alreadyPlaced = false;
                for(size_t row = 0; row < outDf->size(); row++){
                    if(outDf->getElement<std::string>(row, 0) == chave){
                        alreadyPlaced = true;
                        outDf->setElement<int>(row, 1, outDf->getElement<int>(row, 1) + valor);
                        break;
                    }
                }
                if(not alreadyPlaced){
                    auto row = std::vector<std::any>{chave, valor};
                    outDf->addRow(row);
                }
            }
        }
        std::cout << "[AgeSumTransformer] Concluído: somas foram computadas." << std::endl;
    }
};


class SalarySumTransformer : public Transformer {
public:
    void transform(std::vector<DataFrame*>& outputs,
                   const std::vector<std::pair<std::vector<int>, DataFrame*>>& inputs) override
    {
        if (inputs.empty()) {
            std::cout << "[SalarySumTransformer] Nenhum DataFrame na entrada.\n";
            return;
        }

        DataFrame* df = inputs[0].second;
        if (!df) {
            std::cout << "[SalarySumTransformer] DataFrame nulo.\n";
            return;
        }

        std::cout << "[SalarySumTransformer] Tamanho do dataframe de entrada: " << df->size() << std::endl;
        //std::cout << "[SalarySumTransformer] Tamanho da array de índices: " << inputs[0].first.size() << std::endl;

        std::unordered_map<std::string, double> sum;

        double repeatTimes = 20;
        for (int i = 0; i < repeatTimes; i++){
            for (auto index : inputs[0].first) {
                auto categoria = df->getElement<std::string>(index, 0);
                auto salario = df->getElement<double>(index, 3);
                sum[categoria] += salario/repeatTimes;
            }
        }

        auto outDf = outputs.at(0);

        for (const auto& [chave, valor] : sum){
            auto row = std::vector<std::any>{chave, valor};
            outDf->addRow(row);
        }
        std::cout << "[SalarySumTransformer] Concluído: somas foram computadas." << std::endl;
    }
};

class CounterTransformer : public Transformer {
private:
    std::mutex counterMutex;
public:
    CounterTransformer(): counterMutex() {};
    void transform(std::vector<DataFrame*>& outputs,
                   const std::vector<std::pair<std::vector<int>, DataFrame*>>& inputs) override
    {
        if (inputs.empty()) {
            std::cout << "[CounterTransformer] Nenhum DataFrame na entrada.\n";
            return;
        }

        DataFrame* df = inputs[0].second;
        if (!df) {
            std::cout << "[CounterTransformer] DataFrame nulo.\n";
            return;
        }

        std::cout << "[CounterTransformer] Tamanho do dataframe de entrada: " << df->size() << " | ";
        std::cout << "Tamanho da array de índices: " << inputs[0].first.size() << std::endl;

        std::unordered_map<std::string, int> counts;

        for (auto index : inputs[0].first) {
            auto categoria = df->getElement<std::string>(index, 0);
            counts[categoria] += 1;
        }

        auto outDf = outputs.at(0);

        for (const auto& [chave, valor] : counts){
            {
                std::unique_lock<std::mutex> lock(counterMutex);
                bool alreadyPlaced = false;
                for(size_t row = 0; row < outDf->size(); row++){
                    if(outDf->getElement<std::string>(row, 0) == chave){
                        alreadyPlaced = true;
                        outDf->setElement<int>(row, 1, outDf->getElement<int>(row, 1) + valor);
                        break;
                    }
                }
                if(not alreadyPlaced){
                    auto row = std::vector<std::any>{chave, valor};
                    outDf->addRow(row);
                }
            }
        }
        std::cout << "[CounterTransformer] Concluído: contagens foram computadas." << std::endl;
    }
};

class MeanTransformer : public Transformer {
public:
    void transform(std::vector<DataFrame*>& outputs,
                   const std::vector<std::pair<std::vector<int>, DataFrame*>>& inputs) override
    {
        if (inputs.empty()) {
            std::cout << "[MeanTransformer] Nenhum DataFrame na entrada.\n";
            return;
        }

        DataFrame* dfSum = inputs[0].second;
        if (!dfSum) {
            std::cout << "[MeanTransformer] DataFrame sum nulo.\n";
            return;
        }
        DataFrame* dfCount = inputs[1].second;
        if (!dfCount) {
            std::cout << "[MeanTransformer] DataFrame count nulo.\n";
            return;
        }

        std::cout << "[MeanTransformer] Tamanho do dataframe 0 de entrada: " << dfSum->size() << std::endl;
        //std::cout << "[MeanTransformer] Tamanho da array de índices 0: " << inputs[0].first.size() << std::endl;
        std::cout << "[MeanTransformer] Tamanho do dataframe 1 de entrada: " << dfCount->size() << std::endl;
        //std::cout << "[MeanTransformer] Tamanho da array de índices 1: " << inputs[1].first.size() << std::endl;

        auto outDf = outputs.at(0);
        for (auto index : inputs[0].first) {
            auto categoria = dfSum->getElement<std::string>(index, 0);
            auto soma = dfSum->getElement<int>(index, 1);
            int equivalentLine = -1;
            for (size_t i = 0; i < dfCount->size(); i++){
                if (dfCount->getElement<std::string>(i, 0) == categoria){
                    equivalentLine = i;
                    break;
                }
            }
            auto contagem = dfCount->getElement<int>(equivalentLine, 1);
            double mean = soma/contagem;
            auto row = std::vector<std::any>{categoria, soma, contagem, mean};
            outDf->addRow(row);
        }

        std::cout << "[MeanTransformer] Concluído: medias foram computadas." << std::endl;
    }
};

void testeGeralEmap(int nThreads = 1){
    //================================================//
    //Definições dos dataframes de saída de cada bloco//
    //================================================//
    DataFrame* dfOutE = new DataFrame();
    dfOutE->addColumn(std::make_shared<Column<std::string>>("posicao", 0, ""));
    dfOutE->addColumn(std::make_shared<Column<int>>("idade", 1, -1));
    dfOutE->addColumn(std::make_shared<Column<int>>("ano", 2, -1));
    dfOutE->addColumn(std::make_shared<Column<double>>("salario", 3, -1.0));

    DataFrame* dfOut11 = new DataFrame();
    dfOut11->addColumn(std::make_shared<Column<std::string>>("posicao", 0, ""));
    dfOut11->addColumn(std::make_shared<Column<int>>("idade", 1, -1));
    dfOut11->addColumn(std::make_shared<Column<int>>("ano", 2, -1));
    dfOut11->addColumn(std::make_shared<Column<double>>("salario", 3, -1.0));

    DataFrame* dfOut12 = new DataFrame();
    dfOut12->addColumn(std::make_shared<Column<std::string>>("posicao", 0, ""));
    dfOut12->addColumn(std::make_shared<Column<int>>("idade", 1, -1));
    dfOut12->addColumn(std::make_shared<Column<int>>("ano", 2, -1));
    dfOut12->addColumn(std::make_shared<Column<double>>("salario", 3, -1.0));

    DataFrame* dfOut21 = new DataFrame();
    dfOut21->addColumn(std::make_shared<Column<std::string>>("posicao", 0, ""));
    dfOut21->addColumn(std::make_shared<Column<int>>("soma idade", 1, -1));

    DataFrame* dfOut22 = new DataFrame();
    dfOut22->addColumn(std::make_shared<Column<std::string>>("posicao", 0, ""));
    dfOut22->addColumn(std::make_shared<Column<int>>("contagem", 1, 0));

    DataFrame* dfOut23 = new DataFrame();
    dfOut23->addColumn(std::make_shared<Column<std::string>>("posicao", 0, ""));
    dfOut23->addColumn(std::make_shared<Column<double>>("soma salario", 1, -1.0));

    DataFrame* dfOut24 = new DataFrame();
    dfOut24->addColumn(std::make_shared<Column<std::string>>("posicao", 0, ""));
    dfOut24->addColumn(std::make_shared<Column<int>>("soma idade", 1, 0));

//    DataFrame* dfOut3 = new DataFrame();
//    dfOut3->addColumn(std::make_shared<Column<std::string>>("posicao", 0, ""));
//    dfOut3->addColumn(std::make_shared<Column<int>>("soma idade", 1, -1));
//    dfOut3->addColumn(std::make_shared<Column<int>>("contagem", 2, 0));
//    dfOut3->addColumn(std::make_shared<Column<double>>("media", 3, 0));

    cout << "Output dataframe specification for e:\n" << dfOutE->toString() << endl;
    cout << "Output dataframe specification for t1.1 and t1.2:\n" << dfOut11->toString() << endl;
    cout << "Output dataframe specification for t2.1 and t2.4:\n" << dfOut21->toString() << endl;
    cout << "Output dataframe specification for t2.2:\n" << dfOut22->toString() << endl;
    cout << "Output dataframe specification for t2.3:\n" << dfOut23->toString() << endl;
//    cout << "Output dataframe specification for t3:\n" << dfOut3->toString() << endl;


    //================================================//
    //             Definições de cada bloco           //
    //================================================//
    FileRepository* inputRepository = new FileRepository("data/mock_emap.csv", ",", true);
    auto e0 = std::make_shared<Extractor>();
    e0->addOutput(dfOutE);
    e0->addRepo(inputRepository);

    auto t11FilterStrings = std::vector<std::string>{"Secretario", "Professor", "Diretor"};
    auto t11 = std::make_shared<FilterDFTransformer>(t11FilterStrings);
    t11->addOutput(dfOut11);

    auto t12FilterStrings = std::vector<std::string>{"AlunoGrad", "AlunoMesc"};
    auto t12 = std::make_shared<FilterDFTransformer>(t12FilterStrings);
    t12->addOutput(dfOut12);

    auto t21 = std::make_shared<AgeSumTransformer>();
    t21->addOutput(dfOut21);

    auto t22 = std::make_shared<CounterTransformer>();
    t22->addOutput(dfOut22);

    auto t23 = std::make_shared<SalarySumTransformer>();
    t23->addOutput(dfOut23);

    auto t24 = std::make_shared<AgeSumTransformer>();
    t24->addOutput(dfOut24);

//    auto t3 = std::make_shared<MeanTransformer>();
//    t3->addOutput(dfOut3);

    FileRepository* outputRepositoryFilter = new FileRepository("data/output_emap_filtered.csv", ",", true);
    auto l0 = std::make_shared<Loader>();
    l0->addRepo(outputRepositoryFilter);

    FileRepository* outputRepositoryAges = new FileRepository("data/output_emap_age.csv", ",", true);
    auto l1 = std::make_shared<Loader>();
    l1->addRepo(outputRepositoryAges);

    FileRepository* outputRepositorySalary = new FileRepository("data/output_emap_salary.csv", ",", true);
    auto l2 = std::make_shared<Loader>();
    l2->addRepo(outputRepositorySalary);

    //================================================//
    //                Construção do DAG               //
    //================================================//
    e0->addNext(t11);
    e0->addNext(t12);

    t11->addNext(t21);
    t11->addNext(t22);
    t11->addNext(t23);
    t11->addNext(t24);
//    t21->addNext(t3);
//    t22->addNext(t3);

    t12->addNext(l0);
    t23->addNext(l2);
    t24->addNext(l1);

    RequestTrigger trigger;
    trigger.addExtractor(e0);

    //================================================//
    // Inicialização do trigger e execução da pipeline//
    //================================================//
    std::cout << "\nStartando trigger...\n";
    trigger.start(nThreads);
    std::cout << "\nTrigger finalizado.\n";

    cout << "t2.1 dataframe after running:\n" << dfOut21->toString() << endl;
    cout << "t2.2 dataframe after running:\n" << dfOut22->toString() << endl;
    cout << "t2.3 dataframe after running:\n" << dfOut23->toString() << endl;
    cout << "t2.4 dataframe after running:\n" << dfOut24->toString() << endl;
//    cout << "t3 dataframe after running:\n" << dfOut3->toString() << endl;

}

void testeTransformer(int nThreads = 1){
    //================================================//
    //Definições dos dataframes de saída de cada bloco//
    //================================================//
    DataFrame* dfOutE = new DataFrame();
    dfOutE->addColumn(std::make_shared<Column<std::string>>("posicao", 0, ""));
    dfOutE->addColumn(std::make_shared<Column<int>>("idade", 1, -1));
    dfOutE->addColumn(std::make_shared<Column<int>>("ano", 2, -1));
    dfOutE->addColumn(std::make_shared<Column<double>>("salario", 3, -1.0));

    DataFrame* dfOut11 = new DataFrame();
    dfOut11->addColumn(std::make_shared<Column<std::string>>("posicao", 0, ""));
    dfOut11->addColumn(std::make_shared<Column<int>>("idade", 1, -1));
    dfOut11->addColumn(std::make_shared<Column<int>>("ano", 2, -1));
    dfOut11->addColumn(std::make_shared<Column<double>>("salario", 3, -1.0));

    DataFrame* dfOut12 = new DataFrame();
    dfOut12->addColumn(std::make_shared<Column<std::string>>("posicao", 0, ""));
    dfOut12->addColumn(std::make_shared<Column<int>>("idade", 1, -1));
    dfOut12->addColumn(std::make_shared<Column<int>>("ano", 2, -1));
    dfOut12->addColumn(std::make_shared<Column<double>>("salario", 3, -1.0));

    DataFrame* dfOut21 = new DataFrame();
    dfOut21->addColumn(std::make_shared<Column<std::string>>("posicao", 0, ""));
    dfOut21->addColumn(std::make_shared<Column<int>>("soma idade", 1, -1));

    DataFrame* dfOut22 = new DataFrame();
    dfOut22->addColumn(std::make_shared<Column<std::string>>("posicao", 0, ""));
    dfOut22->addColumn(std::make_shared<Column<int>>("contagem", 1, 0));

    //    DataFrame* dfOut3 = new DataFrame();
    //    dfOut3->addColumn(std::make_shared<Column<std::string>>("posicao", 0, ""));
    //    dfOut3->addColumn(std::make_shared<Column<int>>("soma idade", 1, -1));
    //    dfOut3->addColumn(std::make_shared<Column<int>>("contagem", 2, 0));
    //    dfOut3->addColumn(std::make_shared<Column<double>>("media", 3, 0));

    cout << "Output dataframe specification for e:\n" << dfOutE->toString() << endl;
    cout << "Output dataframe specification for t1.1:\n" << dfOut11->toString() << endl;
    cout << "Output dataframe specification for t2.1:\n" << dfOut21->toString() << endl;
    cout << "Output dataframe specification for t2.2:\n" << dfOut22->toString() << endl;
    //    cout << "Output dataframe specification for t3:\n" << dfOut3->toString() << endl;


    //================================================//
    //             Definições de cada bloco           //
    //================================================//
    FileRepository* inputRepository = new FileRepository("data/mock_emap.csv", ",", true);
    auto e0 = std::make_shared<Extractor>();
    e0->addOutput(dfOutE);
    e0->addRepo(inputRepository);

    auto t11FilterStrings = std::vector<std::string>{"Secretario", "Professor", "Diretor"};
    auto t11 = std::make_shared<FilterDFTransformer>(t11FilterStrings);
    t11->addOutput(dfOut11);

    auto t12FilterStrings = std::vector<std::string>{"AlunoGrad", "AlunoMesc"};
    auto t12 = std::make_shared<FilterDFTransformer>(t12FilterStrings);
    t12->addOutput(dfOut12);

    auto t21 = std::make_shared<AgeSumTransformer>();
    t21->addOutput(dfOut21);

    auto t22 = std::make_shared<CounterTransformer>();
    t22->addOutput(dfOut22);

    //    auto t3 = std::make_shared<MeanTransformer>();
    //    t3->addOutput(dfOut3);

    //================================================//
    //                Construção do DAG               //
    //================================================//
    e0->addNext(t11);
    e0->addNext(t12);

//    t11->addNext(t21);
//    t11->addNext(t22);
    //    t21->addNext(t3);
    //    t22->addNext(t3);

    RequestTrigger trigger;
    trigger.addExtractor(e0);

    //================================================//
    // Inicialização do trigger e execução da pipeline//
    //================================================//
    std::cout << "\nStartando trigger...\n";
//    e0->execute();
//    t11->execute(nThreads);
//    t21->execute(nThreads + 1);
//    t22->execute(nThreads + 2);
    trigger.start(nThreads);
    std::cout << "\nTrigger finalizado.\n";

    cout << "t2.1 dataframe after running:\n" << dfOut21->toString() << endl;
    cout << "t2.2 dataframe after running:\n" << dfOut22->toString() << endl;
    //    cout << "t3 dataframe after running:\n" << dfOut3->toString() << endl;

}


int main(int argc, char *argv[]) {
    int nThreads = 1;
    if (argc > 1) {
        nThreads = std::stoi(argv[1]);
    }

    auto start = std::chrono::high_resolution_clock::now();

    //testeTransformer(3);
    testeGeralEmap(1);

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> elapsed = end - start;
    std::cout << "Tempo de execução: " << elapsed.count() << " milissegundos.\n";

    return 0;
}
