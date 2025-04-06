#include "dataframe.h"
#include "datarepository.h"
#include "task.h"
#include "trigger.hpp"

#include <iostream>
#include <vector>
#include <any>


using namespace std;

class PrintColumnInfoTransformer : public Transformer {
public:
    void transform(std::vector<DataFrame*>& outputs,
                   const std::vector<std::pair<std::vector<int>, DataFrame*>>& inputs) override
    {
        if (inputs.empty()) {
            std::cout << "[PrintColumnInfoTransformer] Nenhum DataFrame na entrada.\n";
            return;
        }
        DataFrame* df = inputs[0].second;
        if (!df) {
            std::cout << "[PrintColumnInfoTransformer] DataFrame nulo.\n";
            return;
        }

        std::cout << "\n[PrintColumnInfoTransformer] Informações das colunas:\n";
        // Tentaremos acessar até 5 colunas, pois não temos um getColumnCount() pronto;
        for (size_t i = 0; i < 5; i++) {
            try {
                auto col = df->getColumn(i);
                std::cout << "  - Coluna " << i 
                          << " => Identifier: " << col->getIdentifier()
                          << ", Tipo: " << col->getTypeName()
                          << ", Posição (position): " << col->getPosition()
                          << ", Tamanho (size): " << col->size()
                          << "\n";
            } catch (const std::exception& ex) {
                // Ao estourar a quantidade de colunas, cairá aqui
                std::cout << "  - Coluna " << i << " não encontrada (out_of_range): " << ex.what() << "\n";
            }
        }
        // Este Transformer não gera DataFrame de saída
    }
};

class SumAgeTransformer : public Transformer {
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
        for (size_t i = 0; i < ageCol->size(); i++) {
            sum += std::stoi(ageCol->getValue(i));
        }

        // Cria um novo DataFrame com a soma
        DataFrame* outDf = new DataFrame();
        auto sumColumn = std::make_shared<Column<int>>("age_sum", 0, -1);
        sumColumn->addValue(sum);
        outDf->addColumn(sumColumn);

        outputs.push_back(outDf);
        std::cout << "[SumAgeTransformer] Soma da coluna 'age': " << sum << "\n";
    }
};

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
        for (size_t i = 0; i < ageCol->size(); i++) {
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

class DoubleSalaryTransformer : public Transformer {
public:
    void transform(std::vector<DataFrame*>& outputs,
                   const std::vector<std::pair<std::vector<int>, DataFrame*>>& inputs) override
    {
        if (inputs.empty()) {
            std::cout << "[DoubleSalaryTransformer] Nenhum DataFrame na entrada.\n";
            return;
        }

        DataFrame* df = inputs[0].second;
        if (!df) {
            std::cout << "[DoubleSalaryTransformer] DataFrame nulo.\n";
            return;
        }

        // Procura a coluna "salary"
        int salaryColIndex = -1;
        for (size_t i = 0; i < 3; i++) {
            auto col = df->getColumn(i);
            if (col->getIdentifier() == "salary") {
                salaryColIndex = static_cast<int>(i);
                break;
            }
        }

        if (salaryColIndex < 0) {
            std::cout << "[DoubleSalaryTransformer] Coluna 'salary' não encontrada.\n";
            return;
        }

        // Cria a coluna "salary_x2"
        DataFrame* outDf = new DataFrame();
        auto doubledSalaryCol = std::make_shared<Column<double>>("salary_x2", 0, -1.0);

        auto salaryCol = df->getColumn(salaryColIndex);
        for (size_t i = 0; i < salaryCol->size(); i++) {
            std::string valStr = salaryCol->getValue(i);
            if (valStr == "N/A") {
                doubledSalaryCol->addValue(0.0);
            } else {
                double salaryVal = std::stod(valStr);
                doubledSalaryCol->addValue(salaryVal * 2.0);
            }
        }
        outDf->addColumn(doubledSalaryCol);

        outputs.push_back(outDf);
        std::cout << "[DoubleSalaryTransformer] Concluído: valores de 'salary' foram dobrados.\n";
    }
};

void teste1() {
    // ===========================
    // 1) Monta o DataFrame
    // ===========================
    auto ageColumn = std::make_shared<Column<int>>("age", 0, -1);
    ageColumn->addValue(25);
    ageColumn->addValue(30);
    ageColumn->addValue(45);

    auto nameColumn = std::make_shared<Column<std::string>>("name", 1, "");
    nameColumn->addValue("Ana");
    nameColumn->addValue("Bruno");
    nameColumn->addValue("Carla");

    auto salaryColumn = std::make_shared<Column<double>>("salary", 2, -1.0);
    salaryColumn->addValue(5000.0);
    salaryColumn->addValue(7500.0);
    salaryColumn->addValue(9000.0);

    // Exemplo de coluna extra (int)
    auto extraIntColumn = std::make_shared<Column<int>>("codigo", 3, -100);
    extraIntColumn->addValue(101);
    extraIntColumn->addValue(102);
    extraIntColumn->addValue(103);

    DataFrame df;
    df.addColumn(ageColumn);
    df.addColumn(nameColumn);
    df.addColumn(salaryColumn);
    df.addColumn(extraIntColumn);

    std::cout << "\n== DataFrame original ==\n";
    std::cout << df.toString() << std::endl;

    // ===========================
    // 2) PrintColumnInfoTransformer
    // ===========================
    {
        std::vector<std::pair<std::vector<int>, DataFrame*>> inputs;
        inputs.push_back({{0}, &df});

        std::vector<DataFrame*> outputs;
        auto infoTransformer = std::make_shared<PrintColumnInfoTransformer>();
        infoTransformer->transform(outputs, inputs);
        // Não gera DataFrame de saída
    }

    // ===========================
    // 3) SumAgeTransformer
    // ===========================
    {
        std::vector<std::pair<std::vector<int>, DataFrame*>> inputs;
        inputs.push_back({{0}, &df});

        std::vector<DataFrame*> outputs;
        auto sumTransformer = std::make_shared<SumAgeTransformer>();
        sumTransformer->transform(outputs, inputs);

        if (!outputs.empty()) {
            std::cout << "\n== DataFrame resultante (Soma de 'age') ==\n";
            std::cout << outputs[0]->toString() << std::endl;

            // Libera memória
            for (auto* outPtr : outputs) {
                delete outPtr;
            }
        }
    }


    // ===========================
    // 3.5) SumAgeTransformer testando interface
    // ===========================
    {
        std::vector<std::pair<std::vector<int>, DataFrame*>> inputs;
        inputs.push_back({{0}, &df});

        DataFrame* outDf = new DataFrame();
        auto sumColumn = std::make_shared<Column<int>>("age_sum", 0, -1);
        outDf->addColumn(sumColumn);

        auto sumTransformer = std::make_shared<SumAgeTransformerTestInterface>();
        sumTransformer->addOutput(outDf);

//        sumTransformer->execute(inputs);

        //if (!outputs.empty()) {
            std::cout << "\n== DataFrame resultante (Soma de 'age') ==\n";
            std::cout << outDf->toString() << std::endl;
            delete outDf;
            // Libera memória
        //    for (auto* outPtr : outputs) {
        //        delete outPtr;
        //    }
        //}
    }

    // ===========================
    // 4) DoubleSalaryTransformer
    // ===========================
    {
        std::vector<std::pair<std::vector<int>, DataFrame*>> inputs;
        inputs.push_back({{0}, &df});

        std::vector<DataFrame*> outputs;
        auto doubleTransformer = std::make_shared<DoubleSalaryTransformer>();
        doubleTransformer->transform(outputs, inputs);

        if (!outputs.empty()) {
            std::cout << "\n== DataFrame resultante (Salary dobrado) ==\n";
            std::cout << outputs[0]->toString() << std::endl;

            // Libera memória
            for (auto* outPtr : outputs) {
                delete outPtr;
            }
        }
    }

    // ===========================
    // 5) Recupera e exibe linhas
    // ===========================
    {
        size_t rowIndex = 2;
        try {
            std::vector<std::string> row = df.getRow(rowIndex);
            std::cout << "\n== Row " << rowIndex << " do DataFrame original ==\n";
            for (auto& val : row) {
                std::cout << val << " | ";
            }
            std::cout << "\n";
        } catch (const std::exception &e) {
            std::cout << "Erro: " << e.what() << "\n";
        }
    }

    {
        size_t rowIndex = 0;
        try {
            std::vector<std::string> row = df.getRow(rowIndex);
            std::cout << "\n== Row " << rowIndex << " do DataFrame original ==\n";
            for (auto& val : row) {
                std::cout << val << " | ";
            }
            std::cout << "\n";
        } catch (const std::exception &e) {
            std::cout << "Erro: " << e.what() << "\n";
        }
    }

    // ===========================
    // 6) Testa exceções
    // ===========================
    {
        try {
            std::cout << "\n== Tentando acessar df.getColumn(10) ==\n";
            auto col10 = df.getColumn(10);
            std::cout << "Coluna 10 não deveria existir. Identifier: " << col10->getIdentifier() << "\n";
        } catch (const std::exception& e) {
            std::cout << "Exceção capturada ao acessar coluna 10: " << e.what() << "\n";
        }
    }

    {
        try {
            std::cout << "\n== Tentando acessar df.getRow(99) ==\n";
            auto row99 = df.getRow(99);
            std::cout << "Linha 99 não deveria existir.\n";
        } catch (const std::exception& e) {
            std::cout << "Exceção capturada ao acessar linha 99: " << e.what() << "\n";
        }
    }

    std::cout << "\n== Testando getColumnData e getElement ==\n";

    auto data = df.getColumnData<std::string>(1);
    auto valor = df.getElement<double>(1, 2);

    std::cout << "Valor (1, 2): " << valor << std::endl;

    std::cout << "Dados da coluna 1:\n";
    for (const auto& name : data) {
        std::cout << name << std::endl;
    }
}

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
    cout << "Tamanho do nextTasks do t1: " << t1->getNextTasks().size() << endl;
    cout << "Tamanho do previousTasks do t2: " << t2->getPreviousTasks().size() << endl;

    //Com isso pronto, em teoria o orquestrador, depois de navegar na pipeline e etc, só teria que chamar esse execute pra um tratador cujos anteriores estivessem completos já.
    //Esse inputs é inutil agora, só não mudei a assinatura da função ainda. No futuro deve ser trocado por algo do tipo "threadCount" ou coisa assim
    t0->execute();
    cout << "Output dataframe t0 after operation:\n" << dfOut0->toString() << endl;
    
    t1->execute();
    cout << "Output dataframe t1 after operation:\n" << dfOut1->toString() << endl;

    t2->execute();
    cout << "Output dataframe t2 after operation:\n" << dfOut2->toString() << endl;
}

void teste4() {
    DataFrame df;
    df.addColumn(std::make_shared<Column<std::string>>("STR", 0, ""));
    df.addColumn(std::make_shared<Column<int>>("INT", 1, -1));
    df.addColumn(std::make_shared<Column<double>>("DOUBLE", 2, -1.0));

    FileRepository repo("data/teste_repo.csv", ",", false);
    string line;
    while (true) {
        auto line = repo.getRow();
        if (!repo.hasNext()) { break; }
        df.addRow(repo.parseRow(line));
    }

    cout << df.toString() << endl;

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

int main() {

    auto start = std::chrono::high_resolution_clock::now();

    testeTrigger2();

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> elapsed = end - start;
    std::cout << "Tempo de execução: " << elapsed.count() << " milissegundos.\n";

    return 0;
}
