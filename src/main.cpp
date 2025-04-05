#include <iostream>
#include "dataframe.h"   
#include "task.h"

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

        sumTransformer->execute(inputs);

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
    salaryColumn->addValue(5000.0);
    salaryColumn->addValue(7500.0);
    salaryColumn->addValue(9000.0);

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


    vector<any> row {42, string("John"), 5000.0};

    df.addRow(row);

    cout << "DataFrame after adding a new row:\n" << df.toString() << endl;

    df.addColumn(extraIntColumn);
    cout << "DataFrame after adding a new column:\n" << df.toString() << endl;

    vector<any> row2 {42, string("John"), 5000.0, 102};

    df.addRow(row2);
    cout << "DataFrame after adding a new row:\n" << df.toString() << endl;
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

void teste3() {
    //Dataframe inicial para testes
    auto ageColumn = std::make_shared<Column<int>>("age", 0, 0);
    ageColumn->addValue(25);
    ageColumn->addValue(30);
    ageColumn->addValue(45);
    ageColumn->addValue(10);
    ageColumn->addValue(11);
    ageColumn->addValue(15);
    ageColumn->addValue(90);
    ageColumn->addValue(90);

    auto nameColumn = std::make_shared<Column<std::string>>("name", 1, "");
    nameColumn->addValue("Ana");
    nameColumn->addValue("Bruno");
    nameColumn->addValue("Carla");
    nameColumn->addValue("Vitor");
    nameColumn->addValue("Tokao");
    nameColumn->addValue("Anderson");
    nameColumn->addValue("Tomas");
    nameColumn->addValue("Thiago");

    auto salaryColumn = std::make_shared<Column<double>>("salary", 2, -1);
    salaryColumn->addValue(5000.0);
    salaryColumn->addValue(7500.0);
    salaryColumn->addValue(9000.17);
    salaryColumn->addValue(100.12);
    salaryColumn->addValue(1000000.0);
    salaryColumn->addValue(6969.69);
    salaryColumn->addValue(98.1);
    salaryColumn->addValue(0.1);

    auto extraIntColumn = std::make_shared<Column<int>>("codigo", 3, -100);
    extraIntColumn->addValue(101);
    extraIntColumn->addValue(102);
    extraIntColumn->addValue(103);
    extraIntColumn->addValue(104);
    extraIntColumn->addValue(105);
    extraIntColumn->addValue(106);
    extraIntColumn->addValue(107);
    extraIntColumn->addValue(108);

    DataFrame* df = new DataFrame();
    df->addColumn(ageColumn);
    df->addColumn(nameColumn);
    df->addColumn(salaryColumn);
    df->addColumn(extraIntColumn);

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

    //Com isso pronto, em teoria o orquestrador, depois de navegar na pipeline e etc, só teria que chamar esse execute pra um tratador cujos anteriores estivessem completos já.
    //Esse inputs é inutil agora, só não mudei a assinatura da função ainda. No futuro deve ser trocado por algo do tipo "threadCount" ou coisa assim
    std::vector<std::pair<std::vector<int>, DataFrame*>> inputs;
    t1->execute(inputs);
    cout << "Output dataframe t1 after operation:\n" << dfOut1->toString() << endl;

    t2->execute(inputs);
    cout << "Output dataframe t2 after operation:\n" << dfOut2->toString() << endl;
}

int main() {
    teste3();
    return 0;
}
