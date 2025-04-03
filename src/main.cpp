#include <iostream>
#include "dataframe.h"
#include "task.h"

// ====================================================================
// Novo Transformer para imprimir infos das colunas
// ====================================================================
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

        // Tentaremos acessar até 5 colunas, pois não temos um getColumnCount() pronto;
        // Ajuste se desejar mais/menos colunas. Alternativamente, poderíamos
        // adicionar uma função getColumnCount() em DataFrame, mas aqui demonstramos o uso de try/catch.
        std::cout << "\n[PrintColumnInfoTransformer] Informações das colunas:\n";
        for (size_t i = 0; i < 5; i++) {
            try {
                auto col = df->getColumn(i);
                std::cout << "  - Coluna " << i 
                          << " => Identifier: " << col->getIdentifier()
                          << ", Tipo: " << col->getTypeName()
                          << ", Posição (atributo position): " << col->getPosition()
                          << ", Tamanho (size): " << col->size()
                          << "\n";
            } catch (const std::exception& ex) {
                // Quando estourar a quantidade de colunas, cairá aqui
                std::cout << "  - Coluna " << i << " não encontrada (out_of_range): " << ex.what() << "\n";
            }
        }

        // Este Transformer não gera nenhum DataFrame de saída,
        // mas se você quisesse encadeá-lo, poderia criar e retornar um.
    }
};

// ====================================================================
// 1) Classe derivada de Transformer que soma a coluna "age"
// (repete o que já havia, apenas mantido aqui)
// ====================================================================
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

        // Tenta achar a coluna "age"
        int ageColIndex = -1;
        for (size_t i = 0; i < 3; i++) {
            auto col = df->getColumn(i);
            if (col->getIdentifier() == "age") {
                ageColIndex = (int)i;
                break;
            }
        }

        if (ageColIndex < 0) {
            std::cout << "[SumAgeTransformer] Coluna 'age' não encontrada.\n";
            return;
        }

        // Faz a soma de todos os valores int na coluna "age"
        int sum = 0;
        auto ageCol = df->getColumn(ageColIndex);
        for (size_t i = 0; i < ageCol->size(); i++) {
            sum += std::stoi(ageCol->getValue(i));
        }

        // Cria um novo DataFrame com a soma calculada
        DataFrame* outDf = new DataFrame();
        auto sumColumn = std::make_shared<Column<int>>("age_sum", 0);
        sumColumn->addValue(sum);
        outDf->addColumn(sumColumn);

        outputs.push_back(outDf);
        std::cout << "[SumAgeTransformer] Soma da coluna 'age': " << sum << "\n";
    }
};

// ====================================================================
// 2) Classe derivada de Transformer que dobra a coluna "salary"
//    e produz uma coluna "salary_x2" no novo DataFrame
// (repete o que já havia, apenas mantido aqui)
// ====================================================================
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

        // Tenta achar a coluna "salary"
        int salaryColIndex = -1;
        for (size_t i = 0; i < 3; i++) {
            auto col = df->getColumn(i);
            if (col->getIdentifier() == "salary") {
                salaryColIndex = (int)i;
                break;
            }
        }

        if (salaryColIndex < 0) {
            std::cout << "[DoubleSalaryTransformer] Coluna 'salary' não encontrada.\n";
            return;
        }

        // Cria a coluna "salary_x2"
        DataFrame* outDf = new DataFrame();
        auto doubledSalaryCol = std::make_shared<Column<double>>("salary_x2", 0);

        auto col = df->getColumn(salaryColIndex);
        for (size_t i = 0; i < col->size(); i++) {
            std::string valStr = col->getValue(i);
            if (valStr == "N/A") {
                doubledSalaryCol->addValue(0.0);
            } else {
                double salaryVal = std::stod(valStr);
                doubledSalaryCol->addValue(salaryVal * 2.0);
            }
        }
        outDf->addColumn(doubledSalaryCol);

        outputs.push_back(outDf);
        std::cout << "[DoubleSalaryTransformer] Concluído: valores de salary foram dobrados.\n";
    }
};

int main() {
    // ======================================================
    // 1) Monta o DataFrame original com age, name, salary
    // ======================================================
    auto ageColumn = std::make_shared<Column<int>>("age", 0);
    ageColumn->addValue(25);
    ageColumn->addValue(30);
    ageColumn->addValue(45);

    auto nameColumn = std::make_shared<Column<std::string>>("name", 1);
    nameColumn->addValue("Ana");
    nameColumn->addValue("Bruno");
    nameColumn->addValue("Carla");

    auto salaryColumn = std::make_shared<Column<double>>("salary", 2);
    salaryColumn->addValue(5000.0);
    salaryColumn->addValue(7500.0);
    salaryColumn->addValue(9000.0);

    // Criando uma quarta coluna (int) para demonstrar heterogeneidade de tipos (cada coluna tem um T distinto)
    auto extraIntColumn = std::make_shared<Column<int>>("codigo", 3);
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

    // ======================================================
    // 2) Usa o Transformer que imprime infos das colunas
    // ======================================================
    {
        std::vector<std::pair<std::vector<int>, DataFrame*>> inputs;
        inputs.push_back({{0}, &df});  // "param" e ponteiro para df

        std::vector<DataFrame*> outputs;
        auto infoTransformer = std::make_shared<PrintColumnInfoTransformer>();
        infoTransformer->transform(outputs, inputs);

        // Não gera DataFrame de saída, então nada para liberar em outputs
    }

    // ======================================================
    // 3) Usa o SumAgeTransformer
    // ======================================================
    {
        std::vector<std::pair<std::vector<int>, DataFrame*>> inputs;
        inputs.push_back({{0}, &df});

        std::vector<DataFrame*> outputs;
        auto sumTransformer = std::make_shared<SumAgeTransformer>();
        sumTransformer->transform(outputs, inputs);

        if (!outputs.empty()) {
            std::cout << "\n== DataFrame resultante (Soma de 'age') ==\n";
            std::cout << outputs[0]->toString() << std::endl;

            // Liberar memória
            for (auto* outPtr : outputs) {
                delete outPtr;
            }
        }
    }

    // ======================================================
    // 4) Usa o DoubleSalaryTransformer
    // ======================================================
    {
        std::vector<std::pair<std::vector<int>, DataFrame*>> inputs;
        inputs.push_back({{0}, &df});

        std::vector<DataFrame*> outputs;
        auto doubleTransformer = std::make_shared<DoubleSalaryTransformer>();
        doubleTransformer->transform(outputs, inputs);

        if (!outputs.empty()) {
            std::cout << "\n== DataFrame resultante (Salary dobrado) ==\n";
            std::cout << outputs[0]->toString() << std::endl;

            // Liberar memória
            for (auto* outPtr : outputs) {
                delete outPtr;
            }
        }
    }

    // ======================================================
    // 5) Exemplo: recupera e exibe algumas linhas específicas do DataFrame original
    // ======================================================
    {
        size_t rowIndex = 2;
        try {
            std::vector<std::string> row = df.getRow(rowIndex);
            std::cout << "\n== Row " << rowIndex << " do DataFrame original ==\n";
            for (auto &val : row) {
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
            for (auto &val : row) {
                std::cout << val << " | ";
            }
            std::cout << "\n";
        } catch (const std::exception &e) {
            std::cout << "Erro: " << e.what() << "\n";
        }
    }

    // ======================================================
    // 6) Tentativa de acessar coluna e linha fora do range
    //    para mostrar exception
    // ======================================================
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

    // ======================================================
    // Fim
    // ======================================================
    return 0;
}
