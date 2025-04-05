#include <iostream>
#include "dataframe.h"   
#include "task.h"
#include "dataRepository.h"

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

    vector<any> row2 {42, string("ANY"), 5000.0, nullptr};

    df.addRow(row2);
    cout << "DataFrame after adding a new row:\n" << df.toString() << endl;

    cout << "Testando função de adicionar row de strings com conversão automatica" << endl;
    // A operação tem que ser por linha pra garantir que o registros estejam corretos na hora de paralelizar
    // Se paralelizar pela leitura da coluna pode acontecer da ordem não ser correta
    // Vai evitar criar um mutex pra cada coluna
    // Assumo que o maior gargalo seja quebrar as linhas no separador
    // Assumo que os tipos da coluna são 100% consistentes
    
    vector<string> row3 {"42", "STRING", "5000.0", "123"};
    df.addRow(row3);
    cout << "DataFrame after adding a new row with strings:\n" << df.toString() << endl;

    vector<variantRow> row4 {37, string("VARIANT"), nullptr, 182};
    df.addRow(row4);
    cout << "DataFrame after adding a new row with variantRow:\n" << df.toString() << endl;
}


int main() {
    teste2();
    return 0;
}