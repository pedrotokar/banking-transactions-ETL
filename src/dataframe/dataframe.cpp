#include <iostream>
#include <vector>
#include <string>
#include <sstream>
#include <stdexcept>
#include <memory>
#include <functional>
#include <typeinfo>

// ╰─ g++ -std=c++17 -o main column.cpp                                                                                                       ─╯

using namespace std;

class BaseColumn {
protected:
    string identifier;
    string dataType;
    int position;
    
public:
    BaseColumn(const string &id, int pos, const string &dataType)
        : identifier(id), position(pos), dataType(dataType) {}
    
        
    string getIdentifier() const { return identifier; }
    string getTypeName() const { return dataType; }
    
    virtual string getValue(size_t index) const = 0;
    virtual size_t size() const = 0;
    virtual ~BaseColumn() {}
};

template <typename T>
class Column : public BaseColumn {
private:
    vector<T> data;

public:
    Column(const string &id, int pos)
        : BaseColumn(id, pos, typeid(T).name()) {}

    // Adiciona um valor garantindo que ele seja do tipo T
    void addValue(const T &value) {
        data.push_back(value);
    }

    // Adaptador para o método abstrato da classe base
    // void addValue(const void *value) override {
    //     const T *typedValue = static_cast<const T *>(value);
    //     if (!typedValue) {
    //         throw invalid_argument("Null value provided.");
    //     }
    //     addValue(*typedValue);
    // }

    // Retorna o valor na posição indicada como string
    string getValue(size_t index) const override {
        if (index >= data.size()) {
            throw out_of_range("Index out of column bounds.");
        }
        ostringstream oss;
        oss << data[index];
        return oss.str();
    }

    // Retorna o tamanho da série
    size_t size() const override {
        return data.size();
    }

    // Representa a série como um vetor coluna (cada valor em uma linha)
    string toString() const {
        ostringstream oss;
        oss << "BaseColumn '" << identifier << "' (position: " << position
            << ", type: " << dataType << "):\n";
        for (const auto &value : data) {
            oss << "| " << value << " |\n";
        }
        return oss.str();
    }
};

class DataFrame {
private:
    // Armazena as séries utilizando smart pointers para gerenciar a memória
    vector<shared_ptr<BaseColumn>> columns;

public:
    // Adiciona uma nova série ao DataFrame
    void addColumn(shared_ptr<BaseColumn> column) {
        columns.push_back(column);
    }

    // Retorna a série (coluna) no índice especificado
    shared_ptr<BaseColumn> getColumn(size_t index) const {
        if (index >= columns.size()) {
            throw out_of_range("BaseColumn index out of DataFrame bounds.");
        }
        return columns[index];
    }

    // Retorna uma representação da linha com índice especificado, coletando o valor de cada coluna
    vector<string> getRow(size_t row) const {
        vector<string> rowData;
        if (columns.empty())
            return rowData;

        for (const auto &col : columns) {
            if (row < col->size()) {
                rowData.push_back(col->getValue(row));
            } else {
                rowData.push_back("N/A");
            }
        }
        return rowData;
    }

    string toString() const {
        ostringstream oss;
        size_t maxRows = 0;
        for (const auto &col : columns) {
            if (col->size() > maxRows) {
                maxRows = col->size();
            }
        }
        oss << "DataFrame:\n";
        oss << "Columns: ";
        for (const auto &col : columns) {
            oss << col->getIdentifier() << " ";
        }
        oss << "\nRows:\n";
        for (size_t i = 0; i < maxRows; ++i) {
            oss << "| ";
            vector<string> row = getRow(i);
            for (const auto &value : row) {
                oss << value << " | ";
            }
            oss << "\n";
        }
        return oss.str();
    }
};

int main() {
    // Cria séries com diferentes números de elementos
    auto ageColumn = make_shared<Column<int>>("age", 0);
    ageColumn->addValue(25);
    ageColumn->addValue(30);
    ageColumn->addValue(45);

    auto nameColumn = make_shared<Column<string>>("name", 1);
    nameColumn->addValue("Ana");
    nameColumn->addValue("Bruno");
    nameColumn->addValue("Carla");

    // Cria uma série com menos elementos para testar o caso "N/A"
    auto salaryColumn = make_shared<Column<double>>("salary", 2);
    salaryColumn->addValue(5000.0);
    salaryColumn->addValue(7500.0);
    // Note: Não adicionamos um terceiro valor

    // Cria o DataFrame e adiciona as séries
    DataFrame df;
    df.addColumn(ageColumn);
    df.addColumn(nameColumn);
    df.addColumn(salaryColumn);

    // Exibe o DataFrame completo
    cout << df.toString() << endl;

    // Testa a obtenção de uma linha específica que contenha "N/A"
    try {
        size_t testRow = 2; // terceira linha
        vector<string> row = df.getRow(testRow);
        cout << "Row " << testRow << ": ";
        for (const auto &value : row) {
            cout << value << " | ";
        }
        cout << endl;
    }
    catch (const exception &e) {
        cout << "Error: " << e.what() << endl;
    }

    // Exemplo de processamento direto da coluna 'age'
    try {
        int sum = 0;
        for (size_t i = 0; i < ageColumn->size(); ++i) {
            sum += stoi(ageColumn->getValue(i));
        }
        cout << "Sum of values in column 'age': " << sum << endl;
    }
    catch (const exception &e) {
        cout << "Error: " << e.what() << endl;
    }

    return 0;
}
