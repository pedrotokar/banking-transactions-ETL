#include "dataframe.h"
#include <exception>


BaseColumn::BaseColumn(const std::string &id, int pos, const std::string &dt)
    : identifier(id), position(pos), dataType(dt) {
}

BaseColumn::~BaseColumn() {
}

std::string BaseColumn::getIdentifier() const {
    return identifier;
}

std::string BaseColumn::getTypeName() const {
    return dataType;
}

//se tem zero colunas, seta o tamanho do dataframe para o da coluna. Se tem alguma, então verifica se a coluna bate o tamanho
void DataFrame::addColumn(std::shared_ptr<BaseColumn> column) {
    if (columns.size() == 0){
        columns.push_back(column);
        dataFrameSize = column->size();
    }
    else {
        if (column->size() != dataFrameSize){
            throw "Tried to add a column that doenst have the number of rows of the dataframe";
        } else {
            columns.push_back(column);
        }
    }
}

std::shared_ptr<BaseColumn> DataFrame::getColumn(size_t index) const {
    if (index >= columns.size()) {
        throw std::out_of_range("BaseColumn index out of DataFrame bounds.");
    }
    return columns[index];
}

std::vector<std::string> DataFrame::getRow(size_t row) const {
    std::vector<std::string> rowData;
    if (columns.empty())
        return rowData;

    for (const auto &col : columns) {
        if (row < col->size()) {
            rowData.push_back(col->getValue(row));
        } else {
            // Caso a coluna não tenha tantos elementos
            rowData.push_back("N/A");
        }
    }
    return rowData;
}

std::string DataFrame::toString() const {
    std::ostringstream oss;
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
        std::vector<std::string> row = getRow(i);
        for (const auto &value : row) {
            oss << value << " | ";
        }
        oss << "\n";
    }
    return oss.str();
}
