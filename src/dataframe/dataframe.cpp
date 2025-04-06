#include <iomanip>
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

std::string BaseColumn::toString() const {
    std::ostringstream oss;
    oss << "BaseColumn '" << identifier << "' (position: " << position
        << ", type: " << dataType << "):\n";
    return oss.str();
}

std::string BaseColumn::getTypeName() const {
    return dataType;
}

//se tem zero colunas, seta o tamanho do dataframe para o da coluna. Se tem alguma, então verifica se a coluna bate o tamanho
void DataFrame::addColumn(std::shared_ptr<BaseColumn> column) {
    if (columnMap.find(column->getIdentifier()) != columnMap.end()) {
        throw std::invalid_argument("Column identifier must be unique");
    }
    columnMap[column->getIdentifier()] = this->columns.size();
    
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

void DataFrame::addRow(const std::vector<std::any> &row) {
    for (size_t i = 0; i < columns.size(); ++i) {
        if (row[i].has_value() && row[i].type() == typeid(std::nullptr_t)) {
            columns[i]->appendNA();
            continue;
        }
        columns[i]->addAny(row[i]);
    }
    dataFrameSize++;
}

void DataFrame::addRow(const std::vector<std::string> &row) {
    for (size_t i = 0; i < columns.size(); ++i) {
        if (row[i] == "") {
            columns[i]->appendNA();
            continue;
        }
        columns[i]->addAny(row[i]);
    }
    dataFrameSize++;
}

void DataFrame::addRow(const std::vector<VarCell> &row) {
    for (size_t i = 0; i < columns.size(); ++i) {
        if (std::holds_alternative<std::nullptr_t>(row[i])) {
            columns[i]->appendNA();
            continue;
        }
        columns[i]->addAny(row[i]);
    }
    dataFrameSize++;
}



std::shared_ptr<BaseColumn> DataFrame::getColumn(size_t index) const {
    if (index >= columns.size()) {
        throw std::out_of_range("BaseColumn index out of DataFrame bounds.");
    }
    return columns[index];
}

std::shared_ptr<BaseColumn> DataFrame::getColumn(const std::string &columnName) const {
    auto it = columnMap.find(columnName);
    if (it == columnMap.end()) {
        throw std::out_of_range("Column not in DataFrame.");
    }
    return columns[it->second];
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

std::string DataFrame::toString(size_t n) const {
    std::ostringstream oss;
    size_t nRows = std::min(n, dataFrameSize);;
    size_t nCols = columns.size();

    size_t width = 12;

    oss << "| ";
    for (const auto &col : columns) {
        oss << std::setw(width) << std::left << col->getIdentifier() << " | ";
    }
    oss << "\n";

    oss << "|" <<std::string((width+3)*nCols-1, '=') << "|\n";

    for (size_t i = 0; i < nRows; ++i) {
        oss << "| ";
        std::vector<std::string> row = getRow(i);
        for (const auto &value : row) {
            oss << std::setw(width) << std::left << value << " | ";
        }
        oss << "\n";
    }
    if (dataFrameSize > nRows) {
        oss << "| ";
        for (size_t i = 0; i < nCols; ++i) {
                oss << std::setw(width) << std::left << "..." << " | ";
        }
        oss << "\n";
    }
    return oss.str();
}

std::shared_ptr<DataFrame> DataFrame::emptyCopy() {
    auto df = std::make_shared<DataFrame>();
    for (const auto& col : columns) {
        df->addColumn(col->cloneEmpty());
    }
    return df;
}

std::shared_ptr<DataFrame> DataFrame::emptyCopy(std::vector<std::string> colNames) {
    auto df = std::make_shared<DataFrame>();
    for (const auto& colName : colNames) {
        auto col = getColumn(colName);
        if (col) {
            df->addColumn(col->cloneEmpty());
        }
    }
    return df;
}