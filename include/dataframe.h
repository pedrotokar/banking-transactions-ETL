#ifndef DATAFRAME_H
#define DATAFRAME_H

#include <iostream>
#include <vector>
#include <string>
#include <sstream>
#include <stdexcept>
#include <memory>
#include <typeinfo>

#include <unordered_map> // para identificação da posição das colunas
// tomar cuidado com isso caso a gente implemente uma função de remover colunas

#include <any>
#include <variant>

#include "utils.h"
#include "types.h"

class BaseColumn {
protected:
    std::string identifier;
    int position;
    std::string dataType;

public:
    BaseColumn(const std::string &id, int pos, const std::string &dataType);
    virtual ~BaseColumn();

    std::string getIdentifier() const;
    std::string getTypeName() const;

    void setPosition(int pos) { position = pos; }
    
    virtual std::string getValue(size_t index) const = 0;
    virtual size_t size() const = 0;
    int getPosition() const { return position; }

    virtual std::string toString() const;

    virtual void addAny(const std::any& value) = 0;
    virtual void addAny(const std::string& value) = 0;
    virtual void addAny(const VarCell& value) = 0;
    virtual void appendNA() {};

    virtual std::shared_ptr<BaseColumn> cloneEmpty() const = 0;
};

template <typename T>
class Column : public BaseColumn {
private:
    std::vector<T> data;
    T NAValue;

public:
    Column(const std::string &id, int pos = -1, T NAValue = NullValue<T>::value());

    void addValue(const T &value);

    void addAny(const std::any& value) override {
        data.push_back(std::any_cast<T>(value));
    }

    void addAny(const VarCell& value) override {
        data.push_back(std::get<T>(value));
    }

    void addAny(const std::string& value) override {
        data.push_back(fromString<T>(value));
    }

    void setValue(size_t index, T value) {
        if(index < size()){
            data[index] = value;
        }
    }

    std::string getValue(size_t index) const override;
    size_t size() const override;
    
    std::string toString() const;

    const std::vector<T>& getData() const { return data; }
    
    void appendNA() override;

    std::shared_ptr<BaseColumn> cloneEmpty() const override {
        return std::make_shared<Column<T>>(identifier, position, NAValue);
    }
};


class DataFrame {
private:
    size_t dataFrameSize = 0;
    std::vector<std::shared_ptr<BaseColumn>> columns;
    std::unordered_map<std::string, int> columnMap;

public:
    void addColumn(std::shared_ptr<BaseColumn> column);
    template <typename T>
    void addColumn(std::string id, int pos = -1, T NAValue = NullValue<T>::value());

    std::shared_ptr<BaseColumn> getColumn(size_t index) const;
    std::shared_ptr<BaseColumn> getColumn(const std::string &columnName) const;
    std::vector<std::string> getRow(size_t row) const;
    size_t size() {return dataFrameSize;};
    std::string toString(size_t n = 10) const;

    template <typename T>
    const std::vector<T>& getColumnData(size_t index) const;

    const std::vector<std::string> getHeader() const;

    template <typename T>
    T getElement(size_t rowIdx, size_t colIdx) const;
    template <typename T>
    void setElement(size_t rowIdx, size_t colIdx, T element) const;

    void addRow(const std::vector<std::any> &row);
    void addRow(const std::vector<std::string> &row);
    void addRow(const std::vector<VarCell> &row);

    std::shared_ptr<DataFrame> emptyCopy();
    std::shared_ptr<DataFrame> emptyCopy(std::vector<std::string> colNames);
};


template <typename T>
Column<T>::Column(const std::string &id, int pos, T NAValue)
    : BaseColumn(id, pos, typeid(T).name()), NAValue(NAValue) {
}

template <typename T>
void Column<T>::addValue(const T &value) {
    data.push_back(value);
}

template <typename T>
std::string Column<T>::getValue(size_t index) const {
    if (index >= data.size()) {
        throw std::out_of_range("Index out of column bounds.");
    }
    std::ostringstream oss;
    oss << data[index];
    return oss.str();
}

template <typename T>
size_t Column<T>::size() const {
    return data.size();
}

template <typename T>
std::string Column<T>::toString() const {
    std::ostringstream oss;
    oss << "Column: '" << identifier << "' (position: " << position
        << ", type: " << dataType << "):\n";
    oss << "[ ";
    for (const auto &value : data) {
        oss << value << " ";
    }
    oss << "]";
    return oss.str();
}

template <typename T>
void DataFrame::addColumn(std::string id, int pos, T NAValue) {
    if (pos == -1) { pos = columns.size(); }
    auto column = std::make_shared<Column<T>>(id, pos, NAValue);
    addColumn(column);
}

template <typename T>
void Column<T>::appendNA() {
    data.push_back(NAValue);
}

template <typename T>
const std::vector<T>& DataFrame::getColumnData(size_t index) const {
    auto col = std::dynamic_pointer_cast<Column<T>>(columns[index]);
    if (!col) {
        throw std::bad_cast();
    }
    return col->getData();
}

template <typename T>
T DataFrame::getElement(size_t rowIdx, size_t colIdx) const {
    return getColumnData<T>(colIdx).at(rowIdx);
}

template <typename T>
void DataFrame::setElement(size_t rowIdx, size_t colIdx, T element) const {
    auto col = std::dynamic_pointer_cast<Column<T>>(columns[colIdx]);
    if (!col) {
        throw std::bad_cast();
    }
    col->setValue(rowIdx, element);
}

/// TODO: melhorar verificação das posições

#endif
