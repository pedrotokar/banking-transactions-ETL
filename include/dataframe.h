#ifndef DATAFRAME_H
#define DATAFRAME_H

#include <iostream>
#include <vector>
#include <string>
#include <sstream>
#include <stdexcept>
#include <memory>
#include <typeinfo>

#include <any>


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

    virtual std::string getValue(size_t index) const = 0;
    virtual size_t size() const = 0;
    int getPosition() const { return position; }

    virtual void addAny(const std::any& value) = 0;
    virtual void appendNA() {};
};

template <typename T>
class Column : public BaseColumn {
private:
    std::vector<T> data;
    T NAValue;

public:
    Column(const std::string &id, int pos, T NAValue);

    void addValue(const T &value);

    void addAny(const std::any& value) override {
        data.push_back(std::any_cast<T>(value));
    }

    std::string getValue(size_t index) const override;
    size_t size() const override;
    
    std::string toString() const;

    const std::vector<T>& getData() const { return data; }
    
    void appendNA() override;
};


class DataFrame {
private:
    std::vector<std::shared_ptr<BaseColumn>> columns;
    size_t dataFrameSize = 0;

public:
    void addColumn(std::shared_ptr<BaseColumn> column);
    std::shared_ptr<BaseColumn> getColumn(size_t index) const;
    std::vector<std::string> getRow(size_t row) const;
    size_t size() {return dataFrameSize;};
    std::string toString() const;

    template <typename T>
    const std::vector<T>& getColumnData(size_t index) const;

    template <typename T>
    T getElement(size_t rowIdx, size_t colIdx) const;

    void addRow(const std::vector<std::any> &row);
};


template <typename T>
Column<T>::Column(const std::string &id, int pos, T NAValue)
    : BaseColumn(id, pos, typeid(T).name()), NAValue(NAValue){
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
    oss << "BaseColumn '" << identifier << "' (position: " << position
        << ", type: " << dataType << "):\n";
    for (const auto &value : data) {
        oss << "| " << value << " |\n";
    }
    return oss.str();
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
    return getColumnData<T>(colIdx)[rowIdx];
}

#endif
