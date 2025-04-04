#ifndef SERIES_H
#define SERIES_H

#include <iostream>
#include <vector>
#include <string>
#include <sstream>
#include <stdexcept>
#include <memory>
#include <typeinfo>


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
};

template <typename T>
class Column : public BaseColumn {
private:
    std::vector<T> data;

public:
    Column(const std::string &id, int pos);

    void addValue(const T &value);

    std::string getValue(size_t index) const override;
    size_t size() const override;

    std::string toString() const;
};


class DataFrame {
private:

public:
    std::vector<std::shared_ptr<BaseColumn>> columns; //Mudei isso para público para fazer testes enquanto não existe um método de adicionar linha!
    void addColumn(std::shared_ptr<BaseColumn> column);
    std::shared_ptr<BaseColumn> getColumn(size_t index) const;
    std::vector<std::string> getRow(size_t row) const;
    std::string toString() const;
};


template <typename T>
Column<T>::Column(const std::string &id, int pos)
    : BaseColumn(id, pos, typeid(T).name()) {
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

#endif // SERIES_H
