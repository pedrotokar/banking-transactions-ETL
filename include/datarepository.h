#ifndef DATAREPOSITORY_H
#define DATAREPOSITORY_H

#include <vector>
#include <variant>
#include <any>
#include <string>
#include <any>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <memory>
#include <limits>

#include "types.h"

using DataRow = std::string;

class DataRepository {
public:
    virtual ~DataRepository() = default;

    virtual DataRow getRow() = 0;
    // TODO: REVIEW parseRow() RETURN IMPLEMENTATION;
    // virtual WildRow parseRow(const DataRow& line) const = 0;

    virtual void appendRow(const DataRow& data) = 0;
    virtual void appendRow(const std::vector<std::string>& data) = 0;

    virtual void resetReader() {};
    virtual size_t lineCount() const { return 0; };
};

class FileRepository : public DataRepository {
private:
    std::string fileName;
    std::string separator;
    bool hasHeader;
    
    std::fstream file;
    size_t currentReadLine;
    size_t totalLines;
    std::string currLine;
    
    // TODO: change flag implementation
    bool hasNextLine = true; // Flag: the current line is !EOF (?) terrible name I know 
    
public:
    FileRepository(const std::string& fname,
                   const std::string& sep,
                   bool hasHeader = true);
    
    ~FileRepository() override;

    DataRow getRow() override;
    void appendRow(const DataRow& data) override;
    void appendRow(const std::vector<std::string>& data) override;
    void resetReader() override;
    size_t lineCount() const override { return totalLines; }
    void close();
    
    StrRow parseRow(const DataRow& line) const;

    bool hasNext() const { return hasNextLine; }
};

#endif // DATAREPOSITORY_H