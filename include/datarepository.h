#ifndef DATAREPOSITORY_H
#define DATAREPOSITORY_H

#include <vector>
#include <string>
#include <fstream>
#include <stdexcept>
#include <memory>

#include <sqlite3.h>

#include "dataframe.h"
#include "types.h"

using DataRow = std::string;

class DataRepository {
public:
    virtual ~DataRepository() = default;

    virtual std::string getRow() { return ""; };
    virtual std::string getBatch() { return ""; };

    virtual StrRow parseRow(const std::string& line) { return {}; };
    virtual std::vector<StrRow> parseBatch(const std::string& batch) { return {}; };

    virtual void appendStr(const std::string& data) {};
    virtual void appendRow(const std::vector<std::string>& data) {};
    virtual void appendHeader(const std::vector<std::string>& data) {};

    virtual std::string serializeBatch(const std::vector<StrRow>& data) { return ""; };

    virtual bool hasNext() const = 0;
    
    virtual void resetReader() {};
    virtual void clear() {};
    virtual void close() {};

    virtual void open() {};
};


class FileRepository : public DataRepository {
private:
    size_t chunkSize = 1 << 20; // size of chunk in bytes
    std::vector<char> buffer;

    std::string fileName;
    std::string separator;
    bool hasHeader;
    
    std::ifstream inFile;
    std::ofstream outFile;
    size_t currentReadLine;
    size_t totalLines;
    std::string currLine;
    
    /// TODO: change flag implementation
    bool hasNextLine = true; // Flag: the current line is !EOF (?) terrible name I know 
    
public:
    FileRepository(const std::string& fname,
                   const std::string& sep,
                   bool hasHeader = true);
    
    ~FileRepository() override;

    void open() override;

    std::string getRow() override;
    std::string getBatch() override;

    StrRow parseRow(const DataRow& line) override;
    std::vector<StrRow> parseBatch(const std::string& batch) override;

    void appendStr(const std::string& data) override;
    void appendRow(const std::vector<std::string>& data) override;
    void appendHeader(const std::vector<std::string>& data) override;

    std::string serializeBatch(const std::vector<StrRow>& data) override;

    bool hasNext() const override { return hasNextLine; }

    void resetReader() override;
    void close() override;
    void clear() override;
};


class SQLiteRepository : public DataRepository { 
private:
    sqlite3* db;
    sqlite3_stmt* stmt;

    std::string dbPath;
    std::string tableName;

    size_t nCols;

    std::string selectQuery;
    std::string insertQuery;

    size_t batchSize = 1000;
    bool done = false;

    void checkTable() {
        if (tableName.empty()) 
            throw std::runtime_error("You must set or create a table first.");
    }
public:
    SQLiteRepository(const std::string& dbPath);
    ~SQLiteRepository();

    void open() override;

    void prepareSelect();

    void setTable(const std::string& tableName);
    void createTable(const std::string& tableName, const std::string& schema);

    std::string getRow() override;
    std::string getBatch() override;

    StrRow parseRow(const std::string& row) override;
    std::vector<StrRow> parseBatch(const std::string& batch) override;

    void appendStr(const std::string& data) override;
    void appendRow(const std::vector<std::string>& data) override;
    void appendHeader(const std::vector<std::string>& data) override;

    std::string serializeBatch(const std::vector<StrRow>& data) override;

    bool hasNext() const override { return !done; }

    void resetReader() override;
    void clear() override;
    void close() override;
};


class MemoryRepository : public DataRepository {
private:
    std::shared_ptr<DataFrame> df;
    size_t batchSize = 750;
    size_t currIdx = 0;
    bool done = false;

    std::vector<StrRow> buffer;
public:
    MemoryRepository(std::shared_ptr<DataFrame> dataframe);
    ~MemoryRepository();

    std::string getRow() override;

    StrRow parseRow(const std::string& line) override;

    std::vector<StrRow> parseBatch(const std::string& batch) override;

    void appendRow(const std::vector<std::string>& data) override;

    void appendStr(const std::string& data) override;

    std::string serializeBatch(const std::vector<StrRow>& data) override;

    bool hasNext() const override;
    void clear() override;
};
    
#endif // DATAREPOSITORY_H
