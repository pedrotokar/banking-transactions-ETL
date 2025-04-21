#ifndef DATAREPOSITORY_H
#define DATAREPOSITORY_H

#include <vector>
#include <string>
#include <fstream>
#include <stdexcept>

#include <sqlite3.h>

#include "types.h"

using DataRow = std::string;

class DataRepository {
public:
    virtual ~DataRepository() = default;

    virtual std::string getRow() = 0;
    virtual std::string getBatch() = 0;

    virtual StrRow parseRow(const std::string& line) = 0;
    virtual std::vector<StrRow> parseBatch(const std::string& batch) = 0;

    virtual void appendStr(const std::string& data) = 0;
    virtual void appendRow(const std::vector<std::string>& data) = 0;
    virtual void appendHeader(const std::vector<std::string>& data) = 0;

    virtual std::string serializeBatch(const std::vector<StrRow>& data) = 0;

    virtual void resetReader() {};
    virtual void clear() {};
    virtual void close() {};
};


class FileRepository : public DataRepository {
private:
    size_t chunkSize = 1 << 27; // size of chunk in bytes
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

    std::string getRow() override;
    std::string getBatch() override;

    StrRow parseRow(const DataRow& line) override;
    std::vector<StrRow> parseBatch(const std::string& batch) override;

    void appendStr(const std::string& data) override;
    void appendRow(const std::vector<std::string>& data) override;
    void appendHeader(const std::vector<std::string>& data) override;

    std::string serializeBatch(const std::vector<StrRow>& data) override;

    bool hasNext() const { return hasNextLine; }

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

    void open();

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

    bool hasNext() const { return !done; }

    void resetReader() override;
    void clear() override;
    void close() override;
};
    
#endif // DATAREPOSITORY_H