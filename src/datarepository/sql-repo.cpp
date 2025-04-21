#include <iostream>
#include <sqlite3.h>

#include "types.h"
#include "datarepository.h"

SQLiteRepository::SQLiteRepository(const std::string &dbPath)
    : db(nullptr),
      stmt(nullptr),
      dbPath(dbPath),
      batchSize(1000) {
    
    open();
}

SQLiteRepository::~SQLiteRepository() {
    close();
}

void SQLiteRepository::SQLiteRepository::open() {
    int failed = sqlite3_open(dbPath.c_str(), &db);
    if (failed) {
        std::runtime_error("Failed opening db: " + std::string(sqlite3_errmsg(db)));
    }
}

void SQLiteRepository::prepareSelect() {
    if (stmt) {
        sqlite3_finalize(stmt);
    }
    int rc = sqlite3_prepare_v2(db, selectQuery.c_str(), -1, &stmt, nullptr);
    nCols = sqlite3_column_count(stmt);
    if (rc != SQLITE_OK) {
        std::cerr << "Failed: " << sqlite3_errmsg(db) << std::endl;
    }
}

void SQLiteRepository::setTable(const std::string& tableName) {
    this->tableName = tableName;
    this->selectQuery = "SELECT * FROM '" + tableName + "';";
    prepareSelect();
}

void SQLiteRepository::createTable(const std::string& tableName, const std::string& schema) {
    std::string createTableQuery = "CREATE TABLE IF NOT EXISTS '" + tableName + 
                                   "' (" + schema + ");";

    int rc = sqlite3_exec(db, createTableQuery.c_str(), nullptr, 0, nullptr);
    if (rc != SQLITE_OK) {
        std::cerr << "Failed: " << sqlite3_errmsg(db) << std::endl;
    }
    setTable(tableName);
}

std::string SQLiteRepository::getRow() {
    checkTable();
    switch(sqlite3_step(stmt)) {
        case SQLITE_ROW: {
            std::string row;
            return row;
        }
        case SQLITE_DONE:
            done = true;
            return "";
        default:
            std::cerr << "Failed: " << sqlite3_errmsg(db) << std::endl;
            return "";
    }
}

std::string SQLiteRepository::getBatch() {
    checkTable();
    return "";
}


StrRow SQLiteRepository::parseRow(const std::string& row) {
    StrRow parsedRow;
    parsedRow.reserve(nCols);
    for (size_t i = 0; i < nCols; ++i) {
        const char* text = reinterpret_cast<const char*>(sqlite3_column_text(stmt, i));
        parsedRow.emplace_back(text);
    }
    return parsedRow;
};

std::vector<StrRow> SQLiteRepository::parseBatch(const std::string& batch) {
    std::vector<StrRow> parsedRows;
    parsedRows.reserve(batchSize);
    for (size_t i = 0; i < batchSize; ++i) {
        getRow();
        auto row = parseRow(batch);
        parsedRows.push_back(row);
    }
    return parsedRows;
}

void SQLiteRepository::appendStr(const std::string& data) {
    checkTable();
    std::string sqlInsert = "INSERT INTO " + tableName + " VALUES ";
    sqlInsert += data + ";";

    int rc = sqlite3_exec(db, sqlInsert.c_str(), nullptr, 0, nullptr);
    if (rc != SQLITE_OK) {
        std::cerr << "Failed: " << sqlite3_errmsg(db) << std::endl;
    }
}

void SQLiteRepository::appendRow(const std::vector<std::string>& data) {
    checkTable();
    std::string sqlInsert = "INSERT INTO " + tableName + " VALUES ";
    sqlInsert += "(";
    for (size_t i = 0; i < data.size(); ++i) {
        sqlInsert += data[i];
        if (i < data.size() - 1) {
            sqlInsert += ", ";
        }
    }
    sqlInsert += ");";

    int rc = sqlite3_exec(db, sqlInsert.c_str(), nullptr, 0, nullptr);
    if (rc != SQLITE_OK) {
        std::cerr << "Failed: " << sqlite3_errmsg(db) << std::endl;
    }
}

void SQLiteRepository::appendHeader(const std::vector<std::string>& data) {
    return;
}

std::string SQLiteRepository::serializeBatch(const std::vector<StrRow>& data) {
    checkTable();
    std::string values;
    for (const auto& row : data) {
        values += "(";
        for (size_t i = 0; i < row.size(); ++i) {
            values += row[i];
            if (i < row.size() - 1) {
                values += ",";
            }
        }
        values += "),";
    }
    values.pop_back();
    return values;
}

void SQLiteRepository::resetReader() {
    if (stmt) {
        sqlite3_finalize(stmt);
    }
    prepareSelect();
}

void SQLiteRepository::clear() {
    std::string deleteQuery = "DELETE FROM " + tableName + ";";
    int rc = sqlite3_exec(db, deleteQuery.c_str(), nullptr, 0, nullptr);
    if (rc != SQLITE_OK) {
        std::cerr << "Failed: " << sqlite3_errmsg(db) << std::endl;
    }
}

void SQLiteRepository::close() {
    if (stmt) {
        sqlite3_finalize(stmt);
    }
    if (db) {
        sqlite3_close(db);
    }
}
