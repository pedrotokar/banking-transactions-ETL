#include <iostream>
#include <memory>

#include "types.h"
#include "datarepository.h"

MemoryRepository::MemoryRepository(std::shared_ptr<DataFrame> dataframe) : df(dataframe) {};

MemoryRepository::~MemoryRepository() { clear(); };

std::string MemoryRepository::getRow() {
    if (currIdx < df->size()) {
        currIdx++;
    } else {
        done = true;
    }

    return "";
};

StrRow MemoryRepository::parseRow(const std::string& line) {
    StrRow parsedRow;
    parsedRow = df->getRow(currIdx-1);
    return parsedRow;
};

std::vector<StrRow> MemoryRepository::parseBatch(const std::string& batch) {
    std::vector<StrRow> parsedRows;
    parsedRows.reserve(batchSize);
    for (size_t i = 0; i < batchSize; ++i) {
        if (done) break;
        getRow();
        auto row = parseRow(batch);
        parsedRows.push_back(row);
    }
    return parsedRows;
};

void MemoryRepository::appendRow(const std::vector<std::string>& data) {
    df->addRow(data);
};

void MemoryRepository::appendStr(const std::string& data) {
    for (const auto& row : buffer) {
        appendRow(row);
    }
};

std::string MemoryRepository::serializeBatch(const std::vector<StrRow>& data) {
    buffer = data;
    return "";
};

bool MemoryRepository::hasNext() const {
    return !done;
};
    
void MemoryRepository::clear() {
    // if (df)
    //     df = df->emptyCopy(); 
};