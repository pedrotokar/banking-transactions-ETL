#include <iostream>

#include "types.h"
#include "datarepository.h"

FileRepository::FileRepository(const std::string& fname,
                               const std::string& sep,
                               bool hasHeader)
                               : fileName(fname),
                                 separator(sep),
                                 hasHeader(hasHeader),
                                 currentReadLine(0),
                                 totalLines(0) {

    buffer.resize(chunkSize);
    outFile.open(fileName, std::ios::app | std::ios::out);
    inFile.open(fileName);
    if (!inFile.is_open() || !outFile.is_open()) {
        throw std::runtime_error("Failed opening file: " + fileName);
    }
    if (hasHeader) {
        std::getline(inFile, currLine);
    }
}

FileRepository::~FileRepository() {
    if (inFile.is_open()) {
        inFile.close();
    }
    if (outFile.is_open()) {
        outFile.close();
    }
}

DataRow FileRepository::getRow() {
    if (!inFile.is_open()) {
        throw std::runtime_error("File not open: " + fileName);
    }

    if (std::getline(inFile, currLine)) {
        currentReadLine++;
        return currLine;
    } else {
        hasNextLine = false;
        return "";
    }
}

std::string FileRepository::getBatch() {
    std::string leftover;

    size_t startPos = inFile.tellg();
    inFile.read(buffer.data(), chunkSize);
    size_t bytesRead = inFile.gcount();

    if (bytesRead == 0) { hasNextLine = false; return ""; }

    std::string chunkData = std::string(buffer.data(), bytesRead);

    size_t newlinePos = chunkData.find_last_of('\n');
    if (newlinePos != std::string::npos) {
        chunkData = chunkData.substr(0, newlinePos+1);
        inFile.seekg(startPos + newlinePos + 1);
    } else {
        inFile.seekg(startPos + bytesRead);
    }

    // buffer.clear();
    return chunkData;
}

void FileRepository::appendStr(const std::string& data) {
    if (!outFile.is_open()) {
        throw std::runtime_error("Failed opening file: " + fileName);
    }
    outFile << data << "\n";
}

void FileRepository::appendRow(const std::vector<std::string>& data) {
    if (!outFile.is_open()) {
        throw std::runtime_error("Failed opening file: " + fileName);
    }
    // outFile.clear(); 
    for (size_t i=0; i < data.size(); ++i) {
        if (i)  outFile << ",";
        outFile << data[i];
    }
    outFile << "\n";
}

void FileRepository::appendHeader(const std::vector<std::string>& data) {
    appendRow(data);
}

std::string FileRepository::serializeBatch(const std::vector<StrRow>& data) {
    std::string values;
    for (const auto& row : data) {
        for (size_t i = 0; i < row.size(); ++i) {
            values += row[i];
            if (i < row.size() - 1) {
                values += ",";
            }
        }
        values += "\n";
    }
    values.pop_back();
    return values;
}

StrRow FileRepository::parseRow(const DataRow& line) {
    StrRow parsedRow;
    parsedRow.reserve(3); /// TODO: Adicionar um parametro pro tamanho

    size_t start = 0;
    size_t end = 0;

    while ((end = line.find(separator, start)) != std::string::npos) {
        parsedRow.emplace_back(line.substr(start, end - start));
        start = end + separator.length();
    }
    parsedRow.emplace_back(line.substr(start));
    return parsedRow;
}

std::vector<StrRow> FileRepository::parseBatch(const std::string& batch) {
    std::vector<StrRow> rows;
    StrRow row;

    size_t start = 0;
    size_t end = 0;

    while ((end = batch.find('\n', start)) != std::string::npos) {
        row = parseRow(batch.substr(start, end - start));
        rows.push_back(row);
        row.clear();
        start = end + 1;
    }
    return rows;
}

void FileRepository::close() {
    if (outFile.is_open()) {
        outFile.close();
    }
    if (inFile.is_open()) {
        inFile.close();
    }
}

void FileRepository::resetReader() {
    if (inFile.is_open()) {
        inFile.close();
    }
    inFile.open(fileName);
    currentReadLine = 0;
}

void FileRepository::clear() {
    std::ofstream ofs;
    ofs.open(fileName, std::ofstream::out | std::ofstream::trunc);
    ofs.close();
}
