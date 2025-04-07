
#include "datarepository.h"
#include "types.h"
#include <sstream>
#include <iostream>

FileRepository::FileRepository(const std::string& fname,
                               const std::string& sep,
                               bool hasHeader)
                               : fileName(fname),
                                 separator(sep),
                                 hasHeader(hasHeader),
                                 currentReadLine(0),
                                 totalLines(0) {

    file.open(fileName, std::ios::out | std::ios::in | std::ios::app); 
    if (!file.is_open()) {
        throw std::runtime_error("Failed opening file: " + fileName);
    }
    if (hasHeader) {
        std::getline(file, currLine);
    }
}

FileRepository::~FileRepository() {
    if (file.is_open()) {
        file.close();
    }
}

DataRow FileRepository::getRow() {
    if (!file.is_open()) {
        throw std::runtime_error("File not open: " + fileName);
    }

    if (std::getline(file, currLine)) {
        currentReadLine++;
        return currLine;
    } else {
        hasNextLine = false;
        return "";
    }
}

void FileRepository::appendRow(const DataRow& data) {
    if (!file.is_open()) {
        throw std::runtime_error("Failed opening file: " + fileName);
    }
    file << data << "\n";
}

void FileRepository::appendRow(const std::vector<std::string>& data) {
    if (!file.is_open()) {
        throw std::runtime_error("Failed opening file: " + fileName);
    }
    /// TODO: Verificar estado do file, tem algo estranho rolando aqui
    file.clear(); 
    for (size_t i=0; i < data.size(); ++i) {
        if (i)  file << ",";
        file << data[i];
    }
    file << "\n";
}

void FileRepository::appendHeader(const std::vector<std::string>& data) {
    appendRow(data);
}

StrRow FileRepository::parseRow(const DataRow& line) const {
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

void FileRepository::close() {
    if (file.is_open()) {
        file.close();
    }
}

void FileRepository::resetReader() {
    if (file.is_open()) {
        file.close();
    }
    file.open(fileName);
    currentReadLine = 0;
}

void FileRepository::clear(){
    std::ofstream outputFile(fileName, std::ios::out | std::ios::trunc);

    if (!outputFile.is_open()) {
        throw std::runtime_error("Failed opening file: " + fileName);
    }
}