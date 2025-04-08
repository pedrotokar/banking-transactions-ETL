
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

void FileRepository::appendRow(const DataRow& data) {
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