
#include "datarepository.h"
#include "types.h"
#include <sstream>

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
    std::ofstream outputFile(fileName, std::ios::app);
    if (!outputFile.is_open()) {
        throw std::runtime_error("Failed opening file: " + fileName);
    }
    outputFile << data << "\n";
}

void FileRepository::appendRow(const std::vector<std::string>& data) {
    std::ofstream outputFile(fileName, std::ios::app);
    std::string dataStr;
    std::ostringstream oss;
    for (size_t i=0; i < data.size(); ++i) {
        if (i) {
            oss << ",";
        }
        oss << data[i];
    }
    if (!outputFile.is_open()) {
        throw std::runtime_error("Failed opening file: " + fileName);
    }
    outputFile << "\n" << oss.str();
}

StrRow FileRepository::parseRow(const DataRow& line) const {
    StrRow parsedRow;
    std::stringstream ss(line);
    std::string item;
    while (std::getline(ss, item, separator[0])) {
        parsedRow.push_back(item);
    }
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