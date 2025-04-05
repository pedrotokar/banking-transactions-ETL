#ifndef UTILS_H
#define UTILS_H

#include <string>
#include <vector>
#include <sstream>

template<typename T>
T fromString(const std::string& str) {
    std::stringstream ss(str);
    T result;
    ss >> result;
    return result;
}

#endif