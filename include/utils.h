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

template<>
inline std::string fromString<std::string>(const std::string& str) {
    return str;
}

template<>
inline int fromString<int>(const std::string& str) {
    return std::stoi(str);
}

template<>
inline double fromString<double>(const std::string& str) {
    return std::stod(str);
}

template<>
inline float fromString<float>(const std::string& str) {
    return std::stof(str);
}

#endif