#ifndef TYPES_HPP
#define TYPES_HPP

#include <string>
#include <vector>
#include <variant>
#include <any>

#include <limits>
#include <cmath> // std::nan


template<typename T>
struct NullValue {
    static T value() {
        static_assert(sizeof(T) == 0, "NullValue not defined for this type, please define the specialization.");
    }
};

template<>
struct NullValue<std::string> {
    static std::string value() { return ""; }
};

template<>
struct NullValue<int> {
    static int value() { return std::numeric_limits<int>::min(); }
};

template<>
struct NullValue<double> {
    static double value() { return std::numeric_limits<double>::quiet_NaN(); }
};


using VarCell = std::variant<int, double, std::string, std::nullptr_t>;;

using VarRow = std::vector<VarCell>;
using AnyRow = std::vector<std::any>;
using StrRow = std::vector<std::string>;

using WildRow = std::variant<StrRow, VarRow, AnyRow>;

// server-client update
using DataBatch = std::vector<VarRow>;

#endif // TYPES_HPP
