#ifndef TYPES_HPP
#define TYPES_HPP

#include <string>
#include <vector>
#include <variant>
#include <any>

using VarCell = std::variant<int, double, std::string, std::nullptr_t>;;

using VarRow = std::vector<VarCell>;
using AnyRow = std::vector<std::any>;
using StrRow = std::vector<std::string>;

using WildRow = std::variant<StrRow, VarRow, AnyRow>;

#endif // TYPES_HPP
