#include "type/value.h"

#include "common/exception.h"
#include "common/format.h"
#include "common/utils.h"
#include "type/type.h"
#include "type/type_id.h"

#include <cstdint>
#include <variant>

namespace naivedb::type {
Value Value::eq(const Value &other) const {
    if (type_ != other.type_) {
        throw TypeException(fmt::format("type mismatch. left = {}, right = {}", type_, other.type_));
    }
    return std::visit(
        overload{[&](Boolean) { return Value(std::any_cast<bool>(value_) == std::any_cast<bool>(other.value_)); },
                 [&](Int) { return Value(std::any_cast<int32_t>(value_) == std::any_cast<int32_t>(other.value_)); },
                 [&](Char) {
                     return Value(std::any_cast<std::string>(value_) == std::any_cast<std::string>(other.value_));
                 }},
        type_.type_id());
}

Value Value::ne(const Value &other) const {
    if (type_ != other.type_) {
        throw TypeException(fmt::format("type mismatch. left = {}, right = {}", type_, other.type_));
    }
    return std::visit(
        overload{[&](Boolean) { return Value(std::any_cast<bool>(value_) != std::any_cast<bool>(other.value_)); },
                 [&](Int) { return Value(std::any_cast<int32_t>(value_) != std::any_cast<int32_t>(other.value_)); },
                 [&](Char) {
                     return Value(std::any_cast<std::string>(value_) != std::any_cast<std::string>(other.value_));
                 }},
        type_.type_id());
}

Value Value::lt(const Value &other) const {
    if (type_ != other.type_) {
        throw TypeException(fmt::format("type mismatch. left = {}, right = {}", type_, other.type_));
    }
    return std::visit(
        overload{
            [&](Boolean) { return Value(std::any_cast<bool>(value_) < std::any_cast<bool>(other.value_)); },
            [&](Int) { return Value(std::any_cast<int32_t>(value_) < std::any_cast<int32_t>(other.value_)); },
            [&](Char) { return Value(std::any_cast<std::string>(value_) < std::any_cast<std::string>(other.value_)); }},
        type_.type_id());
}

Value Value::le(const Value &other) const {
    if (type_ != other.type_) {
        throw TypeException(fmt::format("type mismatch. left = {}, right = {}", type_, other.type_));
    }
    return std::visit(
        overload{[&](Boolean) { return Value(std::any_cast<bool>(value_) <= std::any_cast<bool>(other.value_)); },
                 [&](Int) { return Value(std::any_cast<int32_t>(value_) <= std::any_cast<int32_t>(other.value_)); },
                 [&](Char) {
                     return Value(std::any_cast<std::string>(value_) <= std::any_cast<std::string>(other.value_));
                 }},
        type_.type_id());
}

Value Value::gt(const Value &other) const {
    if (type_ != other.type_) {
        throw TypeException(fmt::format("type mismatch. left = {}, right = {}", type_, other.type_));
    }
    return std::visit(
        overload{
            [&](Boolean) { return Value(std::any_cast<bool>(value_) > std::any_cast<bool>(other.value_)); },
            [&](Int) { return Value(std::any_cast<int32_t>(value_) > std::any_cast<int32_t>(other.value_)); },
            [&](Char) { return Value(std::any_cast<std::string>(value_) > std::any_cast<std::string>(other.value_)); }},
        type_.type_id());
}

Value Value::ge(const Value &other) const {
    if (type_ != other.type_) {
        throw TypeException(fmt::format("type mismatch. left = {}, right = {}", type_, other.type_));
    }
    return std::visit(
        overload{[&](Boolean) { return Value(std::any_cast<bool>(value_) >= std::any_cast<bool>(other.value_)); },
                 [&](Int) { return Value(std::any_cast<int32_t>(value_) >= std::any_cast<int32_t>(other.value_)); },
                 [&](Char) {
                     return Value(std::any_cast<std::string>(value_) >= std::any_cast<std::string>(other.value_));
                 }},
        type_.type_id());
}

Value Value::land(const Value &other) const {
    if (type_ != other.type_) {
        throw TypeException(fmt::format("type mismatch. left = {}, right = {}", type_, other.type_));
    }
    return std::visit(
        overload{[&](Boolean) { return Value(std::any_cast<bool>(value_) && std::any_cast<bool>(other.value_)); },
                 [&](Int) { return Value(std::any_cast<int32_t>(value_) && std::any_cast<int32_t>(other.value_)); },
                 [&](Char) {
                     throw TypeException("char type does not support land operator");
                     return Value();
                 }},
        type_.type_id());
}

Value Value::lor(const Value &other) const {
    if (type_ != other.type_) {
        throw TypeException(fmt::format("type mismatch. left = {}, right = {}", type_, other.type_));
    }
    return std::visit(
        overload{[&](Boolean) { return Value(std::any_cast<bool>(value_) || std::any_cast<bool>(other.value_)); },
                 [&](Int) { return Value(std::any_cast<int32_t>(value_) || std::any_cast<int32_t>(other.value_)); },
                 [&](Char) {
                     throw TypeException("char type does not support lor operator");
                     return Value();
                 }},
        type_.type_id());
}

Value Value::deserialize(const char *buffer, Type type) {
    return std::visit(overload{[&](Boolean) { return Value(*reinterpret_cast<const bool *>(buffer)); },
                               [&](Int) { return Value(*reinterpret_cast<const int32_t *>(buffer)); },
                               [&](Char c) {
                                   auto len = *reinterpret_cast<const uint32_t *>(buffer);
                                   return Value(c.len(), std::string_view(buffer + sizeof(uint32_t), len));
                               }},
                      type.type_id());
}

void Value::serialize(char *buffer) const {
    std::visit(overload{[&](Boolean) { *reinterpret_cast<bool *>(buffer) = std::any_cast<bool>(value_); },
                        [&](Int) { *reinterpret_cast<int32_t *>(buffer) = std::any_cast<int32_t>(value_); },
                        [&](Char) {
                            auto str = std::any_cast<std::string>(value_);
                            *reinterpret_cast<uint32_t *>(buffer) = str.size();
                            std::memcpy(buffer + sizeof(uint32_t), str.c_str(), str.size());
                        }},
               type_.type_id());
}

bool Value::operator==(const Value &other) const {
    if (type_ != other.type_) {
        return false;
    }
    return std::visit(
        overload{[&](Boolean) { return std::any_cast<bool>(value_) == std::any_cast<bool>(other.value_); },
                 [&](Int) { return std::any_cast<int32_t>(value_) == std::any_cast<int32_t>(other.value_); },
                 [&](Char) { return std::any_cast<std::string>(value_) == std::any_cast<std::string>(other.value_); }},
        type_.type_id());
}

Value Value::get_default(Type type) {
    return visit(overload{[](Boolean) { return Value(false); },
                          [](Int) { return Value(0); },
                          [](Char c) { return Value(c.len(), ""); }},
                 type.type_id());
}

Value Value::add(const Value &other) const {
    if (type_ != other.type_) {
        throw TypeException(fmt::format("type mismatch. left = {}, right = {}", type_, other.type_));
    }
    return std::visit(
        overload{[&](Boolean) { return Value(std::any_cast<bool>(value_) + std::any_cast<bool>(other.value_)); },
                 [&](Int) { return Value(std::any_cast<int32_t>(value_) + std::any_cast<int32_t>(other.value_)); },
                 [&](Char) {
                     throw TypeException("char type does not support add operator");
                     return Value();
                 }},
        type_.type_id());
}

Value Value::max(const Value &other) const {
    if (type_ != other.type_) {
        throw TypeException(fmt::format("type mismatch. left = {}, right = {}", type_, other.type_));
    }
    return std::visit(overload{[&](Boolean) {
                                   auto lhs = std::any_cast<bool>(value_);
                                   auto rhs = std::any_cast<bool>(other.value_);
                                   return Value(std::max(lhs, rhs));
                               },
                               [&](Int) {
                                   auto lhs = std::any_cast<int32_t>(value_);
                                   auto rhs = std::any_cast<int32_t>(other.value_);
                                   return Value(std::max(lhs, rhs));
                               },
                               [&](Char c) {
                                   auto lhs = std::any_cast<std::string>(value_);
                                   auto rhs = std::any_cast<std::string>(other.value_);
                                   return Value(c.len(), std::max(lhs, rhs));
                               }},
                      type_.type_id());
}

Value Value::min(const Value &other) const {
    if (type_ != other.type_) {
        throw TypeException(fmt::format("type mismatch. left = {}, right = {}", type_, other.type_));
    }
    return std::visit(overload{[&](Boolean) {
                                   auto lhs = std::any_cast<bool>(value_);
                                   auto rhs = std::any_cast<bool>(other.value_);
                                   return Value(std::min(lhs, rhs));
                               },
                               [&](Int) {
                                   auto lhs = std::any_cast<int32_t>(value_);
                                   auto rhs = std::any_cast<int32_t>(other.value_);
                                   return Value(std::min(lhs, rhs));
                               },
                               [&](Char c) {
                                   auto lhs = std::any_cast<std::string>(value_);
                                   auto rhs = std::any_cast<std::string>(other.value_);
                                   return Value(c.len(), std::min(lhs, rhs));
                               }},
                      type_.type_id());
}
}  // namespace naivedb::type

namespace std {
using namespace naivedb;
size_t hash<type::Value>::operator()(const type::Value &value) const {
    return visit(overload{
                     [&](type::Boolean) { return hash<bool>()(value.as<bool>()); },
                     [&](type::Int) { return hash<int32_t>()(value.as<int32_t>()); },
                     [&](type::Char) { return hash<string>()(value.as<string>()); },
                 },
                 value.type().type_id());
}
}  // namespace std
