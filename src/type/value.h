#pragma once

#include "common/format.h"
#include "common/utils.h"
#include "type/type.h"

#include <any>
#include <string_view>

namespace naivedb::type {
class Value {
  public:
    // Invalid
    Value() = default;

    // Boolean
    explicit Value(bool value) : type_(Type(Boolean())), value_(value) {}

    // Int
    explicit Value(int32_t value) : type_(Type(Int())), value_(value) {}

    // Char
    explicit Value(uint32_t len, std::string_view value)
        : type_(Type(Char(len))), value_(std::string(value.substr(0, len))) {}

    bool operator==(const Value &other) const;

    bool operator!=(const Value &other) const { return !(*this == other); }

    Type type() const { return type_; }

    size_t size() const { return type_.size(); }

    template <typename T>
    T as() const {
        return std::any_cast<T>(value_);
    }

    static Value get_default(Type type);

    Value eq(const Value &other) const;
    Value ne(const Value &other) const;
    Value lt(const Value &other) const;
    Value le(const Value &other) const;
    Value gt(const Value &other) const;
    Value ge(const Value &other) const;
    Value land(const Value &other) const;
    Value lor(const Value &other) const;

    Value add(const Value &other) const;
    Value max(const Value &other) const;
    Value min(const Value &other) const;

    /**
     * @brief Get a value object by deserializing from the given buffer
     *
     * @param buffer
     * @param type
     * @return Value
     */
    static Value deserialize(const char *buffer, Type type);

    /**
     * @brief Serialize the value to the given buffer
     *
     * @param buffer
     */
    void serialize(char *buffer) const;

  private:
    Type type_;
    std::any value_;
};
}  // namespace naivedb::type

namespace std {
template <>
struct hash<naivedb::type::Value> {
    size_t operator()(const naivedb::type::Value &value) const;
};
}  // namespace std

namespace fmt {
template <>
struct formatter<naivedb::type::Value> : public naivedb_base_formatter {
    template <typename FormatContext>
    auto format(const naivedb::type::Value &obj, FormatContext &ctx) -> decltype(ctx.out()) {
        using namespace naivedb;
        using namespace type;
        return std::visit(
            overload{
                [&](Boolean) {
                    return format_to(ctx.out(), "Value {{ type_: {}, value_: {} }}", obj.type(), obj.as<bool>());
                },
                [&](Int) {
                    return format_to(ctx.out(), "Value {{ type_: {}, value_: {} }}", obj.type(), obj.as<int32_t>());
                },
                [&](Char) {
                    return format_to(ctx.out(), "Value {{ type_: {}, value_: {} }}", obj.type(), obj.as<std::string>());
                },
            },
            obj.type().type_id());
    }
};
}  // namespace fmt
