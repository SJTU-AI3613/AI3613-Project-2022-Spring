#pragma once

#include <cstddef>
#include <utility>
#include <vector>

namespace naivedb {
template <typename... Ts>
struct overload : Ts... {
    using Ts::operator()...;
};

template <typename... Ts>
overload(Ts...) -> overload<Ts...>;

template <typename T, typename...>
struct First {
    using type = T;
};
template <typename... Ts>
using first = typename First<Ts...>::type;

template <typename T>
constexpr std::vector<T> make_vector_inner(std::vector<T> &&vec) {
    return std::move(vec);
}

template <typename T, typename... Ts>
constexpr std::vector<T> make_vector_inner(std::vector<T> &&vec, T &&first_element, Ts &&...elements) {
    vec.emplace_back(std::move(first_element));
    return make_vector_inner(std::move(vec), std::move(elements)...);
}

template <typename... Ts>
constexpr std::vector<first<Ts...>> make_vector(Ts &&...elements) {
    return make_vector_inner({}, std::move(elements)...);
}
}  // namespace naivedb