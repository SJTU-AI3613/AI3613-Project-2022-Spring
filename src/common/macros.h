#pragma once

#include <common/exception.h>
#include <fmt/format.h>

namespace naivedb {
#define STRINGIZE_DETAIL(x) #x
#define STRINGIZE(x) STRINGIZE_DETAIL(x)

#define UNIMPLEMENTED throw NotImplementedException(__FILE__ ":" STRINGIZE(__LINE__) ": not implemented.")
#define UNREACHABLE throw UnreachableException(__FILE__ ":" STRINGIZE(__LINE__) ": entered unreachable code.")

#define DISALLOW_COPY(cname)                                                                                           \
    cname(const cname &) = delete;                                                                                     \
    cname &operator=(const cname &) = delete;

#define DISALLOW_MOVE(cname)                                                                                           \
    cname(cname &&) = delete;                                                                                          \
    cname &operator=(cname &&) = delete;

#define DISALLOW_COPY_AND_MOVE(cname)                                                                                  \
    DISALLOW_COPY(cname)                                                                                               \
    DISALLOW_MOVE(cname)
}  // namespace naivedb