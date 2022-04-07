#pragma once

#include <stdexcept>
#include <string>

namespace naivedb {
#define DEF_EXCEPTION(name)                                                                                            \
    class name : public std::runtime_error {                                                                           \
      public:                                                                                                          \
        explicit name(const std::string &msg) : std::runtime_error(msg) {}                                             \
    }

DEF_EXCEPTION(NotImplementedException);
DEF_EXCEPTION(UnreachableException);
DEF_EXCEPTION(IOException);
DEF_EXCEPTION(QueryException);
DEF_EXCEPTION(StorageException);
DEF_EXCEPTION(TypeException);
}  // namespace naivedb