#pragma once

#include "common/types.h"

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
DEF_EXCEPTION(TypeException);

class TransactionAbortException : public std::exception {
  public:
    TransactionAbortException(txn_id_t txn_id) : txn_id_(txn_id) {}

    txn_id_t transaction_id() const { return txn_id_; }

  private:
    txn_id_t txn_id_;
};
}  // namespace naivedb