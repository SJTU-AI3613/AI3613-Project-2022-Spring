#include "query/execution/executor/seq_scan_executor.h"

#include "catalog/catalog.h"
#include "catalog/table_info.h"
#include "common/utils.h"
#include "lock/lock_manager.h"
#include "storage/table/table_heap.h"
#include "storage/tuple/tuple.h"
#include "transaction/transaction.h"

namespace naivedb::query {
void SeqScanExecutor::init() {
    // TODO(Project-1): Implement this method
    UNIMPLEMENTED;
}

std::vector<storage::Tuple> SeqScanExecutor::next() {
    // TODO(Project-1): Implement this method
    UNIMPLEMENTED;
    return {};
}
}  // namespace naivedb::query