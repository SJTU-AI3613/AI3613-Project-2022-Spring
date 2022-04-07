#pragma once

#include <cstdint>

namespace naivedb {
using frame_id_t = int64_t;   // frame id type
using page_id_t = int64_t;    // page id type
using txn_id_t = int64_t;     // transaction id type
using lsn_t = int64_t;        // log sequence number type
using slot_id_t = int32_t;    // slot id type
using tuple_id_t = int64_t;   // tuple id type
using table_id_t = int64_t;   // table id type
using column_id_t = int32_t;  // column id type
}  // namespace naivedb