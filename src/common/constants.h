#pragma once

#include "common/types.h"

#include <stddef.h>

namespace naivedb {
constexpr frame_id_t INVALID_FRAME_ID = -1;
constexpr page_id_t INVALID_PAGE_ID = -1;
constexpr txn_id_t INVALID_TXN_ID = -1;
constexpr slot_id_t INVALID_SLOT_ID = -1;
constexpr tuple_id_t INVALID_TUPLE_ID = -1;
constexpr table_id_t INVALID_TABLE_ID = -1;
constexpr lsn_t INVALID_LSN = -1;
constexpr column_id_t INVALID_COLUMN_ID = -1;
constexpr size_t PAGE_SIZE = 4096;
constexpr page_id_t ROOT_CATALOG_PAGE_ID = 0;
constexpr size_t MAX_TABLE_NAME_SIZE = 32;
constexpr size_t MAX_SCHEMA_SIZE = 512;
}  // namespace naivedb