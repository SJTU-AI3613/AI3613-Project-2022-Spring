#pragma once

#include "common/constants.h"
#include "common/types.h"
#include "log/log_record.h"

#include <mutex>
#include <vector>

namespace naivedb {
namespace buffer {
class BufferManager;
}
}  // namespace naivedb

/**
 * @brief LogManager manages the Write-Ahead Log (WAL) in the system.
 *
 */
namespace naivedb::log {
class LogManager {
  public:
    /**
     * @brief Construct a new LogManager object.
     *
     * @param buffer_manager
     */
    LogManager(buffer::BufferManager *buffer_manager);

    /**
     * @brief Append a new log record into the log buffer. If the buffer is full, this will flush the buffer.
     *
     * @param record
     * @return lsn_t INVALID_LSN if the record is larger than a page. Otherwise, return the log sequence number of the
     * record.
     */
    lsn_t append_record(const LogRecord &record);

    /**
     * @brief Get a log record with log sequence number lsn.
     *
     * @param lsn
     * @return LogRecord
     */
    LogRecord get_record(lsn_t lsn);

    /**
     * @brief Flush the log buffer.
     *
     */
    void flush();

  private:
    buffer::BufferManager *buffer_manager_;

    page_id_t page_id_;
    size_t page_offset_;

    std::mutex latch_;
};
}  // namespace naivedb::log