#include "log/log_manager.h"

#include "buffer/buffer_manager.h"
#include "common/constants.h"
#include "common/exception.h"
#include "log/log_record.h"
#include "storage/page/page_guard.h"

#include <mutex>

namespace naivedb::log {
LogManager::LogManager(buffer::BufferManager *buffer_manager) : buffer_manager_(buffer_manager) {
    auto page = buffer_manager_->new_page();
    assert(page);
    page_id_ = page->page_id();
    page_offset_ = 0;
}

lsn_t LogManager::append_record(const LogRecord &record) {
    if (record.size() > PAGE_SIZE) {
        return INVALID_LSN;
    }
    std::scoped_lock latch(latch_);
    if (page_offset_ + record.size() > PAGE_SIZE) {
        buffer_manager_->flush_page(page_id_);
        auto page = buffer_manager_->new_page();
        page_id_ = page->page_id();
        page_offset_ = 0;
    }
    auto page = buffer_manager_->fetch_page(page_id_);
    record.serialize(page->data_mut() + page_offset_);
    auto old_offset = page_offset_;
    page_offset_ += record.size();
    return page_id_ * PAGE_SIZE + old_offset;
}

LogRecord LogManager::get_record(lsn_t lsn) {
    assert(lsn != INVALID_LSN);
    std::scoped_lock latch(latch_);
    page_id_t page_id = lsn / PAGE_SIZE;
    size_t offset = lsn % PAGE_SIZE;
    auto page = buffer_manager_->fetch_page(page_id);
    return LogRecord::deserialize(page->data() + offset);
}

void LogManager::flush() {
    std::scoped_lock latch(latch_);
    buffer_manager_->flush_page(page_id_);
}
}  // namespace naivedb::log