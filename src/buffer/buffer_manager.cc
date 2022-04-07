#include "buffer/buffer_manager.h"

#include "buffer/lru_replacer.h"
#include "common/constants.h"
#include "common/types.h"
#include "io/disk_manager.h"
#include "storage/page/page_guard.h"

#include <cstring>
#include <mutex>

namespace naivedb::buffer {
BufferManager::BufferManager(size_t pool_size, io::DiskManager *disk_manager)
    : pool_size_(pool_size)
    , frames_(std::make_unique<BufferFrame[]>(pool_size))
    , replacer_(std::make_unique<LruReplacer>())
    , disk_manager_(disk_manager) {
    for (size_t i = 0; i < pool_size; ++i) {
        free_list_.emplace_back(i);
    }
}

std::optional<storage::PageGuard> BufferManager::fetch_page(page_id_t page_id) {
    std::scoped_lock latch(latch_);
    if (auto iter = page_table_.find(page_id); iter != page_table_.end()) {
        if (frames_[iter->second].pin_count() == 0) {
            replacer_->pin(iter->second);
        }
        frames_[iter->second].pin();
        return storage::PageGuard(
            frames_[iter->second].page(), page_id, [this, page_id](bool dirty) { unpin_page(page_id, dirty); });
    }
    if (!disk_manager_->page_allocated(page_id)) {
        return std::nullopt;
    }
    auto frame_id = get_victim_frame();
    if (frame_id == INVALID_FRAME_ID) {
        return std::nullopt;
    }
    auto &frame = frames_[frame_id];
    if (frame.dirty()) {
        disk_manager_->write_page(frame.page_id(), frame.page());
    }
    reset_frame_metadata(frame_id, page_id);
    disk_manager_->read_page(page_id, frame.page());
    frame.pin();
    return storage::PageGuard(frame.page(), page_id, [this, page_id](bool dirty) { unpin_page(page_id, dirty); });
}

std::optional<storage::PageGuard> BufferManager::new_page() {
    std::scoped_lock latch(latch_);
    auto page_id = disk_manager_->alloc_page();
    auto frame_id = get_victim_frame();
    if (frame_id == INVALID_FRAME_ID) {
        disk_manager_->free_page(page_id);
        return std::nullopt;
    }
    auto &frame = frames_[frame_id];
    if (frame.dirty()) {
        disk_manager_->write_page(frame.page_id(), frame.page());
    }
    reset_frame_metadata(frame_id, page_id);
    frame.pin();
    std::memset(frame.page(), 0, PAGE_SIZE);
    return storage::PageGuard(frame.page(), page_id, [this, page_id](bool dirty) { unpin_page(page_id, dirty); });
}

bool BufferManager::delete_page(page_id_t page_id) {
    std::scoped_lock latch(latch_);
    auto iter = page_table_.find(page_id);
    if (iter != page_table_.end()) {
        auto frame_id = iter->second;
        auto &frame = frames_[frame_id];
        if (frame.pin_count() != 0) {
            return false;
        }
        reset_frame_metadata(frame_id, INVALID_PAGE_ID);
        free_list_.emplace_back(frame_id);
    }
    disk_manager_->free_page(page_id);
    return true;
}

bool BufferManager::flush_page(page_id_t page_id) {
    std::scoped_lock latch(latch_);
    auto iter = page_table_.find(page_id);
    if (iter == page_table_.end()) {
        return false;
    }
    auto frame_id = iter->second;
    auto &frame = frames_[frame_id];
    disk_manager_->write_page(page_id, frame.page());
    frame.set_dirty(false);
    return true;
}

void BufferManager::flush_all_pages() {
    std::scoped_lock latch(latch_);
    for (auto [page_id, frame_id] : page_table_) {
        auto &frame = frames_[frame_id];
        disk_manager_->write_page(page_id, frame.page());
        frame.set_dirty(false);
    }
}

bool BufferManager::page_allocated(page_id_t page_id) {
    std::scoped_lock latch(latch_);
    return disk_manager_->page_allocated(page_id);
}

void BufferManager::unpin_page(page_id_t page_id, bool dirty) {
    std::scoped_lock latch(latch_);
    auto iter = page_table_.find(page_id);
    auto frame_id = iter->second;
    auto &frame = frames_[frame_id];
    frame.unpin();
    if (frame.pin_count() == 0) {
        replacer_->unpin(frame_id);
    }
    if (dirty) {
        frame.set_dirty(true);
    }
}

frame_id_t BufferManager::get_victim_frame() {
    if (!free_list_.empty()) {
        auto victim = free_list_.front();
        free_list_.pop_front();
        return victim;
    }
    return replacer_->victim();
}

void BufferManager::reset_frame_metadata(frame_id_t frame_id, page_id_t new_page_id) {
    auto &frame = frames_[frame_id];

    page_table_.erase(frame.page_id());
    if (new_page_id != INVALID_PAGE_ID) {
        page_table_.emplace(new_page_id, frame_id);
    }

    frame.set_page_id(new_page_id);
    frame.set_dirty(false);
}
}  // namespace naivedb::buffer