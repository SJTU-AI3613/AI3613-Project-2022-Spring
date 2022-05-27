#pragma once

#include "common/constants.h"
#include "common/types.h"

#include <shared_mutex>

namespace naivedb::buffer {
/**
 * @brief BufferFrame is the container of disk pages and page metadata in memory.
 *
 */
class BufferFrame {
  public:
    BufferFrame() : page_id_(INVALID_PAGE_ID), pin_count_(0), dirty_(false) {}

    uint32_t pin_count() const { return pin_count_; }
    void pin() { ++pin_count_; }
    void unpin() { --pin_count_; }

    char *page() { return page_; }

    page_id_t page_id() const { return page_id_; }
    void set_page_id(page_id_t page_id) { page_id_ = page_id; }

    bool dirty() const { return dirty_; }
    void set_dirty(bool dirty) { dirty_ = dirty; }

    std::shared_mutex &rwlatch() { return rwlatch_; }

  private:
    char page_[PAGE_SIZE];

    page_id_t page_id_;
    uint32_t pin_count_;
    bool dirty_;

    std::shared_mutex rwlatch_;
};
}  // namespace naivedb::buffer