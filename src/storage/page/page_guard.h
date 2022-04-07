#pragma once

#include "common/constants.h"
#include "common/macros.h"
#include "common/types.h"

#include <cstring>
#include <functional>

namespace naivedb::storage {
/**
 * @brief PageGuard is a wrapper of a raw page in memory. With RAII technique, the page can be unpinned automatically
 * when going out of scope.
 *
 */
class PageGuard {
    DISALLOW_COPY(PageGuard)

  public:
    PageGuard() : data_(nullptr), page_id_(INVALID_PAGE_ID), dirty_(false), unpin_callback_(NOP_CALLBACK) {}

    PageGuard(char *data, page_id_t page_id, std::function<void(bool)> &&unpin_callback)
        : data_(data), page_id_(page_id), dirty_(false), unpin_callback_(std::move(unpin_callback)) {}

    PageGuard(PageGuard &&page_guard)
        : data_(page_guard.data_)
        , page_id_(page_guard.page_id_)
        , dirty_(page_guard.dirty_)
        , unpin_callback_(std::move(page_guard.unpin_callback_)) {
        page_guard.unpin_callback_ = NOP_CALLBACK;
    }

    PageGuard &operator=(PageGuard &&page_guard) {
        unpin_callback_(dirty_);
        data_ = page_guard.data_;
        page_id_ = page_guard.page_id_;
        dirty_ = page_guard.dirty_;
        unpin_callback_ = std::move(page_guard.unpin_callback_);
        page_guard.unpin_callback_ = NOP_CALLBACK;
        return *this;
    }

    ~PageGuard() { unpin_callback_(dirty_); }

    const char *data() const { return data_; }

    char *data_mut() {
        dirty_ = true;
        return data_;
    }

    void clear() {
        dirty_ = true;
        std::memset(data_, 0, PAGE_SIZE);
    }

    page_id_t page_id() const { return page_id_; }

  private:
    char *data_;
    page_id_t page_id_;
    bool dirty_;
    std::function<void(bool)> unpin_callback_;

    static constexpr auto NOP_CALLBACK = [](bool) {};
};
}  // namespace naivedb::storage