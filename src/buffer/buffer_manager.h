#pragma once

#include "buffer/buffer_frame.h"
#include "buffer/replacer.h"
#include "common/macros.h"
#include "common/types.h"

#include <list>
#include <memory>
#include <mutex>
#include <optional>
#include <stddef.h>
#include <unordered_map>
#include <utility>

namespace naivedb {
namespace io {
class DiskManager;
}
namespace storage {
class PageGuard;
}
}  // namespace naivedb

namespace naivedb::buffer {
/**
 * @brief BufferManager reads disk pages to and from its internal buffer pool.
 *
 */
class BufferManager {
    DISALLOW_COPY_AND_MOVE(BufferManager)

  public:
    BufferManager(size_t pool_size, io::DiskManager *disk_manager);

    /**
     * @brief Get the size of the buffer pool.
     *
     * @return size_t
     */
    size_t size() const { return pool_size_; }

    /**
     * @brief Fetch a page from the buffer pool and pin it. Return the page if it has been loaded in memory. Otherwise,
     * load the page from disk to memory and return it.
     *
     * @param page_id
     * @return std::optional<storage::PageGuard>
     */
    std::optional<storage::PageGuard> fetch_page(page_id_t page_id);

    /**
     * @brief Allocate a new page on the disk and fetch it.
     *
     * @return std::optional<storage::PageGuard>
     */
    std::optional<storage::PageGuard> new_page();

    /**
     * @brief Deallocate a page. The page cannot be used after this call.
     *
     * @param page_id
     * @return true if the page is deallocated successfully
     * @return false if the page is pinned
     */
    bool delete_page(page_id_t page_id);

    /**
     * @brief Flush the page to the disk.
     *
     * @param page_id
     * @return true
     * @return false if the page is not in the buffer pool
     */
    bool flush_page(page_id_t page_id);

    /**
     * @brief Flush all the pages in the buffer pool to disk.
     *
     */
    void flush_all_pages();

    /**
     * @brief Check whether the given page is allocated.
     *
     * @param page_id
     * @return true
     * @return false
     */
    bool page_allocated(page_id_t page_id);

  private:
    /**
     * @brief Unpin the page from the buffer pool.
     * @warning This method should be called by PageGuard. Do not use this manually!
     *
     * @param page_id
     * @param dirty
     */
    void unpin_page(page_id_t page_id, bool dirty);

    frame_id_t get_victim_frame();
    void reset_frame_metadata(frame_id_t frame_id, page_id_t new_page_id);

    const size_t pool_size_;

    std::unique_ptr<BufferFrame[]> frames_;
    std::unique_ptr<Replacer> replacer_;
    io::DiskManager *disk_manager_;
    std::unordered_map<page_id_t, frame_id_t> page_table_;
    std::list<frame_id_t> free_list_;
    std::mutex latch_;
};
}  // namespace naivedb::buffer