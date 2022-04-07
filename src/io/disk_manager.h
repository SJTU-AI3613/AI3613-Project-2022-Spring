#pragma once

#include "common/constants.h"
#include "common/exception.h"
#include "common/macros.h"
#include "common/types.h"

#include <array>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <vector>

namespace naivedb::io {
/**
 * @brief An implementation of a disk space manager with two levels of header pages, allowing for (with page size of 4K)
 * 256G worth of data at most.
 *
 * MasterPage layout (4096 / 2 = 2048 entries):
 *  -----------------------------------------------------------------------
 * | N_HeaderPage_0 (2) | N_HeaderPage_1 (2) | ... | N_HeaderPage_2047 (2) |
 *  -----------------------------------------------------------------------
 *
 * HeaderPage_i layout (4096 * 8 = 32768 bits):
 *  -------------------------------------------------------------------------------------------------------------------
 * | DataPage_(32768*i) | ... | DataPage_(32768*i+7) | ... | DataPage_(32768*i+32760) | ... | DataPage_(32768*i+32767) |
 *  -------------------------------------------------------------------------------------------------------------------
 * |<------------------  8bits  -------------------->|
 *
 * Each header page stores a bitmap, indicating whether each of the data pages has been allocated, and manages 4K * 8 =
 * 32K pages. The master page stores 16-bit integers for each of the header pages indicating the number of data pages
 * that have been allocated under the header page (managing 4K / 2 = 2K header pages). A single file may therefore
 * have a maximum of 2K * 32K = 64M data pages.
 *
 * Master and header pages are cached permanently in memory; changes to these are immediately flushed to disk. This
 * caching is done separately from the buffer manager's caching.
 *
 * The file should be stored in the following manner:
 * - the master page is the 0th page of the file
 * - the first header page is the 1st page of the file
 * - the next 32K pages are data pages managed by the first header page
 * - the second header page follows
 * - the next 32K pages are data pages managed by the second header page
 * - etc.
 *
 * File layout:
 *  -----------------------------------------------------------------------------------------------
 * | MasterPage | HeaderPage_0 | DataPage_0 | ... | DataPage_31 | HeaderPage_1 | DataPage_32 | ... |
 *  -----------------------------------------------------------------------------------------------
 */
class DiskManager {
    DISALLOW_COPY_AND_MOVE(DiskManager)

  public:
    explicit DiskManager(std::string_view file_name);

    ~DiskManager();

    /**
     * @brief Allocate a new page
     *
     * @return page_id_t
     */
    page_id_t alloc_page();

    /**
     * @brief Deallocate a page
     *
     * @param page_id
     */
    void free_page(page_id_t page_id);

    /**
     * @brief Read data from a page
     *
     * @param page_id
     * @param page_data
     */
    void read_page(page_id_t page_id, char *page_data);

    /**
     * @brief Write data to a page
     *
     * @param page_id
     * @param page_data
     */
    void write_page(page_id_t page_id, const char *page_data);

    /**
     * @brief Check whether the given page is allocated
     *
     * @param page_id
     * @return true
     * @return false
     */
    bool page_allocated(page_id_t page_id);

  private:
    static constexpr size_t MAX_HEADER_PAGES = 2048;
    static constexpr uint16_t DATA_PAGES_PER_HEADER = 32768;

    size_t file_size();

    void read_page_with_offset(size_t offset, char *page_data);
    void write_page_with_offset(size_t offset, const char *page_data);

    size_t page_id_to_offset(page_id_t page_id);

    bool bit(const char *bitmap, size_t i);
    void set_bit(char *bitmap, size_t i);
    void clear_bit(char *bitmap, size_t i);

    void flush_master_page();
    void flush_header_page(size_t index);

    void read_master_page();
    void read_header_page(size_t index);

    std::string file_name_;
    int fd_;

    uint16_t master_page_[MAX_HEADER_PAGES];
    std::unique_ptr<char[]> header_pages_[MAX_HEADER_PAGES];

    std::mutex latch_;
};
}  // namespace naivedb::io