#include "io/disk_manager.h"

#include "common/constants.h"
#include "common/exception.h"
#include "common/format.h"
#include "common/types.h"

#include <cassert>
#include <cstring>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace naivedb::io {
DiskManager::DiskManager(std::string_view file_name) : file_name_(file_name), master_page_{}, header_pages_{} {
    fd_ = open(file_name.data(), O_DIRECT | O_SYNC | O_RDWR);
    // directory or file does not exist
    if (fd_ < 0) {
        // create a new file
        fd_ = creat(file_name.data(), S_IRUSR | S_IWUSR | S_IROTH | S_IWOTH | S_IRGRP | S_IWGRP);
        close(fd_);
        // reopen with original mode
        fd_ = open(file_name.data(), O_DIRECT | O_SYNC | O_RDWR);
        if (fd_ < 0) {
            throw IOException(fmt::format("cannot open file {}", file_name));
        }
        flush_master_page();
    } else {
        read_master_page();
        size_t max_header_index = -1;
        for (size_t i = MAX_HEADER_PAGES - 1; i >= 0; --i) {
            if (master_page_[i] > 0) {
                max_header_index = i;
                break;
            }
        }
        for (size_t i = 0; i <= max_header_index; ++i) {
            read_header_page(i);
        }
    }
}

DiskManager::~DiskManager() { close(fd_); }

page_id_t DiskManager::alloc_page() {
    std::scoped_lock latch(latch_);
    size_t header_index;
    for (header_index = 0; header_index < MAX_HEADER_PAGES; ++header_index) {
        if (master_page_[header_index] < DATA_PAGES_PER_HEADER) {
            break;
        }
    }
    assert(header_index != MAX_HEADER_PAGES);

    size_t page_index;
    if (!header_pages_[header_index]) {
        page_index = 0;
        header_pages_[header_index] = std::make_unique<char[]>(PAGE_SIZE);
        set_bit(header_pages_[header_index].get(), 0);
    } else {
        for (page_index = 0; page_index < DATA_PAGES_PER_HEADER; ++page_index) {
            if (!bit(header_pages_[header_index].get(), page_index)) {
                set_bit(header_pages_[header_index].get(), page_index);
                break;
            }
        }
        assert(page_index != DATA_PAGES_PER_HEADER);
    }
    page_id_t page_id = header_index * DATA_PAGES_PER_HEADER + page_index;
    char zeros[PAGE_SIZE] = {0};
    write_page_with_offset(page_id_to_offset(page_id), zeros);
    ++master_page_[header_index];
    flush_master_page();
    flush_header_page(header_index);
    return page_id;
}

void DiskManager::free_page(page_id_t page_id) {
    std::scoped_lock latch(latch_);
    size_t header_index = page_id / DATA_PAGES_PER_HEADER;
    size_t page_index = page_id % DATA_PAGES_PER_HEADER;

    if (!header_pages_[header_index]) {
        throw IOException(fmt::format("cannot free unallocated page (page_id = {})", page_id));
    }
    if (!bit(header_pages_[header_index].get(), page_index)) {
        throw IOException(fmt::format("cannot free unallocated page (page_id = {})", page_id));
    }
    clear_bit(header_pages_[header_index].get(), page_index);
    --master_page_[header_index];
    flush_master_page();
    flush_header_page(header_index);
}

void DiskManager::read_page(page_id_t page_id, char *page_data) {
    std::scoped_lock latch(latch_);
    read_page_with_offset(page_id_to_offset(page_id), page_data);
}

void DiskManager::write_page(page_id_t page_id, const char *page_data) {
    std::scoped_lock latch(latch_);
    write_page_with_offset(page_id_to_offset(page_id), page_data);
}

bool DiskManager::page_allocated(page_id_t page_id) {
    std::scoped_lock latch(latch_);
    size_t header_index = page_id / DATA_PAGES_PER_HEADER;
    size_t page_index = page_id % DATA_PAGES_PER_HEADER;
    if (header_pages_[header_index]) {
        return bit(header_pages_[header_index].get(), page_index);
    }
    return false;
}

size_t DiskManager::file_size() {
    struct stat buf;
    if (fstat(fd_, &buf) < 0) {
        throw IOException("cannot get file size");
    }
    return buf.st_size;
}

void DiskManager::read_page_with_offset(size_t offset, char *page_data) {
    // the read buffer must be block-aligned if the file is opened with O_DIRECT
    char aligned_buffer[PAGE_SIZE] __attribute__((aligned(PAGE_SIZE))) = {0};

    if (auto file_size = this->file_size(); offset >= file_size) {
        throw IOException(fmt::format("I/O error reading past EOF (offset = {}, file_size = {})", offset, file_size));
    }
    lseek(fd_, offset, SEEK_SET);
    if (read(fd_, aligned_buffer, PAGE_SIZE) < 0) {
        throw IOException("I/O error while reading");
    }
    std::memcpy(page_data, aligned_buffer, PAGE_SIZE);
}

void DiskManager::write_page_with_offset(size_t offset, const char *page_data) {
    // the write buffer must be block-aligned if the file is opened with O_DIRECT
    char aligned_buffer[PAGE_SIZE] __attribute__((aligned(PAGE_SIZE))) = {0};

    lseek(fd_, offset, SEEK_SET);
    std::memcpy(aligned_buffer, page_data, PAGE_SIZE);
    if (write(fd_, aligned_buffer, PAGE_SIZE) < 0) {
        throw IOException("I/O error while writing");
    }
}

size_t DiskManager::page_id_to_offset(page_id_t page_id) {
    return (page_id + 2 + page_id / DATA_PAGES_PER_HEADER) * PAGE_SIZE;
}

bool DiskManager::bit(const char *bitmap, size_t i) { return bitmap[i / 8] & (1 << (i % 8)); }

void DiskManager::set_bit(char *bitmap, size_t i) { bitmap[i / 8] |= (1 << (i % 8)); }

void DiskManager::clear_bit(char *bitmap, size_t i) { bitmap[i / 8] &= ~(1 << (i % 8)); }

void DiskManager::flush_master_page() { write_page_with_offset(0, reinterpret_cast<const char *>(master_page_)); }

void DiskManager::flush_header_page(size_t index) {
    write_page_with_offset((index * DATA_PAGES_PER_HEADER + index + 1) * PAGE_SIZE, header_pages_[index].get());
}

void DiskManager::read_master_page() { read_page_with_offset(0, reinterpret_cast<char *>(master_page_)); }

void DiskManager::read_header_page(size_t index) {
    if (!header_pages_[index]) {
        header_pages_[index] = std::make_unique<char[]>(PAGE_SIZE);
    }
    read_page_with_offset((index * DATA_PAGES_PER_HEADER + index + 1) * PAGE_SIZE, header_pages_[index].get());
}

}  // namespace naivedb::io