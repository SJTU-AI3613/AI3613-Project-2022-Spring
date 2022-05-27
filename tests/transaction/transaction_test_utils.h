#pragma once

#define BUILD_ENV                                                                                                      \
    remove("test.db");                                                                                                 \
    remove("test.log.db");                                                                                             \
    io::DiskManager disk_manager("test.db");                                                                           \
    io::DiskManager log_disk_manager("test.log.db");                                                                   \
    buffer::BufferManager buffer_manager(16, &disk_manager);                                                           \
    buffer::BufferManager log_buffer_manager(16, &log_disk_manager);                                                   \
    catalog::Catalog catalog(&buffer_manager);                                                                         \
    lock::LockManager lock_manager(true);                                                                              \
    log::LogManager log_manager(&log_buffer_manager);                                                                  \
    transaction::TransactionManager txn_manager(&lock_manager, &log_manager, &buffer_manager);
