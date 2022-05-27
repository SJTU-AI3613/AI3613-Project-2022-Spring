#pragma once

#include "common/macros.h"

#include <functional>
#include <list>
#include <thread>
#include <vector>

namespace naivedb {
class TaskQueue {
    DISALLOW_COPY_AND_MOVE(TaskQueue)

  public:
    TaskQueue() : next_barrier_id_(0) {}

    TaskQueue &push(std::function<void()> &&task) {
        tasks_.emplace_back(std::move(task));
        return *this;
    }

    void wait() {
        for (auto &&task : tasks_) {
            workers_.emplace_back([task = std::move(task)]() { task(); });
        }
        for (auto &worker : workers_) {
            worker.join();
        }
        reset();
    }

    void reset() {
        next_barrier_id_ = 0;
        workers_.clear();
        tasks_.clear();
    }

  private:
    size_t next_barrier_id_;
    std::vector<std::thread> workers_;
    std::list<std::function<void()>> tasks_;
};
}  // namespace naivedb