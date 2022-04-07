#include "buffer/lru_replacer.h"

#include "common/constants.h"
#include "common/types.h"

#include <utility>

namespace naivedb::buffer {
frame_id_t LruReplacer::victim() {
    if (frames_.empty()) {
        return INVALID_FRAME_ID;
    }
    auto victim = frames_.back();
    frames_.pop_back();
    map_.erase(victim);
    return victim;
}

void LruReplacer::pin(frame_id_t frame_id) {
    if (auto iter = map_.find(frame_id); iter != map_.end()) {
        frames_.erase(iter->second);
        map_.erase(iter);
    }
}

void LruReplacer::unpin(frame_id_t frame_id) {
    if (auto iter = map_.find(frame_id); iter == map_.end()) {
        frames_.push_front(frame_id);
        map_.insert({frame_id, frames_.begin()});
    }
}

size_t LruReplacer::size() const { return frames_.size(); }
}  // namespace naivedb::buffer