#pragma once

#include "buffer/replacer.h"
#include "common/types.h"

#include <list>
#include <stddef.h>
#include <unordered_map>

namespace naivedb::buffer {
/**
 * @brief An implementation of the replacer with LRU replacement policy.
 *
 */
class LruReplacer : public Replacer {
  public:
    LruReplacer() = default;
    ~LruReplacer() = default;

    frame_id_t victim() override;
    void pin(frame_id_t frame_id) override;
    void unpin(frame_id_t frame_id) override;
    size_t size() const override;

  private:
    std::list<frame_id_t> frames_;
    std::unordered_map<frame_id_t, std::list<frame_id_t>::iterator> map_;
};
}  // namespace naivedb::buffer