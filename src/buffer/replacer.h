#pragma once

#include "common/types.h"

#include <cstddef>

namespace naivedb::buffer {
/**
 * @brief Replacer is an abstract class that tracks page usage.
 *
 */
class Replacer {
  public:
    virtual ~Replacer() = default;

    /**
     * Remove the victim frame as defined by the replacement policy.
     * @return frame id if a victim frame is found, INVALID_FRAME_ID otherwise.
     */
    virtual frame_id_t victim() = 0;

    /**
     * Pins a frame, indicating that it should not be victimized until it is unpinned.
     * @param frame_id the id of the frame to pin
     */
    virtual void pin(frame_id_t frame_id) = 0;

    /**
     * Unpins a frame, indicating that it can now be victimized.
     * @param frame_id the id of the frame to unpin
     */
    virtual void unpin(frame_id_t frame_id) = 0;

    /** @return the number of elements in the replacer that can be victimized */
    virtual size_t size() const = 0;
};
}  // namespace naivedb::buffer