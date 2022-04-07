#include "buffer/lru_replacer.h"
#include "common/constants.h"
#include "test_utils.h"

using namespace naivedb;

int main() {
    buffer::LruReplacer replacer;

    replacer.unpin(1);
    replacer.unpin(2);
    replacer.unpin(3);
    replacer.unpin(4);
    replacer.unpin(5);
    replacer.unpin(6);
    replacer.unpin(1);
    TEST_ASSERT_EQ(replacer.size(), 6);

    auto victim = replacer.victim();
    TEST_ASSERT_EQ(victim, 1);
    victim = replacer.victim();
    TEST_ASSERT_EQ(victim, 2);
    victim = replacer.victim();
    TEST_ASSERT_EQ(victim, 3);

    replacer.pin(3);
    replacer.pin(4);
    TEST_ASSERT_EQ(replacer.size(), 2);

    replacer.unpin(4);
    victim = replacer.victim();
    TEST_ASSERT_EQ(victim, 5);
    victim = replacer.victim();
    TEST_ASSERT_EQ(victim, 6);
    victim = replacer.victim();
    TEST_ASSERT_EQ(victim, 4);
    victim = replacer.victim();
    TEST_ASSERT_EQ(victim, naivedb::INVALID_FRAME_ID);

    return EXIT_SUCCESS;
}