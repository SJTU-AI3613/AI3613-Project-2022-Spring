#pragma once

#include <cstdio>
#include <cstdlib>

#define TEST_ASSERT(cond)                                                                                              \
    do {                                                                                                               \
        if (!(cond)) {                                                                                                 \
            printf("%s:%d: %s: Assertion `%s` failed.\n", __FILE__, __LINE__, __PRETTY_FUNCTION__, #cond);             \
            exit(EXIT_FAILURE);                                                                                        \
        }                                                                                                              \
    } while (0)

#define TEST_ASSERT_EQ(a, b)                                                                                           \
    do {                                                                                                               \
        if ((a) != (b)) {                                                                                              \
            printf("%s:%d: %s: Assertion `%s == %s` failed.\n", __FILE__, __LINE__, __PRETTY_FUNCTION__, #a, #b);      \
            exit(EXIT_FAILURE);                                                                                        \
        }                                                                                                              \
    } while (0)

#define TEST_ASSERT_NE(a, b)                                                                                           \
    do {                                                                                                               \
        if ((a) == (b)) {                                                                                              \
            printf("%s:%d: %s: Assertion `%s != %s` failed.\n", __FILE__, __LINE__, __PRETTY_FUNCTION__, #a, #b);      \
            exit(EXIT_FAILURE);                                                                                        \
        }                                                                                                              \
    } while (0)
