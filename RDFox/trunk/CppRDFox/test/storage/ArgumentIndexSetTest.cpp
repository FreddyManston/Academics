// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifdef WITH_TEST

#define SUITE_NAME        ArgumentIndexSetTest

#include <CppTest/AutoTest.h>

#include "../../src/storage/ArgumentIndexSet.h"

static void doTest(const size_t numberOfVariables) {
    ArgumentIndexSet set1;
    ArgumentIndexSet set2;
    ArgumentIndexSet set3;
    ASSERT_EQUAL(0, set1.size());
    ASSERT_EQUAL(0, set2.size());
    ASSERT_EQUAL(0, set3.size());
    size_t passes = 0;
    for (ArgumentIndex argumentIndex = 10; argumentIndex < numberOfVariables; argumentIndex += 64) {
        passes++;
        // add elements to set1
        ASSERT_FALSE(set1.contains(argumentIndex));
        ASSERT_FALSE(set1.contains(argumentIndex + 2));
        set1.add(argumentIndex);
        set1.add(argumentIndex + 2);
        ASSERT_TRUE(set1.contains(argumentIndex));
        ASSERT_TRUE(set1.contains(argumentIndex + 2));
        // add elements to set2
        ASSERT_FALSE(set2.contains(argumentIndex));
        set2.add(argumentIndex);
        ASSERT_TRUE(set2.contains(argumentIndex));
        // add elements to set3
        ASSERT_FALSE(set3.contains(argumentIndex + 2));
        ASSERT_FALSE(set3.contains(argumentIndex + 4));
        set3.add(argumentIndex + 2);
        set3.add(argumentIndex + 4);
        ASSERT_TRUE(set3.contains(argumentIndex + 2));
        ASSERT_TRUE(set3.contains(argumentIndex + 4));
    }
    ASSERT_EQUAL(2 * passes, set1.size());
    ASSERT_EQUAL(passes, set2.size());
    ASSERT_EQUAL(2 * passes, set3.size());
    ASSERT_TRUE(set1.contains(set1));
    ASSERT_TRUE(set1.contains(set2));
    ASSERT_EQUAL(numberOfVariables == 0, set1.contains(set3));
    set1.intersectWith(set2);
    ASSERT_EQUAL(passes, set1.size());
    for (ArgumentIndex argumentIndex = 10; argumentIndex < numberOfVariables; argumentIndex += 64) {
        ASSERT_TRUE(set1.contains(argumentIndex));
        ASSERT_FALSE(set1.contains(argumentIndex + 2));
        ASSERT_FALSE(set1.contains(argumentIndex + 4));
    }
    set1.unionWith(set3);
    ASSERT_EQUAL(3 * passes, set1.size());
    for (ArgumentIndex argumentIndex = 10; argumentIndex < numberOfVariables; argumentIndex += 64) {
        ASSERT_TRUE(set1.contains(argumentIndex));
        ASSERT_TRUE(set1.contains(argumentIndex + 2));
        ASSERT_TRUE(set1.contains(argumentIndex + 4));
    }
    set1.removeAll(set2);
    ASSERT_EQUAL(2 * passes, set1.size());
    for (ArgumentIndex argumentIndex = 10; argumentIndex < numberOfVariables; argumentIndex += 64) {
        ASSERT_FALSE(set1.contains(argumentIndex));
        ASSERT_TRUE(set1.contains(argumentIndex + 2));
        ASSERT_TRUE(set1.contains(argumentIndex + 4));
    }
}

TEST(testTermSet) {
    doTest(0);
    doTest(22);
    doTest(22 + 64);
    doTest(22 + 64 * 2);
    doTest(22 + 64 * 3);
    doTest(22 + 64 * 4);
}

#endif
