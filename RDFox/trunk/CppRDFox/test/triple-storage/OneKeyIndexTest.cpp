// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifdef WITH_TEST

#include <CppTest/Checks.h>

#include "../../src/util/MemoryManager.h"
#include "../../src/triple-storage/sequential/SequentialOneKeyIndexImpl.h"
#include "../../src/triple-storage/concurrent/ConcurrentOneKeyIndexImpl.h"

template<class OneKeyIndexType>
class OneKeyIndexTest {

protected:

    typedef OneKeyIndexTest<OneKeyIndexType> OneKeyIndexTestType;
    static const size_t NUMBER_OF_THREADS = 8;
    static const size_t NUMBER_OF_RESOURCES = 431;
    static const size_t NUMBER_OF_ITERATIONS = 4000000;

    MemoryManager m_memoryManager;
    OneKeyIndexType m_index;

public:

    OneKeyIndexTest() : m_memoryManager(1000000), m_index(m_memoryManager) {
    }

    void initialize() {
        m_index.initialize();
    }

    void testBasicParent() {
        m_index.extendToResourceID(10);
        ASSERT_EQUAL(INVALID_TUPLE_INDEX, m_index.getHeadTripleIndex(3));

        ASSERT_TRUE(m_index.setHeadTripleIndexConditional(3, 0, 10));
        ASSERT_EQUAL(static_cast<TupleIndex>(10), m_index.getHeadTripleIndex(3));
        ASSERT_EQUAL(static_cast<size_t>(0), m_index.getTripleCount(3));
        m_index.incrementTripleCount(3);
        ASSERT_EQUAL(static_cast<size_t>(1), m_index.getTripleCount(3));
        m_index.decrementTripleCount(3);
        ASSERT_EQUAL(static_cast<size_t>(0), m_index.getTripleCount(3));
    }

};

#define SUITE_NAME    SequentialOneKeyIndexTest

#include <CppTest/AutoTest.h>

TEST3(SUITE_NAME, testBasic, OneKeyIndexTest<SequentialOneKeyIndex>) {
    testBasicParent();
}

#undef SUITE_NAME
#undef AUTOTEST_H
#undef TEST
#define SUITE_NAME    ConcurrentOneKeyIndexTest

#include <CppTest/AutoTest.h>

TEST3(SUITE_NAME, testBasic, OneKeyIndexTest<ConcurrentOneKeyIndex<TupleIndex> >) {
    testBasicParent();
}

#endif
