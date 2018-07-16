// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifdef WITH_TEST

#include <CppTest/Checks.h>
#include "../../src/util/MemoryManager.h"
#include "../../src/triple-storage/sequential/SequentialTripleListImpl.h"
#include "../../src/triple-storage/concurrent/ConcurrentTripleListImpl.h"
#include "../../src/util/Thread.h"

template<class TripleListType>
class TripleListTest {

protected:

    static const ResourceID MAX_RESOURCE_ID = 150;
    static const ResourceID NUMBER_OF_RESOURCE_IDS = MAX_RESOURCE_ID + 1;
    static const ResourceID NUMBER_OF_THREADS = 18;

    MemoryManager m_memoryManager;
    TripleListType m_tripleList;

public:

    TripleListTest() : m_memoryManager(500000000), m_tripleList(m_memoryManager) {
    }

    void initialize() {
        m_tripleList.initialize(0);
    }

    void testBasicParent() {
        ASSERT_EQUAL(static_cast<size_t>(0), m_tripleList.getExactTripleCount(TUPLE_STATUS_COMPLETE, TUPLE_STATUS_COMPLETE));

        TupleIndex tupleIndex1 = m_tripleList.add(2, 3, 4);
        m_tripleList.setTripleStatus(tupleIndex1, TUPLE_STATUS_COMPLETE);
        ASSERT_NOT_EQUAL(INVALID_TUPLE_INDEX, tupleIndex1);
        ASSERT_EQUAL(static_cast<ResourceID>(2), m_tripleList.getS(tupleIndex1));
        ASSERT_EQUAL(static_cast<ResourceID>(3), m_tripleList.getP(tupleIndex1));
        ASSERT_EQUAL(static_cast<ResourceID>(4), m_tripleList.getO(tupleIndex1));
        ASSERT_EQUAL(static_cast<size_t>(1), m_tripleList.getExactTripleCount(TUPLE_STATUS_COMPLETE, TUPLE_STATUS_COMPLETE));

        TupleIndex tupleIndex2 = m_tripleList.add(5, 6, 7);
        m_tripleList.setTripleStatus(tupleIndex2, TUPLE_STATUS_COMPLETE);
        ASSERT_NOT_EQUAL(INVALID_TUPLE_INDEX, tupleIndex2);
        ASSERT_EQUAL(static_cast<ResourceID>(5), m_tripleList.getS(tupleIndex2));
        ASSERT_EQUAL(static_cast<ResourceID>(6), m_tripleList.getP(tupleIndex2));
        ASSERT_EQUAL(static_cast<ResourceID>(7), m_tripleList.getO(tupleIndex2));
        ASSERT_EQUAL(static_cast<size_t>(2), m_tripleList.getExactTripleCount(TUPLE_STATUS_COMPLETE, TUPLE_STATUS_COMPLETE));

        TupleIndex tupleIndex3 = m_tripleList.add(8, 9, 1);
        m_tripleList.setTripleStatus(tupleIndex3, TUPLE_STATUS_COMPLETE);
        ASSERT_NOT_EQUAL(INVALID_TUPLE_INDEX, tupleIndex3);
        ASSERT_EQUAL(static_cast<ResourceID>(8), m_tripleList.getS(tupleIndex3));
        ASSERT_EQUAL(static_cast<ResourceID>(9), m_tripleList.getP(tupleIndex3));
        ASSERT_EQUAL(static_cast<ResourceID>(1), m_tripleList.getO(tupleIndex3));
        ASSERT_EQUAL(static_cast<size_t>(3), m_tripleList.getExactTripleCount(TUPLE_STATUS_COMPLETE, TUPLE_STATUS_COMPLETE));

        TupleIndex tupleIndex4 = m_tripleList.add(3, 3, 3);
        m_tripleList.setTripleStatus(tupleIndex4, TUPLE_STATUS_COMPLETE);
        ASSERT_NOT_EQUAL(INVALID_TUPLE_INDEX, tupleIndex4);
        ASSERT_EQUAL(static_cast<ResourceID>(3), m_tripleList.getS(tupleIndex4));
        ASSERT_EQUAL(static_cast<ResourceID>(3), m_tripleList.getP(tupleIndex4));
        ASSERT_EQUAL(static_cast<ResourceID>(3), m_tripleList.getO(tupleIndex4));
        ASSERT_EQUAL(static_cast<size_t>(4), m_tripleList.getExactTripleCount(TUPLE_STATUS_COMPLETE, TUPLE_STATUS_COMPLETE));
    }

    void assertCorrect(const TripleListType& list, int firstObjectID, int objectStep) {
        std::vector<bool> seenTuples;
        seenTuples.insert(seenTuples.begin(), NUMBER_OF_RESOURCE_IDS * NUMBER_OF_RESOURCE_IDS * NUMBER_OF_RESOURCE_IDS, false);
        const size_t tripleCount = list.getExactTripleCount(TUPLE_STATUS_COMPLETE, TUPLE_STATUS_COMPLETE);
        for (TupleIndex ti = 1; ti <= tripleCount; ti++)
            seenTuples[list.getS(ti) * NUMBER_OF_RESOURCE_IDS * NUMBER_OF_RESOURCE_IDS + list.getP(ti) * NUMBER_OF_RESOURCE_IDS + list.getO(ti)] = true;
        for (ResourceID s = 1; s <= MAX_RESOURCE_ID; s++)
            for (ResourceID p = 1; p <= MAX_RESOURCE_ID; p++)
                for (ResourceID o = firstObjectID; o <= MAX_RESOURCE_ID; o += objectStep)
                    ASSERT_TRUE(seenTuples[s * NUMBER_OF_RESOURCE_IDS * NUMBER_OF_RESOURCE_IDS + p * NUMBER_OF_RESOURCE_IDS + o]);
    }

    class WriterThread : public Thread {

    protected:

        TripleListType& m_list;
        const ResourceID m_firstObjectID;

    public:

        WriterThread(TripleListType& list, const ResourceID firstObjectID) : m_list(list), m_firstObjectID(firstObjectID) {
        }

        virtual void run() {
            populateList(m_list, m_firstObjectID, NUMBER_OF_THREADS);
        }

    };

    void testConcurrentParent() {
        // Test the list in just one thread
        TripleListType list(m_memoryManager);
        list.initialize(0);
        size_t numberOfAddedTriples = populateList(list, 1, 1);
        ASSERT_EQUAL(numberOfAddedTriples, list.getExactTripleCount(TUPLE_STATUS_COMPLETE, TUPLE_STATUS_COMPLETE));
        assertCorrect(list, 1, 1);

        // Repeat on many threads
        list.initialize(0);
        ASSERT_EQUAL(0, list.getExactTripleCount(TUPLE_STATUS_COMPLETE, TUPLE_STATUS_COMPLETE));
        unique_ptr_vector<WriterThread> threads;
        for (uint32_t threadIndex = 0; threadIndex < NUMBER_OF_THREADS; ++threadIndex) {
            std::unique_ptr<WriterThread> thread(new WriterThread(list, threadIndex + 1));
            threads.push_back(std::move(thread));
            threads.back()->start();
        }
        for (uint32_t threadIndex = 0; threadIndex < NUMBER_OF_THREADS; threadIndex++)
            threads[threadIndex]->join();
        ASSERT_EQUAL(numberOfAddedTriples, list.getExactTripleCount(TUPLE_STATUS_COMPLETE, TUPLE_STATUS_COMPLETE));
        assertCorrect(list, 1, 1);
    }

    static size_t populateList(TripleListType& list, ResourceID firstObjectID, ResourceID objectStep) {
        size_t numberOfAddedTriples = 0;
        for (ResourceID s = 1; s <= MAX_RESOURCE_ID; s++)
            for (ResourceID p = 1; p <= MAX_RESOURCE_ID; p++)
                for (ResourceID o = firstObjectID; o <= MAX_RESOURCE_ID; o += objectStep) {
                    TupleIndex tupleIndex = list.add(s, p, o);
                    ASSERT_NOT_EQUAL(tupleIndex, INVALID_TUPLE_INDEX);
                    list.setTripleStatus(tupleIndex, TUPLE_STATUS_COMPLETE);
                    numberOfAddedTriples++;
                }
        return numberOfAddedTriples;
    }

    static void insertIntoList(void* data) {
         std::pair<TripleListType*, int>* dataTyped = static_cast<std::pair<TripleListType*, int>* >(data);
         populateList(*(dataTyped->first), dataTyped->second, NUMBER_OF_THREADS);
    }

};

#define SUITE_NAME    SequentialTripleListTest

#include <CppTest/AutoTest.h>

TEST3(SUITE_NAME, testBasic, TripleListTest<SequentialTripleList>) {
    testBasicParent();
}

#undef SUITE_NAME
#undef TEST
#undef AUTOTEST_H
#define SUITE_NAME    ConcurrentTripleListTest

typedef ConcurrentTripleList<ResourceID, TupleIndex> ConcurrentTripleListWW;

#include <CppTest/AutoTest.h>

TEST3(SUITE_NAME, testBasic, TripleListTest<ConcurrentTripleListWW>) {
    testBasicParent();
}

TEST3(SUITE_NAME, testConcurrent, TripleListTest<ConcurrentTripleListWW>) {
    testConcurrentParent();
}

#endif
