// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifdef WITH_TEST

#include "TripleData.h"
#include "../../src/Common.h"
#include "../../src/util/MemoryManager.h"
#include "../../src/util/SequentialHashTableImpl.h"
#include "../../src/util/ParallelHashTableImpl.h"
#include "../../src/util/Thread.h"
#include "../../src/util/ThreadContext.h"
#include "../../src/triple-storage/sequential/SequentialTripleListImpl.h"
#include "../../src/triple-storage/sequential/SequentialThreeKeysIndexPolicyImpl.h"
#include "../../src/triple-storage/concurrent/ConcurrentTripleListImpl.h"
#include "../../src/triple-storage/concurrent/ConcurrentThreeKeysIndexPolicyImpl.h"

template<class TripleListType, class ThreeKeysIndexPolicyType, class ThreeKeysIndexType, size_t NumberOfThreads>
class ThreeKeysIndexTest {

protected:

    typedef ThreeKeysIndexTest<TripleListType, ThreeKeysIndexPolicyType, ThreeKeysIndexType, NumberOfThreads> ThreeKeysIndexTestType;

    static const ResourceID NUMBER_OF_TRIPLES = 2000000;
    static const ResourceID MAX_P = 71;
    static const ResourceID MAX_O = 213;
    MemoryManager m_memoryManager;
    TripleListType m_tripleList;
    ThreeKeysIndexType m_index;

    TupleIndex insert(const ResourceID s, const ResourceID p, const ResourceID o) {
        return insert(m_tripleList, m_index, s, p, o);
    }

    static TupleIndex insert(TripleListType& tripleList, ThreeKeysIndexType& index, const ResourceID s, const ResourceID p, const ResourceID o) {
        TupleIndex tupleIndex = insertIntoList(tripleList, s, p, o);
        insertIntoIndex<false>(index, s, p, o, tupleIndex);
        tripleList.setTripleStatus(tupleIndex, TUPLE_STATUS_COMPLETE | TUPLE_STATUS_IDB);
        return tupleIndex;
    }

    static TupleIndex insertIntoList(TripleListType& tripleList, const ResourceID& s, const ResourceID& p, const ResourceID& o) {
        TupleIndex tupleIndex = tripleList.add(s, p, o);
        tripleList.setNext(tupleIndex, RC_S, INVALID_TUPLE_INDEX);
        tripleList.setNext(tupleIndex, RC_P, INVALID_TUPLE_INDEX);
        tripleList.setNext(tupleIndex, RC_O, INVALID_TUPLE_INDEX);
        return tupleIndex;
    }

    template<bool ensureNew>
    static void insertIntoIndex(ThreeKeysIndexType& index, const ResourceID s, const ResourceID p, const ResourceID o, TupleIndex tupleIndex) {
        typename ThreeKeysIndexType::BucketDescriptor bucketDescriptor;
        ThreadContext& threadContext = ThreadContext::getCurrentThreadContext();
        index.acquireBucket(threadContext, bucketDescriptor, s, p, o);
        BucketStatus bucketStatus;
        while ((bucketStatus = index.continueBucketSearch(threadContext, bucketDescriptor, s, p, o)) == BUCKET_EMPTY) {
            if (index.getPolicy().setTripleIndexConditional(bucketDescriptor.m_bucket, INVALID_TUPLE_INDEX, tupleIndex)) {
                index.acknowledgeInsert(threadContext, bucketDescriptor);
                break;
            }
        }
        if (ensureNew)
            ASSERT_TRUE(bucketStatus == BUCKET_EMPTY);
        index.releaseBucket(threadContext, bucketDescriptor);
    }

    TupleIndex getTupleIndex(ResourceID s, ResourceID p, ResourceID o) {
        return getTupleIndex(m_index, s, p, o);
    }

    static TupleIndex getTupleIndex(ThreeKeysIndexType& index, ResourceID s, ResourceID p, ResourceID o) {
        typename ThreeKeysIndexType::BucketDescriptor bucketDescriptor;
        ThreadContext& threadContext = ThreadContext::getCurrentThreadContext();
        index.acquireBucket(threadContext, bucketDescriptor, s, p, o);
        index.continueBucketSearch(threadContext, bucketDescriptor, s, p, o);
        index.releaseBucket(threadContext, bucketDescriptor);
        return bucketDescriptor.m_bucketContents.m_tripleIndex;
    }

    static void insertTriples(TripleListType& tripleList, ThreeKeysIndexType& index, size_t start, size_t delta) {
        for (ResourceID spo = static_cast<ResourceID>(start); spo < NUMBER_OF_TRIPLES; spo += static_cast<ResourceID>(delta)) {
            ResourceID s = spo / (MAX_P * MAX_O) + 1;
            ResourceID po = spo % (MAX_P * MAX_O);
            ResourceID p = po / MAX_O + 1;
            ResourceID o = po % MAX_O + 1;
            insert(tripleList, index, s, p, o);
        }
    }

    static void checkTriples(ThreeKeysIndexType& index) {
        for (ResourceID spo = 0; spo < NUMBER_OF_TRIPLES; spo++) {
            ResourceID s = spo / (MAX_P * MAX_O) + 1;
            ResourceID po = spo % (MAX_P * MAX_O);
            ResourceID p = po / MAX_O + 1;
            ResourceID o = po % MAX_O + 1;
            ASSERT_NOT_EQUAL(INVALID_TUPLE_INDEX, getTupleIndex(index, s, p, o));
        }
    }

    class InsertThread : public Thread {

    protected:

        ThreeKeysIndexTestType& m_threeKeysIndexTest;
        const size_t m_start;
        const size_t m_delta;

    public:

        InsertThread(ThreeKeysIndexTestType& threeKeysIndexTest, const size_t start, const size_t delta) : m_threeKeysIndexTest(threeKeysIndexTest), m_start(start), m_delta(delta) {
        }

        virtual void run() {
            insertTriples(m_threeKeysIndexTest.m_tripleList, m_threeKeysIndexTest.m_index, m_start, m_delta);
        }

    };

public:

    ThreeKeysIndexTest() : m_memoryManager(500000000), m_tripleList(m_memoryManager), m_index(m_memoryManager, HASH_TABLE_LOAD_FACTOR, m_tripleList) {
        m_index.setNumberOfThreads(NumberOfThreads);
    }

    void initialize() {
        m_tripleList.initialize(0);
        m_index.initialize(HASH_TABLE_INITIAL_SIZE);
    }

    void testBasicParent() {
        ASSERT_EQUAL(INVALID_TUPLE_INDEX, getTupleIndex(5, 4, 3));

        TupleIndex tupleIndex = insert(5, 4, 3);
        ASSERT_EQUAL(tupleIndex, getTupleIndex(5, 4, 3));
        ASSERT_EQUAL(static_cast<size_t>(1), m_index.getNumberOfUsedBuckets());

        tupleIndex = insert(5, 6, 3);
        ASSERT_EQUAL(tupleIndex, getTupleIndex(5, 6, 3));
        ASSERT_EQUAL(static_cast<size_t>(2), m_index.getNumberOfUsedBuckets());

        tupleIndex = insert(7, 6, 3);
        ASSERT_EQUAL(tupleIndex, getTupleIndex(7, 6, 3));
        ASSERT_EQUAL(static_cast<size_t>(3), m_index.getNumberOfUsedBuckets());
    }

    void testBulkParent() {
        TestData testData;
        generateTestDataMinimalRepetition(testData, 200000, 711, 257, 345);
        std::vector<TupleIndex> tupleIndexes(testData.getNumberOfBuckets(), INVALID_TUPLE_INDEX);

        // Store the data into the tables
        for (size_t testDataBucketIndex = 0; testDataBucketIndex < testData.getNumberOfBuckets(); ++testDataBucketIndex) {
            const uint8_t* testDataBucket = testData.getBucket(testDataBucketIndex);
            if (!testData.isEmpty(testDataBucket)) {
                const ResourceTriple& resourceTriple = testData.get(testDataBucket);
                TupleIndex tupleIndex = insertIntoList(m_tripleList, resourceTriple.m_s, resourceTriple.m_p, resourceTriple.m_o);
                tupleIndexes[testDataBucketIndex] = tupleIndex;
                insertIntoIndex<true>(m_index, resourceTriple.m_s, resourceTriple.m_p, resourceTriple.m_o, tupleIndex);
                m_tripleList.setTripleStatus(tupleIndex, TUPLE_STATUS_COMPLETE);
            }
        }

        // Check whether the data was stored correctly
        for (size_t testDataBucketIndex = 0; testDataBucketIndex < testData.getNumberOfBuckets(); ++testDataBucketIndex) {
            const uint8_t* testDataBucket = testData.getBucket(testDataBucketIndex);
            if (!testData.isEmpty(testDataBucket)) {
                const ResourceTriple& resourceTriple = testData.get(testDataBucket);
                TupleIndex tupleIndex = getTupleIndex(resourceTriple.m_s, resourceTriple.m_p, resourceTriple.m_o);
                ASSERT_EQUAL(tupleIndexes[testDataBucketIndex], tupleIndex);
                ASSERT_EQUAL(resourceTriple.m_s, m_tripleList.getS(tupleIndex));
                ASSERT_EQUAL(resourceTriple.m_p, m_tripleList.getP(tupleIndex));
                ASSERT_EQUAL(resourceTriple.m_o, m_tripleList.getO(tupleIndex));
            }
        }
        ASSERT_EQUAL(testData.getSize(), static_cast<size_t>(m_tripleList.getExactTripleCount(TUPLE_STATUS_COMPLETE, TUPLE_STATUS_COMPLETE)));

        std::vector<ResourceTriple> testDataSorted;
        testData.toVector(testDataSorted);
        std::sort(testDataSorted.begin(), testDataSorted.end());
    }

    void testCouncrrentParent() {
        TripleListType tripleList(m_memoryManager);
        ThreeKeysIndexType index(m_memoryManager, HASH_TABLE_LOAD_FACTOR, tripleList);
        index.setNumberOfThreads(NumberOfThreads);
        ASSERT_TRUE(tripleList.initialize(0));
        ASSERT_TRUE(index.initialize(HASH_TABLE_INITIAL_SIZE));
        insertTriples(tripleList, index, 0, 1);
        checkTriples(index);
        unique_ptr_vector<InsertThread> threads;
        for (size_t threadIndex = 0; threadIndex < NumberOfThreads; threadIndex++) {
            std::unique_ptr<InsertThread> thread(new InsertThread(*this, threadIndex, NumberOfThreads));
            threads.push_back(std::move(thread));
            threads.back()->start();
        }
        for (size_t threadIndex = 0; threadIndex < NumberOfThreads; threadIndex++)
            threads[threadIndex]->join();
        checkTriples(m_index);
    }

};

typedef ThreeKeysIndexTest<SequentialTripleList, SequentialThreeKeysIndexPolicy<SequentialTripleList>, SequentialHashTable<SequentialThreeKeysIndexPolicy<SequentialTripleList> >, 1> SequentialThreeKeysIndexTestType;

#define SUITE_NAME SequentialThreeKeysIndexTest

#include <CppTest/AutoTest.h>

TEST3(SUITE_NAME, testBasic, SequentialThreeKeysIndexTestType) {
    testBasicParent();
}

TEST3(SUITE_NAME, testBulk, SequentialThreeKeysIndexTestType) {
    testBulkParent();
}

typedef ThreeKeysIndexTest<
    ConcurrentTripleList<ResourceID, TupleIndex>,
    ConcurrentThreeKeysIndexPolicy<ConcurrentTripleList<ResourceID, TupleIndex> >,
    ParallelHashTable<ConcurrentThreeKeysIndexPolicy<ConcurrentTripleList<ResourceID, TupleIndex> > >,
    4
>ConcurrentThreeKeysIndexTestType;

#undef SUITE_NAME
#undef AUTOTEST_H
#undef TEST
#define SUITE_NAME ConcurrentThreeKeysIndexTest

#include <CppTest/AutoTest.h>

TEST3(SUITE_NAME, testBasic, ConcurrentThreeKeysIndexTestType) {
    testBasicParent();
}

TEST3(SUITE_NAME, testBulk, ConcurrentThreeKeysIndexTestType) {
    testBulkParent();
}

TEST3(SUITE_NAME, testConcurrent, ConcurrentThreeKeysIndexTestType) {
    testCouncrrentParent();
}

#endif
