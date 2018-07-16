// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifdef WITH_TEST

#include <CppTest/Checks.h>
#include "../../src/util/MemoryManager.h"
#include "../../src/util/SequentialHashTableImpl.h"
#include "../../src/util/ParallelHashTableImpl.h"
#include "../../src/util/ThreadContext.h"
#include "../../src/triple-storage/sequential/SequentialTripleListImpl.h"
#include "../../src/triple-storage/sequential/SequentialTwoKeysIndexPolicyImpl.h"
#include "../../src/triple-storage/concurrent/ConcurrentTripleListImpl.h"
#include "../../src/triple-storage/concurrent/ConcurrentTwoKeysIndexPolicyImpl.h"

template<class TripleListType, class TwoKeysIndexPolicyType, class TwoKeysIndexType, size_t NumberOfThreads>
class TwoKeysIndexTest {

protected:

    MemoryManager m_memoryManager;
    TripleListType m_tripleList;
    TwoKeysIndexType m_index;

    static TupleIndex insertIntoList(TripleListType& tripleList, const ResourceID& s, const ResourceID& p, const ResourceID& o) {
        TupleIndex tupleIndex = tripleList.add(s, p, o);
        ASSERT_NOT_EQUAL(INVALID_TUPLE_INDEX, tupleIndex);
        tripleList.setNext(tupleIndex, RC_S, INVALID_TUPLE_INDEX);
        tripleList.setNext(tupleIndex, RC_P, INVALID_TUPLE_INDEX);
        tripleList.setNext(tupleIndex, RC_O, INVALID_TUPLE_INDEX);
        return tupleIndex;
    }

    static void insertIntoIndex(TwoKeysIndexType& index, const ResourceID s, const ResourceID p, TupleIndex tupleIndex) {
        typename TwoKeysIndexType::BucketDescriptor bucketDescriptor;
        ThreadContext& threadContext = ThreadContext::getCurrentThreadContext();
        index.acquireBucket(threadContext, bucketDescriptor, s, p);
        ASSERT_TRUE(index.continueBucketSearch(threadContext, bucketDescriptor, s, p) == BUCKET_EMPTY);
        index.getPolicy().setHeadTripleIndex(bucketDescriptor.m_bucket, tupleIndex);
        index.acknowledgeInsert(threadContext, bucketDescriptor);
        index.releaseBucket(threadContext, bucketDescriptor);
    }

    static TupleIndex getTupleIndex(TwoKeysIndexType& index, ResourceID s, ResourceID p) {
        typename TwoKeysIndexType::BucketDescriptor bucketDescriptor;
        ThreadContext& threadContext = ThreadContext::getCurrentThreadContext();
        index.acquireBucket(threadContext, bucketDescriptor, s, p);
        index.continueBucketSearch(threadContext, bucketDescriptor, s, p);
        index.releaseBucket(threadContext, bucketDescriptor);
        return bucketDescriptor.m_bucketContents.m_headTripleIndex;
    }

    TupleIndex insert(ResourceID s, ResourceID p, ResourceID o) {
        TupleIndex tupleIndex = insertIntoList(m_tripleList, s, p, o);
        insertIntoIndex(m_index, s, p, tupleIndex);
        m_tripleList.setTripleStatus(tupleIndex, TUPLE_STATUS_COMPLETE);
        return tupleIndex;
    }

    TupleIndex getTupleIndex(ResourceID s, ResourceID p) {
        return getTupleIndex(m_index, s, p);
    }

public:

    struct ResourcePair {
        ResourceID m_s;
        ResourceID m_p;

        ResourcePair(ResourceID s, ResourceID p) : m_s(s), m_p(p) {
        }

        static always_inline bool isLessThan(const ResourcePair& pair1, const ResourcePair& pair2) {
            return
                pair1.m_s < pair2.m_s ||
                (pair1.m_s == pair2.m_s && pair1.m_p < pair2.m_p);
        }
    };

    TwoKeysIndexTest() : m_memoryManager(100000000), m_tripleList(m_memoryManager), m_index(m_memoryManager, HASH_TABLE_LOAD_FACTOR, m_tripleList) {
        m_index.setNumberOfThreads(NumberOfThreads);
    }

    void initialize() {
        ASSERT_TRUE(m_tripleList.initialize(0));
        ASSERT_TRUE(m_index.initialize(HASH_TABLE_INITIAL_SIZE));
    }

    void testBulkParent() {
        ThreadContext& threadContext = ThreadContext::getCurrentThreadContext();
        std::vector<ResourcePair> testData;
        generateTestData(testData, 1000000);
        std::vector<TupleIndex> tripleIndexes(testData.size(), 0);

        // Store the data into the tables
        for (size_t index = 0; index < testData.size(); index++) {
            ResourceID s = testData[index].m_s;
            ResourceID p = testData[index].m_p;
            typename TwoKeysIndexType::BucketDescriptor bucketDescriptor;
            m_index.acquireBucket(threadContext, bucketDescriptor, s, p);
            if (m_index.continueBucketSearch(threadContext, bucketDescriptor, s, p) == BUCKET_EMPTY) {
                tripleIndexes[index] = insertIntoList(m_tripleList, s, p, 2);
                m_index.getPolicy().setHeadTripleIndex(bucketDescriptor.m_bucket, tripleIndexes[index]);
                m_index.acknowledgeInsert(threadContext, bucketDescriptor);
            }
            else
                tripleIndexes[index] = m_index.getPolicy().getHeadTripleIndex(bucketDescriptor.m_bucket);
            m_index.releaseBucket(threadContext, bucketDescriptor);

            ASSERT_EQUAL(s, m_tripleList.getS(tripleIndexes[index]));
            ASSERT_EQUAL(p, m_tripleList.getP(tripleIndexes[index]));
            ASSERT_EQUAL(static_cast<ResourceID>(2), m_tripleList.getO(tripleIndexes[index]));
        }

        // Check whether the data was stored correctly
        for (size_t index = 0; index < testData.size(); index++) {
            TupleIndex tupleIndex = getTupleIndex(testData[index].m_s, testData[index].m_p);
            ASSERT_EQUAL(tripleIndexes[index], tupleIndex);
            ASSERT_EQUAL(testData[index].m_s, m_tripleList.getS(tupleIndex));
            ASSERT_EQUAL(testData[index].m_p, m_tripleList.getP(tupleIndex));
            ASSERT_EQUAL(static_cast<ResourceID>(2), m_tripleList.getO(tupleIndex));
        }

        // Check the counters
        std::vector<ResourcePair> testDataSorted(testData);
        std::sort(testDataSorted.begin(), testDataSorted.end(), ResourcePair::isLessThan);
        size_t totalCount = 0;
        for (size_t index = 0; index < testDataSorted.size();) {
            ResourceID currentS = testDataSorted[index].m_s;
            ResourceID currentP = testDataSorted[index].m_p;
            while (index < testDataSorted.size() && currentS == testDataSorted[index].m_s && currentP == testDataSorted[index].m_p)
                index++;
            typename TwoKeysIndexType::BucketDescriptor bucketDescriptor;
            m_index.acquireBucket(threadContext, bucketDescriptor, currentS, currentP);
            m_index.continueBucketSearch(threadContext, bucketDescriptor, currentS, currentP);
            ASSERT_EQUAL(currentS, m_tripleList.getS(bucketDescriptor.m_bucketContents.m_headTripleIndex));
            ASSERT_EQUAL(currentP, m_tripleList.getP(bucketDescriptor.m_bucketContents.m_headTripleIndex));
            totalCount++;
            m_index.releaseBucket(threadContext, bucketDescriptor);
        }
        ASSERT_EQUAL(totalCount, m_index.getNumberOfUsedBuckets());
    }

    void testBasicParent() {
        ASSERT_EQUAL(INVALID_TUPLE_INDEX, getTupleIndex(5, 4));

        TupleIndex tupleIndex = insert(5, 4, 3);
        ASSERT_EQUAL(tupleIndex, getTupleIndex(5, 4));
        ASSERT_EQUAL(static_cast<size_t>(1), m_index.getNumberOfUsedBuckets());

        tupleIndex = insert(5, 6, 3);
        ASSERT_EQUAL(tupleIndex, getTupleIndex(5, 6));
        ASSERT_EQUAL(static_cast<size_t>(2), m_index.getNumberOfUsedBuckets());

        tupleIndex = insert(7, 6, 3);
        ASSERT_EQUAL(tupleIndex, getTupleIndex(7, 6));
        ASSERT_EQUAL(static_cast<size_t>(3), m_index.getNumberOfUsedBuckets());
    }


    static void generateTestData(std::vector<ResourcePair>& testData,int numberOfPairs) {
        ResourceID maxS = 711;
        ResourceID maxP = 257;

        ResourceID currentS = 1;
        ResourceID currentP = 1;

        testData.reserve(numberOfPairs);
        for (int index = 0; index < numberOfPairs; index++) {
            if(currentS > maxS)
                currentS = 1;
            if(currentP > maxP)
                currentP = 1;
            testData.push_back(ResourcePair(currentS++, currentP++));
        }
    }

};

#define SUITE_NAME    SequentialTwoKeysIndexTest
#include <CppTest/AutoTest.h>

typedef TwoKeysIndexTest<
    SequentialTripleList,
    SequentialTwoKeysIndexPolicy<SequentialTripleList, RC_S, RC_P>,
    SequentialHashTable<SequentialTwoKeysIndexPolicy<SequentialTripleList, RC_S, RC_P> >,
    1
> SequentialTwoKeysIndexTest;

TEST3(SUITE_NAME, testBasic, SequentialTwoKeysIndexTest) {
    testBasicParent();
}

TEST3(SUITE_NAME, testBulk, SequentialTwoKeysIndexTest) {
    testBulkParent();
}

#undef SUITE_NAME
#undef AUTOTEST_H
#undef TEST
#define SUITE_NAME    ConcurrentTwoKeysIndexTest
#include <CppTest/AutoTest.h>

typedef TwoKeysIndexTest<
    ConcurrentTripleList<ResourceID, TupleIndex>,
    ConcurrentTwoKeysIndexPolicy<ConcurrentTripleList<ResourceID, TupleIndex>, RC_S, RC_P>,
    ParallelHashTable<ConcurrentTwoKeysIndexPolicy<ConcurrentTripleList<ResourceID, TupleIndex>, RC_S, RC_P> >,
    4
> ConcurrentTwoKeysIndexTest;

TEST3(SUITE_NAME, testBasic, ConcurrentTwoKeysIndexTest) {
    testBasicParent();
}

TEST3(SUITE_NAME, testBulk, ConcurrentTwoKeysIndexTest) {
    testBulkParent();
}

#endif
