// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifdef WITH_TEST

#include "TripleData.h"
#include "../../src/util/MemoryManager.h"
#include "../../src/util/SequentialHashTableImpl.h"
#include "../../src/util/ParallelHashTableImpl.h"
#include "../../src/util/Thread.h"
#include "../../src/util/ThreadContext.h"
#include "../../src/storage/Parameters.h"
#include "../../src/triple-storage/sequential/SequentialTripleListImpl.h"
#include "../../src/triple-storage/sequential/SequentialOneKeyIndexImpl.h"
#include "../../src/triple-storage/sequential/SequentialTwoKeysIndexPolicyImpl.h"
#include "../../src/triple-storage/concurrent/ConcurrentTripleListImpl.h"
#include "../../src/triple-storage/concurrent/ConcurrentOneKeyIndexImpl.h"
#include "../../src/triple-storage/concurrent/ConcurrentTwoKeysIndexPolicyImpl.h"
#include "../../src/triple-storage/managers/TwoKeysManagerGroupByTwoImpl.h"

#define START_QUERY(queryS, queryP, queryO) \
    startQuery(__FILE__, __LINE__, queryS, queryP, queryO)

#define ASSERT_TRIPLE(expectedS, expectedP, expectedO) \
    assertTriple(__FILE__, __LINE__, expectedS, expectedP, expectedO)

#define ASSERT_END() \
    ASSERT_TRUE(!isOnCurrent())

template<class OneKeyIndexT, template<class, ResourceComponent, ResourceComponent> class TwoKeysIndexPolicyT>
struct SequentialTKMGBTConfiguration {
    static const ResourceComponent COMPONENT1 = RC_S;
    static const ResourceComponent COMPONENT2 = RC_P;
    static const ResourceComponent COMPONENT3 = RC_O;
    typedef SequentialTripleList TripleListType;
    typedef OneKeyIndexT OneKeyIndexType;
    typedef TwoKeysIndexPolicyT<TripleListType, COMPONENT1, COMPONENT2> TwoKeysIndexPolicyType;
    typedef SequentialHashTable<TwoKeysIndexPolicyType> TwoKeysIndexType;
};

template<class OneKeyIndexT, template<class, ResourceComponent, ResourceComponent> class TwoKeysIndexPolicyT>
struct ConcurrentTKMGBTConfiguration {
    static const ResourceComponent COMPONENT1 = RC_S;
    static const ResourceComponent COMPONENT2 = RC_P;
    static const ResourceComponent COMPONENT3 = RC_O;
    typedef ConcurrentTripleList<ResourceID, TupleIndex> TripleListType;
    typedef OneKeyIndexT OneKeyIndexType;
    typedef TwoKeysIndexPolicyT<TripleListType, COMPONENT1, COMPONENT2> TwoKeysIndexPolicyType;
    typedef ParallelHashTable<TwoKeysIndexPolicyType> TwoKeysIndexType;
};

template<class Configuration, template<class> class TwoKeysManagerGroupByTwoT>
class TwoKeysManager : public TwoKeysManagerGroupByTwoT<Configuration> {

    typedef TwoKeysManagerGroupByTwoT<Configuration> TwoKeysManagerBase;

public:

    TwoKeysManager(MemoryManager& memoryManager, typename Configuration::TripleListType& tripleList) :
        TwoKeysManagerBase(memoryManager, tripleList, 1) {
    }

    using TwoKeysManagerBase::m_oneKeyIndex;
    using TwoKeysManagerBase::m_twoKeysIndex;

};

template<class Configuration, template<class> class TwoKeysManagerGroupByTwoT>
class TwoKeysManagerGroupByTwoTest : public QueryFixture {

protected:

    typedef TwoKeysManager<Configuration, TwoKeysManagerGroupByTwoT> TwoKeysManagerType;
    typedef TwoKeysManagerGroupByTwoTest<Configuration, TwoKeysManagerGroupByTwoT> TwoKeysManagerGroupByTwoTestType;

    static const uint32_t NUMBER_OF_THREADS = 4;
    static const size_t MAX_MEMORY = 1000000000;
    static const ResourceID NUMBER_OF_TRIPLES = 1000000;
    static const ResourceID MAX_P = 71;
    static const ResourceID MAX_O = 213;

    MemoryManager m_memoryManager;
    typename Configuration::TripleListType m_tripleList;
    TwoKeysManagerGroupByTwoT<Configuration> m_twoKeysManager;
    TupleIndex m_currentTripleIndex;
    ResourceID m_currentCompareResourceID;
    TupleIndex m_currentCompareGroupedMask;

    TupleIndex insert(ResourceID s, ResourceID p, ResourceID o) {
        ThreadContext& threadContext = ThreadContext::getCurrentThreadContext();
        TupleIndex insertedTripleIndex = m_tripleList.add(s, p, o);
        ASSERT_NOT_EQUAL(insertedTripleIndex, INVALID_TUPLE_INDEX);
        ASSERT_TRUE(m_twoKeysManager.insertTriple(threadContext, insertedTripleIndex, s, p, o));
        m_tripleList.setTripleStatus(insertedTripleIndex, TUPLE_STATUS_COMPLETE);
        return insertedTripleIndex;
    }

    virtual bool startQuery(const char* const fileName, const long lineNumber, ResourceID queryS, ResourceID queryP, ResourceID queryO, ResourceID& s, ResourceID& p, ResourceID& o) {
        CppTest::assertTrue(queryS != INVALID_RESOURCE_ID, fileName, lineNumber, "S must be fixed in a query.");
        CppTest::assertTrue(queryO == INVALID_RESOURCE_ID, fileName, lineNumber, "O must not be fixed in a query.");
        ThreadContext& threadContext = ThreadContext::getCurrentThreadContext();
        if (queryP == INVALID_RESOURCE_ID) {
            m_currentTripleIndex = m_twoKeysManager.getFirstTripleIndex1(threadContext, queryS, queryP, queryO);
            m_currentCompareResourceID = INVALID_RESOURCE_ID;
        }
        else {
            m_currentTripleIndex = m_twoKeysManager.getFirstTripleIndex12(threadContext, queryS, queryP, queryO, m_currentCompareResourceID, m_currentCompareGroupedMask);
            while (m_currentTripleIndex != INVALID_TUPLE_INDEX) {
                if (m_tripleList.getResourceID(m_currentTripleIndex, RC_P) == m_currentCompareResourceID)
                    break;
                else
                    m_currentTripleIndex = m_currentCompareGroupedMask & m_tripleList.getNext(m_currentTripleIndex, TwoKeysManagerType::COMPONENT1);
            }
        }
        if (m_currentTripleIndex != INVALID_TUPLE_INDEX) {
            m_tripleList.getResourceIDs(m_currentTripleIndex, s, p, o);
            return true;
        }
        else
            return false;
    }

    virtual bool startQuery(const char* const fileName, const long lineNumber, ResourceID queryS, ResourceID queryP, ResourceID queryO) {
        ResourceID s;
        ResourceID p;
        ResourceID o;
        return startQuery(fileName, lineNumber, queryS, queryP, queryO, s, p ,o);
    }

    virtual bool getNextTriple(const char* const fileName, const long lineNumber, ResourceID& s, ResourceID& p, ResourceID& o) {
        m_currentTripleIndex = m_tripleList.getNext(m_currentTripleIndex, TwoKeysManagerType::COMPONENT1);
        if (m_currentCompareResourceID != INVALID_RESOURCE_ID) {
            while (m_currentTripleIndex != INVALID_TUPLE_INDEX) {
                if (m_tripleList.getResourceID(m_currentTripleIndex, RC_P) == m_currentCompareResourceID)
                    break;
                else
                    m_currentTripleIndex = m_currentCompareGroupedMask & m_tripleList.getNext(m_currentTripleIndex, TwoKeysManagerType::COMPONENT1);
            }
        }
        if (m_currentTripleIndex != INVALID_TUPLE_INDEX) {
            m_tripleList.getResourceIDs(m_currentTripleIndex, s, p, o);
            return true;
        }
        else
            return false;
    }

    virtual bool isOnCurrent() {
        return m_currentTripleIndex != INVALID_TUPLE_INDEX;
    }

    virtual void assertTriple(const char* const fileName, const long lineNumber, const ResourceID expectedS, const ResourceID expectedP, const ResourceID expectedO) {
        CppTest::assertTrue(isOnCurrent(), fileName, lineNumber, "Not on current");
        ResourceID currentS;
        ResourceID currentP;
        ResourceID currentO;
        m_tripleList.getResourceIDs(m_currentTripleIndex, currentS, currentP, currentO);
        CppTest::assertEqual(expectedS, currentS, fileName, lineNumber);
        CppTest::assertEqual(expectedP, currentP, fileName, lineNumber);
        CppTest::assertEqual(expectedO, currentO, fileName, lineNumber);
        getNextTriple(fileName, lineNumber, currentS, currentP, currentO);
    }

    bool isBucketEmptyFor(ResourceID value1, ResourceID value2) {
        typename Configuration::TwoKeysIndexType::BucketDescriptor readBucket;
        m_twoKeysManager.m_twoKeysIndex.acquireBucket(readBucket, value1, value2);
        BucketStatus bucketStatus = m_twoKeysManager.m_twoKeysIndex.continueBucketSearch(readBucket, value1, value2);
        m_twoKeysManager.m_twoKeysIndex.releaseBucket(readBucket);
        return bucketStatus == BUCKET_EMPTY;
    }

    void testBasicParent() {
        insert(1, 2, 3);
        insert(1, 4, 5);
        insert(2, 2, 3);
        insert(1, 2, 7);

        ASSERT_QUERY(1, INVALID_RESOURCE_ID, INVALID_RESOURCE_ID,
            T(1, 2, 7),
            T(1, 4, 5),
            T(1, 2, 3)
        );

        ASSERT_QUERY(1, 2, INVALID_RESOURCE_ID,
            T(1, 2, 7),
            T(1, 2, 3)
        );

        ASSERT_QUERY(1, 4, INVALID_RESOURCE_ID,
            T(1, 4, 5)
        );

        ASSERT_QUERY(1, 3, INVALID_RESOURCE_ID);

        ASSERT_QUERY(7, 3, INVALID_RESOURCE_ID);

        ASSERT_QUERY(7, INVALID_RESOURCE_ID, INVALID_RESOURCE_ID);
    }

    class InsertWorker : public Thread {

    protected:

        TwoKeysManagerGroupByTwoTestType& m_twoKeysManagerGroupByTwoTest;
        const size_t m_start;

    public:

        InsertWorker(TwoKeysManagerGroupByTwoTestType& twoKeysManagerGroupByTwoTest, const size_t start) : m_twoKeysManagerGroupByTwoTest(twoKeysManagerGroupByTwoTest), m_start(start) {
        }

        virtual void run() {
            m_twoKeysManagerGroupByTwoTest.insertTriples(m_start, NUMBER_OF_THREADS);
        }

    };

    void testConcurrentParent() {
        unique_ptr_vector<InsertWorker> threads;
        for (size_t threadIndex = 0; threadIndex < NUMBER_OF_THREADS; threadIndex++) {
            std::unique_ptr<InsertWorker> thread(new InsertWorker(*this, threadIndex));
            threads.push_back(std::move(thread));
            threads.back()->start();
        }
        for (size_t threadIndex = 0; threadIndex < NUMBER_OF_THREADS; threadIndex++)
            threads[threadIndex]->join();
        TestData expected;
        ASSERT_QUERY_DATA(1, 17, 0, insertTriples(expected, 1, 17, 0));
        expected.clear();
        ASSERT_QUERY_DATA(1, 1, INVALID_RESOURCE_ID, insertTriples(expected, 1, 1, INVALID_RESOURCE_ID));
        expected.clear();
        ASSERT_QUERY_DATA(1, 5, INVALID_RESOURCE_ID, insertTriples(expected, 1, 5, INVALID_RESOURCE_ID));
        expected.clear();
        ASSERT_QUERY_DATA(12, 53, INVALID_RESOURCE_ID, insertTriples(expected, 12, 53, INVALID_RESOURCE_ID));
    }

    void insertTriples(size_t start, size_t delta) {
        for (ResourceID spo = static_cast<ResourceID>(start); spo < NUMBER_OF_TRIPLES; spo += static_cast<ResourceID>(delta)) {
            ResourceID s = spo / (MAX_P * MAX_O) + 1;
            ResourceID po = spo % (MAX_P * MAX_O);
            ResourceID p = po / MAX_O + 1;
            ResourceID o = po % MAX_O + 1;
            insert(s, p, o);
        }
    }

    const TestData& insertTriples(TestData& testData, ResourceID fS, ResourceID fP, ResourceID fO) {
        for (ResourceID spo = 0; spo < NUMBER_OF_TRIPLES; spo++) {
            ResourceID s = spo / (MAX_P * MAX_O) + 1;
            ResourceID po = spo % (MAX_P * MAX_O);
            ResourceID p = po / MAX_O + 1;
            ResourceID o = po % MAX_O + 1;
            if ((fS == INVALID_RESOURCE_ID || fS == s) && (fP == INVALID_RESOURCE_ID || fP == p) && (fO == INVALID_RESOURCE_ID || fO == o))
                testData.add(s, p, o);
        }
        return testData;
    }


public:

    TwoKeysManagerGroupByTwoTest() :
        m_memoryManager(100000000),
        m_tripleList(m_memoryManager),
        m_twoKeysManager(m_memoryManager, m_tripleList, Parameters()),
        m_currentTripleIndex(INVALID_TUPLE_INDEX),
        m_currentCompareResourceID(INVALID_RESOURCE_ID),
        m_currentCompareGroupedMask(NOT_GROUPED_MASK)
    {
        m_twoKeysManager.setNumberOfThreads(NUMBER_OF_THREADS);
    }

    void initialize() {
        ASSERT_TRUE(m_tripleList.initialize(0));
        ASSERT_TRUE(m_twoKeysManager.initialize(0, 0));
    }
};

#define SUITE_NAME    SequentialTwoKeysManagerGroupByTwoTest

#include <CppTest/AutoTest.h>

typedef TwoKeysManagerGroupByTwoTest<SequentialTKMGBTConfiguration<SequentialOneKeyIndex, SequentialTwoKeysIndexPolicy>, TwoKeysManagerGroupByTwo> SequentialTwoKeysManagerGroupByTwoTest;

TEST3(SUITE_NAME, testBasic, SequentialTwoKeysManagerGroupByTwoTest) {
    testBasicParent();
}

#undef SUITE_NAME
#undef AUTOTEST_H
#undef TEST
#define SUITE_NAME    ConcurrentTwoKeysManagerGroupByTwoTest

#include <CppTest/AutoTest.h>

typedef TwoKeysManagerGroupByTwoTest<ConcurrentTKMGBTConfiguration<ConcurrentOneKeyIndex<TupleIndex>, ConcurrentTwoKeysIndexPolicy>, TwoKeysManagerGroupByTwo> ConcurrentTwoKeysManagerGroupByTwoTest;

TEST3(SUITE_NAME, testBasic, ConcurrentTwoKeysManagerGroupByTwoTest) {
    testBasicParent();
}

TEST3(SUITE_NAME, testConcurrent, ConcurrentTwoKeysManagerGroupByTwoTest) {
    testConcurrentParent();
}

#endif
