// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifdef WITH_TEST

#include "TripleData.h"
#include "../../src/Common.h"
#include "../../src/util/MemoryManager.h"
#include "../../src/util/SequentialHashTableImpl.h"
#include "../../src/util/ParallelHashTableImpl.h"
#include "../../src/util/Thread.h"
#include "../../src/util/ThreadContext.h"
#include "../../src/storage/Parameters.h"
#include "../../src/triple-storage/sequential/SequentialTripleListImpl.h"
#include "../../src/triple-storage/sequential/SequentialThreeKeysIndexPolicyImpl.h"
#include "../../src/triple-storage/concurrent/ConcurrentTripleListImpl.h"
#include "../../src/triple-storage/concurrent/ConcurrentThreeKeysIndexPolicyImpl.h"
#include "../../src/triple-storage/managers/ThreeKeysManagerImpl.h"

template<class TripleListT, template<class> class HashTableTemplate, template<class> class ThreeKeysIndexPolicyTemplate>
struct ThreeKeysManagerConfiguration {
    typedef TripleListT TripleListType;
    typedef ThreeKeysIndexPolicyTemplate<TripleListType> ThreeKeysIndexPolicyType;
    typedef HashTableTemplate<ThreeKeysIndexPolicyType> ThreeKeysIndexType;
};

template<class Configuration>
class ThreeKeysManagerDerived : public ThreeKeysManager<Configuration> {

public:

    typedef typename Configuration::TripleListType TripleListType;
    typedef ThreeKeysManager<Configuration> ThreeKeysManagerBase;

    ThreeKeysManagerDerived(MemoryManager& memoryManager, TripleListType& tripleList) : ThreeKeysManagerBase(memoryManager, tripleList, Parameters()) {
    }

    using ThreeKeysManagerBase::m_threeKeysIndex;

};

template<class Configuration>
class ThreeKeysManagerTest : public QueryFixture {

protected:

    typedef ThreeKeysManagerTest<Configuration> ThreeKeysManagerTestType;

    static const uint32_t  NUMBER_OF_THREADS = 4;
    static const size_t MAX_MEMORY = 500000000;
    static const ResourceID NUMBER_OF_TRIPLES = 200000;
    static const ResourceID MAX_P = 71;
    static const ResourceID MAX_O = 213;

    MemoryManager m_memoryManager;
    typename Configuration::TripleListType m_tripleList;
    ThreeKeysManagerDerived<Configuration> m_threeKeysManager;

    void insert(ResourceID s, ResourceID p, ResourceID o) {
        typename ThreeKeysManager<Configuration>::InsertToken threeInsertToken;
        ThreadContext& threadContext = ThreadContext::getCurrentThreadContext();
        bool alreadyExists;
        ASSERT_TRUE(m_threeKeysManager.getInsertToken(threadContext, s, p, o, threeInsertToken, alreadyExists));
        if (!alreadyExists) {
            TupleIndex insertedTripleIndex = m_tripleList.add(s, p, o);
            ASSERT_NOT_EQUAL(INVALID_TUPLE_INDEX, insertedTripleIndex);
            m_threeKeysManager.updateOnInsert(threadContext, threeInsertToken, insertedTripleIndex, s, p, o);
            m_tripleList.setTripleStatus(insertedTripleIndex, TUPLE_STATUS_COMPLETE | TUPLE_STATUS_IDB);
        }
        m_threeKeysManager.releaseInsertToken(threadContext, threeInsertToken);
    }

    virtual bool startQuery(const char* const fileName, const long lineNumber, ResourceID queryS, ResourceID queryP, ResourceID queryO, ResourceID& s, ResourceID& p, ResourceID& o) {
        if (m_threeKeysManager.contains(ThreadContext::getCurrentThreadContext(), queryS, queryP, queryO, TUPLE_STATUS_IDB, TUPLE_STATUS_IDB)) {
            s = queryS;
            p = queryP;
            o = queryO;
            return true;
        }
        else
            return false;
    }

    virtual bool getNextTriple(const char* const fileName, const long lineNumber, ResourceID& s, ResourceID& p, ResourceID& o) {
        return false;
    }

    size_t getCountEstimate(ResourceID queryS, ResourceID queryP, ResourceID queryO) {
        return m_threeKeysManager.getCountEstimate(ThreadContext::getCurrentThreadContext(), queryS, queryP, queryO);
    }

    bool isBucketEmptyFor(ResourceID queryS, ResourceID queryP, ResourceID queryO) {
        typename ThreeKeysManager<Configuration>::BucketDescriptor bucketDescriptor;
        m_threeKeysManager.m_threeKeysIndex.acquireBucket(bucketDescriptor, queryS, queryP, queryO);
        BucketStatus bucketStatus = m_threeKeysManager.m_threeKeysIndex.continueBucketSearch(bucketDescriptor, queryS, queryP, queryO);
        m_threeKeysManager.m_threeKeysIndex.releaseBucket(bucketDescriptor);
        return bucketStatus == BUCKET_EMPTY;
    }

public:

    ThreeKeysManagerTest() :
        m_memoryManager(160000000),
        m_tripleList(m_memoryManager),
        m_threeKeysManager(m_memoryManager, m_tripleList)
    {
    }

    void initialize() {
        m_tripleList.initialize(0);
        m_threeKeysManager.initialize(0);
    }

    void testBasicParent() {
        insert(1, 2, 3);
        insert(1, 2, 7);
        insert(1, 2, 8);
        insert(1, 4, 5);

        ASSERT_EQUAL(static_cast<size_t>(1), getCountEstimate(1, 2, 3));
        ASSERT_EQUAL(static_cast<size_t>(1), getCountEstimate(1, 2, 7));
        ASSERT_EQUAL(static_cast<size_t>(1), getCountEstimate(1, 2, 8));
        ASSERT_EQUAL(static_cast<size_t>(1), getCountEstimate(1, 4, 5));
        ASSERT_EQUAL(static_cast<size_t>(0), getCountEstimate(2, 4, 2));

        ASSERT_QUERY(1, 2, 3,
            T(1, 2, 3)
        );
        ASSERT_QUERY(2, 4, 2);

        insert(1, 4, 6);

        ASSERT_EQUAL(static_cast<size_t>(1), getCountEstimate(1, 2, 3));
        ASSERT_EQUAL(static_cast<size_t>(1), getCountEstimate(1, 2, 7));
        ASSERT_EQUAL(static_cast<size_t>(1), getCountEstimate(1, 2, 8));
        ASSERT_EQUAL(static_cast<size_t>(1), getCountEstimate(1, 4, 5));
        ASSERT_EQUAL(static_cast<size_t>(1), getCountEstimate(1, 4, 6));
        ASSERT_EQUAL(static_cast<size_t>(0), getCountEstimate(2, 4, 2));

        ASSERT_QUERY(1, 4, 6,
            T(1, 4, 6)
        );

        insert(1, 2, 9);

        ASSERT_EQUAL(static_cast<size_t>(1), getCountEstimate(1, 2, 3));
        ASSERT_EQUAL(static_cast<size_t>(1), getCountEstimate(1, 2, 7));
        ASSERT_EQUAL(static_cast<size_t>(1), getCountEstimate(1, 2, 8));
        ASSERT_EQUAL(static_cast<size_t>(1), getCountEstimate(1, 2, 9));
        ASSERT_EQUAL(static_cast<size_t>(1), getCountEstimate(1, 4, 5));
        ASSERT_EQUAL(static_cast<size_t>(1), getCountEstimate(1, 4, 6));
        ASSERT_EQUAL(static_cast<size_t>(0), getCountEstimate(2, 4, 2));

        ASSERT_QUERY(1, 2, 9,
            T(1, 2, 9)
        );

        insert(1, 2, 10);

        ASSERT_EQUAL(static_cast<size_t>(1), getCountEstimate(1, 2, 10));
        ASSERT_EQUAL(static_cast<size_t>(0), getCountEstimate(2, 4, 2));
    }

    class WriterThread : public Thread {

    protected:

        ThreeKeysManagerTestType& m_threeKeysManagerTest;
        const size_t m_threadIndex;

    public:

        WriterThread(ThreeKeysManagerTestType& threeKeysManagerTest, const size_t threadIndex) : m_threeKeysManagerTest(threeKeysManagerTest), m_threadIndex(threadIndex) {
        }

        virtual void run() {
            m_threeKeysManagerTest.insertTriples(m_threadIndex, NUMBER_OF_THREADS);
        }

    };

    class CheckerThread : public Thread {

    protected:

        ThreeKeysManagerTestType& m_threeKeysManagerTest;
        const size_t m_threadIndex;

    public:

        CheckerThread(ThreeKeysManagerTestType& threeKeysManagerTest, const size_t threadIndex) : m_threeKeysManagerTest(threeKeysManagerTest), m_threadIndex(threadIndex) {
        }

        virtual void run() {
            m_threeKeysManagerTest.checkTriples(m_threadIndex, NUMBER_OF_THREADS);
        }

    };

    void testConcurrentParent() {
        unique_ptr_vector<Thread> threads;
        for (size_t threadIndex = 0; threadIndex < NUMBER_OF_THREADS; threadIndex++) {
            std::unique_ptr<WriterThread> thread(new WriterThread(*this, threadIndex));
            threads.push_back(std::move(thread));
            threads.back()->start();
        }
        for (size_t  threadIndex = 0; threadIndex < NUMBER_OF_THREADS; threadIndex++)
            threads[threadIndex]->join();
        threads.clear();
        for (size_t threadIndex = 0; threadIndex < NUMBER_OF_THREADS; threadIndex++) {
            std::unique_ptr<CheckerThread> thread(new CheckerThread(*this, threadIndex));
            threads.push_back(std::move(thread));
            threads.back()->start();
        }
        for (size_t threadIndex = 0; threadIndex < NUMBER_OF_THREADS; threadIndex++)
            threads[threadIndex]->join();
        threads.clear();
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

    void checkTriples(size_t start, size_t delta) {
        for (ResourceID spo = static_cast<ResourceID>(start); spo < NUMBER_OF_TRIPLES; spo += static_cast<ResourceID>(delta)) {
            ResourceID s = spo / (MAX_P * MAX_O) + 1;
            ResourceID po = spo % (MAX_P * MAX_O);
            ResourceID p = po / MAX_O + 1;
            ResourceID o = po % MAX_O + 1;
            ASSERT_QUERY(s, p, o, T(s, p, o));
        }
    }

};

#define SUITE_NAME    SequentialThreeKeysManagerTest

#include <CppTest/AutoTest.h>

typedef ThreeKeysManagerTest<ThreeKeysManagerConfiguration<SequentialTripleList, SequentialHashTable, SequentialThreeKeysIndexPolicy> > SequentialThreeKeysManagerTest;

TEST3(SUITE_NAME, testBasic, SequentialThreeKeysManagerTest) {
    testBasicParent();
}

#undef SUITE_NAME
#undef AUTOTEST_H
#undef TEST
#define SUITE_NAME    ConcurrentThreeKeysManagerTest

#include <CppTest/AutoTest.h>

typedef ThreeKeysManagerTest<ThreeKeysManagerConfiguration<ConcurrentTripleList<ResourceID, TupleIndex>, ParallelHashTable, ConcurrentThreeKeysIndexPolicy> > ConcurrentThreeKeysManagerTest;

TEST3(SUITE_NAME, testConcurrent, ConcurrentThreeKeysManagerTest) {
    testConcurrentParent();
}

#endif
