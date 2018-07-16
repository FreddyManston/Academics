// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifdef WITH_TEST

#define SUITE_NAME HashTableTest

#include <CppTest/AutoTest.h>
#include "../../src/Common.h"
#include "../../src/util/SequentialHashTableImpl.h"
#include "../../src/util/ParallelHashTableImpl.h"
#include "../../src/util/Thread.h"
#include "../../src/util/ThreadContext.h"

// PolicyBase

class PolicyBase {

public:

    static const size_t BUCKET_SIZE = sizeof(size_t);

    struct BucketContents {
        size_t m_value;
    };

    static always_inline void invalidate(uint8_t* const bucket) {
        ::atomicWrite(*reinterpret_cast<size_t*>(bucket), 0);
    }

    static always_inline void getBucketContents(const uint8_t* const bucket, BucketContents& bucketContents) {
        bucketContents.m_value = get(bucket);
    }

    static always_inline BucketStatus getBucketContentsStatus(const BucketContents& bucketContents, const size_t valuesHashCode, const size_t value) {
        if (bucketContents.m_value == 0)
            return BUCKET_EMPTY;
        else if (bucketContents.m_value == value)
            return BUCKET_CONTAINS;
        else
            return BUCKET_NOT_CONTAINS;
    }

    static always_inline bool isBucketContentsEmpty(const BucketContents& bucketContents) {
        return bucketContents.m_value == 0;
    }

    static always_inline size_t getBucketContentsHashCode(const BucketContents& bucketContents) {
        return hashCodeFor(bucketContents.m_value);
    }

    static always_inline size_t hashCodeFor(const size_t value) {
        size_t hash = 0;
        hash += value;
        hash += (hash << 10);
        hash ^= (hash >> 6);

        hash += (hash << 3);
        hash ^= (hash >> 11);
        hash += (hash << 15);
        return hash;
    }

    static always_inline size_t get(const uint8_t* bucket) {
        return ::atomicRead(*reinterpret_cast<const size_t*>(bucket));
    }

    static always_inline void set(uint8_t* bucket, const size_t value) {
        ::atomicWrite(*reinterpret_cast<size_t*>(bucket), value);
    }

};

// PolicySequential

class PolicySequential : public PolicyBase {

public:

    static always_inline bool setBucketContentsIfEmpty(uint8_t* const bucket, const BucketContents& bucketContents) {
        if (get(bucket) == 0) {
            ::atomicWrite(*reinterpret_cast<size_t*>(bucket), bucketContents.m_value);
            return true;
        }
        else
            return false;
    }

    static always_inline bool setConditional(uint8_t* bucket, const size_t expectedValue, const size_t value) {
        set(bucket, value);
        return true;
    }

};

// PolicyConcurrent

class PolicyConcurrent : public PolicyBase {

public:

    static always_inline bool setBucketContentsIfEmpty(uint8_t* const bucket, const BucketContents& bucketContents) {
        return ::atomicConditionalSet(*reinterpret_cast<size_t*>(bucket), 0, bucketContents.m_value);
    }

    static always_inline bool setConditional(uint8_t* bucket, const size_t expectedValue, const size_t value) {
        return ::atomicConditionalSet(*reinterpret_cast<size_t*>(bucket), expectedValue, value);
    }

};

// PolicySelector

template<template <class> class HashTable>
struct PolicySelector {
    typedef PolicyConcurrent Policy;
};

template<>
struct PolicySelector<SequentialHashTable> {
    typedef PolicySequential Policy;
};

// HashTableTester

template<template <class> class HashTable>
class HashTableTester {

protected:

    typedef typename PolicySelector<HashTable>::Policy Policy;
    typedef typename HashTable<Policy>::BucketDescriptor BucketDescriptor;

    void add(const size_t value) {
        BucketDescriptor bucketDescriptor;
        ThreadContext& threadContext = ThreadContext::getCurrentThreadContext();
        m_hashTable.acquireBucket(threadContext, bucketDescriptor, value);
        BucketStatus bucketStatus;
        while ((bucketStatus = m_hashTable.continueBucketSearch(threadContext, bucketDescriptor, value)) == BUCKET_EMPTY) {
            if (m_hashTable.getPolicy().setConditional(bucketDescriptor.m_bucket, 0, value)) {
                m_hashTable.acknowledgeInsert(threadContext, bucketDescriptor);
                break;
            }
        }
        ASSERT_TRUE(bucketStatus != BUCKET_NOT_CONTAINS);
        m_hashTable.releaseBucket(threadContext, bucketDescriptor);
    }

    bool contains(const size_t element) {
        BucketDescriptor bucketDescriptor;
        ThreadContext& threadContext = ThreadContext::getCurrentThreadContext();
        m_hashTable.acquireBucket(threadContext, bucketDescriptor, element);
        BucketStatus bucketStatus = m_hashTable.continueBucketSearch(threadContext, bucketDescriptor, element);
        m_hashTable.releaseBucket(threadContext, bucketDescriptor);
        return bucketStatus != BUCKET_EMPTY;
    }

    class WriterThread : public Thread {

    protected:

        HashTableTester<HashTable>& m_hashTableTester;
        const size_t m_start;
        const size_t m_end;

    public:

        WriterThread(HashTableTester<HashTable>& hashTableTester, const size_t start, const size_t end) : m_hashTableTester(hashTableTester), m_start(start), m_end(end) {
        }

        virtual void run() {
            for (size_t item = m_start; item <= m_end; ++item)
                m_hashTableTester.add(item);
        }

    };

    class ReaderThread : public Thread {

    protected:

        HashTableTester<HashTable>& m_hashTableTester;
        const size_t m_start;
        const size_t m_end;

    public:

        ReaderThread(HashTableTester<HashTable>& hashTableTester, const size_t start, const size_t end) : m_hashTableTester(hashTableTester), m_start(start), m_end(end) {
        }

        virtual void run() {
            for (size_t item = m_start; item <= m_end; ++item)
                if (!m_hashTableTester.contains(item)) {
                    std::ostringstream message;
                    message << "Missing item " << item;
                    FAIL2(message.str());
                }
        }

    };

public:

    MemoryManager m_memoryManager;
    HashTable<Policy> m_hashTable;
    const size_t m_numberOfThreads;

    HashTableTester(const size_t numberOfThreads) : m_memoryManager(6000000000ULL), m_hashTable(m_memoryManager, HASH_TABLE_LOAD_FACTOR), m_numberOfThreads(numberOfThreads) {
        m_hashTable.setNumberOfThreads(numberOfThreads);
    }

    void runTest() {
        ASSERT_TRUE(m_hashTable.initialize(HASH_TABLE_INITIAL_SIZE));
        const size_t totalWork = 30000000;
        const size_t threadChunkSize = totalWork/m_numberOfThreads;
        size_t currentIndex = 1;
        unique_ptr_vector<Thread> threads;
        for (size_t threadIndex = 0; threadIndex < m_numberOfThreads; threadIndex++) {
            size_t start = currentIndex;
            size_t end = (currentIndex += threadChunkSize);
            if (threadIndex == m_numberOfThreads - 1)
                end = totalWork;
            std::unique_ptr<WriterThread> thread(new WriterThread(*this, start, end));
            threads.push_back(std::move(thread));
            threads.back()->start();
        }
        for (size_t threadIndex = 0; threadIndex < m_numberOfThreads; threadIndex++)
            threads[threadIndex]->join();
        threads.clear();
        currentIndex = 1;
        for (size_t threadIndex = 0; threadIndex < m_numberOfThreads; threadIndex++) {
            size_t start = currentIndex;
            size_t end = (currentIndex += threadChunkSize);
            if (threadIndex == m_numberOfThreads - 1)
                end = totalWork;
            std::unique_ptr<ReaderThread> thread(new ReaderThread(*this, start, end));
            threads.push_back(std::move(thread));
            threads.back()->start();
        }
        for (size_t threadIndex = 0; threadIndex < m_numberOfThreads; threadIndex++)
            threads[threadIndex]->join();
        threads.clear();
        for (size_t number = 1; number <= totalWork; number++)
            ASSERT_TRUE(contains(number));
    }

};

// SequentialHashTable

TEST(SequentialHashTable_1) {
    HashTableTester<SequentialHashTable>(1).runTest();
}

// ParallelResizeHashTable

TEST(ParallelHashTable_1) {
    HashTableTester<ParallelHashTable>(1).runTest();
}

TEST(ParallelHashTable_2) {
    HashTableTester<ParallelHashTable>(2).runTest();
}

TEST(ParallelHashTable_3) {
    HashTableTester<ParallelHashTable>(3).runTest();
}

TEST(ParallelHashTable_4) {
    HashTableTester<ParallelHashTable>(4).runTest();
}

TEST(ParallelHashTable_5) {
    HashTableTester<ParallelHashTable>(5).runTest();
}

TEST(ParallelHashTable_6) {
    HashTableTester<ParallelHashTable>(6).runTest();
}

TEST(ParallelHashTable_7) {
    HashTableTester<ParallelHashTable>(7).runTest();
}

TEST(ParallelHashTable_8) {
    HashTableTester<ParallelHashTable>(8).runTest();
}

#endif
