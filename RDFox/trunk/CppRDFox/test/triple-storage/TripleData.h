// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#if defined(WITH_TEST) && !defined(TRIPLEDATA_H_)
#define TRIPLEDATA_H_

#include <CppTest/Checks.h>

#include "../../src/all.h"
#include "../../src/Common.h"
#include "../../src/util/MemoryManager.h"
#include "../../src/util/HashTable.h"
#include "../../src/storage/ArgumentIndexSet.h"
#include "../../src/storage/TupleIterator.h"
#include "../../src/triple-storage/TripleTableIterator.h"

struct ResourceTriple {
    ResourceID m_s;
    ResourceID m_p;
    ResourceID m_o;

    ResourceTriple() : m_s(INVALID_RESOURCE_ID), m_p(INVALID_RESOURCE_ID), m_o(INVALID_RESOURCE_ID) {
    }
    ResourceTriple(ResourceID s, ResourceID p, ResourceID o) : m_s(s), m_p(p), m_o(o) {
    }
    ResourceTriple(const ResourceTriple& that) : m_s(that.m_s), m_p(that.m_p), m_o(that.m_o) {
    }

    always_inline ResourceTriple& operator=(const ResourceTriple& that) {
        m_s = that.m_s;
        m_p = that.m_p;
        m_o = that.m_o;
        return *this;
    }
    always_inline bool operator==(const ResourceTriple& that) const {
        return m_s == that.m_s && m_p == that.m_p && m_o == that.m_o;
    }
    always_inline bool operator!=(const ResourceTriple& that) const {
        return m_s != that.m_s && m_p != that.m_p && m_o != that.m_o;
    }
};

always_inline bool operator<(const ResourceTriple& triple1, const ResourceTriple& triple2) {
    return
        triple1.m_s < triple2.m_s ||
        (triple1.m_s == triple2.m_s && triple1.m_p < triple2.m_p) ||
        (triple1.m_s == triple2.m_s && triple1.m_p == triple2.m_p && triple1.m_o < triple2.m_o);
}

class TestData : protected MemoryManager, HashTable<TestData> {

protected:

    friend class HashTable<TestData>;

    static const size_t BUCKET_SIZE = sizeof(ResourceTriple);

    static always_inline void copy(const uint8_t* const sourceBucket, uint8_t* const targetBucket) {
        ::memcpy(targetBucket, sourceBucket, BUCKET_SIZE);
    }

    static always_inline void invalidate(uint8_t* const bucket) {
        ::memset(bucket, 0, BUCKET_SIZE);
    }

    static always_inline size_t hashCode(const uint8_t* const bucket) {
        const ResourceTriple& resourceTriple = get(bucket);
        return hashCodeFor(resourceTriple.m_s, resourceTriple.m_p, resourceTriple.m_o);
    }

    static always_inline size_t hashCodeFor(const ResourceID value1, const ResourceID value2, const ResourceID value3) {
        size_t hash = 0;
        hash += value1;
        hash += (hash << 10);
        hash ^= (hash >> 6);

        hash += value2;
        hash += (hash << 10);
        hash ^= (hash >> 6);

        hash += value3;
        hash += (hash << 10);
        hash ^= (hash >> 6);

        hash += (hash << 3);
        hash ^= (hash >> 11);
        hash += (hash << 15);
        return hash;
    }

    static always_inline bool isValidAndNotContains(const uint8_t* const bucket, const size_t valuesHashCode, const ResourceID value1, const ResourceID value2, const ResourceID value3) {
        const ResourceTriple& resourceTriple = get(bucket);
        return resourceTriple.m_s != INVALID_RESOURCE_ID  && (resourceTriple.m_s != value1 || resourceTriple.m_p != value2 || resourceTriple.m_o != value3);
    }

public:

    TestData(size_t initialNumberOfBuckets = 16);

    always_inline size_t getSize() const { return m_numberOfUsedBuckets; }

    always_inline size_t getNumberOfBuckets() const { return m_numberOfBuckets; }

    void clear();

    void add(const ResourceID s, const ResourceID p, const ResourceID o);

    static always_inline bool isEmpty(const uint8_t* const bucket) {
        return get(bucket).m_s == INVALID_RESOURCE_ID;
    }

    const uint8_t* findBucket(const ResourceID s, const ResourceID p, const ResourceID o) const {
        size_t valuesHashCode;
        return getBucketFor(valuesHashCode, s, p, o);
    }

    always_inline const uint8_t* getBucket(const size_t bucketIndex) const {
        return m_buckets + bucketIndex * BUCKET_SIZE;
    }

    size_t getBucketIndex(const uint8_t* const bucket) const {
        return (bucket - m_buckets) / BUCKET_SIZE;
    }

    static always_inline const ResourceTriple& get(const uint8_t* bucket) {
        return *reinterpret_cast<const ResourceTriple*>(bucket);
    }

    void toVector(std::vector<ResourceTriple>& vector) const;

    void add(const std::vector<ResourceTriple>& vector, const size_t indexToAddExclusive);

    using HashTable<TestData>::remove;

};

extern void generateTestDataMinimalRepetition(TestData& testData, const int numberOfTriples, const ResourceID maxS, const ResourceID maxP, const ResourceID maxO);

extern void generateTestDataWithRepetition(TestData& testData, const ResourceID maxS, const ResourceID maxP, const ResourceID maxO);

extern void filterTestData(const TestData& testData, TestData& result, const ResourceID queryS, const ResourceID queryP, const ResourceID queryO, const bool compareSP, const bool compareSO, const bool comparePO);

extern void getTestData(TestData& testData, va_list arguments);

class QueryFixture {

protected:

    virtual void assertQuery(const char* const fileName, const long lineNumber, ResourceID queryS, ResourceID queryP, ResourceID queryO, ...);

    virtual void assertQueryData(const char* const fileName, const long lineNumber, ResourceID queryS, ResourceID queryP, ResourceID queryO, const TestData& testData);

    virtual bool startQuery(const char* const fileName, const long lineNumber, ResourceID queryS, ResourceID queryP, ResourceID queryO, ResourceID& s, ResourceID& p, ResourceID& o) = 0;

    virtual bool getNextTriple(const char* const fileName, const long lineNumber, ResourceID& s, ResourceID& p, ResourceID& o) = 0;

public:

    virtual ~QueryFixture() {}

};

class IteratorQueryFixture : public QueryFixture {

protected:

    std::vector<ResourceID> m_queryBuffer;
    std::vector<ArgumentIndex> m_argumentIndexes;
    ArgumentIndexSet m_allInputArguments;
    ArgumentIndexSet m_surelyBoundInputArguments;
    bool m_surelyBoundS;
    bool m_surelyBoundP;
    bool m_surelyBoundO;
    std::unique_ptr<TupleIterator> m_currentIterator;

    virtual std::unique_ptr<TupleIterator> createIterator() = 0;

    virtual bool startQuery(const char* const fileName, const long lineNumber, ResourceID queryS, ResourceID queryP, ResourceID queryO, ResourceID& s, ResourceID& p, ResourceID& o);

    virtual bool getNextTriple(const char* const fileName, const long lineNumber, ResourceID& s, ResourceID& p, ResourceID& o);

public:

    IteratorQueryFixture() : m_queryBuffer(), m_argumentIndexes(), m_allInputArguments(), m_surelyBoundInputArguments(), m_surelyBoundS(true), m_surelyBoundP(true), m_surelyBoundO(true), m_currentIterator() {
        m_queryBuffer.push_back(INVALID_RESOURCE_ID);
        m_queryBuffer.push_back(INVALID_RESOURCE_ID);
        m_queryBuffer.push_back(INVALID_RESOURCE_ID);
        m_argumentIndexes.push_back(0);
        m_argumentIndexes.push_back(1);
        m_argumentIndexes.push_back(2);
    }

};

#define ASSERT_QUERY(s, p, o, ...) \
    assertQuery(__FILE__, __LINE__, static_cast<ResourceID>(s), static_cast<ResourceID>(p), static_cast<ResourceID>(o), ##__VA_ARGS__, static_cast<ResourceID>(INVALID_RESOURCE_ID))

#define ASSERT_QUERY_DATA(s, p, o, data)    \
    assertQueryData(__FILE__, __LINE__, static_cast<ResourceID>(s), static_cast<ResourceID>(p), static_cast<ResourceID>(o), (data))

#define T(s,p,o)    static_cast<ResourceID>(s), static_cast<ResourceID>(p), static_cast<ResourceID>(o)

#endif // TRIPLEDATA_H_
