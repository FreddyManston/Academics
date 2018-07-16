// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifdef WITH_TEST

#include "../../src/all.h"
#include "../../src/Common.h"
#include "../../src/util/HashTableImpl.h"
#include "TripleData.h"

TestData::TestData(size_t initialNumberOfBuckets) : MemoryManager(1024 * 1024 * 1024), HashTable<TestData>(*static_cast<MemoryManager*>(this)) {
    initialize(initialNumberOfBuckets);
}

void TestData::clear() {
    ::memset(m_buckets, 0, m_numberOfBuckets * BUCKET_SIZE);
    m_numberOfUsedBuckets = 0;
}

void TestData::add(const ResourceID s, const ResourceID p, const ResourceID o) {
    assert(s != INVALID_RESOURCE_ID && p != INVALID_RESOURCE_ID && o != INVALID_RESOURCE_ID);
    size_t valuesHashCode;
    ResourceTriple& resourceTriple = *reinterpret_cast<ResourceTriple*>(getBucketFor(valuesHashCode, s, p, o));
    if (resourceTriple.m_s == INVALID_RESOURCE_ID) {
        resourceTriple.m_s = s;
        resourceTriple.m_p = p;
        resourceTriple.m_o = o;
        ++m_numberOfUsedBuckets;
        resizeIfNeeded();
    }
}

void TestData::toVector(std::vector<ResourceTriple>& vector) const {
    for (size_t bucketIndex = 0; bucketIndex < getNumberOfBuckets(); bucketIndex++) {
        const uint8_t* bucket = getBucket(bucketIndex);
        if (!isEmpty(bucket))
            vector.push_back(get(bucket));
    }
}

void TestData::add(const std::vector<ResourceTriple>& vector, const size_t indexToAddExclusive) {
    for (size_t tupleIndex = 0; tupleIndex < indexToAddExclusive; tupleIndex++)
        add(vector[tupleIndex].m_s, vector[tupleIndex].m_p, vector[tupleIndex].m_o);
}

void getTestData(TestData& testData, va_list arguments) {
    ResourceID s = va_arg(arguments, ResourceID);
    while (s != INVALID_RESOURCE_ID) {
        ResourceID p = va_arg(arguments, ResourceID);
        ResourceID o = va_arg(arguments, ResourceID);
        testData.add(s, p, o);
        s = va_arg(arguments, ResourceID);
    }
}

void generateTestDataMinimalRepetition(TestData& testData, const int numberOfTriples, const ResourceID maxS, const ResourceID maxP, const ResourceID maxO) {
    ResourceID currentS = 1;
    ResourceID currentP = 1;
    ResourceID currentO = 1;
    for (int index = 0; index < numberOfTriples; index++) {
        testData.add(currentS, currentP, currentO);
        currentS++;
        if (currentS >= maxS)
            currentS = 1;
        currentP++;
        if (currentP >= maxP)
            currentP = 1;
        currentO++;
        if (currentO >= maxO)
            currentO = 1;
    }
}

void generateTestDataWithRepetition(TestData& testData, const ResourceID maxS, const ResourceID maxP, const ResourceID maxO) {
    for (ResourceID currentS = 1; currentS <= maxS; ++currentS)
        for (ResourceID currentP = 1; currentP <= maxP; ++currentP)
            for (ResourceID currentO = 1; currentO <= maxO; ++currentO)
                testData.add(currentS, currentP, currentO);
}

void filterTestData(const TestData& testData, TestData& result, const ResourceID queryS, const ResourceID queryP, const ResourceID queryO, const bool compareSP, const bool compareSO, const bool comparePO) {
    for (size_t bucketIndex = 0; bucketIndex < testData.getNumberOfBuckets(); bucketIndex++) {
        const uint8_t* bucket = testData.getBucket(bucketIndex);
        if (!testData.isEmpty(bucket)) {
            const ResourceTriple& resourceTriple = testData.get(bucket);
            if ((queryS == INVALID_RESOURCE_ID || queryS == resourceTriple.m_s) && (queryP == INVALID_RESOURCE_ID || queryP == resourceTriple.m_p) && (queryO == INVALID_RESOURCE_ID || queryO == resourceTriple.m_o) &&
                (!compareSP || resourceTriple.m_s == resourceTriple.m_p) && (!compareSO || resourceTriple.m_s == resourceTriple.m_o) && (!comparePO || resourceTriple.m_p == resourceTriple.m_o))
                result.add(resourceTriple.m_s, resourceTriple.m_p, resourceTriple.m_o);
        }
    }
}

void QueryFixture::assertQuery(const char* const fileName, const long lineNumber, ResourceID queryS, ResourceID queryP, ResourceID queryO, ...) {
    TestData testData;
    va_list arguments;
    va_start(arguments, queryO);
    getTestData(testData, arguments);
    va_end(arguments);
    assertQueryData(fileName, lineNumber, queryS, queryP, queryO, testData);
}

void QueryFixture::assertQueryData(const char* const fileName, const long lineNumber, ResourceID queryS, ResourceID queryP, ResourceID queryO, const TestData& testData) {
    std::stringstream errors;
    std::vector<bool> seenBuckets(testData.getNumberOfBuckets(), false);
    bool hasNotInExpected = false;
    ResourceID currentS;
    ResourceID currentP;
    ResourceID currentO;
    bool onTriple = startQuery(fileName, lineNumber, queryS, queryP, queryO, currentS, currentP, currentO);
    while (onTriple) {
        const uint8_t* bucket = testData.findBucket(currentS, currentP, currentO);
        if (testData.isEmpty(bucket)) {
            if (!hasNotInExpected) {
                hasNotInExpected = true;
                errors << "Triples not found in expected:" << std::endl;
                errors << "------------------------------------------" << std::endl;
            }
            errors << "[" << currentS << ", " << currentP << ", " << currentO << "]" << std::endl;
        }
        else
            seenBuckets[testData.getBucketIndex(bucket)] = true;
        onTriple = getNextTriple(fileName, lineNumber, currentS, currentP, currentO);
    }
    if (hasNotInExpected)
        errors << "------------------------------------------" << std::endl;
    bool hasNotInIterator = false;
    for (size_t bucketIndex = 0; bucketIndex < testData.getNumberOfBuckets(); ++bucketIndex) {
        const uint8_t* bucket = testData.getBucket(bucketIndex);
        if (!testData.isEmpty(bucket) && !seenBuckets[bucketIndex]) {
            if (!hasNotInIterator) {
                hasNotInIterator = true;
                errors << "Triples not found in the result:" << std::endl;
                errors << "------------------------------------------" << std::endl;
            }
            const ResourceTriple& resourceTriple = testData.get(bucket);
            errors << "[" << resourceTriple.m_s << ", " << resourceTriple.m_p << ", " << resourceTriple.m_o << "]" << std::endl;
        }
    }
    if (hasNotInIterator)
        errors << "------------------------------------------" << std::endl;
    if (hasNotInExpected || hasNotInIterator)
        throw CppTest::AssertionError(fileName, lineNumber, errors.str());
}

bool IteratorQueryFixture::startQuery(const char* const fileName, const long lineNumber, ResourceID queryS, ResourceID queryP, ResourceID queryO, ResourceID& s, ResourceID& p, ResourceID& o) {
    ArgumentIndex indexS = m_argumentIndexes[0];
    ArgumentIndex indexP = m_argumentIndexes[1];
    ArgumentIndex indexO = m_argumentIndexes[2];
    m_queryBuffer[indexS] = queryS;
    m_queryBuffer[indexP] = queryP;
    m_queryBuffer[indexO] = queryO;
    m_allInputArguments.clear();
    m_surelyBoundInputArguments.clear();
    if (m_queryBuffer[indexS] != INVALID_RESOURCE_ID) {
        m_allInputArguments.add(indexS);
        if (m_surelyBoundS)
            m_surelyBoundInputArguments.add(indexS);
    }
    if (m_queryBuffer[indexP] != INVALID_RESOURCE_ID) {
        m_allInputArguments.add(indexP);
        if (m_surelyBoundP)
            m_surelyBoundInputArguments.add(indexP);
    }
    if (m_queryBuffer[indexO] != INVALID_RESOURCE_ID) {
        m_allInputArguments.add(indexO);
        if (m_surelyBoundO)
            m_surelyBoundInputArguments.add(indexO);
    }
    m_currentIterator = createIterator();
    size_t multiplicity = m_currentIterator->open();
    if (multiplicity == 0)
        return false;
    else {
        s = m_queryBuffer[indexS];
        p = m_queryBuffer[indexP];
        o = m_queryBuffer[indexO];
        CppTest::assertEqual(1, multiplicity, fileName, lineNumber);
        return true;
    }
}

bool IteratorQueryFixture::getNextTriple(const char* const fileName, const long lineNumber, ResourceID& s, ResourceID& p, ResourceID& o) {
    size_t multiplicity = m_currentIterator->advance();
    if (multiplicity == 0)
        return false;
    else {
        s = m_queryBuffer[m_argumentIndexes[0]];
        p = m_queryBuffer[m_argumentIndexes[1]];
        o = m_queryBuffer[m_argumentIndexes[2]];
        CppTest::assertEqual(1, multiplicity, fileName, lineNumber);
        return true;
    }
}

#endif
