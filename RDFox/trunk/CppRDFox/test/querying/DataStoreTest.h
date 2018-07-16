// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#if defined(WITH_TEST) && !defined(DATASTORETEST_H_)
#define DATASTORETEST_H_

#include <CppTest/Checks.h>

#include "../../src/logic/Logic.h"
#include "../../src/storage/Parameters.h"
#include "../../src/storage/DataStore.h"
#include "../../src/storage/TupleTable.h"
#include "../../src/storage/TupleIterator.h"
#include "../../src/storage/Parameters.h"
#include "../../src/util/Prefixes.h"
#include "../../src/util/HashTableImpl.h"

class Dictionary;
class ResourceValueCache;
class TupleTable;

typedef std::vector<ResourceID> ResourceIDTuple;

struct ResourceIDMultituple {
    ResourceIDTuple m_tuple;
    size_t m_multiplicity;

    ResourceIDMultituple(const ResourceIDTuple& tuple, size_t multiplicity = 0) : m_tuple(tuple), m_multiplicity(multiplicity) {
    }

};

class ResourceIDTupleMultiset : protected MemoryManager, HashTable<ResourceIDTupleMultiset> {

protected:

    friend class HashTable<ResourceIDTupleMultiset>;

    struct Bucket {
        ResourceIDMultituple* m_multituple;
    };

    static const size_t BUCKET_SIZE = sizeof(Bucket);

    static always_inline void copy(const uint8_t* const sourceBucket, uint8_t* const targetBucket) {
        ::memcpy(targetBucket, sourceBucket, BUCKET_SIZE);
    }

    static always_inline void invalidate(uint8_t* const bucket) {
        ::memset(bucket, 0, BUCKET_SIZE);
    }

    static always_inline size_t hashCode(const uint8_t* const bucket) {
        return hashCodeFor(reinterpret_cast<const Bucket*>(bucket)->m_multituple->m_tuple);
    }

    static always_inline size_t hashCodeFor(const ResourceIDTuple& tuple) {
        size_t hash = 0;
        for (size_t index = 0; index < tuple.size(); ++index) {
            hash += tuple[index];
            hash += (hash << 10);
            hash ^= (hash >> 6);
        }
        hash += (hash << 3);
        hash ^= (hash >> 11);
        hash += (hash << 15);
        return hash;
    }

    static always_inline bool isValidAndNotContains(const uint8_t* const bucket, const size_t valuesHashCode, const ResourceIDTuple& tuple) {
        const ResourceIDMultituple* multisetEntry = reinterpret_cast<const Bucket*>(bucket)->m_multituple;
        if (multisetEntry != 0) {
            const ResourceIDTuple& entryTuple = multisetEntry->m_tuple;
            if (entryTuple.size() != tuple.size())
                return true;
            for (size_t index = 0; index < tuple.size(); ++index)
                if (entryTuple[index] != tuple[index])
                    return true;
        }
        return false;
    }

public:

    ResourceIDTupleMultiset(size_t initialNumberOfBuckets = 16);

    ~ResourceIDTupleMultiset();

    always_inline size_t getSize() const {
        return m_numberOfUsedBuckets;
    }

    always_inline size_t getNumberOfBuckets() const {
        return m_numberOfBuckets;
    }

    void clear();

    void add(const ResourceIDTuple& tuple, const size_t multiplicity = 1);

    static always_inline bool isEmpty(const uint8_t* const bucket) {
        return reinterpret_cast<const Bucket*>(bucket)->m_multituple == 0;
    }

    static always_inline const ResourceIDMultituple& get(const uint8_t* const bucket) {
        return *reinterpret_cast<const Bucket*>(bucket)->m_multituple;
    }

    always_inline const uint8_t* getFirstBucket() const {
        return m_buckets;
    }

    static always_inline const uint8_t* getNextBucket(const uint8_t* const bucket) {
        return bucket + BUCKET_SIZE;
    }

    always_inline bool isAfterLastBucket(const uint8_t* const bucket) const {
        return bucket == m_afterLastBucket;
    }

    bool containsAll(const ResourceIDTupleMultiset& containee) const;

    void loadTriples(const char* const tuplesText, Dictionary& dictionary, const Prefixes& prefixes);

    void toString(std::ostream& output, const ResourceValueCache& resourceValueCache, const Prefixes& prefixes) const;

};

class DataStoreTest {

protected:

    static ArgumentIndex s_tupleTableArguments[3];

    LogicFactory m_factory;
    Prefixes m_prefixes;
    std::unique_ptr<DataStore> m_dataStore;
    TupleTable& m_tripleTable;
    Dictionary& m_dictionary;
    Parameters m_queryParameters;
    bool m_traceReasoning;
    bool m_processComponentsByLevels;
    bool m_useDRed;

    virtual void initializeQueryParameters();

    ResourceID resolve(const std::string& iri) const;

    ResourceID resolve(const DatatypeID datatypeID, const std::string& lexicalForm) const;

    ResourceID resolveToNormal(const std::string& iri) const;

    std::vector<ResourceID> equivalents(const std::string& iri) const;

    std::string lexicalForm(const ResourceID resourceID) const;

    void checkTupleStatuses();

    void addTriples(const char* const triplesText);

    void addTriple(const ResourceID subjectID, const ResourceID predicateID, const ResourceID objectID, const TupleStatus deleteTupleStatus = 0, const TupleStatus addTupleStatus = TUPLE_STATUS_EDB | TUPLE_STATUS_IDB);

    void addTriple(const std::string& subjectIRI, const std::string& predicateIRI, const std::string& objectIRI, const TupleStatus deleteTupleStatus = 0, const TupleStatus addTupleStatus = TUPLE_STATUS_EDB | TUPLE_STATUS_IDB);

    void removeTriples(const char* const triplesText);

    void removeTriple(const ResourceID subjectID, const ResourceID predicateID, const ResourceID objectID);

    void removeTriple(const std::string& subjectIRI, const std::string& predicateIRI, const std::string& objectIRI);

    void forAddition(const char* const triplesText);

    void forAddition(const ResourceID subjectID, const ResourceID predicateID, const ResourceID objectID);

    void forAddition(const std::string& subjectIRI, const std::string& predicateIRI, const std::string& objectIRI);

    void forDeletion(const char* const triplesText);

    void forDeletion(const ResourceID subjectID, const ResourceID predicateID, const ResourceID objectID);

    void forDeletion(const std::string& subjectIRI, const std::string& predicateIRI, const std::string& objectIRI);

    void clearRulesAndMakeFactsExplicit();

    void addRules(const char* const programText);

    void removeRules(const char* const programText);

    void applyRules(const size_t numberOfThreads = 1);

    void assertApplyRules(const char* const fileName, const long lineNumber, const char* const expectedTrace);

    void applyRulesIncrementally(const size_t numberOfThreads = 1);

    void assertApplyRulesIncrementally(const char* const fileName, const long lineNumber, const char* const expectedTrace);

    Query parseQuery(const char* const queryText);

    void assertQuery(const char* const fileName, const long lineNumber, const char* const queryText, const char* const expectedTriplesText, const char* const expectedPlanText = nullptr);

    void assertQuery(const char* const fileName, const long lineNumber, const char* const queryText, const ResourceIDTupleMultiset& expected, const char* const expectedPlanText = nullptr);

    void assertQuery(const char* const fileName, const long lineNumber, const Query& query, const char* const expectedTriplesText, const char* const expectedPlanText = nullptr);

    void assertQuery(const char* const fileName, const long lineNumber, const Query& query, const ResourceIDTupleMultiset& expected, const char* const expectedPlanText = nullptr);

    always_inline static Parameters getDataStoreParameters(const EqualityAxiomatizationType equalityAxiomatizationType) {
        Parameters dataStoreParameters;
        switch (equalityAxiomatizationType) {
        case EQUALITY_AXIOMATIZATION_NO_UNA:
            dataStoreParameters.setString("equality", "noUNA");
            break;
        case EQUALITY_AXIOMATIZATION_UNA:
            dataStoreParameters.setString("equality", "UNA");
            break;
        case EQUALITY_AXIOMATIZATION_OFF:
        default:
            dataStoreParameters.setString("equality", "off");
            break;
        }
        return dataStoreParameters;
    }

public:

    DataStoreTest(const char* const dataStoreName = "seq", const EqualityAxiomatizationType equalityAxiomatizationType = EQUALITY_AXIOMATIZATION_OFF);

    void initialize();

};

template<typename T>
std::ostream& operator<<(std::ostream& output, const std::vector<T>& vector) {
    output << "[";
    bool first = true;
    for (typename std::vector<T>::const_iterator iterator = vector.begin(); iterator != vector.end(); ++iterator) {
        if (first)
            first = false;
        else
            output << ",";
        output << " " << *iterator;
    }
    output << " ]";
    return output;
}

#define ASSERT_QUERY(query, expected) \
    assertQuery(__FILE__, __LINE__, query, expected)

#define ASSERT_APPLY_RULES(expectedTrace) \
    assertApplyRules(__FILE__, __LINE__, expectedTrace)

#define ASSERT_QUERY_AND_PLAN(query, expected, expectedPlan) \
    assertQuery(__FILE__, __LINE__, query, expected, expectedPlan)

#define ASSERT_APPLY_RULES_INCREMENTALLY(expectedTrace) \
    assertApplyRulesIncrementally(__FILE__, __LINE__, expectedTrace)

#endif // DATASTORETEST_H_
