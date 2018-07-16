// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifdef WITH_TEST

#define AUTO_TEST    TripleTableByOneTest

#include <CppTest/AutoTest.h>

#include "TripleData.h"
#include "../../src/util/MemoryManager.h"
#include "../../src/util/SequentialHashTableImpl.h"
#include "../../src/storage/Parameters.h"
#include "../../src/triple-storage/sequential/SequentialOneKeyIndexImpl.h"
#include "../../src/triple-storage/sequential/SequentialTripleListImpl.h"
#include "../../src/triple-storage/sequential/SequentialThreeKeysIndexPolicyImpl.h"
#include "../../src/triple-storage/managers/TwoKeysManagerGroupByOneImpl.h"
#include "../../src/triple-storage/managers/ThreeKeysManagerImpl.h"
#include "../../src/triple-storage/QueryPatternHandlers.h"
#include "../../src/triple-storage/TripleTableImpl.h"

struct SequentialTripleTableByOneConfiguration {
    typedef Query1On1<SequentialTripleTableByOneConfiguration> QP1HandlerType;      // Query: s * *
    typedef Query2On1<SequentialTripleTableByOneConfiguration> QP2HandlerType;      // Query: * p *
    typedef Query1On12<SequentialTripleTableByOneConfiguration> QP3HandlerType;     // Query: s p *
    typedef Query3On1<SequentialTripleTableByOneConfiguration> QP4HandlerType;      // Query: * * o
    typedef Query3On12<SequentialTripleTableByOneConfiguration> QP5HandlerType;     // Query: s * o
    typedef Query2On12 <SequentialTripleTableByOneConfiguration> QP6HandlerType;    // Query: * p o
    typedef TripleTable<SequentialTripleTableByOneConfiguration> TripleTableType;
    typedef SequentialTripleList TripleListType;
    typedef SequentialOneKeyIndex OneKeyIndexType;

    struct TwoKeysManager1Configuration {
        static const ResourceComponent COMPONENT1 = RC_S;
        static const ResourceComponent COMPONENT2 = RC_P;
        static const ResourceComponent COMPONENT3 = RC_O;
        typedef SequentialTripleTableByOneConfiguration::TripleListType TripleListType;
        typedef SequentialTripleTableByOneConfiguration::OneKeyIndexType OneKeyIndexType;
    };

    struct TwoKeysManager2Configuration {
        static const ResourceComponent COMPONENT1 = RC_P;
        static const ResourceComponent COMPONENT2 = RC_O;
        static const ResourceComponent COMPONENT3 = RC_S;
        typedef SequentialTripleTableByOneConfiguration::TripleListType TripleListType;
        typedef SequentialTripleTableByOneConfiguration::OneKeyIndexType OneKeyIndexType;
    };

    struct TwoKeysManager3Configuration {
        static const ResourceComponent COMPONENT1 = RC_O;
        static const ResourceComponent COMPONENT2 = RC_S;
        static const ResourceComponent COMPONENT3 = RC_P;
        typedef SequentialTripleTableByOneConfiguration::TripleListType TripleListType;
        typedef SequentialTripleTableByOneConfiguration::OneKeyIndexType OneKeyIndexType;
    };

    typedef TwoKeysManagerGroupByOne<TwoKeysManager1Configuration> TwoKeysManager1Type;
    typedef TwoKeysManagerGroupByOne<TwoKeysManager2Configuration> TwoKeysManager2Type;
    typedef TwoKeysManagerGroupByOne<TwoKeysManager3Configuration> TwoKeysManager3Type;

    struct ThreeKeysManagerConfiguration {
        typedef SequentialTripleTableByOneConfiguration::TripleListType TripleListType;
        typedef SequentialThreeKeysIndexPolicy<TripleListType> ThreeKeysIndexPolicyType;
        typedef SequentialHashTable<ThreeKeysIndexPolicyType> ThreeKeysIndexType;
    };

    typedef ThreeKeysManager<ThreeKeysManagerConfiguration> ThreeKeysManagerType;

};

typedef TripleTable<SequentialTripleTableByOneConfiguration> SequentialTripleTableByOne;

template<>
const char* const TripleTableTraits<SequentialTripleTableByOne>::TYPE_NAME = "seq-by-one";

template class TripleTable<SequentialTripleTableByOneConfiguration>;

class TripleTableByOneTest : public IteratorQueryFixture {

protected:

    MemoryManager m_memoryManager;
    SequentialTripleTableByOne m_tripleTable;

    virtual std::unique_ptr<TupleIterator> createIterator() {
        return m_tripleTable.createTupleIterator(m_queryBuffer, m_argumentIndexes, m_allInputArguments, m_surelyBoundInputArguments);
    }

public:

    TripleTableByOneTest() : m_memoryManager(1000000), m_tripleTable(m_memoryManager, Parameters()) {
    }

    void initialize() {
        m_tripleTable.initialize(0, 0);
    }

    void addTriple(ResourceID s, ResourceID p, ResourceID o) {
        m_queryBuffer[0] = s;
        m_queryBuffer[1] = p;
        m_queryBuffer[2] = o;
        m_tripleTable.addTuple(m_queryBuffer, m_argumentIndexes, 0, TUPLE_STATUS_IDB);
    }

};

TEST(testBasic) {
    addTriple(2, 3, 4);
    addTriple(2, 3, 5);
    addTriple(2, 3, 6);
    addTriple(2, 4, 4);
    addTriple(2, 4, 5);
    addTriple(3, 3, 4);
    addTriple(3, 3, 5);

    ASSERT_QUERY(2, 3, 4,
        T(2, 3, 4)
    );

    ASSERT_QUERY(2, 3, INVALID_RESOURCE_ID,
        T(2, 3, 4),
        T(2, 3, 5),
        T(2, 3, 6)
    );

    ASSERT_QUERY(2, INVALID_RESOURCE_ID, 4,
        T(2, 3, 4),
        T(2, 4, 4)
    );

    ASSERT_QUERY(2, INVALID_RESOURCE_ID, INVALID_RESOURCE_ID,
        T(2, 3, 4),
        T(2, 3, 5),
        T(2, 3, 6),
        T(2, 4, 4),
        T(2, 4, 5)
    );

    ASSERT_QUERY(INVALID_RESOURCE_ID, 3, 4,
        T(2, 3, 4),
        T(3, 3, 4)
    );

    ASSERT_QUERY(INVALID_RESOURCE_ID, 3, INVALID_RESOURCE_ID,
        T(2, 3, 4),
        T(2, 3, 5),
        T(2, 3, 6),
        T(3, 3, 4),
        T(3, 3, 5)
    );

    ASSERT_QUERY(INVALID_RESOURCE_ID, INVALID_RESOURCE_ID, 4,
        T(2, 3, 4),
        T(2, 4, 4),
        T(3, 3, 4)
    );

    ASSERT_QUERY(INVALID_RESOURCE_ID, INVALID_RESOURCE_ID, INVALID_RESOURCE_ID,
        T(2, 3, 4),
        T(2, 3, 5),
        T(2, 3, 6),
        T(2, 4, 4),
        T(2, 4, 5),
        T(3, 3, 4),
        T(3, 3, 5)
    );

}

#endif
