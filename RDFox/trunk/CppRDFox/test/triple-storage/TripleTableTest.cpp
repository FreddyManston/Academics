// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifdef WITH_TEST

#include "TripleData.h"
#include "../../src/util/MemoryManager.h"
#include "../../src/util/Thread.h"
#include "../../src/storage/TupleTable.h"
#include "../../src/storage/Parameters.h"
#include "../../src/storage/DataStore.h"

class TripleTableTest : public IteratorQueryFixture {

protected:

    static const size_t NUMBER_OF_THREADS = 2;
    std::unique_ptr<DataStore> m_dataStore;
    TupleTable& m_tripleTable;
    TestData m_testData;


    virtual std::unique_ptr<TupleIterator> createIterator() {
        return m_tripleTable.createTupleIterator(m_queryBuffer, m_argumentIndexes, m_allInputArguments, m_surelyBoundInputArguments, TUPLE_STATUS_COMPLETE | TUPLE_STATUS_IDB | TUPLE_STATUS_IDB_MERGED, TUPLE_STATUS_COMPLETE | TUPLE_STATUS_IDB);
    }

    static Parameters getDataStoreParameters() {
        Parameters dataStoreParameters;
        dataStoreParameters.setString("equality", "off");
        return dataStoreParameters;
    }

public:

    TripleTableTest(const char* const storeName) : m_dataStore(::newDataStore(storeName, getDataStoreParameters())), m_tripleTable(m_dataStore->getTupleTable("internal$rdf")), m_testData() {
        if (m_dataStore->isConcurrent())
            m_dataStore->setNumberOfThreads(NUMBER_OF_THREADS);
    }

    void initialize() {
        m_dataStore->initialize(0, 0);
    }

    void addTriple(ResourceID s, ResourceID p, ResourceID o) {
        // WARNING: this works only in single-threaded mode because m_queryBuffer is global
        m_queryBuffer[0] = s;
        m_queryBuffer[1] = p;
        m_queryBuffer[2] = o;
        m_tripleTable.addTuple(m_queryBuffer, m_argumentIndexes, 0, TUPLE_STATUS_IDB);
    }

    bool deleteTriple(ResourceID s, ResourceID p, ResourceID o) {
        // WARNING: this works only in single-threaded mode because m_queryBuffer is global
        m_queryBuffer[0] = s;
        m_queryBuffer[1] = p;
        m_queryBuffer[2] = o;
        const TupleIndex tupleIndex = m_tripleTable.getTupleIndex(m_queryBuffer, m_argumentIndexes);
        return m_tripleTable.deleteTupleStatus(tupleIndex, TUPLE_STATUS_IDB);
    }

    size_t getCountEstimate(ResourceID s, ResourceID p, ResourceID o) {
        // WARNING: this works only in single-threaded mode because m_queryBuffer and m_allInputArguments are global
        m_queryBuffer[0] = s;
        m_queryBuffer[1] = p;
        m_queryBuffer[2] = o;
        m_allInputArguments.clear();
        if (s != INVALID_RESOURCE_ID)
            m_allInputArguments.add(0);
        if (p != INVALID_RESOURCE_ID)
            m_allInputArguments.add(1);
        if (o != INVALID_RESOURCE_ID)
            m_allInputArguments.add(2);
        return m_tripleTable.getCountEstimate(m_queryBuffer, m_argumentIndexes, m_allInputArguments);
    }

    void runSymmetricQueries(const ResourceID maxS, const ResourceID maxP, const ResourceID maxO, const bool checkCount = true) {
        TestData filteredTestData;
        ResourceID max[3] = { maxS, maxP, maxO };
        ResourceID query[3] = { INVALID_RESOURCE_ID, INVALID_RESOURCE_ID, INVALID_RESOURCE_ID };

        if (checkCount)
            ASSERT_EQUAL(m_testData.getSize(), m_tripleTable.getTupleCount(TUPLE_STATUS_IDB, TUPLE_STATUS_IDB));

        // Run the query with no bindings
        ASSERT_QUERY_DATA(INVALID_RESOURCE_ID, INVALID_RESOURCE_ID, INVALID_RESOURCE_ID, m_testData);

        // Run the queries with one binding
        for (int query1 = RC_S; query1 <= RC_O; ++query1) {
            for (query[query1] = 1; query[query1] < max[query1]; ++query[query1]) {
                filteredTestData.clear();
                filterTestData(m_testData, filteredTestData, query[0], query[1], query[2], false, false, false);
                ASSERT_QUERY_DATA(query[0], query[1], query[2], filteredTestData);
            }
            query[query1] = INVALID_RESOURCE_ID;
        }

        // Run the queries with two bindings
        for (int query1 = RC_S; query1 < RC_O; ++query1) {
            for (query[query1] = 1; query[query1] < max[query1]; ++query[query1]) {
                for (int query2 = query1 + 1; query2 <= RC_O; ++query2) {
                    for (query[query2] = 1; query[query2] < max[query2]; ++query[query2]) {
                        filteredTestData.clear();
                        filterTestData(m_testData, filteredTestData, query[0], query[1], query[2], false, false, false);
                        ASSERT_QUERY_DATA(query[0], query[1], query[2], filteredTestData);
                    }
                    query[query2] = INVALID_RESOURCE_ID;
                }
            }
            query[query1] = INVALID_RESOURCE_ID;
        }

        // Run the queries with three bindings
        for (size_t bucketIndex = 0; bucketIndex < m_testData.getNumberOfBuckets(); ++bucketIndex) {
            const uint8_t* bucket = m_testData.getBucket(bucketIndex);
            if (!m_testData.isEmpty(bucket)) {
                const ResourceTriple& resourceTriple = m_testData.get(bucket);
                ASSERT_QUERY(resourceTriple.m_s, resourceTriple.m_p, resourceTriple.m_o,
                    T(resourceTriple.m_s, resourceTriple.m_p, resourceTriple.m_o)
                );
            }
        }
    }

    void runEqualityQueries(const ResourceID maxS, const ResourceID maxP, const ResourceID maxO) {
        for (uint8_t compareSP = 0; compareSP <= 1; ++compareSP)
            for (uint8_t compareSO = 0; compareSO <= 1; ++compareSO)
                for (uint8_t comparePO = 0; comparePO <= 1; ++comparePO) {
                    m_argumentIndexes[0] = 0;
                    m_argumentIndexes[1] = 1;
                    m_argumentIndexes[2] = 2;
                    if (compareSP)
                        m_argumentIndexes[1] = m_argumentIndexes[0];
                    if (compareSO)
                        m_argumentIndexes[2] = m_argumentIndexes[0];
                    if (comparePO) {
                        if (m_argumentIndexes[1] < m_argumentIndexes[2])
                            m_argumentIndexes[2] = m_argumentIndexes[1];
                        else
                            m_argumentIndexes[1] = m_argumentIndexes[2];
                    }
                    for (ResourceID s = INVALID_RESOURCE_ID; s <= maxS * (m_argumentIndexes[0] == 0); s++)
                        for (ResourceID p = INVALID_RESOURCE_ID; p <= maxP * (m_argumentIndexes[1] == 1); p++)
                            for (ResourceID o = INVALID_RESOURCE_ID; o <= maxO * (m_argumentIndexes[2] == 2); o++) {
                                m_queryBuffer[0] = s;
                                m_queryBuffer[1] = p;
                                m_queryBuffer[2] = o;
                                TestData filteredTestData;
                                filterTestData(m_testData, filteredTestData, m_queryBuffer[m_argumentIndexes[0]], m_queryBuffer[m_argumentIndexes[1]], m_queryBuffer[m_argumentIndexes[2]], compareSP != 0, compareSO != 0, comparePO != 0);
                                ASSERT_QUERY_DATA(m_queryBuffer[m_argumentIndexes[0]], m_queryBuffer[m_argumentIndexes[1]], m_queryBuffer[m_argumentIndexes[2]], filteredTestData);
                            }
                }
    }

    void load() {
        for (size_t bucketIndex = 0; bucketIndex < m_testData.getNumberOfBuckets(); ++bucketIndex) {
            const uint8_t* bucket = m_testData.getBucket(bucketIndex);
            if (!m_testData.isEmpty(bucket)) {
                const ResourceTriple& resourceTriple = m_testData.get(bucket);
                addTriple(resourceTriple.m_s, resourceTriple.m_p, resourceTriple.m_o);
            }
        }
    }

    void testBasicParent() {
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

    void testGetTupleIfExistsParent() {
        addTriple(1, 2, 3);
        addTriple(2, 3, 3);

        std::vector<ResourceID> tupleBuffer;
        tupleBuffer.insert(tupleBuffer.begin(), 3, INVALID_RESOURCE_ID);

        ASSERT_EQUAL(TUPLE_STATUS_COMPLETE | TUPLE_STATUS_IDB, m_tripleTable.getStatusAndTupleIfComplete(1, tupleBuffer));
        ASSERT_EQUAL(1, tupleBuffer[0]);
        ASSERT_EQUAL(2, tupleBuffer[1]);
        ASSERT_EQUAL(3, tupleBuffer[2]);

        ASSERT_EQUAL(TUPLE_STATUS_COMPLETE | TUPLE_STATUS_IDB, m_tripleTable.getStatusAndTupleIfComplete(2, tupleBuffer));
        ASSERT_EQUAL(2, tupleBuffer[0]);
        ASSERT_EQUAL(3, tupleBuffer[1]);
        ASSERT_EQUAL(3, tupleBuffer[2]);

        ASSERT_EQUAL(TUPLE_STATUS_INVALID, m_tripleTable.getStatusAndTupleIfComplete(3, tupleBuffer));
    }

    void testBulkParent() {
        const ResourceID maxS = 99;
        const ResourceID maxP = 39;
        const ResourceID maxO = 400;
        const int numberOfTriples = 10000;
        generateTestDataMinimalRepetition(m_testData, numberOfTriples, maxS, maxP, maxO);
        load();
        runSymmetricQueries(maxS, maxP, maxO);
    }

    void testEqualityQueriesSmallParent() {
        const ResourceID maxS = 4;
        const ResourceID maxP = 4;
        const ResourceID maxO = 4;
        m_testData.add(1, 1, 1);
        m_testData.add(1, 2, 1);
        m_testData.add(1, 2, 2);
        m_testData.add(1, 2, 3);
        m_testData.add(3, 1, 1);
        m_testData.add(3, 1, 2);
        m_testData.add(3, 1, 3);
        m_testData.add(4, 4, 4);
        load();
        runEqualityQueries(maxS, maxP, maxO);
    }

    void testEqualityQueriesLargeParent() {
        const ResourceID maxS = 10;
        const ResourceID maxP = 20;
        const ResourceID maxO = 30;
        generateTestDataWithRepetition(m_testData, maxS, maxP, maxO);
        load();
        runEqualityQueries(maxS, maxP, maxO);
    }

    void testDeletionParent() {
        addTriple(1, 2, 3);
        addTriple(2, 2, 4);
        addTriple(1, 2, 4);
        addTriple(2, 2, 5);
        addTriple(1, 3, 4);

        ASSERT_QUERY(1, 2, 4,
            T(1, 2, 4)
        );
        ASSERT_EQUAL(1, getCountEstimate(1, 2, 4));
        ASSERT_QUERY(1, 2, INVALID_RESOURCE_ID,
            T(1, 2, 3),
            T(1, 2, 4)
        );
        ASSERT_QUERY(1, INVALID_RESOURCE_ID, INVALID_RESOURCE_ID,
            T(1, 2, 3),
            T(1, 2, 4),
            T(1, 3, 4)
        );
        ASSERT_QUERY(INVALID_RESOURCE_ID, INVALID_RESOURCE_ID, INVALID_RESOURCE_ID,
            T(1, 2, 3),
            T(1, 2, 4),
            T(1, 3, 4),
            T(2, 2, 4),
            T(2, 2, 5)
        );

        ASSERT_TRUE(deleteTriple(1, 2, 4));
        ASSERT_QUERY(1, 2, 4);
        ASSERT_EQUAL(0, getCountEstimate(1, 2, 4));
        ASSERT_QUERY(1, 2, INVALID_RESOURCE_ID,
            T(1, 2, 3)
        );
        ASSERT_QUERY(1, INVALID_RESOURCE_ID, INVALID_RESOURCE_ID,
            T(1, 2, 3),
            T(1, 3, 4)
        );
        ASSERT_QUERY(INVALID_RESOURCE_ID, INVALID_RESOURCE_ID, INVALID_RESOURCE_ID,
            T(1, 2, 3),
            T(1, 3, 4),
            T(2, 2, 4),
            T(2, 2, 5)
        );

        ASSERT_FALSE(deleteTriple(1, 2, 4));
        ASSERT_QUERY(1, 2, 4);
        ASSERT_EQUAL(0, getCountEstimate(1, 2, 4));
        ASSERT_QUERY(1, 2, INVALID_RESOURCE_ID,
            T(1, 2, 3)
        );
        ASSERT_QUERY(1, INVALID_RESOURCE_ID, INVALID_RESOURCE_ID,
            T(1, 2, 3),
            T(1, 3, 4)
        );
        ASSERT_QUERY(INVALID_RESOURCE_ID, INVALID_RESOURCE_ID, INVALID_RESOURCE_ID,
            T(1, 2, 3),
            T(1, 3, 4),
            T(2, 2, 4),
            T(2, 2, 5)
        );
    }

    void testVariableBindingParent() {
        addTriple(2, 3, 4);
        addTriple(2, 3, 5);
        addTriple(2, 3, 6);
        addTriple(2, 4, 4);
        addTriple(2, 4, 5);
        addTriple(3, 3, 4);
        addTriple(3, 3, 5);

        m_surelyBoundS = false;
        m_surelyBoundP = false;
        m_surelyBoundO = false;

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

        // Nothing is surely bound, and s = p.

        m_argumentIndexes[1] = 0;

        ASSERT_QUERY(INVALID_RESOURCE_ID, 3, INVALID_RESOURCE_ID,
            T(3, 3, 4),
            T(3, 3, 5)
        );

        // S is surely bound, and s = p.

        m_surelyBoundS = true;

        ASSERT_QUERY(3, 3, INVALID_RESOURCE_ID,
            T(3, 3, 4),
            T(3, 3, 5)
        );

    }

    void testParallelParent() {
        const ResourceID maxS = 99;
        const ResourceID maxP = 39;
        const ResourceID maxO = 400;
        const int numberOfTriples = 1000;
        // Add some data
        generateTestDataMinimalRepetition(m_testData, numberOfTriples, maxS, maxP, maxO);
        unique_ptr_vector<LoaderThread> loaderThreads;
        for (uint32_t threadIndex = 0; threadIndex < NUMBER_OF_THREADS; threadIndex++) {
            loaderThreads.push_back(std::unique_ptr<LoaderThread>(new LoaderThread(*this, threadIndex)));
            loaderThreads.back()->start();
        }
        for (uint32_t threadIndex = 0; threadIndex < NUMBER_OF_THREADS; threadIndex++)
            loaderThreads[threadIndex]->join();
        // Run the queries
        runSymmetricQueries(maxS, maxP, maxO);

        // Now delete half of the data
        std::vector<ResourceTriple> testDataVector;
        m_testData.toVector(testDataVector);
        const size_t firstIndexToDelete = testDataVector.size() / 2;
        unique_ptr_vector<DeleterThread> deleterThreads;
        for (uint32_t threadIndex = 0; threadIndex < NUMBER_OF_THREADS; threadIndex++) {
            deleterThreads.push_back(std::unique_ptr<DeleterThread>(new DeleterThread(*this, testDataVector, firstIndexToDelete + threadIndex)));
            deleterThreads.back()->start();
        }
        for (uint32_t threadIndex = 0; threadIndex < NUMBER_OF_THREADS; threadIndex++)
            deleterThreads[threadIndex]->join();
        m_testData.clear();
        m_testData.add(testDataVector, firstIndexToDelete);
        // Run the queries again
        runSymmetricQueries(maxS, maxP, maxO);
    }

    class LoaderThread : public Thread {

    protected:

        TripleTableTest& m_tripleTableTest;
        const size_t m_start;

    public:

        LoaderThread(TripleTableTest& tripleTableTest, const size_t start) : m_tripleTableTest(tripleTableTest), m_start(start) {
        }

        virtual void run() {
            std::vector<ResourceID> tripleBuffer;
            tripleBuffer.insert(tripleBuffer.begin(), 3, INVALID_RESOURCE_ID);
            for (size_t bucketIndex = m_start; bucketIndex < m_tripleTableTest.m_testData.getNumberOfBuckets(); bucketIndex += NUMBER_OF_THREADS) {
                const uint8_t* bucket = m_tripleTableTest.m_testData.getBucket(bucketIndex);
                if (!m_tripleTableTest.m_testData.isEmpty(bucket)) {
                    const ResourceTriple& resourceTriple = m_tripleTableTest.m_testData.get(bucket);
                    tripleBuffer[0] = resourceTriple.m_s;
                    tripleBuffer[1] = resourceTriple.m_p;
                    tripleBuffer[2] = resourceTriple.m_o;
                    ASSERT_TRUE(m_tripleTableTest.m_tripleTable.addTuple(tripleBuffer, m_tripleTableTest.m_argumentIndexes, 0, TUPLE_STATUS_IDB).first);
                }
            }
        }
    };

    class DeleterThread : public Thread {

    protected:

        TripleTableTest& m_tripleTableTest;
        const std::vector<ResourceTriple>& m_testDataVector;
        const size_t m_firstIndexToDelete;

    public:

        DeleterThread(TripleTableTest& tripleTableTest, const std::vector<ResourceTriple>& testDataVector, const size_t firstIndexToDelete) : m_tripleTableTest(tripleTableTest), m_testDataVector(testDataVector), m_firstIndexToDelete(firstIndexToDelete) {
        }

        virtual void run() {
            std::vector<ResourceID> tripleBuffer;
            tripleBuffer.insert(tripleBuffer.begin(), 3, INVALID_RESOURCE_ID);
            for (size_t index = m_firstIndexToDelete; index < m_testDataVector.size(); index += NUMBER_OF_THREADS) {
                const ResourceTriple& resourceTriple = m_testDataVector[index];
                tripleBuffer[0] = resourceTriple.m_s;
                tripleBuffer[1] = resourceTriple.m_p;
                tripleBuffer[2] = resourceTriple.m_o;
                const TupleIndex tupleIndex = m_tripleTableTest.m_tripleTable.getTupleIndex(tripleBuffer, m_tripleTableTest.m_argumentIndexes);
                ASSERT_TRUE(m_tripleTableTest.m_tripleTable.deleteTupleStatus(tupleIndex, TUPLE_STATUS_IDB));
                ASSERT_FALSE(m_tripleTableTest.m_tripleTable.deleteTupleStatus(tupleIndex, TUPLE_STATUS_IDB));
            }
        }
    };

};

#define SUITE_NAME    SequentialHeadTripleTableTest

#include <CppTest/AutoTest.h>

class SequentialHeadTripleTableTest : public TripleTableTest {

public:

    SequentialHeadTripleTableTest() : TripleTableTest("seq") {
    }

};

TEST3(SUITE_NAME, testBasic, SequentialHeadTripleTableTest) {
    testBasicParent();
}

TEST3(SUITE_NAME, testGetTupleIfExists, SequentialHeadTripleTableTest) {
    testGetTupleIfExistsParent();
}

TEST3(SUITE_NAME, testBulk, SequentialHeadTripleTableTest) {
    testBulkParent();
}

TEST3(SUITE_NAME, testEqualityQueriesSmall, SequentialHeadTripleTableTest) {
    testEqualityQueriesSmallParent();
}

TEST3(SUITE_NAME, testEqualityQueriesLarge, SequentialHeadTripleTableTest) {
    testEqualityQueriesLargeParent();
}

TEST3(SUITE_NAME, testDeletion, SequentialHeadTripleTableTest) {
    testDeletionParent();
}

TEST3(SUITE_NAME, testVariableBinding, SequentialHeadTripleTableTest) {
    testVariableBindingParent();
}

#undef SUITE_NAME
#undef AUTOTEST_H
#undef TEST
#define SUITE_NAME  ParallelComplexNNTripleTableTest

#include <CppTest/AutoTest.h>

class ParallelComplexNNTripleTableTest : public TripleTableTest {

public:

    ParallelComplexNNTripleTableTest() : TripleTableTest("par-complex-nn") {
    }

};

TEST3(SUITE_NAME, testBasic, ParallelComplexNNTripleTableTest) {
    testBasicParent();
}

TEST3(SUITE_NAME, testGetTupleIfExists, ParallelComplexNNTripleTableTest) {
    testGetTupleIfExistsParent();
}

TEST3(SUITE_NAME, testBulk, ParallelComplexNNTripleTableTest) {
    testBulkParent();
}

TEST3(SUITE_NAME, testEqualityQueriesSmall, ParallelComplexNNTripleTableTest) {
    testEqualityQueriesSmallParent();
}

TEST3(SUITE_NAME, testEqualityQueriesLarge, ParallelComplexNNTripleTableTest) {
    testEqualityQueriesLargeParent();
}

TEST3(SUITE_NAME, testDeletion, ParallelComplexNNTripleTableTest) {
    testDeletionParent();
}

TEST3(SUITE_NAME, testVariableBinding, ParallelComplexNNTripleTableTest) {
    testVariableBindingParent();
}

TEST3(SUITE_NAME, testParallel, ParallelComplexNNTripleTableTest) {
    testParallelParent();
}

#undef SUITE_NAME
#undef AUTOTEST_H
#undef TEST
#define SUITE_NAME  ParallelComplexNWTripleTableTest

#include <CppTest/AutoTest.h>

class ParallelComplexNWTripleTableTest : public TripleTableTest {

public:

    ParallelComplexNWTripleTableTest() : TripleTableTest("par-complex-nw") {
    }

};

TEST3(SUITE_NAME, testBasic, ParallelComplexNWTripleTableTest) {
    testBasicParent();
}

TEST3(SUITE_NAME, testGetTupleIfExists, ParallelComplexNWTripleTableTest) {
    testGetTupleIfExistsParent();
}

TEST3(SUITE_NAME, testBulk, ParallelComplexNWTripleTableTest) {
    testBulkParent();
}

TEST3(SUITE_NAME, testEqualityQueriesSmall, ParallelComplexNWTripleTableTest) {
    testEqualityQueriesSmallParent();
}

TEST3(SUITE_NAME, testEqualityQueriesLarge, ParallelComplexNWTripleTableTest) {
    testEqualityQueriesLargeParent();
}

TEST3(SUITE_NAME, testDeletion, ParallelComplexNWTripleTableTest) {
    testDeletionParent();
}

TEST3(SUITE_NAME, testVariableBinding, ParallelComplexNWTripleTableTest) {
    testVariableBindingParent();
}

TEST3(SUITE_NAME, testParallel, ParallelComplexNWTripleTableTest) {
    testParallelParent();
}

#undef SUITE_NAME
#undef AUTOTEST_H
#undef TEST
#define SUITE_NAME  ParallelComplexWWTripleTableTest

#include <CppTest/AutoTest.h>

class ParallelComplexWWTripleTableTest : public TripleTableTest {

public:

    ParallelComplexWWTripleTableTest() : TripleTableTest("par-complex-ww") {
    }

};

TEST3(SUITE_NAME, testBasic, ParallelComplexWWTripleTableTest) {
    testBasicParent();
}

TEST3(SUITE_NAME, testGetTupleIfExists, ParallelComplexWWTripleTableTest) {
    testGetTupleIfExistsParent();
}

TEST3(SUITE_NAME, testBulk, ParallelComplexWWTripleTableTest) {
    testBulkParent();
}

TEST3(SUITE_NAME, testEqualityQueriesSmall, ParallelComplexWWTripleTableTest) {
    testEqualityQueriesSmallParent();
}

TEST3(SUITE_NAME, testEqualityQueriesLarge, ParallelComplexWWTripleTableTest) {
    testEqualityQueriesLargeParent();
}

TEST3(SUITE_NAME, testDeletion, ParallelComplexWWTripleTableTest) {
    testDeletionParent();
}

TEST3(SUITE_NAME, testVariableBinding, ParallelComplexWWTripleTableTest) {
    testVariableBindingParent();
}

TEST3(SUITE_NAME, testParallel, ParallelComplexWWTripleTableTest) {
    testParallelParent();
}

#undef SUITE_NAME
#undef AUTOTEST_H
#undef TEST
#define SUITE_NAME  ParallelSimpleNNTripleTableTest

#include <CppTest/AutoTest.h>

class ParallelSimpleNNTripleTableTest : public TripleTableTest {

public:

    ParallelSimpleNNTripleTableTest() : TripleTableTest("par-simple-nn") {
    }

};

TEST3(SUITE_NAME, testBasic, ParallelSimpleNNTripleTableTest) {
    testBasicParent();
}

TEST3(SUITE_NAME, testGetTupleIfExists, ParallelSimpleNNTripleTableTest) {
    testGetTupleIfExistsParent();
}

TEST3(SUITE_NAME, testBulk, ParallelSimpleNNTripleTableTest) {
    testBulkParent();
}

TEST3(SUITE_NAME, testEqualityQueriesSmall, ParallelSimpleNNTripleTableTest) {
    testEqualityQueriesSmallParent();
}

TEST3(SUITE_NAME, testEqualityQueriesLarge, ParallelSimpleNNTripleTableTest) {
    testEqualityQueriesLargeParent();
}

TEST3(SUITE_NAME, testDeletion, ParallelSimpleNNTripleTableTest) {
    testDeletionParent();
}

TEST3(SUITE_NAME, testVariableBinding, ParallelSimpleNNTripleTableTest) {
    testVariableBindingParent();
}

TEST3(SUITE_NAME, testParallel, ParallelSimpleNNTripleTableTest) {
    testParallelParent();
}

#undef SUITE_NAME
#undef AUTOTEST_H
#undef TEST
#define SUITE_NAME  ParallelSimpleNWTripleTableTest

#include <CppTest/AutoTest.h>

class ParallelSimpleNWTripleTableTest : public TripleTableTest {

public:

    ParallelSimpleNWTripleTableTest() : TripleTableTest("par-simple-nw") {
    }

};

TEST3(SUITE_NAME, testBasic, ParallelSimpleNWTripleTableTest) {
    testBasicParent();
}

TEST3(SUITE_NAME, testGetTupleIfExists, ParallelSimpleNWTripleTableTest) {
    testGetTupleIfExistsParent();
}

TEST3(SUITE_NAME, testBulk, ParallelSimpleNWTripleTableTest) {
    testBulkParent();
}

TEST3(SUITE_NAME, testEqualityQueriesSmall, ParallelSimpleNWTripleTableTest) {
    testEqualityQueriesSmallParent();
}

TEST3(SUITE_NAME, testEqualityQueriesLarge, ParallelSimpleNWTripleTableTest) {
    testEqualityQueriesLargeParent();
}

TEST3(SUITE_NAME, testDeletion, ParallelSimpleNWTripleTableTest) {
    testDeletionParent();
}

TEST3(SUITE_NAME, testVariableBinding, ParallelSimpleNWTripleTableTest) {
    testVariableBindingParent();
}

TEST3(SUITE_NAME, testParallel, ParallelSimpleNWTripleTableTest) {
    testParallelParent();
}

#undef SUITE_NAME
#undef AUTOTEST_H
#undef TEST
#define SUITE_NAME  ParallelSimpleWWTripleTableTest

#include <CppTest/AutoTest.h>

class ParallelSimpleWWTripleTableTest : public TripleTableTest {

public:

    ParallelSimpleWWTripleTableTest() : TripleTableTest("par-simple-ww") {
    }

};

TEST3(SUITE_NAME, testBasic, ParallelSimpleWWTripleTableTest) {
    testBasicParent();
}

TEST3(SUITE_NAME, testGetTupleIfExists, ParallelSimpleWWTripleTableTest) {
    testGetTupleIfExistsParent();
}

TEST3(SUITE_NAME, testBulk, ParallelSimpleWWTripleTableTest) {
    testBulkParent();
}

TEST3(SUITE_NAME, testEqualityQueriesSmall, ParallelSimpleWWTripleTableTest) {
    testEqualityQueriesSmallParent();
}

TEST3(SUITE_NAME, testEqualityQueriesLarge, ParallelSimpleWWTripleTableTest) {
    testEqualityQueriesLargeParent();
}

TEST3(SUITE_NAME, testDeletion, ParallelSimpleWWTripleTableTest) {
    testDeletionParent();
}

TEST3(SUITE_NAME, testVariableBinding, ParallelSimpleWWTripleTableTest) {
    testVariableBindingParent();
}

TEST3(SUITE_NAME, testParallel, ParallelSimpleWWTripleTableTest) {
    testParallelParent();
}

#endif
