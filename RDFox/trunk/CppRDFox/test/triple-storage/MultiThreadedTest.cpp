// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifdef WITH_TEST

#include "TripleData.h"
#include "../../src/Common.h"
#include "../../src/util/MemoryManager.h"
#include "../../src/util/Thread.h"
#include "../../src/util/ThreadContext.h"
#include "../../src/storage/Parameters.h"
#include "../../src/storage/DataStore.h"
#include "../../src/storage/TupleTable.h"
#include "../../src/storage/TupleTableProxy.h"

class MultiThreadedTest {

protected:

    class Reader : public Thread {

    protected:

        static const uint32_t INDEXS = 0;
        static const uint32_t INDEXP = 1;
        static const uint32_t INDEXO = 2;

        MultiThreadedTest& m_multiThreadedTest;
        TupleTable& m_tripleTable;
        ResourceComponent m_component1;
        ResourceComponent m_component2;
        std::vector<ArgumentIndex> m_argumentIndexes;
        std::vector<ResourceID> m_argumentBuffer;
        std::vector<bool> m_seenBuffer;

    public:

        Reader(MultiThreadedTest& multiThreadedTest, TupleTable& tripleTable, ResourceComponent component1, ResourceComponent component2) :
            m_multiThreadedTest(multiThreadedTest), m_tripleTable(tripleTable), m_component1(component1), m_component2(component2), m_argumentIndexes(), m_argumentBuffer(), m_seenBuffer()
        {
            m_argumentIndexes.push_back(0);
            m_argumentIndexes.push_back(1);
            m_argumentIndexes.push_back(2);
            m_argumentBuffer.push_back(INVALID_RESOURCE_ID);
            m_argumentBuffer.push_back(INVALID_RESOURCE_ID);
            m_argumentBuffer.push_back(INVALID_RESOURCE_ID);
        }

        always_inline void setQuery(ResourceID& queryS, ResourceID& queryP, ResourceID& queryO, ResourceComponent component, ResourceID s, ResourceID p, ResourceID o) {
            switch (component) {
            case RC_S:
                queryS = s;
                break;
            case RC_P:
                queryP = p;
                break;
            case RC_O:
                queryO = o;
                break;
            }
        }

        virtual void run() {
            ResourceID lastCheckedSPO = 0;
            ResourceID queryS = INVALID_RESOURCE_ID;
            ResourceID queryP = INVALID_RESOURCE_ID;
            ResourceID queryO = INVALID_RESOURCE_ID;
            while (m_multiThreadedTest.writing()) {
                const ResourceID lastProcessedSPO = m_multiThreadedTest.getLastProcessedSPO();
                if (lastCheckedSPO < lastProcessedSPO) {
                    // Run the query with no bindings
                    assertQuery(INVALID_RESOURCE_ID, INVALID_RESOURCE_ID, INVALID_RESOURCE_ID, lastProcessedSPO);
                    // Check all triples that were added from the last time
                    for (ResourceID spo = lastCheckedSPO + 1; spo <= lastProcessedSPO; ++spo) {
                        ResourceID s;
                        ResourceID p;
                        ResourceID o;
                        decode(spo, s, p, o);
                        assertExists(s, p, o);
                        if ((spo & 63) == 0) {
                            setQuery(queryS, queryP, queryO, m_component1, s, p, o);
                            setQuery(queryS, queryP, queryO, m_component2, s, p, o);
                            assertQuery(queryS, queryP, queryO, lastProcessedSPO);
                        }
                    }
                    lastCheckedSPO = lastProcessedSPO;
                }
            }
        }

        always_inline void assertQuery(const ResourceID queryS, const ResourceID queryP, const ResourceID queryO, const ResourceID lastProcessedSPO) {
            m_seenBuffer.clear();
            if (m_seenBuffer.size() <= lastProcessedSPO)
                m_seenBuffer.insert(m_seenBuffer.end(), lastProcessedSPO - m_seenBuffer.size() + 1, false);
            ArgumentIndexSet allInputArguments;
            allInputArguments.clear();
            if (queryS != INVALID_RESOURCE_ID)
                allInputArguments.add(INDEXS);
            if (queryP != INVALID_RESOURCE_ID)
                allInputArguments.add(INDEXP);
            if (queryO != INVALID_RESOURCE_ID)
                allInputArguments.add(INDEXO);
            std::unique_ptr<TupleIterator> currentIterator = m_tripleTable.createTupleIterator(m_argumentBuffer, m_argumentIndexes, allInputArguments, allInputArguments);
            size_t multiplicity = currentIterator->open();
            while (multiplicity > 0) {
                ResourceID s = m_argumentBuffer[INDEXS];
                ResourceID p = m_argumentBuffer[INDEXP];
                ResourceID o = m_argumentBuffer[INDEXO];
                if ((queryS != INVALID_RESOURCE_ID && queryS != s) || (queryP != INVALID_RESOURCE_ID && queryP != p) || (queryO != INVALID_RESOURCE_ID && queryO != o))
                    FAIL2("Unexpected query result.");
                ResourceID spo = encode(s, p, o);
                if (spo <= lastProcessedSPO)
                    m_seenBuffer[spo] = true;
                multiplicity = currentIterator->advance();
            }
            ResourceID s;
            ResourceID p;
            ResourceID o;
            for (ResourceID spo = 1; spo <= lastProcessedSPO; ++spo) {
                decode(spo, s, p, o);
                if ((queryS == INVALID_RESOURCE_ID || queryS == s) && (queryP == INVALID_RESOURCE_ID || queryP == p) && (queryO == INVALID_RESOURCE_ID || queryO == 0) && !m_seenBuffer[spo])
                    FAIL2("Missing query result.");
            }
        }

        always_inline void assertExists(const ResourceID queryS, const ResourceID queryP, const ResourceID queryO) {
            m_argumentBuffer[INDEXS] = queryS;
            m_argumentBuffer[INDEXP] = queryP;
            m_argumentBuffer[INDEXO] = queryO;
            ASSERT_TRUE(m_tripleTable.containsTuple(m_argumentBuffer, m_argumentIndexes, TUPLE_STATUS_IDB, TUPLE_STATUS_IDB));
        }

    };

    class Writer : public Thread {

    public:

        MultiThreadedTest& m_multiThreadedTest;
        std::unique_ptr<TupleTableProxy> m_tupleTableProxy;
        TupleReceiver* m_tupleReceiver;
        ResourceID m_firstSPO;
        ResourceID m_spoDelta;
        ResourceID m_maxSPO;
        ResourceID m_lastProcessedSPO;

        Writer(MultiThreadedTest& multiThreadedTest, TupleTable& tripleTable, const bool withProxy, ResourceID firstSPO, ResourceID spoDelta, ResourceID maxSPO) :
            m_multiThreadedTest(multiThreadedTest),
            m_tupleTableProxy(),
            m_tupleReceiver(0),
            m_firstSPO(firstSPO),
            m_spoDelta(spoDelta),
            m_maxSPO(maxSPO),
            m_lastProcessedSPO(0)
        {
               if (withProxy) {
                   m_tupleTableProxy = tripleTable.createTupleTableProxy(200 * CACHE_LINE_SIZE);
                   m_tupleTableProxy->initialize();
                   m_tupleReceiver = m_tupleTableProxy.get();
               }
               else
                   m_tupleReceiver = &tripleTable;
        }

        virtual void run() {
            ThreadContext& threadContext = ThreadContext::getCurrentThreadContext();
            std::vector<ArgumentIndex> argumentIndexes;
            argumentIndexes.push_back(0);
            argumentIndexes.push_back(1);
            argumentIndexes.push_back(2);
            std::vector<ResourceID> tripleBuffer;
            tripleBuffer.insert(tripleBuffer.begin(), 3, INVALID_RESOURCE_ID);
            for (ResourceID spo = m_firstSPO; spo < m_maxSPO; spo += m_spoDelta) {
                ResourceID s, p, o;
                decode(spo, s, p, o);
                tripleBuffer[0] = s;
                tripleBuffer[1] = p;
                tripleBuffer[2] = o;
                ASSERT_TRUE(m_tupleReceiver->addTuple(threadContext, tripleBuffer, argumentIndexes, 0, TUPLE_STATUS_IDB).first);
                ::atomicWrite(m_lastProcessedSPO, spo);
            }
            if (m_tupleTableProxy != 0)
                m_tupleTableProxy->invalidateRemainingBuffer(threadContext);
        }

        ResourceID getLastProcessedSPO() {
            return ::atomicRead(m_lastProcessedSPO);
        }

    };

    static const ResourceID NUMBER_OF_TRIPLES = 10000000;
    unique_ptr_vector<Writer> m_writers;
    unique_ptr_vector<Reader> m_readers;
    int m_writing;

    always_inline void addReader(TupleTable& tripleTable, ResourceComponent component1, ResourceComponent component2) {
        std::unique_ptr<Reader> reader(new Reader(*this, tripleTable, component1, component2));
        m_readers.push_back(std::move(reader));
        m_readers.back()->start();
    }

    static always_inline ResourceID encode(const ResourceID first, const ResourceID second) {
        return (((first + second - 2) * (first + second - 1)) >> 1) + first;
    }

    static always_inline ResourceID encode(const ResourceID first, const ResourceID second, const ResourceID third) {
        return encode(encode(first, second), third);
    }

    static always_inline void decode(const ResourceID pair, ResourceID& first, ResourceID& second) {
        ResourceID w = static_cast<ResourceID>(std::floor(std::sqrt(2 * pair) - 0.5));
        first = pair - (w * (w + 1)) / 2;
        second = (w + 2) - first;
    }

    static always_inline void decode(const ResourceID spo, ResourceID& first, ResourceID& second, ResourceID& third) {
        decode(spo, first, third);
        decode(first, first, second);
    }

public:

    MultiThreadedTest() : m_writers(), m_readers(), m_writing(0) {
    }

    always_inline bool writing() {
        return ::atomicRead(m_writing) == 1;
    }

    always_inline ResourceID getLastProcessedSPO() {
        ResourceID minResourceID = m_writers[0]->getLastProcessedSPO();
        for (uint32_t workerIndex = 1; workerIndex < m_writers.size(); workerIndex++) {
            ResourceID lastProcessedSPO = m_writers[workerIndex]->getLastProcessedSPO();
            if (minResourceID > lastProcessedSPO)
                minResourceID = lastProcessedSPO;
        }
        return minResourceID;
    }

    void testParent(const char* const dataStoreTypeName, const ResourceID numberOfWriters, const bool withProxy) {
        Parameters dataStoreParameters;
        dataStoreParameters.setString("equality", "off");
        std::unique_ptr<DataStore> dataStore(::newDataStore(dataStoreTypeName, dataStoreParameters));
        dataStore->initialize();
        dataStore->setNumberOfThreads(numberOfWriters);
        TupleTable& tripleTable = dataStore->getTupleTable("internal$rdf");
        // Add writers
        ::atomicWrite(m_writing, 1);
        for (ResourceID writerIndex = 1; writerIndex <= numberOfWriters; writerIndex++) {
            std::unique_ptr<Writer> writer(new Writer(*this, tripleTable, withProxy, writerIndex, numberOfWriters, NUMBER_OF_TRIPLES));
            m_writers.push_back(std::move(writer));
            m_writers.back()->start();
        }
        // Wait for writers
        for (uint32_t writerIndex = 0; writerIndex < m_writers.size(); writerIndex++)
            m_writers[writerIndex]->join();
        // Tell readers to stop
        ::atomicWrite(m_writing, 0);
        // Wait for readers
        for (uint32_t readerIndex = 0; readerIndex < m_readers.size(); readerIndex++)
            m_readers[readerIndex]->join();
    }

};

// seq tests

#define SUITE_NAME  SequentialHeadMultiThreadedTest

#include <CppTest/AutoTest.h>

TEST3(SUITE_NAME, test1WriterNoProxy, MultiThreadedTest) {
    testParent("seq", 1, false);
}

// par-complex-ww tests

#undef SUITE_NAME
#undef AUTOTEST_H
#undef TEST
#define SUITE_NAME  ParallelHeadMultiThreadedTest

#include <CppTest/AutoTest.h>

TEST3(SUITE_NAME, test1WriterNoProxy, MultiThreadedTest) {
    testParent("par-complex-ww", 1, false);
}

TEST3(SUITE_NAME, test4WritersNoProxy, MultiThreadedTest) {
    testParent("par-complex-ww", 4, false);
}

TEST3(SUITE_NAME, test6WritersNoProxy, MultiThreadedTest) {
    testParent("par-complex-ww", 6, false);
}

TEST3(SUITE_NAME, test7WritersNoProxy, MultiThreadedTest) {
    testParent("par-complex-ww", 7, false);
}

TEST3(SUITE_NAME, test8WritersNoProxy, MultiThreadedTest) {
    testParent("par-complex-ww", 8, false);
}

TEST3(SUITE_NAME, test1WriterWithProxy, MultiThreadedTest) {
    testParent("par-complex-ww", 1, true);
}

TEST3(SUITE_NAME, test4WritersWithProxy, MultiThreadedTest) {
    testParent("par-complex-ww", 4, true);
}

TEST3(SUITE_NAME, test6WritersWithProxy, MultiThreadedTest) {
    testParent("par-complex-ww", 6, true);
}

TEST3(SUITE_NAME, test7WritersWithProxy, MultiThreadedTest) {
    testParent("par-complex-ww", 7, true);
}

TEST3(SUITE_NAME, test8WritersWithProxy, MultiThreadedTest) {
    testParent("par-complex-ww", 8, true);
}

#endif
