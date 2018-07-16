// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifdef WITH_TEST

#define AUTO_TEST   NodeServerTest

#include <CppTest/AutoTest.h>

#include "../../src/dictionary/Dictionary.h"
#include "../../src/dictionary/ResourceValueCache.h"
#include "../../src/storage/Parameters.h"
#include "../../src/storage/DataStore.h"
#include "../../src/storage/TupleTable.h"
#include "../../src/storage/TupleIterator.h"
#include "../../src/formats/InputOutput.h"
#include "../../src/formats/InputConsumer.h"
#include "../../src/formats/sources/MemorySource.h"
#include "../../src/formats/turtle/AbstractParserImpl.h"
#include "../../src/logic/Logic.h"
#include "../../src/node-server/NodeServer.h"
#include "../../src/node-server/NodeServerListener.h"
#include "../../src/node-server/NodeServerMonitor.h"
#include "../../src/node-server/NodeServerQueryListener.h"
#include "../../src/node-server/OccurrenceManagerImpl.h"
#include "../../src/node-server/ResourceIDMapperImpl.h"
#include "../../src/tasks/Tasks.h"
#include "../../src/util/Mutex.h"
#include "../../src/util/Condition.h"
#include "../../src/util/ThreadContext.h"
#include "../../src/util/MemoryManager.h"
#include "../../src/tasks/Tasks.h"

// ResourceTuple

class ResourceTuple : protected std::vector<ResourceValue> {

protected:

    size_t m_hashCode;

    always_inline size_t computeHashCode() const {
        size_t hashCode = 0;
        for (const_iterator iterator = begin(); iterator != end(); ++iterator) {
            hashCode += static_cast<size_t>(iterator->hashCode());
            hashCode += (hashCode << 10);
            hashCode ^= (hashCode >> 6);
        }
        hashCode += (hashCode << 3);
        hashCode ^= (hashCode >> 11);
        hashCode += (hashCode << 15);
        return hashCode;
    }

public:

    always_inline ResourceTuple() : std::vector<ResourceValue>(), m_hashCode(0) {
    }

    always_inline ResourceTuple(const ResourceTuple& other) : std::vector<ResourceValue>(other), m_hashCode(other.m_hashCode) {
    }

    always_inline ResourceTuple(ResourceTuple&& other) : std::vector<ResourceValue>(other), m_hashCode(other.m_hashCode) {
    }

    always_inline ResourceTuple(const std::vector<ResourceValue>& other) : std::vector<ResourceValue>(other), m_hashCode(computeHashCode()) {
    }

    always_inline ResourceTuple(std::vector<ResourceValue>&& other) : std::vector<ResourceValue>(other), m_hashCode(computeHashCode()) {
    }

    always_inline ResourceTuple& operator=(const ResourceTuple& other) {
        std::vector<ResourceValue>::operator=(other);
        m_hashCode = other.m_hashCode;
        return *this;
    }

    always_inline bool operator==(const ResourceTuple& other) const {
        if (m_hashCode != other.m_hashCode || getArity() != other.getArity())
            return false;
        for (const_iterator i1 = begin(), i2 = other.begin(); i1 != end(); ++i1, ++i2)
            if (*i1 != *i2)
                return false;
        return true;
    }

    always_inline bool operator!=(const ResourceTuple& other) const {
        return !(*this == other);
    }

    always_inline size_t getArity() const {
        return size();
    }

    using std::vector<ResourceValue>::const_iterator;

    always_inline const ResourceValue& operator[](const size_t index) const {
        return std::vector<ResourceValue>::operator[](index);
    }

    always_inline const_iterator begin() const {
        return std::vector<ResourceValue>::begin();
    }

    always_inline const_iterator end() const {
        return std::vector<ResourceValue>::end();
    }

    always_inline void print(std::ostream& output, Prefixes& prefixes) const {
        std::string literalText;
        for (auto iterator = begin(); iterator != end(); ++iterator) {
            if (iterator != begin())
                output << ", ";
            Dictionary::toTurtleLiteral(*iterator, prefixes, literalText);
            output << literalText;
        }
    }

    always_inline size_t getHashCode() const {
        return m_hashCode;
    }

};

namespace std {

    template<>
    struct hash<ResourceTuple> {
        always_inline size_t operator()(const ResourceTuple& resourceTuple) const {
            return resourceTuple.getHashCode();
        }
    };

}

// QueryAnswer

class QueryAnswer : public NodeServerQueryListener {

    friend class QueryAnswerParser;

protected:

    enum State { WAITING, COMPLETE, INTERRUPTED };

    Dictionary& m_masterDictionary;
    std::unordered_map<ResourceTuple, size_t> m_expectedAnswers;
    std::unordered_map<ResourceTuple, size_t> m_unexpectedAnswers;
    Mutex m_mutex;
    Condition m_condition;
    State m_state;

public:

    QueryAnswer(Dictionary& masterDictionary) : m_masterDictionary(masterDictionary), m_expectedAnswers(), m_unexpectedAnswers(), m_mutex(), m_condition(), m_state(WAITING) {
    }

    virtual void queryAnswer(const ResourceValueCache& resourceValueCache, const ResourceIDMapper& resourceIDMapper, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const size_t multiplicity) {
        MutexHolder mutexHolder(m_mutex);
        std::vector<ResourceValue> tuple;
        ResourceValue resourceValue;
        for (auto iterator = argumentIndexes.begin(); iterator != argumentIndexes.end(); ++iterator) {
            const ResourceID resourceID = argumentsBuffer[*iterator];
            if (!ResourceValueCache::isPermanentValue(resourceID) || !isGlobalResourceID(resourceID)) {
                if (!resourceValueCache.getResource(resourceID, resourceValue)) {
                    std::ostringstream message;
                    message << "Cannot resolve resource ID '" << resourceID << "' in the ResourceValueCache.";
                    throw RDF_STORE_EXCEPTION(message.str());
                }
            }
            else {
                const ResourceID masterResourceID = unmaskGlobalResourceID(resourceID);
                if (!m_masterDictionary.getResource(masterResourceID, resourceValue)) {
                    std::ostringstream message;
                    message << "Cannot resolve master resource ID '" << masterResourceID << "' in the master dictionary.";
                    throw RDF_STORE_EXCEPTION(message.str());
                }
            }
            tuple.emplace_back(resourceValue);
        }
        ResourceTuple resourceTuple(std::move(tuple));
        auto iterator = m_expectedAnswers.find(resourceTuple);
        size_t rest = 0;
        if (iterator == m_expectedAnswers.end())
            rest = multiplicity;
        else {
            if (multiplicity <= iterator->second)
                iterator->second -= multiplicity;
            else {
                rest = multiplicity - iterator->second;
                iterator->second = 0;
            }
            if (iterator->second == 0)
                m_expectedAnswers.erase(iterator);
        }
        if (rest != 0)
            m_unexpectedAnswers[resourceTuple] += multiplicity;
    }

    virtual void queryAnswersComplete() {
        MutexHolder mutexHolder(m_mutex);
        m_state = COMPLETE;
        m_condition.signalAll();
    }

    virtual void queryInterrupted() {
        MutexHolder mutexHolder(m_mutex);
        m_state = INTERRUPTED;
        m_condition.signalAll();
    }

    void compareAnswers(Prefixes& prefixes) {
        MutexHolder mutexHolder(m_mutex);
        while (m_state == WAITING)
            m_condition.wait(m_mutex);
        if (m_state == INTERRUPTED)
            FAIL2("Query answering was interrupted");
        else {
            if (!m_expectedAnswers.empty()) {
                std::cout
                    << "The following expected answers have not been returned:" << std::endl
                    << "------------------------------------------------------" << std::endl;
                for (auto iterator = m_expectedAnswers.begin(); iterator != m_expectedAnswers.end(); ++iterator) {
                    iterator->first.print(std::cout, prefixes);
                    if (iterator->second != 1)
                        std::cout << " * " << iterator->second;
                    std::cout << std::endl;
                }
                std::cout << "------------------------------------------------------" << std::endl;
            }
            if (!m_unexpectedAnswers.empty()) {
                std::cout
                << "The following returned answers have not been expected:" << std::endl
                << "------------------------------------------------------" << std::endl;
                for (auto iterator = m_unexpectedAnswers.begin(); iterator != m_unexpectedAnswers.end(); ++iterator) {
                    iterator->first.print(std::cout, prefixes);
                    if (iterator->second != 1)
                        std::cout << " * " << iterator->second;
                    std::cout << std::endl;
                }
                std::cout << "------------------------------------------------------" << std::endl;
            }
            if (!m_expectedAnswers.empty() || !m_unexpectedAnswers.empty())
                FAIL2("The actual answers differ from the expected ones.");
        }
    }

};

// QueryAnswerParser

class QueryAnswerParser : protected AbstractParser<QueryAnswerParser> {

    friend class AbstractParser<QueryAnswerParser>;

protected:

    void doReportError(const size_t line, const size_t column, const char* const errorDescription);

    void prefixMappingParsed(const std::string& prefixName, const std::string& prefixIRI);

public:

    QueryAnswerParser(Prefixes& prefixes);

    void parse(InputSource& inputSource, QueryAnswer& queryAnswer);

};

always_inline void QueryAnswerParser::doReportError(const size_t line, const size_t column, const char* const errorDescription) {
    std::ostringstream message;
    message << "Error  at line = " << line << ", column = " << column << ": " << errorDescription << std::endl;
    throw RDF_STORE_EXCEPTION(message.str());
}

always_inline void QueryAnswerParser::prefixMappingParsed(const std::string& prefixName, const std::string& prefixIRI) {
}

QueryAnswerParser::QueryAnswerParser(Prefixes& prefixes) : AbstractParser<QueryAnswerParser>(prefixes) {
}

void QueryAnswerParser::parse(InputSource& inputSource, QueryAnswer& queryAnswer) {
    m_tokenizer.initialize(inputSource);
    nextToken();
    ResourceText resourceText;
    std::vector<ResourceValue> tuple;
    while (m_tokenizer.isGood()) {
        if (m_tokenizer.tokenEquals(TurtleTokenizer::LANGUAGE_TAG, "@prefix")) {
            nextToken();
            parsePrefixMapping();
            if (!m_tokenizer.nonSymbolTokenEquals('.'))
                reportError("Punctuation character '.' expected after '@prefix'.");
            nextToken();
        }
        else if (m_tokenizer.symbolLowerCaseTokenEquals("prefix"))
            parsePrefixMapping();
        else {
            tuple.clear();
            while (m_tokenizer.isGood() && !m_tokenizer.nonSymbolTokenEquals('.') && !m_tokenizer.nonSymbolTokenEquals('*')) {
                parseResource(resourceText);
                tuple.emplace_back();
                Dictionary::parseResourceValue(tuple.back(), resourceText);
            }
            size_t multiplicity = 1;
            if (m_tokenizer.nonSymbolTokenEquals('*')) {
                m_tokenizer.nextToken();
                if (!m_tokenizer.isNumber())
                    reportError("The multiplicity is missing.");
                multiplicity = atoi(m_tokenizer.getToken().c_str());
                m_tokenizer.nextToken();
            }
            if (!m_tokenizer.nonSymbolTokenEquals('.'))
                reportError("Missing '.' at the end of the answer.");
            m_tokenizer.nextToken();
            queryAnswer.m_expectedAnswers[std::move(tuple)] += multiplicity;
        }
    }
    m_tokenizer.deinitialize();
}

// NodeServerTestListener

class NodeServerTestListener : public NodeServerListener {

public:

    virtual void nodeServerStarting() {
    }

    virtual void nodeServerReady() {
    }

    virtual void nodeServerFinishing() {
    }

    virtual void nodeServerError(const std::exception& error) {
        throw;
    }

};

// NodeServerImporter

class NodeServerImporter : public InputConsumer {

protected:

    ThreadContext* m_threadContext;
    Dictionary& m_masterDictionary;
    Prefixes& m_prefixes;
    ResourceIDMapper& m_targetResourceIDMapper;
    Dictionary& m_targetDictionary;
    TupleTable& m_targetTupleTable;
    const EqualityManager& m_targetEqualityMangager;
    std::vector<ResourceID> m_argumentsBuffer;
    std::vector<ArgumentIndex> m_argumentIndexes;

    ResourceID resolve(const ResourceText& resourceText) {
        const ResourceID maxResourceIDBefore = m_targetDictionary.getMaxResourceID();
        const ResourceID resourceID = m_targetDictionary.resolveResource(*m_threadContext, nullptr, resourceText);
        if (m_targetDictionary.getMaxResourceID() != maxResourceIDBefore) {
            const ResourceID masterResourceID = m_masterDictionary.resolveResource(*m_threadContext, nullptr, resourceText);
            m_targetResourceIDMapper.addMapping(*m_threadContext, resourceID, masterResourceID);
        }
        return resourceID;
    }

public:

    NodeServerImporter(Dictionary& masterDicitonary, Prefixes& prefixes, NodeServer& nodeServer) :
        m_threadContext(nullptr),
        m_masterDictionary(masterDicitonary),
        m_prefixes(prefixes),
        m_targetResourceIDMapper(nodeServer.getResourceIDMapper()),
        m_targetDictionary(nodeServer.getDataStore().getDictionary()),
        m_targetTupleTable(nodeServer.getDataStore().getTupleTable("internal$rdf")),
        m_targetEqualityMangager(nodeServer.getDataStore().getEqualityManager()),
        m_argumentsBuffer(3, INVALID_RESOURCE_ID),
        m_argumentIndexes()
    {
        m_argumentIndexes.push_back(0);
        m_argumentIndexes.push_back(1);
        m_argumentIndexes.push_back(2);
    }

    virtual void start() {
        m_threadContext = &ThreadContext::getCurrentThreadContext();
    }

    virtual void reportError(const size_t line, const size_t column, const char* const errorDescription) {
        std::ostringstream message;
        message << "Error at line = " << line << ", column = " << column << ": " << errorDescription;
        throw RDF_STORE_EXCEPTION(message.str());
    }

    virtual void consumeTriple(const size_t line, const size_t column, const ResourceText& subject, const ResourceText& predicate, const ResourceText& object) {
        m_argumentsBuffer[0] = resolve(subject);
        m_argumentsBuffer[1] = resolve(predicate);
        m_argumentsBuffer[2] = resolve(object);
        ::addTuple(*m_threadContext, m_targetTupleTable, m_targetEqualityMangager, m_argumentsBuffer, m_argumentIndexes);
    }

};

// NodeServerTest

class NodeServerTest {

protected:

    MemoryManager m_memoryManager;
    Dictionary m_masterDictionary;
    unique_ptr_vector<DataStore> m_dataStores;
    NodeServerTestListener m_nodeServerTestListener;
    unique_ptr_vector<NodeServer> m_nodeServers;

    void assertQueryAnswers(const NodeID nodeID, const char* const queryText, const char* const expectedQueryAnswersText) {
        Prefixes prefixes;
        prefixes.declareStandardPrefixes();
        prefixes.declarePrefix(":", "http://test.com/onto#");
        QueryAnswer queryAnswer(m_masterDictionary);
        MemorySource memorySource(expectedQueryAnswersText, ::strlen(expectedQueryAnswersText));
        QueryAnswerParser queryAnswerParser(prefixes);
        queryAnswerParser.parse(memorySource, queryAnswer);
        assertQueryAnswers(nodeID, queryText, queryAnswer);
    }

    void assertQueryAnswers(const NodeID nodeID, const char* const queryText, QueryAnswer& expectedQueryAnswer) {
        Prefixes prefixes;
        prefixes.declareStandardPrefixes();
        prefixes.declarePrefix(":", "http://test.com/onto#");
        const std::map<std::string, std::string>& byNames = prefixes.getPrefixIRIsByPrefixNames();
        std::ostringstream queryWithPrefixes;
        for (auto iterator = byNames.begin(); iterator != byNames.end(); ++iterator)
            queryWithPrefixes << "PREFIX " << iterator->first << " <" << iterator->second << '>' << std::endl;
        queryWithPrefixes << queryText;
        std::string fullQueryText = queryWithPrefixes.str();
        m_nodeServers[nodeID]->answerQuery(fullQueryText.c_str(), fullQueryText.length(), expectedQueryAnswer);
        expectedQueryAnswer.compareAnswers(prefixes);
    }

    void load(const NodeID nodeID, const char* const dataText) {
        Prefixes prefixes;
        prefixes.declareStandardPrefixes();
        prefixes.declarePrefix(":", "http://test.com/onto#");
        MemorySource memorySource(dataText, ::strlen(dataText));
        LogicFactory logicFactory;
        std::string formatName;
        NodeServerImporter nodeServerImporter(m_masterDictionary, prefixes, *m_nodeServers[nodeID]);
        ::load(memorySource, prefixes, logicFactory, nodeServerImporter, formatName);
    }

    void doneLoading() {
        ThreadContext& threadContext = ThreadContext::getCurrentThreadContext();
        const size_t numberOfNodeServers = m_nodeServers.size();
        for (NodeID nodeID = 0; nodeID < numberOfNodeServers; ++nodeID) {
            OccurrenceManager& occurrenceManager = m_nodeServers[nodeID]->getOccurrenceManager();
            Dictionary& dictionary = m_dataStores[nodeID]->getDictionary();
            const ResourceID maxResourceID = dictionary.getMaxResourceID();
            for (ResourceID resourceID = 0; resourceID <= maxResourceID; ++resourceID)
                occurrenceManager.clearAll(occurrenceManager.getOccurrenceSetSafe(resourceID, 0));
        }
        ArgumentIndexSet noBindings;
        std::vector<ResourceID> argumentsBuffer(3, INVALID_RESOURCE_ID);
        std::vector<ArgumentIndex> argumentIndexes{ 0, 1, 2 };
        for (NodeID outerNodeID = 0; outerNodeID < numberOfNodeServers; ++outerNodeID) {
            DataStore& dataStore = *m_dataStores[outerNodeID];
            ResourceIDMapper& resourceIDMapper = m_nodeServers[outerNodeID]->getResourceIDMapper();
            std::unique_ptr<TupleIterator> tupleIterator = dataStore.getTupleTable("internal$rdf").createTupleIterator(argumentsBuffer, argumentIndexes, noBindings, noBindings);
            size_t multiplicity = tupleIterator->open();
            while (multiplicity != 0) {
                for (uint32_t position = 0; position < argumentIndexes.size(); ++position) {
                    const ResourceID globalResourceID = resourceIDMapper.getGlobalResourceID(argumentsBuffer[argumentIndexes[position]]);
                    for (NodeID innerNodeID = 0; innerNodeID < numberOfNodeServers; ++innerNodeID) {
                        const ResourceID localResourceID = m_nodeServers[innerNodeID]->getResourceIDMapper().getLocalResourceID(threadContext, globalResourceID);
                        if (localResourceID != INVALID_RESOURCE_ID) {
                            OccurrenceManager& innerOccurrenceManager = m_nodeServers[innerNodeID]->getOccurrenceManager();
                            innerOccurrenceManager.add(innerOccurrenceManager.getOccurrenceSetSafe(localResourceID, position), outerNodeID);
                        }
                    }
                }
                multiplicity = tupleIterator->advance();
            }
        }
    }

public:

    static const NodeID NUMBER_OF_SERVERS = 3;
    static const size_t NUMBER_OF_THREADS_PER_SERVER = 2;

    NodeServerTest() : m_memoryManager(1024 * 1024 * 1024), m_masterDictionary(m_memoryManager, true) {
    }

    virtual ~NodeServerTest() {
    }

    void initialize() {
        m_masterDictionary.initialize();
        m_dataStores.clear();
        m_nodeServers.clear();
        for (NodeID myNodeID = 0; myNodeID < NUMBER_OF_SERVERS; ++myNodeID) {
            Parameters dataStoreParameters;
            dataStoreParameters.setString("equality", "off");
            m_dataStores.push_back(::newDataStore("par-complex-nn", dataStoreParameters));
            m_dataStores.back()->initialize();
            m_nodeServers.push_back(std::unique_ptr<NodeServer>(new NodeServer(NUMBER_OF_THREADS_PER_SERVER, NUMBER_OF_SERVERS, myNodeID, 1024, 10 * 1024, *m_dataStores.back(), &m_nodeServerTestListener, getNodeServerMonitor())));
        }
    }

    void start() {
        for (NodeID myNodeID = 0; myNodeID < m_nodeServers.size(); ++myNodeID)
            m_nodeServers[myNodeID]->startServer(20000);
        for (NodeID myNodeID = 0; myNodeID < m_nodeServers.size(); ++myNodeID)
            ASSERT_TRUE(m_nodeServers[myNodeID]->waitForReady());
    }

    void stop() {
        for (NodeID myNodeID = 0; myNodeID < m_nodeServers.size(); ++myNodeID) {
            m_nodeServers[myNodeID]->stopServer();
            ASSERT_EQUAL(NodeServer::NOT_RUNNING, m_nodeServers[myNodeID]->getState());
        }
    }

    virtual NodeServerMonitor* getNodeServerMonitor() {
        return nullptr;
    }

};

// NodeServerTestCountPartialAnswers

class NodeServerTestCountPartialAnswers : public NodeServerTest, public NodeServerMonitor {

protected:

    Mutex m_mutex;

public:

    aligned_size_t m_numberOfAnswersReceived;
    std::vector<size_t> m_numberOfPartialAnswersPerConjunct;

    NodeServerTestCountPartialAnswers() : m_mutex(), m_numberOfAnswersReceived(0), m_numberOfPartialAnswersPerConjunct() {
    }

    virtual NodeServerMonitor* getNodeServerMonitor() {
        return this;
    }

    virtual void queryProcessingStarted(const QueryID queryID, const char* const queryText, const size_t queryTextLength) {
    }

    virtual void queryAtomMatched(const QueryID queryID, const size_t threadIndex) {
    }

    virtual void queryAnswerProduced(const QueryID queryID, const size_t threadIndex, const ResourceValueCache& resourceValueCache, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& answerArgumentIndexes, const size_t multiplicity) {
        ::atomicIncrement(m_numberOfAnswersReceived);
    }

    virtual void messageProcessingStarted(const QueryID queryID, const size_t threadIndex, const MessageType messageType, const uint8_t* const message, const size_t messageSize) {
    }

    virtual void messageProcessingFinished(const QueryID queryID, const size_t threadIndex) {
    }

    virtual void messageSent(const QueryID queryID, const size_t threadIndex, const NodeID destinationNodeID, const MessageType messageType, const uint8_t* const message, const size_t messageSize) {
        if (PARTIAL_BINDINGS_0_MESSAGE <= messageType && messageType <= PARTIAL_BINDINGS_LAST_MESSAGE) {
            const uint8_t conjunctIndex = static_cast<uint8_t>(messageType - PARTIAL_BINDINGS_0_MESSAGE);
            MutexHolder mutexHolder(m_mutex);
            while (m_numberOfPartialAnswersPerConjunct.size() <= conjunctIndex)
                m_numberOfPartialAnswersPerConjunct.push_back(0);
            ++m_numberOfPartialAnswersPerConjunct[conjunctIndex];
        }
    }

    virtual void queryProcessingFinished(const QueryID queryID, const size_t threadIndex) {
    }

    virtual void queryProcessingAborted(const QueryID queryID) {
    }

};

TEST(testStartStop) {
    start();
    ::sleepMS(1000);
    stop();
}

TEST(testMoreBasic) {
    load(0,
        ":a1 :R :b1 . "
    );
    load(1,
        ":b1 :R :c1 . "
    );
    load(2,
        ""
    );
    doneLoading();

    start();

    assertQueryAnswers(1,
        "SELECT ?X ?Y ?Z WHERE { ?X :R ?Y . ?Y :R ?Z  }",
        ":a1 :b1 :c1 . "
    );
}

TEST(testBasic) {
    load(0,
        ":a1 :R :b1 . "
        ":a2 :R :b2 . "
        ":u1 :S :v1 . "
        ":u2 :S :v2 . "
    );
    load(1,
        ":b1 :R :c1 . "
        ":b2 :R :c2 . "
        ":v1 :S :w1 . "
        ":v2 :S :w2 . "
    );
    load(2,
        ":c1 :R :d1 . "
        ":c2 :R :d2 . "
        ":w1 :S :z1 . "
        ":w2 :S :z2 . "
    );
    doneLoading();

    start();

    assertQueryAnswers(0,
        "SELECT ?X ?Y WHERE { ?X :R ?Y }",
        ":a1 :b1 . "
        ":a2 :b2 . "
        ":b1 :c1 . "
        ":b2 :c2 . "
        ":c1 :d1 . "
        ":c2 :d2 . "
    );

    assertQueryAnswers(1,
        "SELECT ?X ?Y ?Z WHERE { ?X :R ?Y . ?Y :R ?Z  }",
        ":a1 :b1 :c1 . "
        ":a2 :b2 :c2 . "
        ":b1 :c1 :d1 . "
        ":b2 :c2 :d2 . "
    );

    assertQueryAnswers(2,
        "SELECT ?X ?Y ?Z ?W WHERE { ?X :R ?Y . ?Y :R ?Z . ?Z :R ?W }",
        ":a1 :b1 :c1 :d1 . "
        ":a2 :b2 :c2 :d2 . "
    );

    stop();
}

TEST(testJumps) {
    load(0,
        ":i1  :R :i2  . "
        ":i2  :S :i31 . "
        ":i31 :T :i41 . "
        ":i2  :U :i51 . "
        ":i51 :V :i61 . "
    );
    load(1,
        ":i2  :S :i32 . "
        ":i52 :V :i62 . "
    );
    load(2,
        ":i32 :T :i42 . "
        ":i2  :U :i52 . "
    );
    doneLoading();

    start();

    assertQueryAnswers(0,
        "SELECT ?X1 ?X2 ?X3 ?X4 ?X5 ?X6 WHERE { ?X1 :R ?X2 . ?X2 :S ?X3 . ?X3 :T ?X4 . ?X2 :U ?X5 . ?X5 :V ?X6 }",
        ":i1 :i2 :i31 :i41 :i51 :i61 . "
        ":i1 :i2 :i31 :i41 :i52 :i62 . "
        ":i1 :i2 :i32 :i42 :i51 :i61 . "
        ":i1 :i2 :i32 :i42 :i52 :i62 . "
    );

    assertQueryAnswers(2,
        "SELECT ?X1 ?X2 ?X3 ?X4 ?X5 ?X6 WHERE { ?X1 :R ?X2 . ?X2 :S ?X3 . ?X3 :T ?X4 . ?X2 :U ?X5 . ?X5 :V ?X6 }",
        ":i1 :i2 :i31 :i41 :i51 :i61 . "
        ":i1 :i2 :i31 :i41 :i52 :i62 . "
        ":i1 :i2 :i32 :i42 :i51 :i61 . "
        ":i1 :i2 :i32 :i42 :i52 :i62 . "
    );

    assertQueryAnswers(0,
        "SELECT ?X1 ?X5 ?X6 WHERE { ?X1 :R ?X2 . ?X2 :S ?X3 . ?X3 :T ?X4 . ?X2 :U ?X5 . ?X5 :V ?X6 }",
        ":i1 :i51 :i61 * 2 . "
        ":i1 :i52 :i62 * 2 . "
    );

    // The algorithm should still work even if the query is not term-connected.
    assertQueryAnswers(2,
        "SELECT ?X1 ?X2 ?X3 ?X4 ?X5 ?X6 WHERE { ?X1 :R ?X2 . ?X5 :V ?X6 . ?X3 :T ?X4 . ?X2 :U ?X5 . ?X2 :S ?X3 }",
        ":i1 :i2 :i31 :i41 :i51 :i61 . "
        ":i1 :i2 :i31 :i41 :i52 :i62 . "
        ":i1 :i2 :i32 :i42 :i51 :i61 . "
        ":i1 :i2 :i32 :i42 :i52 :i62 . "
    );

    assertQueryAnswers(1,
        "SELECT ?X1 ?X2 ?X3 ?X4 WHERE { ?X1 :S ?X2 . ?X3 :V ?X4 }",
        ":i2 :i31 :i51 :i61 . "
        ":i2 :i31 :i52 :i62 . "
        ":i2 :i32 :i51 :i61 . "
        ":i2 :i32 :i52 :i62 . "
    );

    stop();
}

TEST(testTriangleCycle) {
    load(0,
        ":i1 :R :i2 . "
    );
    load(1,
        ":i2 :R :i3 . "
    );
    load(2,
        ":i3 :R :i1 . "
    );
    doneLoading();

    start();

    assertQueryAnswers(0,
        "SELECT ?X1 ?X2 ?X3 WHERE { ?X1 :R ?X2 . ?X2 :R ?X3 . ?X3 :R ?X1 }",
        ":i1 :i2 :i3 . "
        ":i2 :i3 :i1 . "
        ":i3 :i1 :i2 . "
    );

    stop();
}

TEST(testFilter) {
    load(0,
        ":a1 :R :b1 . "
        ":a2 :R :b2 . "

        ":b3 :S -10 . "
        ":b3 :S 7 . "
    );
    load(1,
        ":b1 :S 1 . "
        ":b1 :S 2 . "
        ":b1 :S 8 . "
        ":b1 :S 9 . "

        ":a3 :R :b3 . "
    );
    load(2,
        ":b2 :S 0 . "
        ":b2 :S 5 . "
        ":b2 :S 10 . "
    );
    doneLoading();

    start();

    assertQueryAnswers(0,
        "SELECT ?X1 ?X2 ?X3 WHERE { ?X1 :R ?X2 . ?X2 :S ?X3 . FILTER(?X3 >= 5) }",
        ":a1 :b1 8 . "
        ":a1 :b1 9 . "
        ":a2 :b2 5 . "
        ":a2 :b2 10 . "
        ":a3 :b3 7 . "
    );
    
    stop();
}

TEST(testBind) {
    load(0,
        ":a1 :R 1 . "
        ":b1 :S 101 . "

        ":a2 :R 2 . "

        ":a3 :R 3 . "

        ":a4 :R 4 . "
    );
    load(1,
        ":b2 :S 102 . "
    );
    load(2,
        ":b3 :S 103 . "
    );
    doneLoading();

    start();

    assertQueryAnswers(0,
        "SELECT ?X1 ?X2 ?X3 ?X4 WHERE { ?X1 :R ?X2 . BIND(?X2 + 100 AS ?X3) . ?X4 :S ?X3 }",
        ":a1 1 101 :b1 . "
        ":a2 2 102 :b2 . "
        ":a3 3 103 :b3 . "
    );
    
    stop();
}

TEST(testBindFirst) {
    load(0,
        ":a1 :R 1 . "
        ":a2 :R 2 . "
    );
    load(1,
        ":a3 :R 1 . "
        ":a4 :R 4 . "
    );
    load(2,
        ":a5 :R 1 . "
        ":a6 :R 6 . "
    );
    doneLoading();

    start();

    assertQueryAnswers(0,
        "SELECT ?X1 WHERE { BIND(1 AS ?X1) }",
        "1 . "
    );
    assertQueryAnswers(0,
        "SELECT ?X1 ?X2 WHERE { BIND(1 AS ?X1) . ?X2 :R ?X1 }",
        "1 :a1 . "
        "1 :a3 . "
        "1 :a5 . "
    );
    
    stop();
}

TEST3(SUITE_NAME, testNoCommunicationOnSubjectJoins, NodeServerTestCountPartialAnswers) {
    load(0,
        ":a :R :b . "
        ":a :S :c . "
        ":a :S :d . "
    );
    load(1,
        ":b :R :a . "
        ":b :R :b . "
        ":b :S :c . "
    );
    load(2,
        ":e :R :a . "
        ":e :S :g . "
    );
    doneLoading();

    start();

    assertQueryAnswers(0,
        "SELECT ?X ?Y1 ?Y2 WHERE { ?X :R ?Y1 . ?X :S ?Y2 }",
        ":a :b :c . "
        ":a :b :d . "
        ":b :a :c . "
        ":b :b :c . "
        ":e :a :g . "
    );

    ASSERT_EQUAL(5, m_numberOfAnswersReceived);
    ASSERT_EQUAL(0, m_numberOfPartialAnswersPerConjunct[1]);
    ASSERT_EQUAL(3, m_numberOfPartialAnswersPerConjunct[2]);

    stop();
}

TEST3(SUITE_NAME, testOptimizedProjection, NodeServerTestCountPartialAnswers) {
    load(0,
        ":a :R :b . "
        ":a :S :c . "
        ":a :S :d . "

        ":b :R :b . "
        ":b :S :c . "
        ":b :S :d . "
    );
    load(1,
        ":c :R :a . "
        ":c :R :b . "
        ":c :S :c . "
        ":c :S :d . "
    );
    load(2,
        ":e :R :a . "
        ":e :S :g . "

        ":f :R :a . "
        ":f :S :g . "
    );
    doneLoading();

    start();

    assertQueryAnswers(0,
        "SELECT ?X WHERE { ?X :R ?Y1 . ?X :S ?Y2 }",
        ":a * 2 . "
        ":b * 2 . "
        ":c * 4 . "
        ":e * 1 . "
        ":f * 1 . "
    );

    ASSERT_EQUAL(5, m_numberOfAnswersReceived);
    ASSERT_EQUAL(0, m_numberOfPartialAnswersPerConjunct[1]);
    ASSERT_EQUAL(3, m_numberOfPartialAnswersPerConjunct[2]);

    stop();
}

TEST(testPausing) {
    std::ostringstream triples1Buffer;
    std::ostringstream triples2Buffer;
    std::ostringstream answerBuffer;
    for (uint32_t tripleIndex = 0; tripleIndex < 10 * 1024; ++tripleIndex) {
        triples1Buffer << ":a" << tripleIndex << " :R :b" << tripleIndex << " . ";
        triples2Buffer << ":b" << tripleIndex << " :S :c" << tripleIndex << " . ";
        answerBuffer << ":a" << tripleIndex << " :b" << tripleIndex << " :c" << tripleIndex << " . ";
    }

    load(1, triples1Buffer.str().c_str());
    load(2, triples2Buffer.str().c_str());
    doneLoading();

    start();

    assertQueryAnswers(0,
        "SELECT ?X ?Y ?Z WHERE { ?X :R ?Y . ?Y :S ?Z }",
        answerBuffer.str().c_str()
    );

    stop();
}

#endif
