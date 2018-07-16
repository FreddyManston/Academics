// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifdef WITH_TEST

#include "../../src/RDFStoreException.h"
#include "../../src/formats/InputOutput.h"
#include "../../src/formats/sources/MemorySource.h"
#include "../../src/formats/turtle/AbstractParserImpl.h"
#include "../../src/formats/turtle/SPARQLParser.h"
#include "../../src/dictionary/Dictionary.h"
#include "../../src/dictionary/ResourceValueCache.h"
#include "../../src/querying/TermArray.h"
#include "../../src/querying/QueryIterator.h"
#include "../../src/equality/EqualityManager.h"
#include "../../src/util/ThreadContext.h"
#include "../../src/util/InputImporter.h"
#include "../../src/util/IteratorPrinting.h"
#include "../../src/util/MaterializationTracer.h"
#include "../../src/util/IncrementalTracer.h"
#include "DataStoreTest.h"

// ResourceIDTupleParser

class ResourceIDTupleParser : public AbstractParser<ResourceIDTupleParser> {

    friend class AbstractParser<ResourceIDTupleParser>;

protected:

    always_inline void doReportError(const size_t line, const size_t column, const char* const errorDescription) {
        std::ostringstream message;
        message << "Error at line " << line << ", column " << column << ": " << errorDescription;
        throw RDF_STORE_EXCEPTION(message.str());
    }

public:

    ResourceIDTupleParser(Prefixes& prefixes) : AbstractParser<ResourceIDTupleParser>(prefixes) {
    }

    void parse(const char* const tuplesText, const size_t tuplesTextLength, Dictionary& dictionary, ResourceIDTupleMultiset& tupleMutliset);

};

void ResourceIDTupleParser::parse(const char* const tuplesText, const size_t tuplesTextLength, Dictionary& dictionary, ResourceIDTupleMultiset& tupleMutliset) {
    ResourceText resourceText;
    ResourceIDTuple currentTuple;
    MemorySource memorySource(tuplesText, tuplesTextLength);
    m_tokenizer.initialize(memorySource);
    m_tokenizer.nextToken();
    while (m_tokenizer.isGood()) {
        currentTuple.clear();
        size_t currentMultiplicity = 1;
        while (m_tokenizer.isGood() && !m_tokenizer.nonSymbolTokenEquals('.') && !m_tokenizer.nonSymbolTokenEquals('*')) {
            if (m_tokenizer.symbolLowerCaseTokenEquals("undef")) {
                m_tokenizer.nextToken();
                currentTuple.push_back(INVALID_RESOURCE_ID);
            }
            else {
                parseResource(resourceText);
                ResourceID resourceID = dictionary.resolveResource(resourceText);
                currentTuple.push_back(resourceID);
            }
        }
        if (!m_tokenizer.isGood())
            reportError("Invalid tuple.");
        else if (m_tokenizer.nonSymbolTokenEquals('*')) {
            m_tokenizer.nextToken();
            if (!m_tokenizer.isNumber())
                reportError("Invalid multiplicity specification.");
            std::istringstream input(m_tokenizer.getToken());
            input >> currentMultiplicity;
            m_tokenizer.nextToken();
        }
        tupleMutliset.add(currentTuple, currentMultiplicity);
        if (!m_tokenizer.nonSymbolTokenEquals('.'))
            reportError("Invalid tuple terminator.");
        m_tokenizer.nextToken();
    }
    if (!m_tokenizer.isEOF())
        reportError("Invalid tuple.");
}

// ResourceIDTupleMultiset

ResourceIDTupleMultiset::ResourceIDTupleMultiset(size_t initialNumberOfBuckets) : MemoryManager(1024 * 1024 * 1024), HashTable<ResourceIDTupleMultiset>(*static_cast<MemoryManager*>(this)) {
    initialize(initialNumberOfBuckets);
}

ResourceIDTupleMultiset::~ResourceIDTupleMultiset() {
    clear();
}

void ResourceIDTupleMultiset::clear() {
    for (uint8_t* currentBucket = m_buckets; currentBucket != m_afterLastBucket; currentBucket += BUCKET_SIZE)
        delete reinterpret_cast<Bucket*>(currentBucket)->m_multituple;
    ::memset(m_buckets, 0, m_numberOfBuckets * BUCKET_SIZE);
    m_numberOfUsedBuckets = 0;
}

void ResourceIDTupleMultiset::add(const ResourceIDTuple& tuple, const size_t multiplicity) {
    size_t valuesHashCode;
    Bucket& bucket = *reinterpret_cast<Bucket*>(getBucketFor(valuesHashCode, tuple));
    if (bucket.m_multituple == nullptr) {
        bucket.m_multituple = new ResourceIDMultituple(tuple);
        ++m_numberOfUsedBuckets;
    }
    bucket.m_multituple->m_multiplicity += multiplicity;
    resizeIfNeeded();
}

bool ResourceIDTupleMultiset::containsAll(const ResourceIDTupleMultiset& containee) const {
    if (getSize() < containee.getSize())
        return false;
    for (const uint8_t* bucket = containee.getFirstBucket(); !containee.isAfterLastBucket(bucket); bucket = containee.getNextBucket(bucket)) {
        if (!containee.isEmpty(bucket)) {
            const ResourceIDMultituple& multituple = containee.get(bucket);
            size_t valuesHashCode;
            Bucket& bucket = *reinterpret_cast<Bucket*>(getBucketFor(valuesHashCode, multituple.m_tuple));
            if (bucket.m_multituple == nullptr || bucket.m_multituple->m_multiplicity < multituple.m_multiplicity)
                return false;
        }
    }
    return true;
}

void ResourceIDTupleMultiset::loadTriples(const char* const tuplesText, Dictionary& dictionary, const Prefixes& prefixes) {
    ResourceIDTupleParser resourceIDTupleParser(const_cast<Prefixes&>(prefixes));
    resourceIDTupleParser.parse(tuplesText, ::strlen(tuplesText), dictionary, *this);
}

void ResourceIDTupleMultiset::toString(std::ostream& output, const ResourceValueCache& resourceValueCache, const Prefixes& prefixes) const {
    std::string xsdString(XSD_NS);
    xsdString.append("string");
    ResourceValue resourceValue;
    output << getSize() << std::endl;
    output << "----------------------------------------------" << std::endl;
    for (const uint8_t* bucket = getFirstBucket(); !isAfterLastBucket(bucket); bucket = getNextBucket(bucket)) {
        if (!isEmpty(bucket)) {
            const ResourceIDMultituple& multituple = get(bucket);
            for (size_t index = 0; index < multituple.m_tuple.size(); ++index) {
                if (index > 0)
                    output << ' ';
                const ResourceID resourceID = multituple.m_tuple[index];
                if (resourceID == INVALID_RESOURCE_ID)
                    output << "UNDEF";
                else {
                    resourceValueCache.getResource(resourceID, resourceValue);
                    output << resourceValue.toString(prefixes);
                }
            }
            if (multituple.m_multiplicity != 1)
                output << " * " << multituple.m_multiplicity;
            output << " ." << std::endl;
        }
    }
    output << "----------------------------------------------" << std::endl;
}

// RDFDeleter

class RDFDeleter : public InputConsumer {

protected:

    std::vector<ResourceID> m_argumentsBuffer;
    std::vector<ArgumentIndex> m_argumentIndexes;
    DataStore& m_dataStore;
    Dictionary& m_dictionary;
    TupleTable& m_tupleTable;
    ThreadContext* m_threadContext;

    void resolve(const size_t line, const size_t column, const ResourceText& resourceText, ResourceID& resourceID, bool& hasError);

public:

    RDFDeleter(DataStore& dataStore);

    virtual void start();

    virtual void reportError(const size_t line, const size_t column, const char* const errorDescription);

    virtual void consumeTriple(const size_t line, const size_t column, const ResourceText& subject, const ResourceText& predicate, const ResourceText& object);

};

void RDFDeleter::resolve(const size_t line, const size_t column, const ResourceText& resourceText, ResourceID& resourceID, bool& hasError) {
    try {
        resourceID = m_dictionary.resolveResource(*m_threadContext, 0, resourceText);
    }
    catch (const RDFStoreException& error) {
        std::ostringstream message;
        message << "Invalid resource " << resourceText.toString(Prefixes::s_defaultPrefixes) << ": " << error;
        reportError(line, column, message.str().c_str());
    }
}

RDFDeleter::RDFDeleter(DataStore& dataStore) :
    m_argumentsBuffer(),
    m_argumentIndexes(),
    m_dataStore(dataStore),
    m_dictionary(m_dataStore.getDictionary()),
    m_tupleTable(m_dataStore.getTupleTable("internal$rdf")),
    m_threadContext(nullptr)
{
    m_argumentsBuffer.push_back(INVALID_RESOURCE_ID);
    m_argumentsBuffer.push_back(INVALID_RESOURCE_ID);
    m_argumentsBuffer.push_back(INVALID_RESOURCE_ID);
    m_argumentIndexes.push_back(0);
    m_argumentIndexes.push_back(1);
    m_argumentIndexes.push_back(2);
}

void RDFDeleter::start() {
    m_threadContext = &ThreadContext::getCurrentThreadContext();
}

void RDFDeleter::reportError(const size_t line, const size_t column, const char* const errorDescription) {
    std::ostringstream message;
    message << "Line = " << line << ", column = " << column << ": " << errorDescription;
    throw RDF_STORE_EXCEPTION(message.str());
}

void RDFDeleter::consumeTriple(const size_t line, const size_t column, const ResourceText& subject, const ResourceText& predicate, const ResourceText& object) {
    bool hasError = false;
    resolve(line, column, subject, m_argumentsBuffer[0], hasError);
    resolve(line, column, predicate, m_argumentsBuffer[1], hasError);
    resolve(line, column, object, m_argumentsBuffer[2], hasError);
    if (!hasError) {
        TupleIndex tupleIndex = m_tupleTable.getTupleIndex(*m_threadContext, m_argumentsBuffer, m_argumentIndexes);
        if (tupleIndex != INVALID_TUPLE_INDEX)
            m_tupleTable.deleteTupleStatus(tupleIndex, TUPLE_STATUS_EDB | TUPLE_STATUS_IDB);
    }
}

// DataStoreTest

void DataStoreTest::initializeQueryParameters() {
}

ResourceID DataStoreTest::resolve(const std::string& iri) const {
    std::string decodedIRI;
    m_prefixes.decodeIRI(iri, decodedIRI);
    return m_dataStore->getDictionary().resolveResource(decodedIRI, D_IRI_REFERENCE);
}

ResourceID DataStoreTest::resolve(const DatatypeID datatypeID, const std::string& lexicalForm) const {
    if (datatypeID == D_IRI_REFERENCE) {
        std::string decodedIRI;
        m_prefixes.decodeIRI(lexicalForm, decodedIRI);
        return m_dataStore->getDictionary().resolveResource(decodedIRI, D_IRI_REFERENCE);
    }
    else
        return m_dataStore->getDictionary().resolveResource(lexicalForm, datatypeID);
}

ResourceID DataStoreTest::resolveToNormal(const std::string& iri) const {
    return m_dataStore->getEqualityManager().normalize(resolve(iri));
}

std::vector<ResourceID> DataStoreTest::equivalents(const std::string& iri) const {
    std::vector<ResourceID> result;
    ResourceID resourceID = resolveToNormal(iri);
    while (resourceID != INVALID_RESOURCE_ID) {
        result.push_back(resourceID);
        resourceID = m_dataStore->getEqualityManager().getNextEqual(resourceID);
    }
    return result;
}

std::string DataStoreTest::lexicalForm(const ResourceID resourceID) const {
    std::string lexicalForm;
    std::string datatypeIRI;
    ResourceType resourceType;
    m_dataStore->getDictionary().getResource(resourceID, resourceType, lexicalForm, datatypeIRI);
    return lexicalForm;
}

void DataStoreTest::checkTupleStatuses() {
    std::vector<ResourceID> argumentsBuffer(3, INVALID_RESOURCE_ID);
    std::vector<ArgumentIndex> argumentIndexes;
    argumentIndexes.push_back(0);
    argumentIndexes.push_back(1);
    argumentIndexes.push_back(2);
    const TupleStatus zeroMask = ~(TUPLE_STATUS_COMPLETE | TUPLE_STATUS_EDB | TUPLE_STATUS_IDB | (m_dataStore->getEqualityAxiomatizationType() != EQUALITY_AXIOMATIZATION_OFF ? TUPLE_STATUS_IDB_MERGED : 0));
    ArgumentIndexSet noBindings;
    std::unique_ptr<TupleIterator> tupleIterator = m_tripleTable.createTupleIterator(argumentsBuffer, argumentIndexes, noBindings, noBindings, TUPLE_STATUS_COMPLETE, TUPLE_STATUS_COMPLETE);
    for (size_t multiplicity = tupleIterator->open(); multiplicity != 0; multiplicity = tupleIterator->advance()) {
        const TupleStatus tupleStatus = m_tripleTable.getTupleStatus(tupleIterator->getCurrentTupleIndex());
        ASSERT_EQUAL(0, tupleStatus & zeroMask);
    }
}

void DataStoreTest::addTriples(const char* const triplesText) {
    InputImporter inputImporter(*m_dataStore, nullptr);
    MemorySource memorySource(triplesText, ::strlen(triplesText));
    std::string formatName;
    ::load(memorySource, m_prefixes, m_factory, inputImporter, formatName);
    m_dataStore->updateStatistics();
}

void DataStoreTest::addTriple(const ResourceID subjectID, const ResourceID predicateID, const ResourceID objectID, const TupleStatus deleteTupleStatus, const TupleStatus addTupleStatus) {
    std::vector<ResourceID> argumentsBuffer;
    argumentsBuffer.push_back(subjectID);
    argumentsBuffer.push_back(predicateID);
    argumentsBuffer.push_back(objectID);
    std::vector<ArgumentIndex> argumentIndexes;
    argumentIndexes.push_back(0);
    argumentIndexes.push_back(1);
    argumentIndexes.push_back(2);
    m_tripleTable.addTuple(argumentsBuffer, argumentIndexes, deleteTupleStatus, addTupleStatus);
}

void DataStoreTest::addTriple(const std::string& subjectIRI, const std::string& predicateIRI, const std::string& objectIRI, const TupleStatus deleteTupleStatus, const TupleStatus addTupleStatus) {
    addTriple(resolve(subjectIRI), resolve(predicateIRI), resolve(objectIRI), deleteTupleStatus, addTupleStatus);
}

void DataStoreTest::removeTriples(const char* const triplesText) {
    RDFDeleter rdfDeleter(*m_dataStore);
    MemorySource memorySource(triplesText, ::strlen(triplesText));
    std::string formatName;
    ::load(memorySource, m_prefixes, m_factory, rdfDeleter, formatName);
}

void DataStoreTest::removeTriple(const ResourceID subjectID, const ResourceID predicateID, const ResourceID objectID) {
    std::vector<ResourceID> argumentsBuffer;
    argumentsBuffer.push_back(subjectID);
    argumentsBuffer.push_back(predicateID);
    argumentsBuffer.push_back(objectID);
    std::vector<ArgumentIndex> argumentIndexes;
    argumentIndexes.push_back(0);
    argumentIndexes.push_back(1);
    argumentIndexes.push_back(2);
    TupleIndex tupleIndex = m_tripleTable.getTupleIndex(argumentsBuffer, argumentIndexes);
    if (tupleIndex != INVALID_TUPLE_INDEX)
        m_tripleTable.deleteTupleStatus(tupleIndex, TUPLE_STATUS_EDB | TUPLE_STATUS_IDB);
}

void DataStoreTest::removeTriple(const std::string& subjectIRI, const std::string& predicateIRI, const std::string& objectIRI) {
    removeTriple(resolve(subjectIRI), resolve(predicateIRI), resolve(objectIRI));
}

void DataStoreTest::forAddition(const char* const triplesText) {
    AdditionInputImporter additionInputImporter(*m_dataStore, nullptr);
    MemorySource memorySource(triplesText, ::strlen(triplesText));
    std::string formatName;
    ::load(memorySource, m_prefixes, m_factory, additionInputImporter, formatName);
}

void DataStoreTest::forAddition(const ResourceID subjectID, const ResourceID predicateID, const ResourceID objectID) {
    std::vector<ResourceID> argumentsBuffer;
    argumentsBuffer.push_back(subjectID);
    argumentsBuffer.push_back(predicateID);
    argumentsBuffer.push_back(objectID);
    std::vector<ArgumentIndex> argumentIndexes;
    argumentIndexes.push_back(0);
    argumentIndexes.push_back(1);
    argumentIndexes.push_back(2);
    m_tripleTable.scheduleForAddition(argumentsBuffer, argumentIndexes);
}

void DataStoreTest::forAddition(const std::string& subjectIRI, const std::string& predicateIRI, const std::string& objectIRI) {
    forAddition(resolve(subjectIRI), resolve(predicateIRI), resolve(objectIRI));
}

void DataStoreTest::forDeletion(const char* const triplesText) {
    DeletionInputImporter deletionInputImporter(*m_dataStore, nullptr);
    MemorySource memorySource(triplesText, ::strlen(triplesText));
    std::string formatName;
    ::load(memorySource, m_prefixes, m_factory, deletionInputImporter, formatName);
}

void DataStoreTest::forDeletion(const ResourceID subjectID, const ResourceID predicateID, const ResourceID objectID) {
    std::vector<ResourceID> argumentsBuffer;
    argumentsBuffer.push_back(subjectID);
    argumentsBuffer.push_back(predicateID);
    argumentsBuffer.push_back(objectID);
    std::vector<ArgumentIndex> argumentIndexes;
    argumentIndexes.push_back(0);
    argumentIndexes.push_back(1);
    argumentIndexes.push_back(2);
    m_tripleTable.scheduleForDeletion(argumentsBuffer, argumentIndexes);
}

void DataStoreTest::forDeletion(const std::string& subjectIRI, const std::string& predicateIRI, const std::string& objectIRI) {
    forDeletion(resolve(subjectIRI), resolve(predicateIRI), resolve(objectIRI));
}

void DataStoreTest::clearRulesAndMakeFactsExplicit() {
    m_dataStore->clearRulesAndMakeFactsExplicit();
}

void DataStoreTest::addRules(const char* const programText) {
    AdditionInputImporter additionInputImporter(*m_dataStore, nullptr);
    MemorySource memorySource(programText, ::strlen(programText));
    std::string formatName;
    ::load(memorySource, m_prefixes, m_factory, additionInputImporter, formatName);
}

void DataStoreTest::removeRules(const char* const programText) {
    DeletionInputImporter deletionInputImporter(*m_dataStore, nullptr);
    MemorySource memorySource(programText, ::strlen(programText));
    std::string formatName;
    ::load(memorySource, m_prefixes, m_factory, deletionInputImporter, formatName);
}

void DataStoreTest::applyRules(const size_t numberOfThreads) {
    m_dataStore->setNumberOfThreads(numberOfThreads);
    if (m_traceReasoning) {
        MaterializationTracer materializationTracer(m_prefixes, m_dataStore->getDictionary(), std::cout);
        m_dataStore->applyRules(m_processComponentsByLevels, &materializationTracer);
    }
    else
        m_dataStore->applyRules(m_processComponentsByLevels, nullptr);
    checkTupleStatuses();
}

void DataStoreTest::assertApplyRules(const char* const fileName, const long lineNumber, const char* const expectedTrace) {
    std::ostringstream output;
    m_dataStore->setNumberOfThreads(1);
    MaterializationTracer materializationTracer(m_prefixes, m_dataStore->getDictionary(), output);
    m_dataStore->applyRules(m_processComponentsByLevels, &materializationTracer);
    CppTest::assertEqual(expectedTrace, output.str(), fileName, lineNumber);
}

void DataStoreTest::applyRulesIncrementally(const size_t numberOfThreads) {
    m_dataStore->setNumberOfThreads(numberOfThreads);
    if (m_traceReasoning) {
        IncrementalTracer incrementalTracer(m_prefixes, m_dataStore->getDictionary(), std::cout);
        m_dataStore->applyRulesIncrementally(m_processComponentsByLevels, m_useDRed, &incrementalTracer);
    }
    else
        m_dataStore->applyRulesIncrementally(m_processComponentsByLevels, m_useDRed, nullptr);
    checkTupleStatuses();
}

void DataStoreTest::assertApplyRulesIncrementally(const char* const fileName, const long lineNumber, const char* const expectedTrace) {
    std::ostringstream output;
    m_dataStore->setNumberOfThreads(1);
    IncrementalTracer incrementalTracer(m_prefixes, m_dataStore->getDictionary(), output);
    m_dataStore->applyRulesIncrementally(m_processComponentsByLevels, m_useDRed, &incrementalTracer);
    CppTest::assertEqual(expectedTrace, output.str(), fileName, lineNumber);
}

Query DataStoreTest::parseQuery(const char* const queryText) {
    SPARQLParser sparqlParser(m_prefixes);
    return sparqlParser.parse(m_factory, queryText, ::strlen(queryText));
}

void DataStoreTest::assertQuery(const char* const fileName, const long lineNumber, const char* const queryText, const char* const expectedTriplesText, const char* const expectedPlanText) {
    ResourceIDTupleMultiset expected;
    expected.loadTriples(expectedTriplesText, m_dataStore->getDictionary(), m_prefixes);
    assertQuery(fileName, lineNumber, queryText, expected, expectedPlanText);
}

void DataStoreTest::assertQuery(const char* const fileName, const long lineNumber, const char* const queryText, const ResourceIDTupleMultiset& expected, const char* const expectedPlanText) {
    Query query = parseQuery(queryText);
    assertQuery(fileName, lineNumber, query, expected, expectedPlanText);
}

void DataStoreTest::assertQuery(const char* const fileName, const long lineNumber, const Query& query, const char* const expectedTriplesText, const char* const expectedPlanText) {
    ResourceIDTupleMultiset expected;
    expected.loadTriples(expectedTriplesText, m_dataStore->getDictionary(), m_prefixes);
    assertQuery(fileName, lineNumber, query, expected, expectedPlanText);
}

void DataStoreTest::assertQuery(const char* const fileName, const long lineNumber, const Query& query, const ResourceIDTupleMultiset& expected, const char* const expectedPlanText) {
    TermArray termArray;
    std::unique_ptr<QueryIterator> queryIterator(m_dataStore->compileQuery(query, termArray, m_queryParameters, nullptr));
    ResourceIDTupleMultiset actual;
    if (expectedPlanText != nullptr) {
        IteratorPrinting iteratorPrinting(m_prefixes, termArray, m_dataStore->getDictionary());
        std::ostringstream output;
        iteratorPrinting.printIteratorSubtree(*queryIterator, output);
        std::string actualPlan = output.str();
        if (actualPlan != expectedPlanText) {
            std::ostringstream message;
            message <<
                "Invalid query plan." << std::endl <<
                "Expected plan:" << std::endl <<
                "----------------------------------------------------" << std::endl <<
                expectedPlanText << std::endl <<
                "----------------------------------------------------" << std::endl <<
                "Actual plan:" << std::endl <<
                "----------------------------------------------------" << std::endl <<
                actualPlan << std::endl;
            throw CppTest::AssertionError(fileName, lineNumber, message.str());
        }
    }
    ResourceIDTuple resourceIDTuple;
    const size_t arity = queryIterator->getArity();
    const std::vector<ResourceID>& argumentsBuffer = queryIterator->getArgumentsBuffer();
    const std::vector<ArgumentIndex>& argumentIndexes = queryIterator->getArgumentIndexes();
    size_t multiplicity = queryIterator->open();
    while (multiplicity != 0) {
        resourceIDTuple.clear();
        for (size_t index = 0; index < arity; ++index)
            resourceIDTuple.push_back(argumentsBuffer[argumentIndexes[index]]);
        actual.add(resourceIDTuple, multiplicity);
        multiplicity = queryIterator->advance();
    }
    bool expectedContainsActual = expected.containsAll(actual);
    bool actualContainsExpected = actual.containsAll(expected);
    if (!expectedContainsActual || !actualContainsExpected) {
        std::ostringstream message;
        message << "Query assertion failure:";
        if (!expectedContainsActual)
            message << " expected not contained in actual";
        if (!expectedContainsActual && !actualContainsExpected)
            message << " and";
        if (!actualContainsExpected)
            message << " actual not contained in expected";
        message << std::endl << "----------------------------------------------" << std::endl;
        message << "Number of expected triples: ";
        expected.toString(message, queryIterator->getResourceValueCache(), m_prefixes);
        message << "Number of actual triples: ";
        actual.toString(message, queryIterator->getResourceValueCache(), m_prefixes);
        throw CppTest::AssertionError(fileName, lineNumber, message.str());
    }
}

DataStoreTest::DataStoreTest(const char* const dataStoreName, const EqualityAxiomatizationType equalityAxiomatizationType) :
    m_factory(newLogicFactory()),
    m_prefixes(),
    m_dataStore(::newDataStore(dataStoreName, getDataStoreParameters(equalityAxiomatizationType))),
    m_tripleTable(m_dataStore->getTupleTable("internal$rdf")),
    m_dictionary(m_dataStore->getDictionary()),
    m_queryParameters(),
    m_traceReasoning(false),
    m_processComponentsByLevels(true),
    m_useDRed(false)
{
    m_prefixes.declareStandardPrefixes();
    m_prefixes.declarePrefix(":", "http://krr.cs.ox.ac.uk/RDF-store/test#");
}

void DataStoreTest::initialize() {
    m_dataStore->initialize();
    initializeQueryParameters();
}

#endif
