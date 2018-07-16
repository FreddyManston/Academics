// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef RDFSTOREIMPL_H_
#define RDFSTOREIMPL_H_

#include "../RDFStoreException.h"
#include "../equality/EqualityManager.h"
#include "../querying/TermArray.h"
#include "../querying/QueryCompiler.h"
#include "../util/ComponentStatistics.h"
#include "../util/InputStream.h"
#include "../util/OutputStream.h"
#include "../util/ThreadContext.h"
#include "../reasoning/RuleIndex.h"
#include "../reasoning/DatalogExplanation.h"
#include "../storage/Parameters.h"
#include "../storage/RuleIterator.h"
#include "../formats/turtle/SPARQLParser.h"
#include "RDFStore.h"
#include "RDFStoreRulePlanIterator.h"

// RDFStoreRuleIterator

template<class TT>
class RDFStoreRuleIterator : public RuleIterator {

protected:

    const RDFStore<TT>& m_rdfStore;
    typename RDFStore<TT>::MutexHolderType m_mutexHolder;
    const RuleInfo* m_currentRuleInfo;

public:

    RDFStoreRuleIterator(const RDFStore<TT>& rdfStore) :
        m_rdfStore(rdfStore),
        m_mutexHolder(m_rdfStore.m_mutex),
        m_currentRuleInfo(nullptr)
    {
    }

    virtual const RuleInfo& getRuleInfo() const {
        return *m_currentRuleInfo;
    }

    virtual const Rule& getRule() const {
        return m_currentRuleInfo->getRule();
    }

    virtual bool isInternalRule() const {
        return m_currentRuleInfo->isInternalRule();
    }

    virtual bool hasNegation() const {
        return m_currentRuleInfo->hasNegation();
    }

    virtual bool hasAggregation() const {
        return m_currentRuleInfo->hasAggregation();
    }

    virtual bool isActive() const {
        return m_currentRuleInfo->isActive();
    }

    virtual bool isJustAdded() const {
        return m_currentRuleInfo->isJustAdded();
    }

    virtual bool isJustDeleted() const {
        return m_currentRuleInfo->isJustDeleted();
    }

    virtual bool isRecursive(const size_t headAtomIndex) const {
        return m_currentRuleInfo->getHeadAtomInfo(headAtomIndex).isRecursive<true>();
    }

    virtual size_t getHeadAtomComponentLevel(const size_t headAtomIndex) const {
        return m_currentRuleInfo->getHeadAtomInfo(headAtomIndex).getComponentLevel();
    }

    virtual size_t getBodyLiteralComponentLevel(const size_t bodyLiteralIndex) const {
        return m_currentRuleInfo->getBodyLiteralComponentIndex(bodyLiteralIndex);
    }

    virtual std::unique_ptr<RulePlanIterator> createRulePlanIterator() const {
        return std::unique_ptr<RulePlanIterator>(new RDFStoreRulePlanIterator(*m_currentRuleInfo));
    }

    virtual bool open() {
        m_currentRuleInfo = m_rdfStore.m_datalogEngine.getRuleIndex().getFirstRuleInfo();
        return m_currentRuleInfo != nullptr;
    }

    virtual bool advance() {
        m_currentRuleInfo = RuleIndex::getNextRuleInfo(m_currentRuleInfo);
        return m_currentRuleInfo != nullptr;
    }

};

// RDFStore::RDFStoreType

template<class TT>
typename RDFStore<TT>::RDFStoreType RDFStore<TT>::s_rdfStoreType;

template<class TT>
RDFStore<TT>::RDFStoreType::RDFStoreType() : DataStoreType(TT::getTypeName()) {
}

template<class TT>
std::unique_ptr<DataStore> RDFStore<TT>::RDFStoreType::newDataStore(const Parameters& dataStoreParameters) const {
    return std::unique_ptr<DataStore>(new RDFStore<TT>(dataStoreParameters));
}

// RDFStore

template<class TT>
EqualityAxiomatizationType RDFStore<TT>::getEqualityAxiomatizationType(const Parameters& dataStoreParameters) {
    const char* const equality = dataStoreParameters.getString("equality", "off");
    if (::strcmp(equality, "off") == 0)
        return EQUALITY_AXIOMATIZATION_OFF;
    else if (::strcmp(equality, "noUNA") == 0)
        return EQUALITY_AXIOMATIZATION_NO_UNA;
    else if (::strcmp(equality, "UNA") == 0)
        return EQUALITY_AXIOMATIZATION_UNA;
    else {
        std::ostringstream message;
        message << "Equality mode '" << equality << "' is invalid: allowed values are 'off', 'noUNA', and 'UNA'.";
        throw RDF_STORE_EXCEPTION(message.str());
    }
}


template<class TT>
void RDFStore<TT>::initializeInternal(const size_t initialTripleCapacity, const size_t initialResourceCapacity) {
    ensureOvercommitSupported();
    m_dictionary.initialize(initialResourceCapacity);
    m_tripleTable.initialize(initialTripleCapacity, initialResourceCapacity);
    if (!m_equalityManager.initialize())
        throw RDF_STORE_EXCEPTION("Cannot initialize the equality manager.");
    setNumberOfThreadsInternal(1);
    m_datalogEngine.clearRules();
}

template<class TT>
void RDFStore<TT>::setNumberOfThreadsInternal(const size_t numberOfThreads) {
    m_numberOfThreads = numberOfThreads;
    m_dictionary.setNumberOfThreads(m_numberOfThreads);
    m_tripleTable.setNumberOfThreads(m_numberOfThreads);
    m_datalogEngine.setNumberOfThreads(m_numberOfThreads);
}

template<class TT>
RDFStore<TT>::RDFStore(const Parameters& dataStoreParameters) :
    m_dataStoreParameters(dataStoreParameters),
    m_equalityAxiomatizationType(getEqualityAxiomatizationType(m_dataStoreParameters)),
    m_memoryManager(static_cast<size_t>(::getTotalPhysicalMemorySize() * 0.9)),
    m_numberOfThreads(0),
    m_dictionary(m_memoryManager, TT::TripleListType::IS_CONCURRENT),
    m_equalityManager(m_memoryManager),
    m_tripleTable(m_memoryManager, m_dataStoreParameters),
    m_tupleTablesByName(&m_tripleTable),
    m_datalogEngine(*this)
{
    setNumberOfThreads(1);
}

template<class TT>
RDFStore<TT>::~RDFStore() {
}

template<class TT>
bool RDFStore<TT>::isConcurrent() const {
    return TT::TripleListType::IS_CONCURRENT;
}

template<class TT>
const DataStoreType& RDFStore<TT>::getType() const {
    return s_rdfStoreType;
}

template<class TT>
const Parameters& RDFStore<TT>::getDataStoreParameters() const {
    return m_dataStoreParameters;
}

template<class TT>
EqualityAxiomatizationType RDFStore<TT>::getEqualityAxiomatizationType() const {
    return m_equalityAxiomatizationType;
}

template<class TT>
void RDFStore<TT>::initialize(const size_t initialTripleCapacity, const size_t initialResourceCapacity) {
    MutexHolderType mutexHolder(m_mutex);
    initializeInternal(initialTripleCapacity, initialResourceCapacity);
}

template<class TT>
void RDFStore<TT>::reindex(const bool dropIDB, const size_t initialTripleCapacity, const size_t initialResourceCapacity) {
    MutexHolderType mutexHolder(m_mutex);
    if (m_equalityAxiomatizationType != EQUALITY_AXIOMATIZATION_OFF && dropIDB && !m_equalityManager.initialize())
        throw RDF_STORE_EXCEPTION("Cannot reinitialize the equality manager.");
    m_tripleTable.reindex(dropIDB, initialTripleCapacity, initialResourceCapacity);
}

template<class TT>
size_t RDFStore<TT>::getNumberOfThreads() const {
    return ::atomicRead(m_numberOfThreads);
}

template<class TT>
void RDFStore<TT>::setNumberOfThreads(const size_t numberOfThreads) {
    if (numberOfThreads == 0)
        throw RDF_STORE_EXCEPTION("The number of threads cannot be zero.");
    if (numberOfThreads > 1 && !isConcurrent())
        throw RDF_STORE_EXCEPTION("This is a sequential store and so the number of threads must be one.");
    MutexHolderType mutexHolder(m_mutex);
    setNumberOfThreadsInternal(numberOfThreads);
}

template<class TT>
MemoryManager& RDFStore<TT>::getMemoryManager() const {
    return const_cast<MemoryManager&>(static_cast<const MemoryManager&>(m_memoryManager));
}

template<class TT>
Dictionary& RDFStore<TT>::getDictionary() const {
    return const_cast<Dictionary&>(static_cast<const Dictionary&>(m_dictionary));
}

template<class TT>
const EqualityManager& RDFStore<TT>::getEqualityManager() const {
    return m_equalityManager;
}


template<class TT>
void RDFStore<TT>::makeFactsExplicit() {
    MutexHolderType mutexHolder(m_mutex);
    m_tripleTable.makeFactsExplicit();
}

template<class TT>
void RDFStore<TT>::clearRulesAndMakeFactsExplicit() {
    MutexHolderType mutexHolder(m_mutex);
    m_datalogEngine.clearRules();
    m_tripleTable.makeFactsExplicit();
    std::vector<ResourceID> argumentsBuffer(3, INVALID_RESOURCE_ID);
    argumentsBuffer[1] = OWL_SAME_AS_ID;
    std::vector<ArgumentIndex> argumentIndexes;
    argumentIndexes.push_back(0);
    argumentIndexes.push_back(1);
    argumentIndexes.push_back(2);
    ThreadContext& threadContext = ThreadContext::getCurrentThreadContext();
    const ResourceID afterLastResourceID = m_equalityManager.getAfterLastResourceID();
    for (ResourceID resourceID = INVALID_RESOURCE_ID + 1; resourceID < afterLastResourceID; ++resourceID) {
        if (m_equalityManager.isNormal(resourceID)) {
            argumentsBuffer[0] = resourceID;
            for (ResourceID equivalentResourceID = m_equalityManager.getNextEqual(resourceID); equivalentResourceID != INVALID_RESOURCE_ID; equivalentResourceID = m_equalityManager.getNextEqual(equivalentResourceID)) {
                argumentsBuffer[2] = equivalentResourceID;
                m_tripleTable.addTuple(threadContext, argumentsBuffer, argumentIndexes, 0, TUPLE_STATUS_IDB_MERGED | TUPLE_STATUS_IDB | TUPLE_STATUS_EDB);
            }
        }
    }
}

template<class TT>
bool RDFStore<TT>::addRule(const Rule& rule) {
    MutexHolderType mutexHolder(m_mutex);
    DatalogProgram datalogProgram;
    datalogProgram.push_back(rule);
    return m_datalogEngine.addRules(datalogProgram);
}

template<class TT>
bool RDFStore<TT>::addRules(const DatalogProgram& datalogProgram) {
    MutexHolderType mutexHolder(m_mutex);
    return m_datalogEngine.addRules(datalogProgram);
}

template<class TT>
bool RDFStore<TT>::removeRule(const Rule& rule) {
    MutexHolderType mutexHolder(m_mutex);
    DatalogProgram datalogProgram;
    datalogProgram.push_back(rule);
    return m_datalogEngine.removeRules(datalogProgram);
}

template<class TT>
bool RDFStore<TT>::removeRules(const DatalogProgram& datalogProgram) {
    MutexHolderType mutexHolder(m_mutex);
    return m_datalogEngine.removeRules(datalogProgram);
}

template<class TT>
void RDFStore<TT>::recompileRules() {
    MutexHolderType mutexHolder(m_mutex);
    m_datalogEngine.recompileRules();
}

template<class TT>
void RDFStore<TT>::applyRules(const bool processComponentsByLevels, MaterializationMonitor* const materializationMonitor) {
    MutexHolderType mutexHolder(m_mutex);
    m_datalogEngine.applyRules(processComponentsByLevels, materializationMonitor);
}

template<class TT>
void RDFStore<TT>::applyRulesIncrementally(const bool processComponentsByLevels, const bool useDRedAlgorithm, IncrementalMonitor* const incrementalMonitor) {
    MutexHolderType mutexHolder(m_mutex);
    m_datalogEngine.applyRulesIncrementally(processComponentsByLevels, useDRedAlgorithm, incrementalMonitor);
}

template<class TT>
std::unique_ptr<RuleIterator> RDFStore<TT>::createRuleIterator() const {
    return std::unique_ptr<RuleIterator>(new RDFStoreRuleIterator<TT>(*this));
}

template<class TT>
const std::map<std::string, TupleTable*>& RDFStore<TT>::getTupleTablesByName() const {
    return m_tupleTablesByName;
}

template<class TT>
bool RDFStore<TT>::containsTupleTable(const std::string& predicateName) const {
    return m_tupleTablesByName.find(predicateName) != m_tupleTablesByName.end();
}

template<class TT>
TupleTable& RDFStore<TT>::getTupleTable(const std::string& predicateName) const {
    std::map<std::string, TupleTable*>::const_iterator iterator = m_tupleTablesByName.find(predicateName);
    if (iterator == m_tupleTablesByName.end()) {
        std::ostringstream message;
        message << "Data store does not contain a table for predicate '" << predicateName << "'.";
        throw RDF_STORE_EXCEPTION(message.str());
    }
    else
        return const_cast<TupleTable&>(*iterator->second);
}

template<class TT>
std::unique_ptr<ExplanationProvider> RDFStore<TT>::createExplanationProvider() const {
    return std::unique_ptr<ExplanationProvider>(new DatalogExplanationProvider<MutexType>(*this, m_mutex, const_cast<DatalogEngine&>(m_datalogEngine).getRuleIndex()));
}

template<class TT>
std::unique_ptr<QueryIterator> RDFStore<TT>::compileQuery(const Query& query, const Parameters& parameters, TupleIteratorMonitor* const tupleIteratorMonitor) const {
    TermArray termArray;
    return compileQuery(query, termArray, parameters, tupleIteratorMonitor);
}

template<class TT>
std::unique_ptr<QueryIterator> RDFStore<TT>::compileQuery(const Query& query, TermArray& termArray, const Parameters& parameters, TupleIteratorMonitor* const tupleIteratorMonitor) const {
    QueryCompiler queryCompiler(*this, parameters, tupleIteratorMonitor);
    return queryCompiler.compileQuery(query, termArray);
}

template<class TT>
std::unique_ptr<QueryIterator> RDFStore<TT>::compileQuery(Prefixes& prefixes, const char* queryText, const Parameters& parameters, TupleIteratorMonitor* const tupleIteratorMonitor) const {
    TermArray termArray;
    return compileQuery(prefixes, queryText, termArray, parameters, tupleIteratorMonitor);
}

template<class TT>
std::unique_ptr<QueryIterator> RDFStore<TT>::compileQuery(Prefixes& prefixes, const char* queryText, TermArray& termArray, const Parameters& parameters, TupleIteratorMonitor* const tupleIteratorMonitor) const {
    SPARQLParser parser(prefixes);
    LogicFactory factory(newLogicFactory());
    Query query = parser.parse(factory, queryText, std::strlen(queryText));
    return compileQuery(query, termArray, parameters);
}

template<class TT>
void RDFStore<TT>::updateStatistics() {
    MutexHolderType mutexHolder(m_mutex);
    m_tripleTable.updateStatistics();
}

template<class TT>
void RDFStore<TT>::saveFormatted(OutputStream& outputStream) const {
    MutexHolderType mutexHolder(m_mutex);
    outputStream.writeString(CURRENT_FORMATTED_STORE_VERSION);
    outputStream.writeString(getType().getDataStoreTypeName());
    m_dataStoreParameters.save(outputStream);
    outputStream.write(m_numberOfThreads);
    m_dictionary.save(outputStream);
    m_equalityManager.save(outputStream);
    m_tripleTable.saveFormatted(outputStream);
    // WARNING: The datalog engine should be loaded after the equality manager so that it can normalize rules appropriately,
    // as well as after the triple table so that the data statistics is availab.e
    m_datalogEngine.save(outputStream);
}

template<class TT>
void RDFStore<TT>::loadFormatted(InputStream& inputStream, const bool formatKindAlreadyCheched, const bool dataStoreParametersAlreadyCheched) {
    MutexHolderType mutexHolder(m_mutex);
    initializeInternal();
    try {
        if (!formatKindAlreadyCheched && !inputStream.checkNextString(CURRENT_FORMATTED_STORE_VERSION))
            throw RDF_STORE_EXCEPTION("Invalid input file: cannot load RDFStore.");
        if (!dataStoreParametersAlreadyCheched) {
            if (!inputStream.checkNextString(getType().getDataStoreTypeName()))
                throw RDF_STORE_EXCEPTION("Invalid input file: the input is incompatible with the type of this data store.");
            Parameters dataStoreParameters;
            dataStoreParameters.load(inputStream);
            if (m_dataStoreParameters != dataStoreParameters)
                throw RDF_STORE_EXCEPTION("Invalid input file: the data store parameters in the input do not match the parameters of this store.");
        }
        const size_t numberOfThreads = inputStream.read<size_t>();
        m_dictionary.load(inputStream);
        m_equalityManager.load(inputStream);
        m_tripleTable.loadFormatted(inputStream);
        // WARNING: The datalog engine should be loaded after the equality manager so that it can normalize rules appropriately,
        // as well as after the triple table so that the data statistics is availab.e
        m_datalogEngine.load(inputStream);
        // We set the number of threads in the end. In this way, the rules in the datalog engine get loaded as if on one thread,
        // and the following call adjusts the engine accordingly.
        setNumberOfThreadsInternal(numberOfThreads);
    }
    catch (const RDFStoreException&) {
        initializeInternal();
        throw;
    }
}

template<class TT>
void RDFStore<TT>::saveUnformatted(OutputStream& outputStream) const {
    MutexHolderType mutexHolder(m_mutex);
    outputStream.writeString(CURRENT_UNFORMATTED_STORE_VERSION);
    m_dictionary.save(outputStream);
    m_equalityManager.save(outputStream);
    m_tripleTable.saveUnformatted(outputStream);
    // WARNING: The datalog engine should be loaded after the equality manager so that it can normalize rules appropriately,
    // as well as after the triple table so that the data statistics is availab.e
    m_datalogEngine.save(outputStream);
}

template<class TT>
void RDFStore<TT>::loadUnformatted(InputStream& inputStream, const bool formatKindAlreadyCheched, const size_t numberOfThreads) {
    MutexHolderType mutexHolder(m_mutex);
    initializeInternal();
    try {
        if (!formatKindAlreadyCheched && !inputStream.checkNextString(CURRENT_UNFORMATTED_STORE_VERSION))
            throw RDF_STORE_EXCEPTION("Invalid input file: cannot load RDFStore.");
        m_dictionary.load(inputStream);
        m_equalityManager.load(inputStream);
        m_tripleTable.loadUnformatted(inputStream, numberOfThreads);
        // WARNING: The datalog engine should be loaded after the equality manager so that it can normalize rules appropriately,
        // as well as after the triple table so that the data statistics is availab.e
        m_datalogEngine.load(inputStream);
    }
    catch (const RDFStoreException&) {
        initializeInternal();
        throw;
    }
}

template<class TT>
std::unique_ptr<ComponentStatistics> RDFStore<TT>::getComponentStatistics() const {
    MutexHolderType mutexHolder(m_mutex);
    std::unique_ptr<ComponentStatistics> result(new ComponentStatistics(std::string("RDFStore[") + std::string(getType().getDataStoreTypeName()) + std::string("]")));
    std::unique_ptr<ComponentStatistics> dictionaryStatistics = m_dictionary.getComponentStatistics();
    std::unique_ptr<ComponentStatistics> equalityManagerStatistics = m_equalityManager.getComponentStatistics();
    std::unique_ptr<ComponentStatistics> tripleTableStatistics = m_tripleTable.getComponentStatistics();
    const uint64_t dictionaryAggregateSize = dictionaryStatistics->getItemIntegerValue("Aggregate size");
    const uint64_t equalityManagerSize = equalityManagerStatistics->getItemIntegerValue("Size");
    const uint64_t tripleTableAggregateSize = tripleTableStatistics->getItemIntegerValue("Aggregate size");
    const uint64_t aggregateSize = dictionaryAggregateSize + equalityManagerSize + tripleTableAggregateSize;
    result->addIntegerItem("Used memory as per memory manager", m_memoryManager.getUsedMemorySize());
    result->addIntegerItem("Available memory as per memory manager", m_memoryManager.getAvailableMemorySize());
    result->addIntegerItem("Aggregate size", aggregateSize);
    uint64_t tripleCount = m_tripleTable.getTupleCount(TUPLE_STATUS_COMPLETE, TUPLE_STATUS_COMPLETE);
    result->addIntegerItem("Triple count", tripleCount);
    if (tripleCount != 0)
        result->addFloatingPointItem("Bytes per triple", aggregateSize / static_cast<double>(tripleCount));
    result->addFloatingPointItem("Dictionary size (%)", dictionaryAggregateSize * 100.0 / static_cast<double>(aggregateSize));
    result->addFloatingPointItem("Triple table size (%)", tripleTableAggregateSize * 100.0 / static_cast<double>(aggregateSize));
    result->addSubcomponent(std::move(dictionaryStatistics));
    result->addSubcomponent(std::move(equalityManagerStatistics));
    result->addSubcomponent(std::move(tripleTableStatistics));
    return result;
}

#endif /* RDFSTOREIMPL_H_ */
