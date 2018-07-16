// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../RDFStoreException.h"
#include "../util/ThreadContext.h"
#include "../storage/DataStore.h"
#include "../storage/TupleIteratorMonitor.h"
#include "../dictionary/ResourceValueCache.h"
#include "../equality/EqualityManager.h"
#include "ValuationCacheImpl.h"
#include "QueryIterator.h"
#include "DistinctIterator.h"

// QueryIterator

QueryIterator::QueryIterator(TupleIteratorMonitor* const tupleIteratorMonitor, std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const ArgumentIndexSet& allInputArguments, const ArgumentIndexSet& surelyBoundInputArguments, const ArgumentIndexSet& allArguments, const ArgumentIndexSet& surelyBoundArguments) :
    TupleIterator(tupleIteratorMonitor, argumentsBuffer, argumentIndexes, allInputArguments, surelyBoundInputArguments, allArguments, surelyBoundArguments)
{
}

QueryIterator::QueryIterator(const QueryIterator& other, CloneReplacements& cloneReplacements) :
    TupleIterator(other, cloneReplacements)
{
}

// QueryIteratorImpl

template<bool callMonitor>
void QueryIteratorImpl<callMonitor>::loadValuationCaches(const TupleIterator* tupleIterator) {
    const DistinctIterator<callMonitor>* distinctIterator = dynamic_cast<const DistinctIterator<callMonitor>*>(tupleIterator);
    if (distinctIterator)
        m_valuationCaches.push_back(&distinctIterator->getValuationCache());
    for (size_t childIteratorIndex = 0; childIteratorIndex < tupleIterator->getNumberOfChildIterators(); ++childIteratorIndex)
        loadValuationCaches(&tupleIterator->getChildIterator(childIteratorIndex));
}

template<bool callMonitor>
QueryIteratorImpl<callMonitor>::QueryIteratorImpl(TupleIteratorMonitor* const tupleIteratorMonitor, const DataStore& dataStore, ResourceValueCache& resourceValueCache, std::unique_ptr<ResourceValueCache> resourceValueCacheOwner, std::vector<ResourceID>& argumentsBuffer, std::unique_ptr<std::vector<ResourceID> > argumentsBufferOwner, const size_t numberOfConstantsToNormalize, const std::vector<ArgumentIndex>& argumentIndexes, std::unique_ptr<TupleIterator> tupleIterator) :
    QueryIterator(tupleIteratorMonitor, argumentsBuffer, argumentIndexes, tupleIterator->getAllInputArguments(), tupleIterator->getSurelyBoundInputArguments(), tupleIterator->getAllArguments(), tupleIterator->getSurelyBoundArguments()),
    m_resourceValueCache(resourceValueCache),
    m_resourceValueCacheOwner(std::move(resourceValueCacheOwner)),
    m_equalityManager(dataStore.getEqualityManager()),
    m_argumentsBufferOwner(std::move(argumentsBufferOwner)),
    m_numberOfConstantsToNormalize(numberOfConstantsToNormalize),
    m_tupleIterator(std::move(tupleIterator)),
    m_valuationCaches()
{
    for (std::vector<ArgumentIndex>::iterator iterator = m_argumentIndexes.begin(); iterator != m_argumentIndexes.end(); ++iterator)
        if (!m_allArguments.contains(*iterator)) {
            m_allArguments.add(*iterator);
            m_surelyBoundArguments.add(*iterator);
        }
    m_allArguments.shrinkToFit();
    m_surelyBoundArguments.shrinkToFit();
    loadValuationCaches(m_tupleIterator.get());
    m_valuationCaches.shrink_to_fit();
}

template<bool callMonitor>
QueryIteratorImpl<callMonitor>::QueryIteratorImpl(const QueryIteratorImpl<callMonitor>& other, CloneReplacements& cloneReplacements) :
    QueryIterator(other, cloneReplacements),
    m_resourceValueCache(*cloneReplacements.getReplacement(&other.m_resourceValueCache)),
    m_resourceValueCacheOwner(&m_resourceValueCache),
    m_equalityManager(other.m_equalityManager),
    m_argumentsBufferOwner(&m_argumentsBuffer),
    m_numberOfConstantsToNormalize(other.m_numberOfConstantsToNormalize),
    m_tupleIterator(other.m_tupleIterator->clone(cloneReplacements)),
    m_valuationCaches()
{
    loadValuationCaches(m_tupleIterator.get());
    m_valuationCaches.shrink_to_fit();
}

template<bool callMonitor>
const ResourceValueCache& QueryIteratorImpl<callMonitor>::getResourceValueCache() const {
    return m_resourceValueCache;
}

template<bool callMonitor>
const char* QueryIteratorImpl<callMonitor>::getName() const {
    return "QueryIterator";
}

template<bool callMonitor>
size_t QueryIteratorImpl<callMonitor>::getNumberOfChildIterators() const {
    return 1;
}

template<bool callMonitor>
const TupleIterator& QueryIteratorImpl<callMonitor>::getChildIterator(const size_t childIteratorIndex) const {
    switch (childIteratorIndex) {
    case 0:
        return *m_tupleIterator;
    default:
        throw RDF_STORE_EXCEPTION("Invalid child iterator index.");
    }
}

template<bool callMonitor>
std::unique_ptr<TupleIterator> QueryIteratorImpl<callMonitor>::clone(CloneReplacements& cloneReplacements) const {
    return std::unique_ptr<TupleIterator>(new QueryIteratorImpl<callMonitor>(*this, cloneReplacements));
}

template<bool callMonitor>
always_inline size_t QueryIteratorImpl<callMonitor>::open(ThreadContext& threadContext) {
    for (std::vector<ValuationCache*>::iterator iterator = m_valuationCaches.begin(); iterator != m_valuationCaches.end(); ++iterator)
        (*iterator)->initialize();
    // Note: in the following loop, *iterator can be INVALID_RESOURCE_ID; however, we do not check for that because DataStore::normalize() handles this case without any problems.
    for (size_t index = 0; index < m_numberOfConstantsToNormalize; ++index)
        m_argumentsBuffer[index] = m_equalityManager.normalize(m_argumentsBuffer[index]);
    const size_t multiplicity = m_tupleIterator->open(threadContext);
    if (callMonitor)
        m_tupleIteratorMonitor->iteratorOpened(*this, multiplicity);
    return multiplicity;
}

template<bool callMonitor>
size_t QueryIteratorImpl<callMonitor>::open() {
    return QueryIteratorImpl<callMonitor>::open(ThreadContext::getCurrentThreadContext());
}

template<bool callMonitor>
size_t QueryIteratorImpl<callMonitor>::advance() {
    const size_t multiplicity = m_tupleIterator->advance();
    if (callMonitor)
        m_tupleIteratorMonitor->iteratorAdvanced(*this, multiplicity);
    return multiplicity;
}

template<bool callMonitor>
bool QueryIteratorImpl<callMonitor>::getCurrentTupleInfo(TupleIndex& tupleIndex, TupleStatus& tupleStatus) const {
    return false;
}

template<bool callMonitor>
TupleIndex QueryIteratorImpl<callMonitor>::getCurrentTupleIndex() const {
    return INVALID_TUPLE_INDEX;
}

template class QueryIteratorImpl<false>;
template class QueryIteratorImpl<true>;
