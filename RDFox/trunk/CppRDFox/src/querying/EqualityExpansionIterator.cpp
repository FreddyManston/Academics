// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../RDFStoreException.h"
#include "../util/ThreadContext.h"
#include "../storage/DataStore.h"
#include "../storage/TupleIteratorMonitor.h"
#include "../equality/EqualityManager.h"
#include "EqualityExpansionIterator.h"

template<bool callMonitor>
EqualityExpansionIterator<callMonitor>::EqualityExpansionIterator(TupleIteratorMonitor* const tupleIteratorMonitor, const DataStore& dataStore, std::unique_ptr<TupleIterator> tupleIterator) :
    TupleIterator(tupleIteratorMonitor, tupleIterator->getArgumentsBuffer(), tupleIterator->getArgumentIndexes(), tupleIterator->getAllInputArguments(), tupleIterator->getSurelyBoundInputArguments(), tupleIterator->getAllArguments(), tupleIterator->getSurelyBoundArguments()),
    m_equalityManager(dataStore.getEqualityManager()),
    m_argumentIndexesToExpand(),
    m_tupleIterator(std::move(tupleIterator)),
    m_currentMultiplicity(0)
{
    eliminateArgumentIndexesRedundancy();
    for (ArgumentIndex argumentIndex = 0; argumentIndex < m_allArguments.size(); ++argumentIndex)
        if (m_allArguments.contains(argumentIndex))
            m_argumentIndexesToExpand.push_back(argumentIndex);
    m_argumentIndexesToExpand.shrink_to_fit();
}

template<bool callMonitor>
EqualityExpansionIterator<callMonitor>::EqualityExpansionIterator(const EqualityExpansionIterator<callMonitor>& other, CloneReplacements& cloneReplacements) :
    TupleIterator(other, cloneReplacements),
    m_equalityManager(other.m_equalityManager),
    m_argumentIndexesToExpand(other.m_argumentIndexesToExpand),
    m_tupleIterator(other.m_tupleIterator->clone(cloneReplacements)),
    m_currentMultiplicity(other.m_currentMultiplicity)
{
}

template<bool callMonitor>
const char* EqualityExpansionIterator<callMonitor>::getName() const {
    return "EqualityExpansionIterator";
}

template<bool callMonitor>
size_t EqualityExpansionIterator<callMonitor>::getNumberOfChildIterators() const {
    return 1;
}

template<bool callMonitor>
const TupleIterator& EqualityExpansionIterator<callMonitor>::getChildIterator(const size_t childIteratorIndex) const {
    switch (childIteratorIndex) {
    case 0:
        return *m_tupleIterator;
    default:
        throw RDF_STORE_EXCEPTION("Invalid child iterator index.");
    }
}

template<bool callMonitor>
std::unique_ptr<TupleIterator> EqualityExpansionIterator<callMonitor>::clone(CloneReplacements& cloneReplacements) const {
    return std::unique_ptr<TupleIterator>(new EqualityExpansionIterator<callMonitor>(*this, cloneReplacements));
}

template<bool callMonitor>
size_t EqualityExpansionIterator<callMonitor>::open(ThreadContext& threadContext) {
    m_currentMultiplicity = m_tupleIterator->open(threadContext);
    if (callMonitor)
        m_tupleIteratorMonitor->iteratorOpened(*this, m_currentMultiplicity);
    return m_currentMultiplicity;
}

template<bool callMonitor>
size_t EqualityExpansionIterator<callMonitor>::open() {
    return EqualityExpansionIterator<callMonitor>::open(ThreadContext::getCurrentThreadContext());
}

template<bool callMonitor>
size_t EqualityExpansionIterator<callMonitor>::advance() {
    std::vector<ArgumentIndex>::iterator iterator = m_argumentIndexesToExpand.end();
    while (iterator != m_argumentIndexesToExpand.begin()) {
        const ArgumentIndex argumentIndex = *(--iterator);
        const ResourceID currentResourceID = m_argumentsBuffer[argumentIndex];
        const ResourceID nextResourceID = m_equalityManager.getNextEqual(currentResourceID);
        if (nextResourceID != INVALID_RESOURCE_ID) {
            m_argumentsBuffer[argumentIndex] = nextResourceID;
            if (callMonitor)
                m_tupleIteratorMonitor->iteratorAdvanced(*this, m_currentMultiplicity);
            return m_currentMultiplicity;
        }
        m_argumentsBuffer[argumentIndex] = m_equalityManager.normalize(currentResourceID);
    }
    m_currentMultiplicity = m_tupleIterator->advance();
    if (callMonitor)
        m_tupleIteratorMonitor->iteratorAdvanced(*this, m_currentMultiplicity);
    return m_currentMultiplicity;
}

template<bool callMonitor>
bool EqualityExpansionIterator<callMonitor>::getCurrentTupleInfo(TupleIndex& tupleIndex, TupleStatus& tupleStatus) const {
    return m_tupleIterator->getCurrentTupleInfo(tupleIndex, tupleStatus);
}

template<bool callMonitor>
TupleIndex EqualityExpansionIterator<callMonitor>::getCurrentTupleIndex() const {
    return m_tupleIterator->getCurrentTupleIndex();
}

template class EqualityExpansionIterator<false>;
template class EqualityExpansionIterator<true>;
