// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../RDFStoreException.h"
#include "../util/ThreadContext.h"
#include "../storage/TupleIteratorMonitor.h"
#include "GroupIterator.h"

// GroupAllBoundOneIterator

template<bool callMonitor>
GroupAllBoundOneIterator<callMonitor>::GroupAllBoundOneIterator(TupleIteratorMonitor* const tupleIteratorMonitor, const std::vector<ArgumentIndex>& argumentIndexes, std::unique_ptr<TupleIterator> tupleIterator) :
    TupleIterator(tupleIteratorMonitor, tupleIterator->getArgumentsBuffer(), argumentIndexes, tupleIterator->getAllInputArguments(), tupleIterator->getSurelyBoundInputArguments()),
    m_tupleIterator(std::move(tupleIterator))
{
    m_allArguments.add(m_argumentIndexes[0]);
    m_surelyBoundArguments.add(m_argumentIndexes[0]);
}

template<bool callMonitor>
GroupAllBoundOneIterator<callMonitor>::GroupAllBoundOneIterator(const GroupAllBoundOneIterator<callMonitor>& other, CloneReplacements& cloneReplacements) :
    TupleIterator(other, cloneReplacements),
    m_tupleIterator(other.m_tupleIterator->clone(cloneReplacements))
{
}

template<bool callMonitor>
const char* GroupAllBoundOneIterator<callMonitor>::getName() const {
    return "GroupAllBoundOneIterator";
}

template<bool callMonitor>
size_t GroupAllBoundOneIterator<callMonitor>::getNumberOfChildIterators() const {
    return 1;
}

template<bool callMonitor>
const TupleIterator& GroupAllBoundOneIterator<callMonitor>::getChildIterator(const size_t childIteratorIndex) const {
    switch (childIteratorIndex) {
    case 0:
        return *m_tupleIterator;
    default:
        throw RDF_STORE_EXCEPTION("Invalid child iterator index.");
    }
}

template<bool callMonitor>
std::unique_ptr<TupleIterator> GroupAllBoundOneIterator<callMonitor>::clone(CloneReplacements& cloneReplacements) const {
    return std::unique_ptr<TupleIterator>(new GroupAllBoundOneIterator<callMonitor>(*this, cloneReplacements));
}

template<bool callMonitor>
always_inline size_t GroupAllBoundOneIterator<callMonitor>::open(ThreadContext& threadContext) {
    const ArgumentIndex argumentIndex = m_argumentIndexes[0];
    size_t totalMultiplicity = 0;
    size_t multiplicity = m_tupleIterator->open(threadContext);
    const ResourceID resourceID = m_argumentsBuffer[argumentIndex];
    while (multiplicity != 0 && m_argumentsBuffer[argumentIndex] == resourceID) {
        totalMultiplicity += multiplicity;
        multiplicity = m_tupleIterator->advance();
    }
    if (callMonitor)
        m_tupleIteratorMonitor->iteratorOpened(*this, totalMultiplicity);
    return totalMultiplicity;
}

template<bool callMonitor>
size_t GroupAllBoundOneIterator<callMonitor>::open() {
    return GroupAllBoundOneIterator<callMonitor>::open(ThreadContext::getCurrentThreadContext());
}

template<bool callMonitor>
size_t GroupAllBoundOneIterator<callMonitor>::advance() {
    if (callMonitor)
        m_tupleIteratorMonitor->iteratorAdvanced(*this, 0);
    return 0;
}

template<bool callMonitor>
bool GroupAllBoundOneIterator<callMonitor>::getCurrentTupleInfo(TupleIndex& tupleIndex, TupleStatus& tupleStatus) const {
    return m_tupleIterator->getCurrentTupleInfo(tupleIndex, tupleStatus);
}

template<bool callMonitor>
TupleIndex GroupAllBoundOneIterator<callMonitor>::getCurrentTupleIndex() const {
    return INVALID_TUPLE_INDEX;
}

template class GroupAllBoundOneIterator<false>;
template class GroupAllBoundOneIterator<true>;

// GroupAllBoundIterator

template<bool callMonitor>
always_inline bool GroupAllBoundIterator<callMonitor>::currentTupleMatches() const {
    for (auto iterator = m_groupedValues.begin(); iterator != m_groupedValues.end(); ++iterator)
        if (iterator->second != m_argumentsBuffer[iterator->first])
            return false;
    return true;
}

template<bool callMonitor>
GroupAllBoundIterator<callMonitor>::GroupAllBoundIterator(TupleIteratorMonitor* const tupleIteratorMonitor, const std::vector<ArgumentIndex>& argumentIndexes, std::unique_ptr<TupleIterator> tupleIterator) :
    TupleIterator(tupleIteratorMonitor, tupleIterator->getArgumentsBuffer(), argumentIndexes, tupleIterator->getAllInputArguments(), tupleIterator->getSurelyBoundInputArguments()),
    m_tupleIterator(std::move(tupleIterator)),
    m_groupedValues()
{
    for (auto iterator = m_argumentIndexes.begin(); iterator != m_argumentIndexes.end(); ++iterator) {
        m_groupedValues.emplace_back(*iterator, INVALID_RESOURCE_ID);
        m_allArguments.add(*iterator);
        if (m_tupleIterator->getSurelyBoundArguments().contains(*iterator))
            m_surelyBoundArguments.add(*iterator);
    }
}

template<bool callMonitor>
GroupAllBoundIterator<callMonitor>::GroupAllBoundIterator(const GroupAllBoundIterator<callMonitor>& other, CloneReplacements& cloneReplacements) :
    TupleIterator(other, cloneReplacements),
    m_tupleIterator(other.m_tupleIterator->clone(cloneReplacements)),
    m_groupedValues(other.m_groupedValues)
{
}

template<bool callMonitor>
const char* GroupAllBoundIterator<callMonitor>::getName() const {
    return "GroupAllBoundIterator";
}

template<bool callMonitor>
size_t GroupAllBoundIterator<callMonitor>::getNumberOfChildIterators() const {
    return 1;
}

template<bool callMonitor>
const TupleIterator& GroupAllBoundIterator<callMonitor>::getChildIterator(const size_t childIteratorIndex) const {
    switch (childIteratorIndex) {
    case 0:
        return *m_tupleIterator;
    default:
        throw RDF_STORE_EXCEPTION("Invalid child iterator index.");
    }
}

template<bool callMonitor>
std::unique_ptr<TupleIterator> GroupAllBoundIterator<callMonitor>::clone(CloneReplacements& cloneReplacements) const {
    return std::unique_ptr<TupleIterator>(new GroupAllBoundIterator<callMonitor>(*this, cloneReplacements));
}

template<bool callMonitor>
always_inline size_t GroupAllBoundIterator<callMonitor>::open(ThreadContext& threadContext) {
    size_t totalMultiplicity = 0;
    size_t multiplicity = m_tupleIterator->open(threadContext);
    for (auto iterator = m_groupedValues.begin(); iterator != m_groupedValues.end(); ++iterator)
        iterator->second = m_argumentsBuffer[iterator->first];
    while (multiplicity != 0 && currentTupleMatches()) {
        totalMultiplicity += multiplicity;
        multiplicity = m_tupleIterator->advance();
    }
    if (callMonitor)
        m_tupleIteratorMonitor->iteratorOpened(*this, totalMultiplicity);
    return totalMultiplicity;
}

template<bool callMonitor>
size_t GroupAllBoundIterator<callMonitor>::open() {
    return GroupAllBoundIterator<callMonitor>::open(ThreadContext::getCurrentThreadContext());
}

template<bool callMonitor>
size_t GroupAllBoundIterator<callMonitor>::advance() {
    if (callMonitor)
        m_tupleIteratorMonitor->iteratorAdvanced(*this, 0);
    return 0;
}

template<bool callMonitor>
bool GroupAllBoundIterator<callMonitor>::getCurrentTupleInfo(TupleIndex& tupleIndex, TupleStatus& tupleStatus) const {
    return m_tupleIterator->getCurrentTupleInfo(tupleIndex, tupleStatus);
}

template<bool callMonitor>
TupleIndex GroupAllBoundIterator<callMonitor>::getCurrentTupleIndex() const {
    return INVALID_TUPLE_INDEX;
}

template class GroupAllBoundIterator<false>;
template class GroupAllBoundIterator<true>;

// GroupOneUnboundIterator

template<bool callMonitor>
GroupOneUnboundIterator<callMonitor>::GroupOneUnboundIterator(TupleIteratorMonitor* const tupleIteratorMonitor, const std::vector<ArgumentIndex>& argumentIndexes, std::unique_ptr<TupleIterator> tupleIterator) :
    TupleIterator(tupleIteratorMonitor, tupleIterator->getArgumentsBuffer(), argumentIndexes, tupleIterator->getAllInputArguments(), tupleIterator->getSurelyBoundInputArguments()),
    m_tupleIterator(std::move(tupleIterator)),
    m_groups(),
    m_currentGroup(m_groups.end())
{
    m_allArguments.add(m_argumentIndexes[0]);
    m_surelyBoundArguments.add(m_argumentIndexes[0]);
}

template<bool callMonitor>
GroupOneUnboundIterator<callMonitor>::GroupOneUnboundIterator(const GroupOneUnboundIterator<callMonitor>& other, CloneReplacements& cloneReplacements) :
    TupleIterator(other, cloneReplacements),
    m_tupleIterator(other.m_tupleIterator->clone(cloneReplacements)),
    m_groups(),
    m_currentGroup(m_groups.end())
{
}

template<bool callMonitor>
const char* GroupOneUnboundIterator<callMonitor>::getName() const {
    return "GroupOneUnboundIterator";
}

template<bool callMonitor>
size_t GroupOneUnboundIterator<callMonitor>::getNumberOfChildIterators() const {
    return 1;
}

template<bool callMonitor>
const TupleIterator& GroupOneUnboundIterator<callMonitor>::getChildIterator(const size_t childIteratorIndex) const {
    switch (childIteratorIndex) {
    case 0:
        return *m_tupleIterator;
    default:
        throw RDF_STORE_EXCEPTION("Invalid child iterator index.");
    }
}

template<bool callMonitor>
std::unique_ptr<TupleIterator> GroupOneUnboundIterator<callMonitor>::clone(CloneReplacements& cloneReplacements) const {
    return std::unique_ptr<TupleIterator>(new GroupOneUnboundIterator<callMonitor>(*this, cloneReplacements));
}

template<bool callMonitor>
always_inline size_t GroupOneUnboundIterator<callMonitor>::open(ThreadContext& threadContext) {
    const ArgumentIndex argumentIndex = m_argumentIndexes[0];
    m_groups.clear();
    size_t multiplicity = m_tupleIterator->open(threadContext);
    while (multiplicity != 0) {
        m_groups[m_argumentsBuffer[argumentIndex]] += multiplicity;
        multiplicity = m_tupleIterator->advance();
    }
    m_currentGroup = m_groups.begin();
    if (m_currentGroup == m_groups.end()) {
        if (callMonitor)
            m_tupleIteratorMonitor->iteratorOpened(*this, 0);
        return 0;
    }
    else {
        m_argumentsBuffer[argumentIndex] = m_currentGroup->first;
        if (callMonitor)
            m_tupleIteratorMonitor->iteratorOpened(*this, m_currentGroup->second);
        return m_currentGroup->second;
    }
}

template<bool callMonitor>
size_t GroupOneUnboundIterator<callMonitor>::open() {
    return GroupOneUnboundIterator<callMonitor>::open(ThreadContext::getCurrentThreadContext());
}

template<bool callMonitor>
size_t GroupOneUnboundIterator<callMonitor>::advance() {
    ++m_currentGroup;
    if (m_currentGroup == m_groups.end()) {
        if (callMonitor)
            m_tupleIteratorMonitor->iteratorAdvanced(*this, 0);
        return 0;
    }
    else {
        m_argumentsBuffer[m_argumentIndexes[0]] = m_currentGroup->first;
        if (callMonitor)
            m_tupleIteratorMonitor->iteratorAdvanced(*this, m_currentGroup->second);
        return m_currentGroup->second;
    }
}

template<bool callMonitor>
bool GroupOneUnboundIterator<callMonitor>::getCurrentTupleInfo(TupleIndex& tupleIndex, TupleStatus& tupleStatus) const {
    return m_tupleIterator->getCurrentTupleInfo(tupleIndex, tupleStatus);
}

template<bool callMonitor>
TupleIndex GroupOneUnboundIterator<callMonitor>::getCurrentTupleIndex() const {
    return INVALID_TUPLE_INDEX;
}

template class GroupOneUnboundIterator<false>;
template class GroupOneUnboundIterator<true>;
