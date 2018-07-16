// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef TRIPLETABLEPROXYIMPL_H_
#define TRIPLETABLEPROXYIMPL_H_

#include "../RDFStoreException.h"
#include "../util/ThreadContext.h"
#include "TripleTableProxy.h"

template<class TT>
TripleTableProxy<TT>::TripleTableProxy(TripleTableType& tripleTable, const Parameters& dataStoreParameters, const size_t windowSize) :
    m_tripleTable(tripleTable),
    m_twoKeysManagerProxy1(m_tripleTable.m_twoKeysManager1, dataStoreParameters),
    m_twoKeysManagerProxy2(m_tripleTable.m_twoKeysManager2, dataStoreParameters),
    m_twoKeysManagerProxy3(m_tripleTable.m_twoKeysManager3, dataStoreParameters),
    m_windowSize(windowSize),
    m_windowStartTripleIndex(INVALID_TUPLE_INDEX),
    m_windowEndTripleIndex(INVALID_TUPLE_INDEX),
    m_windowNextTripleIndex(INVALID_TUPLE_INDEX)
{
}

template<class TT>
void TripleTableProxy<TT>::initialize() {
    if (!m_twoKeysManagerProxy1.initialize() || !m_twoKeysManagerProxy2.initialize() || !m_twoKeysManagerProxy3.initialize())
        throw RDF_STORE_EXCEPTION("Cannot initialize a proxy for the triple table.");
    m_windowStartTripleIndex = m_windowNextTripleIndex = m_windowEndTripleIndex = m_tripleTable.m_tripleList.getFirstFreeTripleIndex();
}

template<class TT>
std::pair<bool, TupleIndex> TripleTableProxy<TT>::addTuple(ThreadContext& threadContext, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const TupleStatus deleteTupleStatus, const TupleStatus addTupleStatus) {
    ensureReservationNotEmptyInternal();
    const ResourceID s = argumentsBuffer[argumentIndexes[0]];
    const ResourceID p = argumentsBuffer[argumentIndexes[1]];
    const ResourceID o = argumentsBuffer[argumentIndexes[2]];
    if (s == INVALID_RESOURCE_ID || p == INVALID_RESOURCE_ID || o == INVALID_RESOURCE_ID)
        throw RDF_STORE_EXCEPTION("A triple contained undefined values cannot be added to the store.");
    m_tripleTable.m_tripleList.addAt(m_windowNextTripleIndex, s, p, o);
    bool tripleIndexInserted;
    TupleIndex resultingTripleIndex;
    if (!m_tripleTable.m_threeKeysManager.insertTriple(threadContext, m_windowNextTripleIndex, s, p, o, tripleIndexInserted, resultingTripleIndex))
        throw RDF_STORE_EXCEPTION("Memory exhausted.");
    TupleStatus completeStatus;
    if (tripleIndexInserted) {
        if (!m_twoKeysManagerProxy1.insertTriple(threadContext, m_windowNextTripleIndex, s, p, o) || !m_twoKeysManagerProxy2.insertTriple(threadContext, m_windowNextTripleIndex, s, p, o) || !m_twoKeysManagerProxy3.insertTriple(threadContext, m_windowNextTripleIndex, s, p, o))
            throw RDF_STORE_EXCEPTION("Memory exhausted.");
        ++m_windowNextTripleIndex;
        completeStatus = TUPLE_STATUS_COMPLETE;
    }
    else
        completeStatus = 0;
    const TupleStatus tupleStatusMask = deleteTupleStatus | addTupleStatus;
    TupleStatus existingTripleStatus;
    TupleStatus newTripleStatus;
    do {
        existingTripleStatus = m_tripleTable.m_tripleList.getTripleStatus(resultingTripleIndex);
        newTripleStatus = (existingTripleStatus & ~tupleStatusMask) | addTupleStatus | completeStatus;
    } while (existingTripleStatus != newTripleStatus && !m_tripleTable.m_tripleList.setTripleStatusConditional(resultingTripleIndex, existingTripleStatus, newTripleStatus));
    return std::make_pair((existingTripleStatus & tupleStatusMask) != addTupleStatus, static_cast<TupleIndex>(resultingTripleIndex));
}

template<class TT>
std::pair<bool, TupleIndex> TripleTableProxy<TT>::addTuple(const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const TupleStatus deleteTupleStatus, const TupleStatus addTupleStatus) {
    return addTuple(ThreadContext::getCurrentThreadContext(), argumentsBuffer, argumentIndexes, deleteTupleStatus, addTupleStatus);
}

template<class TT>
always_inline void TripleTableProxy<TT>::ensureReservationNotEmptyInternal() {
    if (m_windowNextTripleIndex == m_windowEndTripleIndex) {
        ::atomicWrite(m_windowStartTripleIndex, m_tripleTable.m_tripleList.reserveAddWindow(m_windowSize));
        if (m_windowStartTripleIndex == INVALID_TUPLE_INDEX)
            throw RDF_STORE_EXCEPTION("Memory exhausted.");
        ::atomicWrite(m_windowEndTripleIndex, m_windowStartTripleIndex + m_windowSize);
        ::atomicWrite(m_windowNextTripleIndex, m_windowStartTripleIndex);
    }
}

template<class TT>
TupleIndex TripleTableProxy<TT>::getFirstReservedTupleIndex() const {
    return ::atomicRead(m_windowStartTripleIndex);
}

template<class TT>
void TripleTableProxy<TT>::invalidateRemainingBuffer(ThreadContext& threadContext) {
    while (m_windowNextTripleIndex < m_windowEndTripleIndex) {
        m_tripleTable.m_tripleList.addAt(m_windowNextTripleIndex, INVALID_RESOURCE_ID, INVALID_RESOURCE_ID, INVALID_RESOURCE_ID);
        m_tripleTable.m_tripleList.setTripleStatus(m_windowNextTripleIndex, TUPLE_STATUS_COMPLETE);
        ++m_windowNextTripleIndex;
    }
    const TupleIndex firstFreeTripleIndex = m_tripleTable.m_tripleList.getFirstFreeTripleIndex();
    ::atomicWrite(m_windowStartTripleIndex, firstFreeTripleIndex);
    ::atomicWrite(m_windowNextTripleIndex, firstFreeTripleIndex);
    ::atomicWrite(m_windowEndTripleIndex, firstFreeTripleIndex);
}

template<class TT>
TupleIndex TripleTableProxy<TT>::getLowerWriteTupleIndex(const TupleIndex otherWriteTupleIndex) const {
    const TupleIndex proxyWindowNextTripleIndex = ::atomicRead(m_windowNextTripleIndex);
    if (proxyWindowNextTripleIndex != ::atomicRead(m_windowEndTripleIndex) && proxyWindowNextTripleIndex < otherWriteTupleIndex)
        return proxyWindowNextTripleIndex;
    else
        return otherWriteTupleIndex;
}

template<class TT>
std::unique_ptr<ComponentStatistics> TripleTableProxy<TT>::getComponentStatistics() const {
    std::unique_ptr<ComponentStatistics> result(new ComponentStatistics("TripleTableProxy"));
    std::unique_ptr<ComponentStatistics> twoKeysManagerProxy1Statistics = m_twoKeysManagerProxy1.getComponentStatistics();
    std::unique_ptr<ComponentStatistics> twoKeysManagerProxy2Statistics = m_twoKeysManagerProxy1.getComponentStatistics();
    std::unique_ptr<ComponentStatistics> twoKeysManagerProxy3Statistics = m_twoKeysManagerProxy1.getComponentStatistics();
    const uint64_t aggregateSize =
        twoKeysManagerProxy1Statistics->getItemIntegerValue("Aggregate size") +
        twoKeysManagerProxy2Statistics->getItemIntegerValue("Aggregate size") +
        twoKeysManagerProxy3Statistics->getItemIntegerValue("Aggregate size");
    result->addIntegerItem("Aggregate size", aggregateSize);
    result->addSubcomponent(std::move(twoKeysManagerProxy1Statistics));
    result->addSubcomponent(std::move(twoKeysManagerProxy2Statistics));
    result->addSubcomponent(std::move(twoKeysManagerProxy3Statistics));
    return result;
}

#endif /* TRIPLETABLEPROXYIMPL_H_ */
