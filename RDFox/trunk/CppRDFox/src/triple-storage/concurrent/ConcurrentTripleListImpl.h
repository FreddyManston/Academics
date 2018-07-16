// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef CONCURRENTTRIPLELISTIMPL_H_
#define CONCURRENTTRIPLELISTIMPL_H_

#include "../../RDFStoreException.h"
#include "../../util/ComponentStatistics.h"
#include "../../util/InputStream.h"
#include "../../util/OutputStream.h"
#include "ConcurrentTripleList.h"

template<typename RI, typename TI>
always_inline ConcurrentTripleList<RI, TI>::ConcurrentTripleList(MemoryManager& memoryManager) :
    m_resources(memoryManager, 3), m_indexes(memoryManager, 3), m_statuses(memoryManager, 3), m_firstFreeTripleIndex(getFirstWriteTripleIndex())
{
}

template<typename RI, typename TI>
always_inline bool ConcurrentTripleList<RI, TI>::initialize(const size_t initialNumberOfTriples) {
    m_firstFreeTripleIndex = 1;
    if (!m_resources.initializeLarge() || !m_indexes.initializeLarge() || !m_statuses.initializeLarge())
        return false;
    if (initialNumberOfTriples != 0) {
        const size_t afterLastTripleIndex = 3 * initialNumberOfTriples;
        if (!m_resources.ensureEndAtLeast(afterLastTripleIndex, 0) || !m_indexes.ensureEndAtLeast(afterLastTripleIndex, 0) || !m_statuses.ensureEndAtLeast(initialNumberOfTriples, 0))
            return false;
    }
    return true;
}

template<typename RI, typename TI>
always_inline TupleStatus ConcurrentTripleList<RI, TI>::getTripleStatus(const TupleIndex tupleIndex) const {
    if (m_statuses.isBeforeEnd(tupleIndex, 1))
        return ::atomicRead(m_statuses[tupleIndex]);
    else
        return TUPLE_STATUS_INVALID;
}

template<typename RI, typename TI>
always_inline TupleStatus ConcurrentTripleList<RI, TI>::setTripleStatus(const TupleIndex tupleIndex, const TupleStatus tupleStatus) {
    return ::atomicExchange(m_statuses[tupleIndex], tupleStatus);
}

template<typename RI, typename TI>
always_inline bool ConcurrentTripleList<RI, TI>::setTripleStatusConditional(const TupleIndex tupleIndex, const TupleStatus expectedTupleStatus, const TupleStatus newTupleStatus) {
    return ::atomicConditionalSet(m_statuses[tupleIndex], expectedTupleStatus, newTupleStatus);
}

template<typename RI, typename TI>
always_inline void ConcurrentTripleList<RI, TI>::getTriple(const TupleIndex tupleIndex, TupleStatus& tupleStatus, ResourceID& s, ResourceID& p, ResourceID& o, TupleIndex& nextS, TupleIndex& nextP, TupleIndex& nextO) const {
    const size_t tripleOffset = 3 * tupleIndex;
    s = static_cast<ResourceID>(::atomicRead(m_resources[tripleOffset + RC_S]));
    p = static_cast<ResourceID>(::atomicRead(m_resources[tripleOffset + RC_P]));
    o = static_cast<ResourceID>(::atomicRead(m_resources[tripleOffset + RC_O]));
    nextS = static_cast<TupleIndex>(::atomicRead(m_indexes[tripleOffset + RC_S]));
    nextP = static_cast<TupleIndex>(::atomicRead(m_indexes[tripleOffset + RC_P]));
    nextO = static_cast<TupleIndex>(::atomicRead(m_indexes[tripleOffset + RC_O]));
    tupleStatus = ::atomicRead(m_statuses[tupleIndex]);
}

template<typename RI, typename TI>
always_inline void ConcurrentTripleList<RI, TI>::getTripleStatusAndResourceIDs(const TupleIndex tupleIndex, TupleStatus& tupleStatus, ResourceID& s, ResourceID& p, ResourceID& o) const {
    const size_t tripleOffset = 3 * tupleIndex;
    s = static_cast<ResourceID>(::atomicRead(m_resources[tripleOffset + RC_S]));
    p = static_cast<ResourceID>(::atomicRead(m_resources[tripleOffset + RC_P]));
    o = static_cast<ResourceID>(::atomicRead(m_resources[tripleOffset + RC_O]));
    tupleStatus = ::atomicRead(m_statuses[tupleIndex]);
}

template<typename RI, typename TI>
always_inline void ConcurrentTripleList<RI, TI>::getResourceIDs(const TupleIndex tupleIndex, ResourceID& s, ResourceID& p, ResourceID& o) const {
    const size_t tripleOffset = 3 * tupleIndex;
    s = static_cast<ResourceID>(::atomicRead(m_resources[tripleOffset + RC_S]));
    p = static_cast<ResourceID>(::atomicRead(m_resources[tripleOffset + RC_P]));
    o = static_cast<ResourceID>(::atomicRead(m_resources[tripleOffset + RC_O]));
}

template<typename RI, typename TI>
always_inline void ConcurrentTripleList<RI, TI>::getResourceIDs(const TupleIndex tupleIndex, const ResourceComponent component1, const ResourceComponent component2, ResourceID& value1, ResourceID& value2) const {
    const size_t tripleOffset = 3 * tupleIndex;
    value1 = static_cast<ResourceID>(::atomicRead(m_resources[tripleOffset + component1]));
    value2 = static_cast<ResourceID>(::atomicRead(m_resources[tripleOffset + component2]));
}

template<typename RI, typename TI>
always_inline ResourceID ConcurrentTripleList<RI, TI>::getResourceID(const TupleIndex tupleIndex, const ResourceComponent component) const {
    return static_cast<ResourceID>(::atomicRead(m_resources[3 * tupleIndex + component]));
}

template<typename RI, typename TI>
always_inline ResourceID ConcurrentTripleList<RI, TI>::getS(const TupleIndex tupleIndex) const {
    return getResourceID(tupleIndex , RC_S);
}

template<typename RI, typename TI>
always_inline ResourceID ConcurrentTripleList<RI, TI>::getP(const TupleIndex tupleIndex) const {
    return getResourceID(tupleIndex , RC_P);
}

template<typename RI, typename TI>
always_inline ResourceID ConcurrentTripleList<RI, TI>::getO(const TupleIndex tupleIndex) const {
    return getResourceID(tupleIndex , RC_O);
}

template<typename RI, typename TI>
always_inline TupleIndex ConcurrentTripleList<RI, TI>::getNext(const TupleIndex tupleIndex, const ResourceComponent component) const {
    return static_cast<TupleIndex>(::atomicRead(m_indexes[3 * tupleIndex + component]));
}

template<typename RI, typename TI>
always_inline void ConcurrentTripleList<RI, TI>::setNext(const TupleIndex tupleIndex, const ResourceComponent component, const TupleIndex nextTripleIndex) {
    ::atomicWrite(m_indexes[3 * tupleIndex + component], static_cast<StoreTripleIndexType>(nextTripleIndex));
}

template<typename RI, typename TI>
always_inline bool ConcurrentTripleList<RI, TI>::setNextConditional(const TupleIndex tupleIndex, const ResourceComponent component, const TupleIndex testTripleIndex, const TupleIndex nextTripleIndex) {
    return ::atomicConditionalSet(m_indexes[3 * tupleIndex + component], static_cast<StoreTripleIndexType>(testTripleIndex), static_cast<StoreTripleIndexType>(nextTripleIndex));
}

template<typename RI, typename TI>
always_inline TupleIndex ConcurrentTripleList<RI, TI>::getFirstWriteTripleIndex() const {
    return 1;
}

template<typename RI, typename TI>
always_inline TupleIndex ConcurrentTripleList<RI, TI>::getFirstTripleIndex() const {
    return getNextTripleIndex(0);
}

template<typename RI, typename TI>
always_inline TupleIndex ConcurrentTripleList<RI, TI>::getFirstFreeTripleIndex() const {
    return ::atomicRead(m_firstFreeTripleIndex);
}

template<typename RI, typename TI>
always_inline TupleIndex ConcurrentTripleList<RI, TI>::getNextTripleIndex(const TupleIndex tupleIndex) const {
    TupleIndex resultIndex = tupleIndex + 1;
    while (resultIndex < ::atomicRead(m_firstFreeTripleIndex) && m_statuses.isBeforeEnd(resultIndex, 1)) {
        if ((getTripleStatus(resultIndex) & TUPLE_STATUS_COMPLETE) == TUPLE_STATUS_COMPLETE && getS(resultIndex) != INVALID_RESOURCE_ID)
            return resultIndex;
        ++resultIndex;
    }
    return INVALID_TUPLE_INDEX;
}

template<typename RI, typename TI>
always_inline TupleIndex ConcurrentTripleList<RI, TI>::add(const ResourceID s, const ResourceID p, const ResourceID o) {
    const TupleIndex tupleIndex = ::atomicIncrement(m_firstFreeTripleIndex) - 1;
    const size_t tripleOffset = 3 * tupleIndex;
    if (!m_resources.ensureEndAtLeast(tripleOffset, 3) || !m_indexes.ensureEndAtLeast(tripleOffset, 3) || !m_statuses.ensureEndAtLeast(tupleIndex, 1)) {
        ::atomicDecrement(m_firstFreeTripleIndex);
        return INVALID_TUPLE_INDEX;
    }
    ::atomicWrite(m_resources[tripleOffset + RC_S], static_cast<StoreResourceIDType>(s));
    ::atomicWrite(m_resources[tripleOffset + RC_P], static_cast<StoreResourceIDType>(p));
    ::atomicWrite(m_resources[tripleOffset + RC_O], static_cast<StoreResourceIDType>(o));
    return tupleIndex;
}

template<typename RI, typename TI>
always_inline bool ConcurrentTripleList<RI, TI>::supportsWindowedAdds() const {
    return true;
}

template<typename RI, typename TI>
always_inline TupleIndex ConcurrentTripleList<RI, TI>::reserveAddWindow(const size_t windowSize) {
    const TupleIndex windowEndTripleIndex = ::atomicAdd(m_firstFreeTripleIndex, windowSize);
    const size_t windowEndTripleOffset = 3 * windowEndTripleIndex;
    if (!m_resources.ensureEndAtLeast(windowEndTripleOffset, 0) || !m_indexes.ensureEndAtLeast(windowEndTripleOffset, 0) || !m_statuses.ensureEndAtLeast(windowEndTripleIndex, 0)) {
        ::atomicSubtract(m_firstFreeTripleIndex, windowSize);
        return INVALID_TUPLE_INDEX;
    }
    return windowEndTripleIndex - windowSize;
}

template<typename RI, typename TI>
always_inline void ConcurrentTripleList<RI, TI>::addAt(const TupleIndex tupleIndex, const ResourceID s, const ResourceID p, const ResourceID o) {
    const size_t tripleOffset = 3 * tupleIndex;
    ::atomicWrite(m_resources[tripleOffset + RC_S], static_cast<StoreResourceIDType>(s));
    ::atomicWrite(m_resources[tripleOffset + RC_P], static_cast<StoreResourceIDType>(p));
    ::atomicWrite(m_resources[tripleOffset + RC_O], static_cast<StoreResourceIDType>(o));
}

template<typename RI, typename TI>
always_inline void ConcurrentTripleList<RI, TI>::truncate(const TupleIndex newFirstFreeTripleIndex) {
    for (TupleIndex tupleIndex = newFirstFreeTripleIndex; tupleIndex < m_firstFreeTripleIndex; ++tupleIndex) {
        setTripleStatus(tupleIndex, TUPLE_STATUS_INVALID);
        addAt(tupleIndex, INVALID_RESOURCE_ID, INVALID_RESOURCE_ID, INVALID_RESOURCE_ID);
        setNext(tupleIndex, RC_S, INVALID_TUPLE_INDEX);
        setNext(tupleIndex, RC_P, INVALID_TUPLE_INDEX);
        setNext(tupleIndex, RC_O, INVALID_TUPLE_INDEX);
    }
    m_firstFreeTripleIndex = newFirstFreeTripleIndex;
}

template<typename RI, typename TI>
always_inline size_t ConcurrentTripleList<RI, TI>::getExactTripleCount(const TupleStatus tupleStatusMask, const TupleStatus tupleStatusExpectedValue) const {
    size_t tripleCount = 0;
    TupleIndex tupleIndex = getFirstTripleIndex();
    while (tupleIndex != INVALID_TUPLE_INDEX) {
        if ((getTripleStatus(tupleIndex) & tupleStatusMask) == tupleStatusExpectedValue)
            ++tripleCount;
        tupleIndex = getNextTripleIndex(tupleIndex);
    }
    return tripleCount;
}

template<typename RI, typename TI>
always_inline size_t ConcurrentTripleList<RI, TI>::getApproximateTripleCount() const {
    return ::atomicRead(m_firstFreeTripleIndex) - 1;
}

template<typename RI, typename TI>
always_inline void ConcurrentTripleList<RI, TI>::save(OutputStream& outputStream) const {
    outputStream.writeString("ConcurrentTripleList");
    outputStream.writeMemoryRegion(m_resources);
    outputStream.writeMemoryRegion(m_indexes);
    outputStream.writeMemoryRegion(m_statuses);
    outputStream.write(m_firstFreeTripleIndex);
}

template<typename RI, typename TI>
always_inline void ConcurrentTripleList<RI, TI>::load(InputStream& inputStream) {
    if (!inputStream.checkNextString("ConcurrentTripleList"))
        throw RDF_STORE_EXCEPTION("Invalid input file: cannot load ConcurrentTripleList.");
    inputStream.readMemoryRegion(m_resources);
    inputStream.readMemoryRegion(m_indexes);
    inputStream.readMemoryRegion(m_statuses);
    m_firstFreeTripleIndex = inputStream.read<TupleIndex>();
}

template<typename RI, typename TI>
always_inline std::unique_ptr<ComponentStatistics> ConcurrentTripleList<RI, TI>::getComponentStatistics() const {
    std::unique_ptr<ComponentStatistics> result(new ComponentStatistics("ConcurrentTripleList"));
    const size_t exactTripleCount = getExactTripleCount(TUPLE_STATUS_COMPLETE, TUPLE_STATUS_COMPLETE);
    const size_t size = m_firstFreeTripleIndex * (3 * (sizeof(StoreResourceIDType) + sizeof(StoreTripleIndexType)) + sizeof(TupleStatus));
    result->addIntegerItem("Size", size);
    result->addIntegerItem("Approximate triple count", getApproximateTripleCount());
    result->addIntegerItem("Exact triple count", exactTripleCount);
    if (exactTripleCount != 0)
        result->addIntegerItem("Bytes per triple", size / exactTripleCount);
    return result;
}

#endif /* CONCURRENTTRIPLELISTIMPL_H_ */
