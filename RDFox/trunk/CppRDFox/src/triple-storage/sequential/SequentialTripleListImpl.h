// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef SEQUENTIALTRIPLELISTIMPL_H_
#define SEQUENTIALTRIPLELISTIMPL_H_

#include "../../RDFStoreException.h"
#include "../../util/ComponentStatistics.h"
#include "../../util/InputStream.h"
#include "../../util/OutputStream.h"
#include "SequentialTripleList.h"

// The size of tuple status is rounded up to two. This is so that the size of a triple is 32 bytes,
// which is important for the alignment requirements of SPARC.
static const size_t TRIPLE_SIZE = 3 * SEQUENTIAL_RESOURCE_ID_SIZE + 3 * SEQUENTIAL_TUPLE_INDEX_STORAGE_SIZE + 2;

always_inline SequentialTripleList::SequentialTripleList(MemoryManager& memoryManager) :
    m_data(memoryManager), m_firstFreeTripleIndex(getFirstWriteTripleIndex())
{
}

always_inline bool SequentialTripleList::initialize(const size_t initialNumberOfTriples) {
    m_firstFreeTripleIndex = 1;
    if (!m_data.initializeLarge())
        return false;
    if (initialNumberOfTriples != 0)
        if (!m_data.ensureEndAtLeast(initialNumberOfTriples * TRIPLE_SIZE, 0))
            return false;
    return true;
}

always_inline static ResourceID readResourceID(const uint8_t* const tripleData, const ResourceComponent component) {
    return static_cast<ResourceID>(*reinterpret_cast<const uint32_t*>(tripleData + SEQUENTIAL_RESOURCE_ID_SIZE * component));
}

always_inline static void writeResourceID(uint8_t* const tripleData, const ResourceComponent component, const ResourceID resourceID) {
    *reinterpret_cast<uint32_t*>(tripleData + SEQUENTIAL_RESOURCE_ID_SIZE * component) = static_cast<uint32_t>(resourceID);
}

always_inline static TupleIndex readNext(const uint8_t* const tripleData, const ResourceComponent component) {
    const uint8_t* const nextData = tripleData + SEQUENTIAL_RESOURCE_ID_SIZE * 3 + SEQUENTIAL_TUPLE_INDEX_STORAGE_SIZE * component;
    return ::read6(nextData);
}

always_inline static void writeNext(uint8_t* const tripleData, const ResourceComponent component, const TupleIndex nextTripleIndex) {
    uint8_t* const nextData = tripleData + SEQUENTIAL_RESOURCE_ID_SIZE * 3 + SEQUENTIAL_TUPLE_INDEX_STORAGE_SIZE * component;
    ::write6(nextData, nextTripleIndex);
}

always_inline TupleStatus SequentialTripleList::getTripleStatus(const TupleIndex tupleIndex) const {
    return tupleIndex < m_firstFreeTripleIndex ? *reinterpret_cast<const TupleStatus*>(m_data + tupleIndex * TRIPLE_SIZE + 3 * SEQUENTIAL_RESOURCE_ID_SIZE + 3 * SEQUENTIAL_TUPLE_INDEX_STORAGE_SIZE) : TUPLE_STATUS_INVALID;
}

always_inline TupleStatus SequentialTripleList::setTripleStatus(const TupleIndex tupleIndex, const TupleStatus tupleStatus) {
    TupleStatus& listTupleStatus = *reinterpret_cast<TupleStatus*>(m_data + tupleIndex * TRIPLE_SIZE + 3 * SEQUENTIAL_RESOURCE_ID_SIZE + 3 * SEQUENTIAL_TUPLE_INDEX_STORAGE_SIZE);
    const TupleStatus result = listTupleStatus;
    listTupleStatus = tupleStatus;
    return result;
}

always_inline bool SequentialTripleList::setTripleStatusConditional(const TupleIndex tupleIndex, const TupleStatus expectedTupleStatus, const TupleStatus newTupleStatus) {
    TupleStatus& listTupleStatus = *reinterpret_cast<TupleStatus*>(m_data + tupleIndex * TRIPLE_SIZE + 3 * SEQUENTIAL_RESOURCE_ID_SIZE + 3 * SEQUENTIAL_TUPLE_INDEX_STORAGE_SIZE);
    assert(listTupleStatus == expectedTupleStatus);
    listTupleStatus = newTupleStatus;
    return true;
}

always_inline void SequentialTripleList::getTriple(const TupleIndex tupleIndex, TupleStatus& tupleStatus, ResourceID& s, ResourceID& p, ResourceID& o, TupleIndex& nextS, TupleIndex& nextP, TupleIndex& nextO) const {
    uint8_t* tripleData = m_data + tupleIndex * TRIPLE_SIZE;
    s = readResourceID(tripleData, RC_S);
    p = readResourceID(tripleData, RC_P);
    o = readResourceID(tripleData, RC_O);
    nextS = readNext(tripleData, RC_S);
    nextP = readNext(tripleData, RC_P);
    nextO = readNext(tripleData, RC_O);
    tupleStatus = *reinterpret_cast<const TupleStatus*>(tripleData + 3 * SEQUENTIAL_RESOURCE_ID_SIZE + 3 * SEQUENTIAL_TUPLE_INDEX_STORAGE_SIZE);
}

always_inline void SequentialTripleList::getTripleStatusAndResourceIDs(const TupleIndex tupleIndex, TupleStatus& tupleStatus, ResourceID& s, ResourceID& p, ResourceID& o) const {
    const uint8_t* tripleData = m_data + tupleIndex * TRIPLE_SIZE;
    s = readResourceID(tripleData, RC_S);
    p = readResourceID(tripleData, RC_P);
    o = readResourceID(tripleData, RC_O);
    tupleStatus = *reinterpret_cast<const TupleStatus*>(tripleData + 3 * SEQUENTIAL_RESOURCE_ID_SIZE + 3 * SEQUENTIAL_TUPLE_INDEX_STORAGE_SIZE);
}

always_inline void SequentialTripleList::getResourceIDs(const TupleIndex tupleIndex, ResourceID& s, ResourceID& p, ResourceID& o) const {
    const uint8_t* tripleData = m_data + tupleIndex * TRIPLE_SIZE;
    s = readResourceID(tripleData, RC_S);
    p = readResourceID(tripleData, RC_P);
    o = readResourceID(tripleData, RC_O);
}

always_inline void SequentialTripleList::getResourceIDs(const TupleIndex tupleIndex, const ResourceComponent component1, const ResourceComponent component2, ResourceID& value1, ResourceID& value2) const {
    const uint8_t* tripleData = m_data + tupleIndex * TRIPLE_SIZE;
    value1 = readResourceID(tripleData, component1);
    value2 = readResourceID(tripleData, component2);
}

always_inline ResourceID SequentialTripleList::getResourceID(const TupleIndex tupleIndex, const ResourceComponent component) const {
    return readResourceID(m_data + tupleIndex * TRIPLE_SIZE, component);
}

always_inline ResourceID SequentialTripleList::getS(const TupleIndex tupleIndex) const {
    return getResourceID(tupleIndex, RC_S);
}

always_inline ResourceID SequentialTripleList::getP(const TupleIndex tupleIndex) const {
    return getResourceID(tupleIndex, RC_P);
}

always_inline ResourceID SequentialTripleList::getO(const TupleIndex tupleIndex) const {
    return getResourceID(tupleIndex, RC_O);
}

always_inline TupleIndex SequentialTripleList::getNext(const TupleIndex tupleIndex, const ResourceComponent component) const {
    return readNext(m_data + tupleIndex * TRIPLE_SIZE, component);
}

always_inline void SequentialTripleList::setNext(const TupleIndex tupleIndex, const ResourceComponent component, const TupleIndex nextTripleIndex) {
    writeNext(m_data + tupleIndex * TRIPLE_SIZE, component, nextTripleIndex);
}

always_inline bool SequentialTripleList::setNextConditional(const TupleIndex tupleIndex, const ResourceComponent component, const TupleIndex testTripleIndex, const TupleIndex nextTripleIndex) {
    assert(getNext(tupleIndex, component) == testTripleIndex);
    writeNext(m_data + tupleIndex * TRIPLE_SIZE, component, nextTripleIndex);
    return true;
}

always_inline TupleIndex SequentialTripleList::getFirstWriteTripleIndex() const {
    return 1;
}

always_inline TupleIndex SequentialTripleList::getFirstTripleIndex() const {
    return getNextTripleIndex(0);
}

always_inline TupleIndex SequentialTripleList::getFirstFreeTripleIndex() const {
    return m_firstFreeTripleIndex;
}

always_inline TupleIndex SequentialTripleList::getNextTripleIndex(const TupleIndex tupleIndex) const {
    TupleIndex resultIndex = tupleIndex + 1;
    while (resultIndex < m_firstFreeTripleIndex) {
        if ((getTripleStatus(resultIndex) & TUPLE_STATUS_COMPLETE) == TUPLE_STATUS_COMPLETE && getS(resultIndex) != INVALID_RESOURCE_ID)
            return resultIndex;
        ++resultIndex;
    }
    return INVALID_TUPLE_INDEX;
}

always_inline TupleIndex SequentialTripleList::add(const ResourceID s, const ResourceID p, const ResourceID o) {
    if (!m_data.ensureEndAtLeast(m_firstFreeTripleIndex * TRIPLE_SIZE, TRIPLE_SIZE))
        return INVALID_TUPLE_INDEX;
    TupleIndex tupleIndex = m_firstFreeTripleIndex++;
    uint8_t* tripleData = m_data + tupleIndex * TRIPLE_SIZE;
    writeResourceID(tripleData, RC_S, s);
    writeResourceID(tripleData, RC_P, p);
    writeResourceID(tripleData, RC_O, o);
    return tupleIndex;
}

always_inline bool SequentialTripleList::supportsWindowedAdds() const {
    return false;
}

always_inline TupleIndex SequentialTripleList::reserveAddWindow(const size_t windowSize) {
    throw RDF_STORE_EXCEPTION("SequentialTripleList does not support windowed additions.");
}

always_inline void SequentialTripleList::addAt(const TupleIndex tupleIndex, const ResourceID s, const ResourceID p, const ResourceID o) {
    uint8_t* tripleData = m_data + tupleIndex * TRIPLE_SIZE;
    writeResourceID(tripleData, RC_S, s);
    writeResourceID(tripleData, RC_P, p);
    writeResourceID(tripleData, RC_O, o);
}

always_inline void SequentialTripleList::truncate(const TupleIndex newFirstFreeTripleIndex) {
    for (TupleIndex tupleIndex = newFirstFreeTripleIndex; tupleIndex < m_firstFreeTripleIndex; ++tupleIndex) {
        setTripleStatus(tupleIndex, TUPLE_STATUS_INVALID);
        addAt(tupleIndex, INVALID_RESOURCE_ID, INVALID_RESOURCE_ID, INVALID_RESOURCE_ID);
        setNext(tupleIndex, RC_S, INVALID_TUPLE_INDEX);
        setNext(tupleIndex, RC_P, INVALID_TUPLE_INDEX);
        setNext(tupleIndex, RC_O, INVALID_TUPLE_INDEX);
    }
    m_firstFreeTripleIndex = newFirstFreeTripleIndex;
}

always_inline size_t SequentialTripleList::getExactTripleCount(const TupleStatus tupleStatusMask, const TupleStatus tupleStatusExpectedValue) const {
    size_t tripleCount = 0;
    TupleIndex tupleIndex = getFirstTripleIndex();
    while (tupleIndex != INVALID_TUPLE_INDEX) {
        if ((getTripleStatus(tupleIndex) & tupleStatusMask) == tupleStatusExpectedValue)
            ++tripleCount;
        tupleIndex = getNextTripleIndex(tupleIndex);
    }
    return tripleCount;
}

always_inline size_t SequentialTripleList::getApproximateTripleCount() const {
    return m_firstFreeTripleIndex - 1;
}

always_inline void SequentialTripleList::save(OutputStream& outputStream) const {
    outputStream.writeString("SequentialTripleList");
    outputStream.writeMemoryRegion(m_data);
    outputStream.write(m_firstFreeTripleIndex);
}

always_inline void SequentialTripleList::load(InputStream& inputStream) {
    if (!inputStream.checkNextString("SequentialTripleList"))
        throw RDF_STORE_EXCEPTION("Invalid input file: cannot load SequentialTripleList.");
    inputStream.readMemoryRegion(m_data);
    m_firstFreeTripleIndex = inputStream.read<TupleIndex>();
}

always_inline std::unique_ptr<ComponentStatistics> SequentialTripleList::getComponentStatistics() const {
    std::unique_ptr<ComponentStatistics> result(new ComponentStatistics("SequentialTripleList"));
    const size_t exactTripleCount = getExactTripleCount(TUPLE_STATUS_COMPLETE, TUPLE_STATUS_COMPLETE);
    const size_t size = m_firstFreeTripleIndex * TRIPLE_SIZE;
    result->addIntegerItem("Size", size);
    result->addIntegerItem("Approximate triple count", getApproximateTripleCount());
    result->addIntegerItem("Exact triple count", exactTripleCount);
    if (exactTripleCount != 0)
        result->addIntegerItem("Bytes per triple", size / exactTripleCount);
    return result;
}

#endif /* SEQUENTIALTRIPLELISTIMPL_H_ */
