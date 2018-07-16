// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef CONCURRENTONEKEYINDEXIMPL_H_
#define CONCURRENTONEKEYINDEXIMPL_H_

#include "../../RDFStoreException.h"
#include "../../util/ComponentStatistics.h"
#include "../../util/InputStream.h"
#include "../../util/OutputStream.h"
#include "ConcurrentOneKeyIndex.h"

template<typename TI>
always_inline ConcurrentOneKeyIndex<TI>::ConcurrentOneKeyIndex(MemoryManager& memoryManager) : m_tripleIndexes(memoryManager, 3), m_counts(memoryManager, 3) {
}

template<typename TI>
always_inline MemoryManager& ConcurrentOneKeyIndex<TI>::getMemoryManager() const {
    return m_tripleIndexes.getMemoryManager();
}

template<typename TI>
always_inline bool ConcurrentOneKeyIndex<TI>::initialize() {
    return m_tripleIndexes.initializeLarge() && clearCounts();
}

template<typename TI>
always_inline bool ConcurrentOneKeyIndex<TI>::clearCounts() {
    return m_counts.initializeLarge() && m_counts.ensureEndAtLeast(m_tripleIndexes.getEndIndex(), 0);
}

template<typename TI>
always_inline bool ConcurrentOneKeyIndex<TI>::extendToResourceID(const ResourceID resourceID) {
    return m_tripleIndexes.ensureEndAtLeast(resourceID, 1) && m_counts.ensureEndAtLeast(resourceID, 1);
}

template<typename TI>
always_inline TupleIndex ConcurrentOneKeyIndex<TI>::getHeadTripleIndex(const ResourceID resourceID) const {
    if (m_tripleIndexes.isBeforeEnd(resourceID, 1))
        return static_cast<TupleIndex>(::atomicRead(m_tripleIndexes[resourceID]));
    else
        return INVALID_TUPLE_INDEX;
}

template<typename TI>
always_inline void ConcurrentOneKeyIndex<TI>::setHeadTripleIndex(const ResourceID resourceID, const TupleIndex tupleIndex) {
   ::atomicWrite(m_tripleIndexes[resourceID], static_cast<StoreTripleIndexType>(tupleIndex));
}

template<typename TI>
always_inline bool ConcurrentOneKeyIndex<TI>::setHeadTripleIndexConditional(const ResourceID resourceID, const TupleIndex testTripleIndex, const TupleIndex tupleIndex) {
   return ::atomicConditionalSet(m_tripleIndexes[resourceID], static_cast<StoreTripleIndexType>(testTripleIndex), static_cast<StoreTripleIndexType>(tupleIndex));
}

template<typename TI>
always_inline size_t ConcurrentOneKeyIndex<TI>::getTripleCount(const ResourceID resourceID) const {
    if (m_counts.isBeforeEnd(resourceID, 1))
        return ::atomicRead(m_counts[resourceID]);
    else
        return 0;
}

template<typename TI>
always_inline size_t ConcurrentOneKeyIndex<TI>::incrementTripleCount(const ResourceID resourceID, const uint32_t amount) {
    return ::atomicAdd(m_counts[resourceID], amount);
}

template<typename TI>
always_inline size_t ConcurrentOneKeyIndex<TI>::decrementTripleCount(const ResourceID resourceID, const uint32_t amount) {
    return ::atomicSubtract(m_counts[resourceID], amount);
}

template<typename TI>
always_inline void ConcurrentOneKeyIndex<TI>::save(OutputStream& outputStream) const {
    outputStream.writeString("ConcurrentOneKeyIndex");
    outputStream.writeMemoryRegion(m_tripleIndexes);
    outputStream.writeMemoryRegion(m_counts);
}

template<typename TI>
always_inline void ConcurrentOneKeyIndex<TI>::load(InputStream& inputStream) {
    if (!inputStream.checkNextString("ConcurrentOneKeyIndex"))
        throw RDF_STORE_EXCEPTION("Invalid input file: cannot load ConcurrentOneKeyIndex.");
    inputStream.readMemoryRegion(m_tripleIndexes);
    inputStream.readMemoryRegion(m_counts);
}

template<typename TI>
always_inline void ConcurrentOneKeyIndex<TI>::printContents(std::ostream& output) const {
    output << "ConcurrentOneKeyIndex" << std::endl;
    const size_t firstFreeResourceID = m_counts.getEndIndex();
    output << "First free resource ID: " << firstFreeResourceID << std::endl;
    output << "-- START OF RESOURCE ID COUNTS -------------------------------------------------" << std::endl;
    const size_t resourceIDNumberOfDigits = ::getNumberOfDigits(firstFreeResourceID);
    for (ResourceID resourceID = INVALID_RESOURCE_ID; resourceID < firstFreeResourceID; ++resourceID) {
        const std::streamsize oldWidth = output.width();
        const std::ostream::fmtflags oldFlags = output.flags();
        output.width(resourceIDNumberOfDigits);
        output.setf(std::ios::right);
        output << resourceID;
        output.width(oldWidth);
        output.flags(oldFlags);
        output << ": " << getTripleCount(resourceID) << std::endl;
    }
    output << "-- END OF RESOURCE ID COUNTS ---------------------------------------------------" << std::endl;
}

template<typename TI>
always_inline std::unique_ptr<ComponentStatistics> ConcurrentOneKeyIndex<TI>::getComponentStatistics() const {
    uint64_t numberOfUsedBuckets = 0;
    uint64_t sumOfAllTripleCounts = 0;
    uint64_t maxTripleCount = 0;
    const size_t endIndex = m_tripleIndexes.getEndIndex();
    for (ResourceID resourceID = 0; resourceID < endIndex; resourceID++)
        if (getHeadTripleIndex(resourceID) != INVALID_TUPLE_INDEX) {
            numberOfUsedBuckets++;
            size_t tripleCount = getTripleCount(resourceID);
            sumOfAllTripleCounts += tripleCount;
            if (tripleCount >= maxTripleCount)
                maxTripleCount = tripleCount;
        }
    std::unique_ptr<ComponentStatistics> result(new ComponentStatistics("ConcurrentOneKeyIndex"));
    result->addIntegerItem("Size", endIndex * (sizeof(StoreTripleIndexType) + sizeof(uint32_t)));
    result->addIntegerItem("Total number of entries", endIndex);
    result->addIntegerItem("Number of used entries", numberOfUsedBuckets);
    result->addIntegerItem("The sum of all triple counts", sumOfAllTripleCounts);
    result->addIntegerItem("Maximum triple count", maxTripleCount);
    if (numberOfUsedBuckets != 0)
        result->addIntegerItem("Average triple count", static_cast<uint64_t>(sumOfAllTripleCounts / static_cast<double>(numberOfUsedBuckets) + 0.5));
    return result;
}

#endif /* CONCURRENTONEKEYINDEXIMPL_H_ */
