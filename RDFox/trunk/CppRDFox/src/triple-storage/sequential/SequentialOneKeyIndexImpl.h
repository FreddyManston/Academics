// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef SEQUENTIALONEKEYINDEXIMPL_H_
#define SEQUENTIALONEKEYINDEXIMPL_H_

#include "../../RDFStoreException.h"
#include "../../util/ComponentStatistics.h"
#include "../../util/InputStream.h"
#include "../../util/OutputStream.h"
#include "SequentialOneKeyIndex.h"

static const size_t SEQUENTIAL_ONE_KEY_INDEX_BUCKET_SIZE = SEQUENTIAL_TUPLE_INDEX_STORAGE_SIZE + sizeof(uint32_t);

always_inline SequentialOneKeyIndex::SequentialOneKeyIndex(MemoryManager& memoryManager) : m_buckets(memoryManager) {
}

always_inline MemoryManager& SequentialOneKeyIndex::getMemoryManager() const {
    return m_buckets.getMemoryManager();
}

always_inline bool SequentialOneKeyIndex::initialize() {
    return m_buckets.initialize(0xffffffffULL * SEQUENTIAL_ONE_KEY_INDEX_BUCKET_SIZE) && m_buckets.ensureEndAtLeast(0, SEQUENTIAL_ONE_KEY_INDEX_BUCKET_SIZE);
}

always_inline bool SequentialOneKeyIndex::clearCounts() {
    uint8_t* currentBucket = m_buckets.getData();
    // The division and multiplication in the following is needed because m_buckets contains uint8_t
    // so the end index is not the multiple of SEQUENTIAL_ONE_KEY_INDEX_BUCKET_SIZE.
    const uint8_t* const afterLastBucket = currentBucket + (m_buckets.getEndIndex() / SEQUENTIAL_ONE_KEY_INDEX_BUCKET_SIZE) * SEQUENTIAL_ONE_KEY_INDEX_BUCKET_SIZE;
    while (currentBucket < afterLastBucket) {
        ::write4(currentBucket + SEQUENTIAL_TUPLE_INDEX_STORAGE_SIZE, 0);
        currentBucket += SEQUENTIAL_ONE_KEY_INDEX_BUCKET_SIZE;
    }
    return true;
}

always_inline bool SequentialOneKeyIndex::extendToResourceID(const ResourceID resourceID) {
    return m_buckets.ensureEndAtLeast(resourceID * SEQUENTIAL_ONE_KEY_INDEX_BUCKET_SIZE, SEQUENTIAL_ONE_KEY_INDEX_BUCKET_SIZE);
}

always_inline TupleIndex SequentialOneKeyIndex::getHeadTripleIndex(const ResourceID resourceID) const {
    const size_t bucketOffset = resourceID * SEQUENTIAL_ONE_KEY_INDEX_BUCKET_SIZE;
    if (m_buckets.isBeforeEnd(bucketOffset, SEQUENTIAL_ONE_KEY_INDEX_BUCKET_SIZE)) {
        const uint8_t* const bucketData = m_buckets + bucketOffset;
        return ::read6(bucketData);
    }
    else
        return INVALID_TUPLE_INDEX;
}

always_inline void SequentialOneKeyIndex::setHeadTripleIndex(const ResourceID resourceID, const TupleIndex tupleIndex) {
    uint8_t* const bucketData = m_buckets + resourceID * SEQUENTIAL_ONE_KEY_INDEX_BUCKET_SIZE;
    ::write6(bucketData, tupleIndex);
}

always_inline bool SequentialOneKeyIndex::setHeadTripleIndexConditional(const ResourceID resourceID, const TupleIndex testTripleIndex, const TupleIndex tupleIndex) {
    setHeadTripleIndex(resourceID, tupleIndex);
    return true;
}

always_inline size_t SequentialOneKeyIndex::getTripleCount(const ResourceID resourceID) const {
    const size_t bucketOffset = resourceID * SEQUENTIAL_ONE_KEY_INDEX_BUCKET_SIZE;
    if (m_buckets.isBeforeEnd(bucketOffset, SEQUENTIAL_ONE_KEY_INDEX_BUCKET_SIZE))
        return ::read4(m_buckets + bucketOffset + SEQUENTIAL_TUPLE_INDEX_STORAGE_SIZE);
    else
        return 0;
}

always_inline size_t SequentialOneKeyIndex::incrementTripleCount(const ResourceID resourceID, const uint32_t amount) {
    return ::increment4(m_buckets + resourceID * SEQUENTIAL_ONE_KEY_INDEX_BUCKET_SIZE + SEQUENTIAL_TUPLE_INDEX_STORAGE_SIZE, amount);
}

always_inline size_t SequentialOneKeyIndex::decrementTripleCount(const ResourceID resourceID, const uint32_t amount) {
    return ::decrement4(m_buckets + resourceID * SEQUENTIAL_ONE_KEY_INDEX_BUCKET_SIZE + SEQUENTIAL_TUPLE_INDEX_STORAGE_SIZE, amount);
}

always_inline void SequentialOneKeyIndex::save(OutputStream& outputStream) const {
    outputStream.writeString("SequentialOneKeyIndex");
    outputStream.writeMemoryRegion(m_buckets);
}

always_inline void SequentialOneKeyIndex::load(InputStream& inputStream) {
    if (!inputStream.checkNextString("SequentialOneKeyIndex"))
        throw RDF_STORE_EXCEPTION("Invalid input file: cannot load OneKeyIndex.");
    inputStream.readMemoryRegion(m_buckets);
}

always_inline void SequentialOneKeyIndex::printContents(std::ostream& output) const {
    output << "SequentialOneKeyIndex" << std::endl;
    const size_t firstFreeResourceID = m_buckets.getEndIndex() / SEQUENTIAL_ONE_KEY_INDEX_BUCKET_SIZE;
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

always_inline std::unique_ptr<ComponentStatistics> SequentialOneKeyIndex::getComponentStatistics() const {
    uint64_t numberOfUsedBuckets = 0;
    uint64_t sumOfAllTripleCounts = 0;
    uint64_t maxTripleCount = 0;
    const size_t numberOfAvailableItems = m_buckets.getEndIndex() / SEQUENTIAL_ONE_KEY_INDEX_BUCKET_SIZE;
    for (ResourceID resourceID = 0; resourceID < numberOfAvailableItems; resourceID++)
        if (getHeadTripleIndex(resourceID) != INVALID_TUPLE_INDEX) {
            numberOfUsedBuckets++;
            size_t tripleCount = getTripleCount(resourceID);
            sumOfAllTripleCounts += tripleCount;
            if (tripleCount >= maxTripleCount)
                maxTripleCount = tripleCount;
        }
    std::unique_ptr<ComponentStatistics> result(new ComponentStatistics("SequentialOneKeyIndex"));
    result->addIntegerItem("Size", numberOfAvailableItems * SEQUENTIAL_ONE_KEY_INDEX_BUCKET_SIZE);
    result->addIntegerItem("Total number of entries", numberOfAvailableItems);
    result->addIntegerItem("Number of used entries", numberOfUsedBuckets);
    result->addIntegerItem("The sum of all triple counts", sumOfAllTripleCounts);
    result->addIntegerItem("Maximum triple count", maxTripleCount);
    if (numberOfUsedBuckets != 0)
        result->addIntegerItem("Average triple count", static_cast<uint64_t>(sumOfAllTripleCounts / static_cast<double>(numberOfUsedBuckets) + 0.5));
    return result;
}

#endif /* SEQUENTIALONEKEYINDEXIMPL_H_ */
