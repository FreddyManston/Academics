// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef HASHTABLEIMPL_H_
#define HASHTABLEIMPL_H_

#include "../RDFStoreException.h"
#include "HashTable.h"
#include "InputStream.h"
#include "OutputStream.h"

template<class Derived>
HashTable<Derived>::HashTable(MemoryManager& memoryManager) :
    m_buckets(memoryManager),
    m_numberOfBuckets(0),
    m_numberOfUsedBuckets(0)
{
}

template<class Derived>
bool HashTable<Derived>::initialize(const size_t initialNumberOfBuckets) {
    // Make sure that initialNumberOfBuckets is a power of two
    assert((static_cast<size_t>(1) << ::getMostSignificantBitIndex64(initialNumberOfBuckets)) == initialNumberOfBuckets);
    m_numberOfBuckets = initialNumberOfBuckets;
    m_numberOfUsedBuckets = 0;
    m_resizeThreshold = static_cast<size_t>(m_numberOfBuckets * HASH_TABLE_LOAD_FACTOR);
    if (!m_buckets.initialize(m_numberOfBuckets * static_cast<Derived*>(this)->BUCKET_SIZE) || !m_buckets.ensureEndAtLeast(m_numberOfBuckets * static_cast<Derived*>(this)->BUCKET_SIZE, 0))
        return false;
    m_afterLastBucket = m_buckets + m_numberOfBuckets * static_cast<Derived*>(this)->BUCKET_SIZE;
    return true;
}

template<class Derived>
template<typename V>
always_inline uint8_t* HashTable<Derived>::getBucketFor(size_t& valueHashCode, const V value) const {
    return getBucketFor<V>(m_buckets, m_numberOfBuckets, m_afterLastBucket, valueHashCode, value);
}

template<class Derived>
template<typename V1, typename V2>
always_inline uint8_t* HashTable<Derived>::getBucketFor(size_t& valuesHashCode, const V1 value1, const V2 value2) const {
    return getBucketFor<V1, V2>(m_buckets, m_numberOfBuckets, m_afterLastBucket, valuesHashCode, value1, value2);
}

template<class Derived>
template<typename V1, typename V2, typename V3>
always_inline uint8_t* HashTable<Derived>::getBucketFor(size_t& valuesHashCode, const V1 value1, const V2 value2, const V3 value3) const {
    return getBucketFor<V1, V2, V3>(m_buckets, m_numberOfBuckets, m_afterLastBucket, valuesHashCode, value1, value2, value3);
}

template<class Derived>
template<typename V1, typename V2, typename V3, typename V4>
always_inline uint8_t* HashTable<Derived>::getBucketFor(size_t& valuesHashCode, const V1 value1, const V2 value2, const V3 value3, const V4 value4) const {
    return getBucketFor<V1, V2, V3, V4>(m_buckets, m_numberOfBuckets, m_afterLastBucket, valuesHashCode, value1, value2, value3, value4);
}

template<class Derived>
template<typename V>
always_inline uint8_t* HashTable<Derived>::getBucketFor(uint8_t* buckets, const size_t numberOfBuckets, const uint8_t* const afterLast, size_t& valueHashCode, const V value) const {
    valueHashCode = static_cast<const Derived*>(this)->hashCodeFor(value);
    uint8_t* bucket = buckets + (valueHashCode & (numberOfBuckets - 1)) * static_cast<const Derived*>(this)->BUCKET_SIZE;
    return getBucketFor(buckets, bucket, afterLast, valueHashCode, value);
}

template<class Derived>
template<typename V1, typename V2>
always_inline uint8_t* HashTable<Derived>::getBucketFor(uint8_t* buckets, const size_t numberOfBuckets, const uint8_t* const afterLast, size_t& valuesHashCode, const V1 value1, const V2 value2) const {
    valuesHashCode = static_cast<const Derived*>(this)->hashCodeFor(value1, value2);
    uint8_t* bucket = buckets + (valuesHashCode & (numberOfBuckets - 1)) * static_cast<const Derived*>(this)->BUCKET_SIZE;
    while (static_cast<const Derived*>(this)->isValidAndNotContains(bucket, valuesHashCode, value1, value2)) {
        bucket += static_cast<const Derived*>(this)->BUCKET_SIZE;
        if (bucket == afterLast)
            bucket = buckets;
    }
    return bucket;
}

template<class Derived>
template<typename V1, typename V2, typename V3>
always_inline uint8_t* HashTable<Derived>::getBucketFor(uint8_t* buckets, const size_t numberOfBuckets, const uint8_t* const afterLast, size_t& valuesHashCode, const V1 value1, const V2 value2, const V3 value3) const {
    valuesHashCode = static_cast<const Derived*>(this)->hashCodeFor(value1, value2, value3);
    uint8_t* bucket = buckets + (valuesHashCode & (numberOfBuckets - 1)) * static_cast<const Derived*>(this)->BUCKET_SIZE;
    while (static_cast<const Derived*>(this)->isValidAndNotContains(bucket, valuesHashCode, value1, value2, value3)) {
        bucket += static_cast<const Derived*>(this)->BUCKET_SIZE;
        if (bucket == afterLast)
            bucket = buckets;
    }
    return bucket;
}

template<class Derived>
template<typename V1, typename V2, typename V3, typename V4>
always_inline uint8_t* HashTable<Derived>::getBucketFor(uint8_t* buckets, const size_t numberOfBuckets, const uint8_t* const afterLast, size_t& valuesHashCode, const V1 value1, const V2 value2, const V3 value3, const V4 value4) const {
    valuesHashCode = static_cast<const Derived*>(this)->hashCodeFor(value1, value2, value3, value4);
    uint8_t* bucket = buckets + (valuesHashCode & (numberOfBuckets - 1)) * static_cast<const Derived*>(this)->BUCKET_SIZE;
    while (static_cast<const Derived*>(this)->isValidAndNotContains(bucket, valuesHashCode, value1, value2, value3, value4)) {
        bucket += static_cast<const Derived*>(this)->BUCKET_SIZE;
        if (bucket == afterLast)
            bucket = buckets;
    }
    return bucket;
}

template<class Derived>
template<typename V>
always_inline uint8_t* HashTable<Derived>::getBucketFor(uint8_t* buckets, uint8_t* initialBucket, const uint8_t* const afterLast, size_t& valueHashCode, const V value) const {
    uint8_t* bucket = initialBucket;
    while (static_cast<const Derived*>(this)->isValidAndNotContains(bucket, valueHashCode, value)) {
        bucket += static_cast<const Derived*>(this)->BUCKET_SIZE;
        if (bucket == afterLast)
            bucket = buckets;
    }
    return bucket;
}

template<class Derived>
always_inline void HashTable<Derived>::remove(uint8_t* bucket) {
    size_t bucketIndex = (bucket - m_buckets) / static_cast<Derived*>(this)->BUCKET_SIZE;
    assert(!static_cast<Derived*>(this)->isEmpty(bucket));
    static_cast<Derived*>(this)->invalidate(bucket);
    m_numberOfUsedBuckets--;
    size_t numberOfBucketsMinusOne = m_numberOfBuckets - 1;
    size_t scanBucketIndex = (bucketIndex + 1) & numberOfBucketsMinusOne;
    uint8_t* scanBucket = m_buckets + scanBucketIndex * static_cast<Derived*>(this)->BUCKET_SIZE;
    while (!static_cast<Derived*>(this)->isEmpty(scanBucket)) {
        size_t requiredBucketIndex = static_cast<Derived*>(this)->hashCode(scanBucket) & numberOfBucketsMinusOne;
        // The following tests whether not( requiredBucketIndex  lies cyclically in <bucketIndex,scanBucketIndex] ) holds
        if (bucketIndex <= scanBucketIndex ? requiredBucketIndex <= bucketIndex || scanBucketIndex < requiredBucketIndex : requiredBucketIndex <= bucketIndex && scanBucketIndex < requiredBucketIndex) {
            static_cast<Derived*>(this)->copy(scanBucket, bucket);
            static_cast<Derived*>(this)->invalidate(scanBucket);
            bucketIndex = scanBucketIndex;
            bucket = scanBucket;
        }
        scanBucketIndex++;
        scanBucket += static_cast<Derived*>(this)->BUCKET_SIZE;
        if (scanBucket == m_afterLastBucket) {
            scanBucketIndex = 0;
            scanBucket = m_buckets;
        }
    }
}

template<class Derived>
always_inline bool HashTable<Derived>::canInsertBucket() const {
    return m_numberOfUsedBuckets <= m_resizeThreshold;
}

template<class Derived>
always_inline std::unique_ptr<MemoryRegion<uint8_t> > HashTable<Derived>::resizeIfNeededInternal(bool& successful) {
    std::unique_ptr<MemoryRegion<uint8_t> > newBucketsPtr;
    if (m_numberOfUsedBuckets > m_resizeThreshold) {
        if (m_numberOfBuckets == 0x8000000000000000ULL) {
            successful = false;
            return newBucketsPtr;
        }
        size_t newNumberOfBuckets = 2 * m_numberOfBuckets;
        newBucketsPtr.reset(new MemoryRegion<uint8_t>(m_buckets.getMemoryManager()));
        MemoryRegion<uint8_t>& newBuckets = *newBucketsPtr.get();
        if (!newBuckets.initialize(newNumberOfBuckets * static_cast<Derived*>(this)->BUCKET_SIZE) || !newBuckets.ensureEndAtLeast(newNumberOfBuckets * static_cast<Derived*>(this)->BUCKET_SIZE, 0)) {
            successful = false;
            return newBucketsPtr;
        }
        uint8_t* afterLastNewBuckets = newBuckets + newNumberOfBuckets * static_cast<Derived*>(this)->BUCKET_SIZE;
        const size_t newNumberOfBucketsMinusOne = newNumberOfBuckets - 1;
        const uint8_t* oldBucket = m_buckets;
        for (size_t oldBucketIndex = 0; oldBucketIndex < m_numberOfBuckets; oldBucketIndex++, oldBucket += static_cast<Derived*>(this)->BUCKET_SIZE) {
            if (!static_cast<Derived*>(this)->isEmpty(oldBucket)) {
                uint8_t* newBucket = newBuckets + (static_cast<Derived*>(this)->hashCode(oldBucket) & newNumberOfBucketsMinusOne) * static_cast<Derived*>(this)->BUCKET_SIZE;
                while (!static_cast<Derived*>(this)->isEmpty(newBucket)) {
                    newBucket += static_cast<Derived*>(this)->BUCKET_SIZE;
                    if (newBucket == afterLastNewBuckets)
                        newBucket = newBuckets;
                }
                static_cast<Derived*>(this)->copy(oldBucket, newBucket);
            }
        }
        m_buckets.swap(newBuckets);
        m_numberOfBuckets = newNumberOfBuckets;
        m_afterLastBucket = afterLastNewBuckets;
        m_resizeThreshold = static_cast<size_t>(m_numberOfBuckets * HASH_TABLE_LOAD_FACTOR);
    }
    successful = true;
    return newBucketsPtr;
}

template<class Derived>
always_inline bool HashTable<Derived>::resizeIfNeeded() {
    bool successful;
    std::unique_ptr<MemoryRegion<uint8_t> > oldRegion = resizeIfNeededInternal(successful);
    return successful;
}

template<class Derived>
always_inline void HashTable<Derived>::save(OutputStream& outputStream) const {
    outputStream.writeString("HashTable");
    outputStream.write<size_t>(m_numberOfBuckets);
    outputStream.write<size_t>(m_numberOfUsedBuckets);
    outputStream.write<size_t>(m_resizeThreshold);
    outputStream.writeMemoryRegion(m_buckets);
}

template<class Derived>
always_inline void HashTable<Derived>::load(InputStream& inputStream) {
    if (!inputStream.checkNextString("HashTable"))
        throw RDF_STORE_EXCEPTION("Invalid input file: cannot load HashTable.");
    m_numberOfBuckets = inputStream.read<size_t>();
    m_numberOfUsedBuckets = inputStream.read<size_t>();
    m_resizeThreshold = inputStream.read<size_t>();
    MemoryRegion<uint8_t> newBuckets(m_buckets.getMemoryManager());
    if (!newBuckets.initialize(m_numberOfBuckets * static_cast<Derived*>(this)->BUCKET_SIZE) || !newBuckets.ensureEndAtLeast(m_numberOfBuckets * static_cast<Derived*>(this)->BUCKET_SIZE, 0))
        throw RDF_STORE_EXCEPTION("Out of memory while initializing the buckets for a hash table.");
    m_buckets.swap(newBuckets);
    m_afterLastBucket = m_buckets + m_numberOfBuckets * static_cast<Derived*>(this)->BUCKET_SIZE;
    inputStream.readMemoryRegion(m_buckets);
}

#endif /* HASHTABLEIMPL_H_ */
