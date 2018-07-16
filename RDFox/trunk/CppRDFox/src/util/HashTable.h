// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef HASHTABLE_H_
#define HASHTABLE_H_

#include "../Common.h"
#include "MemoryRegion.h"

class MemoryManager;
class InputStream;
class OutputStream;

template<class Derived>
class HashTable : private Unmovable {

protected:

    MemoryRegion<uint8_t> m_buckets;
    uint8_t* m_afterLastBucket;
    size_t m_numberOfBuckets;
    size_t m_numberOfUsedBuckets;
    size_t m_resizeThreshold;

    HashTable(MemoryManager& memoryManager);

    bool initialize(const size_t initialNumberOfBuckets);

    template<typename V>
    uint8_t* getBucketFor(size_t& valueHashCode, const V value) const;

    template<typename V1, typename V2>
    uint8_t* getBucketFor(size_t& valuesHashCode, const V1 value1, const V2 value2) const;

    template<typename V1, typename V2, typename V3>
    uint8_t* getBucketFor(size_t& valuesHashCode, const V1 value1, const V2 value2, const V3 value3) const;

    template<typename V1, typename V2, typename V3, typename V4>
    uint8_t* getBucketFor(size_t& valuesHashCode, const V1 value1, const V2 value2, const V3 value3, const V4 value4) const;

    template<typename V>
    uint8_t* getBucketFor(uint8_t* buckets, const size_t numberOfBuckets, const uint8_t* const afterLast, size_t& valueHashCode, const V value) const;

    template<typename V1, typename V2>
    uint8_t* getBucketFor(uint8_t* buckets, const size_t numberOfBuckets, const uint8_t* const afterLast, size_t& valuesHashCode, const V1 value1, const V2 value2) const;

    template<typename V1, typename V2, typename V3>
    uint8_t* getBucketFor(uint8_t* buckets, const size_t numberOfBuckets, const uint8_t* const afterLast, size_t& valuesHashCode, const V1 value1, const V2 value2, const V3 value3) const;

    template<typename V1, typename V2, typename V3, typename V4>
    uint8_t* getBucketFor(uint8_t* buckets, const size_t numberOfBuckets, const uint8_t* const afterLast, size_t& valuesHashCode, const V1 value1, const V2 value2, const V3 value3, const V4 value4) const;

    template<typename V>
    uint8_t* getBucketFor(uint8_t* buckets, uint8_t* initialBucket, const uint8_t* const afterLast, size_t& valueHashCode, const V value) const;

    void remove(uint8_t* bucket);

    bool canInsertBucket() const;

    std::unique_ptr<MemoryRegion<uint8_t> > resizeIfNeededInternal(bool& successful);

    bool resizeIfNeeded();

    void save(OutputStream& outputStream) const;

    void load(InputStream& inputStream);

};

#endif /* HASHTABLE_H_ */
