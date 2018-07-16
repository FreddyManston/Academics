// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef SEQUENTIALHASHTABLEIMPL_H_
#define SEQUENTIALHASHTABLEIMPL_H_

#include "../RDFStoreException.h"
#include "InputStream.h"
#include "OutputStream.h"
#include "SequentialHashTable.h"

// SequentialHashTable::PolicyBuckets

template<class Policy>
template<typename... Args>
always_inline SequentialHashTable<Policy>::PolicyBuckets::PolicyBuckets(MemoryManager& memoryManager, Args&&... policyArguments) : Policy(std::forward<Args>(policyArguments)...), m_buckets(memoryManager) {
}

// SequentialHashTable

template<class Policy>
template<typename... Args>
always_inline SequentialHashTable<Policy>::SequentialHashTable(MemoryManager& memoryManager, const double loadFactor, Args&&... policyArguments) :
    m_policyBuckets(memoryManager, std::forward<Args>(policyArguments)...),
    m_numberOfBuckets(0),
    m_numberOfBucketsMinusOne(m_numberOfBuckets - 1),
    m_numberOfUsedBuckets(0),
    m_loadFactor(loadFactor),
    m_resizeThreshold(0),
    m_resizeFailed(false)
{
}

template<class Policy>
always_inline typename SequentialHashTable<Policy>::PolicyType& SequentialHashTable<Policy>::getPolicy() {
    return m_policyBuckets;
}

template<class Policy>
always_inline const typename SequentialHashTable<Policy>::PolicyType& SequentialHashTable<Policy>::getPolicy() const {
    return m_policyBuckets;
}

template<class Policy>
always_inline bool SequentialHashTable<Policy>::initialize(const size_t initialNumberOfBuckets) {
    // Make sure that initialNumberOfBuckets is a power of two
    assert((static_cast<size_t>(1) << ::getMostSignificantBitIndex64(initialNumberOfBuckets)) == initialNumberOfBuckets);
    m_numberOfBuckets = initialNumberOfBuckets;
    m_numberOfBucketsMinusOne = m_numberOfBuckets - 1;
    m_numberOfUsedBuckets = 0;
    m_resizeThreshold = static_cast<size_t>(m_numberOfBuckets * m_loadFactor);
    if (!m_policyBuckets.m_buckets.initialize(m_numberOfBuckets * m_policyBuckets.BUCKET_SIZE) || !m_policyBuckets.m_buckets.ensureEndAtLeast(m_numberOfBuckets * m_policyBuckets.BUCKET_SIZE, 0))
        return false;
    m_afterLastBucket = m_policyBuckets.m_buckets + m_numberOfBuckets * m_policyBuckets.BUCKET_SIZE;
    return true;
}

template<class Policy>
always_inline bool SequentialHashTable<Policy>::isInitialized() const {
    return m_policyBuckets.m_buckets.isInitialized();
}

template<class Policy>
always_inline const uint8_t* SequentialHashTable<Policy>::getFirstBucket() const {
    return m_policyBuckets.m_buckets;
}

template<class Policy>
always_inline const uint8_t* SequentialHashTable<Policy>::getAfterLastBucket() const {
    return m_afterLastBucket;
}

template<class Policy>
always_inline void SequentialHashTable<Policy>::setNumberOfThreads(const size_t numberOfThreads) {
}

template<class Policy>
always_inline bool SequentialHashTable<Policy>::resizeNeeded(ThreadContext& threadContext) const {
    return m_numberOfUsedBuckets > m_resizeThreshold;
}

template<class Policy>
void SequentialHashTable<Policy>::handleResizeNeeded() {
    if (!m_resizeFailed) {
        size_t newNumberOfBuckets = 2 * m_numberOfBuckets;
        std::unique_ptr<MemoryRegion<uint8_t> > newBuckets(new MemoryRegion<uint8_t>(m_policyBuckets.m_buckets.getMemoryManager()));
        if (newBuckets->initialize(newNumberOfBuckets * m_policyBuckets.BUCKET_SIZE) && newBuckets->ensureEndAtLeast(newNumberOfBuckets * m_policyBuckets.BUCKET_SIZE, 0)) {
            uint8_t* afterLastNewBuckets = newBuckets->getData() + newNumberOfBuckets * m_policyBuckets.BUCKET_SIZE;
            const size_t newNumberOfBucketsMinusOne = newNumberOfBuckets - 1;
            const uint8_t* oldBucket = m_policyBuckets.m_buckets;
            BucketContents oldBucketContents;
            for (size_t oldBucketIndex = 0; oldBucketIndex < m_numberOfBuckets; oldBucketIndex++, oldBucket += m_policyBuckets.BUCKET_SIZE) {
                m_policyBuckets.getBucketContents(oldBucket, oldBucketContents);
                if (!m_policyBuckets.isBucketContentsEmpty(oldBucketContents)) {
                    uint8_t* newBucket = newBuckets->getData() + (m_policyBuckets.getBucketContentsHashCode(oldBucketContents) & newNumberOfBucketsMinusOne) * m_policyBuckets.BUCKET_SIZE;
                    while (!this->m_policyBuckets.setBucketContentsIfEmpty(newBucket, oldBucketContents)) {
                        newBucket += m_policyBuckets.BUCKET_SIZE;
                        if (newBucket == afterLastNewBuckets)
                            newBucket = newBuckets->getData();
                    }
                }
            }
            m_policyBuckets.m_buckets.swap(*newBuckets);
            m_numberOfBuckets = newNumberOfBuckets;
            m_numberOfBucketsMinusOne = newNumberOfBucketsMinusOne;
            m_afterLastBucket = afterLastNewBuckets;
            m_resizeThreshold = static_cast<size_t>(m_numberOfBuckets * m_loadFactor);
        }
        else
            m_resizeFailed = true;
    }
}

template<class Policy>
template<typename... Args>
always_inline void SequentialHashTable<Policy>::acquireBucket(ThreadContext& threadContext, BucketDescriptor& bucketDescriptor, Args&&... args) {
    bucketDescriptor.m_hashCode = m_policyBuckets.hashCodeFor(std::forward<Args>(args)...);
    acquireBucketNoHash(threadContext, bucketDescriptor);
}

template<class Policy>
always_inline void SequentialHashTable<Policy>::acquireBucketNoHash(ThreadContext& threadContext, BucketDescriptor& bucketDescriptor) {
    if (resizeNeeded(threadContext))
        handleResizeNeeded();
    bucketDescriptor.m_bucket = nullptr;
}

template<class Policy>
template<typename... Args>
always_inline BucketStatus SequentialHashTable<Policy>::continueBucketSearch(ThreadContext& threadContext, BucketDescriptor& bucketDescriptor, Args&&... args) {
    if (!bucketDescriptor.m_bucket)
        bucketDescriptor.m_bucket = m_policyBuckets.m_buckets.getData() + (bucketDescriptor.m_hashCode & m_numberOfBucketsMinusOne) * m_policyBuckets.BUCKET_SIZE;
    uint8_t* const startBucket = bucketDescriptor.m_bucket;
    uint8_t* const afterLastBucket = m_afterLastBucket;
    uint8_t* bucket = startBucket;
    BucketStatus bucketStatus;
    do {
        m_policyBuckets.getBucketContents(bucket, bucketDescriptor.m_bucketContents);
        bucketStatus = m_policyBuckets.getBucketContentsStatus(bucketDescriptor.m_bucketContents, bucketDescriptor.m_hashCode, std::forward<Args>(args)...);
        if (bucketStatus == BUCKET_NOT_CONTAINS) {
            bucket += m_policyBuckets.BUCKET_SIZE;
            if (bucket == afterLastBucket)
                bucket = m_policyBuckets.m_buckets.getData();
        }
        else
            break;
    } while (bucket != startBucket);
    bucketDescriptor.m_bucket = bucket;
    return bucketStatus;
}

template<class Policy>
always_inline void SequentialHashTable<Policy>::deleteBucket(ThreadContext& threadContext, const BucketDescriptor& bucketDescriptor) {
    uint8_t* bucket = bucketDescriptor.m_bucket;
    size_t bucketIndex = (bucket - m_policyBuckets.m_buckets) / m_policyBuckets.BUCKET_SIZE;
    m_policyBuckets.makeBucketEmpty(bucket);
    m_numberOfUsedBuckets--;
    size_t scanBucketIndex = (bucketIndex + 1) & m_numberOfBucketsMinusOne;
    uint8_t* scanBucket = m_policyBuckets.m_buckets + scanBucketIndex * m_policyBuckets.BUCKET_SIZE;
    while (true) {
        BucketContents scanBucketContents;
        m_policyBuckets.getBucketContents(scanBucket, scanBucketContents);
        if (m_policyBuckets.isBucketContentsEmpty(scanBucketContents))
            break;
        else {
            const size_t requiredBucketIndex = m_policyBuckets.getBucketContentsHashCode(scanBucketContents) & m_numberOfBucketsMinusOne;
            // The following tests whether not( requiredBucketIndex  lies cyclically in <bucketIndex,scanBucketIndex] ) holds
            if (bucketIndex <= scanBucketIndex ? requiredBucketIndex <= bucketIndex || scanBucketIndex < requiredBucketIndex : requiredBucketIndex <= bucketIndex && scanBucketIndex < requiredBucketIndex) {
                m_policyBuckets.setBucketContentsIfEmpty(bucket, scanBucketContents);
                m_policyBuckets.makeBucketEmpty(scanBucket);
                bucketIndex = scanBucketIndex;
                bucket = scanBucket;
            }
            scanBucketIndex++;
            scanBucket += m_policyBuckets.BUCKET_SIZE;
            if (scanBucket == m_afterLastBucket) {
                scanBucketIndex = 0;
                scanBucket = m_policyBuckets.m_buckets;
            }
        }
    }
}

template<class Policy>
always_inline void SequentialHashTable<Policy>::releaseBucket(ThreadContext& threadContext, const BucketDescriptor& bucketDescriptor) {
}

template<class Policy>
always_inline void SequentialHashTable<Policy>::acknowledgeInsert(ThreadContext& threadContext, const BucketDescriptor& bucketDescriptor) {
    ++m_numberOfUsedBuckets;
}

template<class Policy>
always_inline size_t SequentialHashTable<Policy>::getNumberOfUsedBuckets() const {
    return m_numberOfUsedBuckets;
}

template<class Policy>
always_inline size_t SequentialHashTable<Policy>::getNumberOfBuckets() const {
    return m_numberOfBuckets;
}

template<class Policy>
always_inline void SequentialHashTable<Policy>::save(OutputStream& outputStream) const {
    outputStream.writeString("SequentialHashTable");
    outputStream.write(m_numberOfBuckets);
    outputStream.write(m_numberOfUsedBuckets);
    outputStream.write(m_resizeThreshold);
    outputStream.write(m_resizeFailed);
    outputStream.writeMemoryRegion(m_policyBuckets.m_buckets);
}

template<class Policy>
always_inline void SequentialHashTable<Policy>::load(InputStream& inputStream) {
    if (!inputStream.checkNextString("SequentialHashTable"))
        throw RDF_STORE_EXCEPTION("Invalid input file: cannot load SequentialHashTable.");
    m_numberOfBuckets = inputStream.read<size_t>();
    m_numberOfBucketsMinusOne = m_numberOfBuckets - 1;
    m_numberOfUsedBuckets = inputStream.read<size_t>();
    m_resizeThreshold = inputStream.read<size_t>();
    m_resizeFailed = inputStream.read<bool>();
    inputStream.readMemoryRegion(m_policyBuckets.m_buckets);
    m_afterLastBucket = m_policyBuckets.m_buckets + m_numberOfBuckets * m_policyBuckets.BUCKET_SIZE;
}

#endif /* SEQUENTIALHASHTABLEIMPL_H_ */
