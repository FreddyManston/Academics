// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef SEQUENTIALHASHTABLE_H_
#define SEQUENTIALHASHTABLE_H_

#include "../Common.h"
#include "MemoryRegion.h"

class MemoryManager;
class InputStream;
class OutputStream;
class ThreadContext;

enum BucketStatus { BUCKET_EMPTY, BUCKET_NOT_CONTAINS, BUCKET_CONTAINS };

template<class Policy>
class SequentialHashTable : private Unmovable {

protected:

    struct PolicyBuckets : public Policy {
        MemoryRegion<uint8_t> m_buckets;

        template<typename... Args>
        PolicyBuckets(MemoryManager& memoryManager, Args&&... policyArguments);

    };

    PolicyBuckets m_policyBuckets;
    uint8_t* m_afterLastBucket;
    size_t m_numberOfBuckets;
    size_t m_numberOfBucketsMinusOne;
    size_t m_numberOfUsedBuckets;
    const double m_loadFactor;
    size_t m_resizeThreshold;
    bool m_resizeFailed;

    bool resizeNeeded(ThreadContext& threadContext) const;

    void handleResizeNeeded();

public:

    typedef Policy PolicyType;
    typedef typename Policy::BucketContents BucketContents;

    struct BucketDescriptor {
        size_t m_hashCode;
        uint8_t* m_bucket;
        BucketContents m_bucketContents;
    };

    template<typename... Args>
    SequentialHashTable(MemoryManager& memoryManager, const double loadFactor, Args&&... policyArguments);

    PolicyType& getPolicy();

    const PolicyType& getPolicy() const;

    bool initialize(const size_t initialNumberOfBuckets);

    bool isInitialized() const;

    const uint8_t* getFirstBucket() const;

    const uint8_t* getAfterLastBucket() const;

    void setNumberOfThreads(const size_t numberOfThreads);

    template<typename... Args>
    void acquireBucket(ThreadContext& threadContext, BucketDescriptor& bucketDescriptor, Args&&... args);

    void acquireBucketNoHash(ThreadContext& threadContext, BucketDescriptor& bucketDescriptor);

    template<typename... Args>
    BucketStatus continueBucketSearch(ThreadContext& threadContext, BucketDescriptor& bucketDescriptor, Args&&... args);

    void deleteBucket(ThreadContext& threadContext, const BucketDescriptor& bucketDescriptor);

    void releaseBucket(ThreadContext& threadContext, const BucketDescriptor& bucketDescriptor);

    void acknowledgeInsert(ThreadContext& threadContext, const BucketDescriptor& bucketDescriptor);

    size_t getNumberOfUsedBuckets() const;

    size_t getNumberOfBuckets() const;

    void save(OutputStream& outputStream) const;

    void load(InputStream& inputStream);

};

#endif /* SEQUENTIALHASHTABLE_H_ */
