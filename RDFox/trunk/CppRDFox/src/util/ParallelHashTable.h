// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef PARALLELHASHTABLE_H_
#define PARALLELHASHTABLE_H_

#include "SequentialHashTable.h"
#include "ThreadContext.h"

const double PARALLEL_HASH_TABLE_WINDOW_FACTOR = 0.1;

template<class Policy>
class ParallelHashTable : public SequentialHashTable<Policy> {

protected:

    static const size_t RESIZE_CHUNK_SIZE = 1024;

    ObjectDescriptor m_objectDescriptor;
    size_t m_numberOfThreads;
    size_t m_counterWindowPerThread;
    size_t m_resizeThreasholdMinusSafety;
    MemoryRegion<uint8_t> m_oldBuckets;
    size_t m_numberOfResizeChunks;
    aligned_size_t m_nextAvailableResizeChunk;
    aligned_size_t m_numberOfResizeChunksToFinish;

    size_t& getLocalInsertionCount(const size_t localThreadIndex) const;

    size_t& getLocalInsertionCount(ThreadContext& threadContext) const;

    void initializeCounterWindow();

    bool resizeNeeded(ThreadContext& threadContext) const;

    void handleResizeNeeded();

    void doResize();

    void startResize(ThreadContext& threadContext);

public:

    struct BucketDescriptor : public SequentialHashTable<Policy>::BucketDescriptor {
    };

    template<typename... Args>
    ParallelHashTable(MemoryManager& memoryManager, const double loadFactor, Args&&... policyArguments);

    bool initialize(const size_t initialNumberOfBuckets);

    void setNumberOfThreads(const size_t numberOfThreads);

    void acknowledgeInsert(ThreadContext& threadContext, const BucketDescriptor& bucketDescriptor);

    size_t getNumberOfUsedBuckets() const;

    template<typename... Args>
    void acquireBucket(ThreadContext& threadContext, BucketDescriptor& bucketDescriptor, Args&&... args);

    void acquireBucketNoHash(ThreadContext& threadContext, BucketDescriptor& bucketDescriptor);

    template<typename... Args>
    BucketStatus continueBucketSearch(ThreadContext& threadContext, BucketDescriptor& bucketDescriptor, Args&&... args);

    void releaseBucket(ThreadContext& threadContext, const BucketDescriptor& bucketDescriptor);

    void save(OutputStream& outputStream) const;

    void load(InputStream& inputStream);

    __ALIGNED(ParallelHashTable<Policy>)

};

#endif /* PARALLELHASHTABLE_H_ */
