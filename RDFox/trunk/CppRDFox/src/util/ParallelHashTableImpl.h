// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef PARALLELHASHTABLEIMPL_H_
#define PARALLELHASHTABLEIMPL_H_

#include "SequentialHashTableImpl.h"
#include "ParallelHashTable.h"

extern const size_t g_numberOfLogicalProecssors;

extern size_t* g_localInsertionCounts;

template<class Policy>
always_inline size_t& ParallelHashTable<Policy>::getLocalInsertionCount(const size_t localThreadIndex) const {
    return *(g_localInsertionCounts + localThreadIndex * (static_cast<size_t>(MAX_OBJECT_ID) + 1) + m_objectDescriptor.m_objectID);
}

template<class Policy>
always_inline size_t& ParallelHashTable<Policy>::getLocalInsertionCount(ThreadContext& threadContext) const {
    return getLocalInsertionCount(threadContext.getThreadContextID() % m_numberOfThreads);
}

template<class Policy>
always_inline void ParallelHashTable<Policy>::initializeCounterWindow() {
    ::atomicWrite(m_counterWindowPerThread, static_cast<size_t>(PARALLEL_HASH_TABLE_WINDOW_FACTOR * static_cast<double>(this->m_numberOfBuckets) / static_cast<double>(m_numberOfThreads)));
    ::atomicWrite(m_resizeThreasholdMinusSafety, this->m_resizeThreshold - m_counterWindowPerThread * (m_numberOfThreads - 1));
}

template<class Policy>
always_inline bool ParallelHashTable<Policy>::resizeNeeded(ThreadContext& threadContext) const {
    return ::atomicRead(this->m_numberOfUsedBuckets) + ::atomicRead(getLocalInsertionCount(threadContext)) > ::atomicRead(m_resizeThreasholdMinusSafety);
}

template<class Policy>
always_inline void ParallelHashTable<Policy>::handleResizeNeeded() {
    SequentialHashTable<Policy>::handleResizeNeeded();
    initializeCounterWindow();
}

template<class Policy>
void ParallelHashTable<Policy>::doResize() {
    const size_t numberOfResizeChunks = ::atomicRead(m_numberOfResizeChunks);
    // Take the next chunk and process it.
    uint8_t* const newBuckets = this->m_policyBuckets.m_buckets.getData();
    uint8_t* const afterLastBucket = this->m_afterLastBucket;
    size_t chunkToResize;
    size_t numberOfResizeChunksToFinish = static_cast<size_t>(-1);
    while ((chunkToResize = ::atomicIncrement(m_nextAvailableResizeChunk) - 1) < numberOfResizeChunks) {
        const uint8_t* oldBucket = m_oldBuckets.getData() + chunkToResize * RESIZE_CHUNK_SIZE * this->m_policyBuckets.BUCKET_SIZE;
        typename ParallelHashTable<Policy>::BucketContents oldBucketContents;
        for (size_t resizeIndex = 0; resizeIndex < RESIZE_CHUNK_SIZE; resizeIndex++, oldBucket += this->m_policyBuckets.BUCKET_SIZE) {
            this->m_policyBuckets.getBucketContents(oldBucket, oldBucketContents);
            if (!this->m_policyBuckets.isBucketContentsEmpty(oldBucketContents)) {
                uint8_t* newBucket = newBuckets + (this->m_policyBuckets.getBucketContentsHashCode(oldBucketContents) & this->m_numberOfBucketsMinusOne) * this->m_policyBuckets.BUCKET_SIZE;
                while (!this->m_policyBuckets.setBucketContentsIfEmpty(newBucket, oldBucketContents)) {
                    newBucket += this->m_policyBuckets.BUCKET_SIZE;
                    if (newBucket == afterLastBucket)
                        newBucket = newBuckets;
                }
            }
        }
        numberOfResizeChunksToFinish = ::atomicDecrement(m_numberOfResizeChunksToFinish);
    }
    // The thread that finishes off the last chunk declares victory, and all other threads wait for resizing to finish
    if (numberOfResizeChunksToFinish == 0) {
        m_oldBuckets.deinitialize();
        ::atomicWrite(m_numberOfResizeChunks, 0);
    }
    else {
        while (::atomicRead(m_numberOfResizeChunks) != 0) {
        }
    }
    // At this point resize cannot be in progress
    assert(::atomicRead(m_numberOfResizeChunks) == 0);
}

template<class Policy>
void ParallelHashTable<Policy>::startResize(ThreadContext& threadContext) {
    // At this point resize should not be in progress -- this should be ensured by the sequence of calls outside
    assert(::atomicRead(m_numberOfResizeChunks) == 0);
    if (threadContext.tryPromoteSharedLockToExclusive(this->m_objectDescriptor)) {
        // The following code is boilerplate; the only interesting point is that we immediately swap out the old and new arrays.
        // This is done so that we can get rid of the old buckets without too much fuss.
        const size_t newNumberOfBuckets = 2 * this->m_numberOfBuckets;
        const size_t newBucketsSize = newNumberOfBuckets * this->m_policyBuckets.BUCKET_SIZE;
        if (m_oldBuckets.initialize(newBucketsSize) && m_oldBuckets.ensureEndAtLeast(newBucketsSize, 0)) {
            m_numberOfResizeChunks = m_numberOfResizeChunksToFinish = this->m_numberOfBuckets / RESIZE_CHUNK_SIZE;
            m_nextAvailableResizeChunk = 0;
            m_oldBuckets.swap(this->m_policyBuckets.m_buckets);
            this->m_afterLastBucket = this->m_policyBuckets.m_buckets + newBucketsSize;
            this->m_numberOfBuckets = newNumberOfBuckets;
            this->m_numberOfBucketsMinusOne = newNumberOfBuckets - 1;
            this->m_resizeThreshold = static_cast<size_t>(newNumberOfBuckets * this->m_loadFactor);
            this->initializeCounterWindow();
        }
        else
            ::atomicWrite(this->m_resizeFailed, true);
        threadContext.downgradeExclusiveLockToShared(this->m_objectDescriptor);
    }
}

template<class Policy>
template<typename... Args>
always_inline ParallelHashTable<Policy>::ParallelHashTable(MemoryManager& memoryManager, const double loadFactor, Args&&... policyArguments) :
    SequentialHashTable<Policy>(memoryManager, loadFactor, std::forward<Args>(policyArguments)...),
    m_objectDescriptor(),
    m_numberOfThreads(1),
    m_counterWindowPerThread(0),
    m_resizeThreasholdMinusSafety(0),
    m_oldBuckets(memoryManager),
    m_numberOfResizeChunks(0),
    m_nextAvailableResizeChunk(0),
    m_numberOfResizeChunksToFinish(0)
{
}

template<class Policy>
always_inline bool ParallelHashTable<Policy>::initialize(const size_t initialNumberOfBuckets) {
    if (SequentialHashTable<Policy>::initialize(initialNumberOfBuckets)) {
        initializeCounterWindow();
        for (size_t localThreadIndex = 0; localThreadIndex < m_numberOfThreads; ++localThreadIndex)
            getLocalInsertionCount(localThreadIndex) = 0;
        m_oldBuckets.deinitialize();
        m_numberOfResizeChunks = 0;
        m_nextAvailableResizeChunk = 0;
        m_numberOfResizeChunksToFinish = 0;
        return true;
    }
    else
        return false;
}

template<class Policy>
always_inline void ParallelHashTable<Policy>::setNumberOfThreads(const size_t numberOfThreads) {
    for (size_t localThreadIndex = 0; localThreadIndex < m_numberOfThreads; ++localThreadIndex)
        this->m_numberOfUsedBuckets += getLocalInsertionCount(localThreadIndex);
    m_numberOfThreads = std::min(numberOfThreads, g_numberOfLogicalProecssors);
    for (size_t localThreadIndex = 0; localThreadIndex < m_numberOfThreads; ++localThreadIndex)
        getLocalInsertionCount(localThreadIndex) = 0;
    initializeCounterWindow();
}

template<class Policy>
always_inline void ParallelHashTable<Policy>::acknowledgeInsert(ThreadContext& threadContext, const BucketDescriptor& bucketDescriptor) {
    size_t& localInsertionCount = getLocalInsertionCount(threadContext);
    if (::atomicIncrement(localInsertionCount) >= m_counterWindowPerThread) {
        const size_t currentLocalCount = ::atomicExchange(localInsertionCount, 0);
        ::atomicAdd(this->m_numberOfUsedBuckets, currentLocalCount);
    }
}

template<class Policy>
always_inline size_t ParallelHashTable<Policy>::getNumberOfUsedBuckets() const {
    const size_t numberOfUsedBuckets = ::atomicRead(this->m_numberOfUsedBuckets);
    size_t localNumberOfBuckets = 0;
    for (size_t localThreadIndex = 0; localThreadIndex < m_numberOfThreads; ++localThreadIndex)
        localNumberOfBuckets += ::atomicRead(getLocalInsertionCount(localThreadIndex));
    return  numberOfUsedBuckets + localNumberOfBuckets;
}

template<class Policy>
template<typename... Args>
always_inline void ParallelHashTable<Policy>::acquireBucket(ThreadContext& threadContext, BucketDescriptor& bucketDescriptor, Args&&... args) {
    bucketDescriptor.m_hashCode = this->m_policyBuckets.hashCodeFor(std::forward<Args>(args)...);
    acquireBucketNoHash(threadContext, bucketDescriptor);
}

template<class Policy>
always_inline void ParallelHashTable<Policy>::acquireBucketNoHash(ThreadContext& threadContext, BucketDescriptor& bucketDescriptor) {
    bucketDescriptor.m_bucket = 0;
    threadContext.lockObjectShared(this->m_objectDescriptor);
}

template<class Policy>
template<typename... Args>
always_inline BucketStatus ParallelHashTable<Policy>::continueBucketSearch(ThreadContext& threadContext, BucketDescriptor& bucketDescriptor, Args&&... args) {
    BucketStatus bucketStatus;
    do {
        if (::atomicRead(m_numberOfResizeChunks) != 0)
            doResize();
        if (this->resizeNeeded(threadContext) && !::atomicRead(this->m_resizeFailed)) {
            bucketStatus = BUCKET_NOT_CONTAINS;
            bucketDescriptor.m_bucket = 0;
            // Give up if resizing failed at some point
            if (!::atomicRead(this->m_resizeFailed))
                startResize(threadContext);
        }
        else
            bucketStatus = SequentialHashTable<Policy>::continueBucketSearch(threadContext, bucketDescriptor, std::forward<Args>(args)...);
    } while (bucketStatus == BUCKET_NOT_CONTAINS);
    return bucketStatus;
}

template<class Policy>
always_inline void ParallelHashTable<Policy>::releaseBucket(ThreadContext& threadContext, const BucketDescriptor& bucketDescriptor) {
    threadContext.unlockObjectShared(this->m_objectDescriptor);
}

template<class Policy>
always_inline void ParallelHashTable<Policy>::save(OutputStream& outputStream) const {
    outputStream.writeString("ParallelHashTable");
    outputStream.write(this->m_numberOfBuckets);
    outputStream.write(this->m_resizeThreshold);
    outputStream.write(this->m_resizeFailed);
    outputStream.write(this->m_numberOfUsedBuckets);
    outputStream.write(this->m_numberOfThreads);
    for (size_t localThreadIndex = 0; localThreadIndex < this->m_numberOfThreads; ++localThreadIndex)
        outputStream.write(getLocalInsertionCount(localThreadIndex));
    outputStream.writeMemoryRegion(this->m_policyBuckets.m_buckets);
}

template<class Policy>
always_inline void ParallelHashTable<Policy>::load(InputStream& inputStream) {
    if (!inputStream.checkNextString("ParallelHashTable"))
        throw RDF_STORE_EXCEPTION("Invalid input file: cannot load ParallelHashTable.");
    this->m_numberOfBuckets = inputStream.read<size_t>();
    this->m_numberOfBucketsMinusOne = this->m_numberOfBuckets - 1;
    this->m_resizeThreshold = inputStream.read<size_t>();
    this->m_resizeFailed = inputStream.read<bool>();
    this->m_numberOfUsedBuckets = inputStream.read<size_t>();
    const size_t numberOfThreadsInFile = inputStream.read<size_t>();
    this->m_numberOfThreads = std::min(numberOfThreadsInFile, g_numberOfLogicalProecssors);
    // First read the counts what we can safely place in the local counts array
    for (size_t localThreadIndex = 0; localThreadIndex < this->m_numberOfThreads; ++localThreadIndex)
        getLocalInsertionCount(localThreadIndex) = inputStream.read<size_t>();
    // If there are more counts stored in the file, add then to the global count
    for (size_t localThreadIndex = this->m_numberOfThreads; localThreadIndex < numberOfThreadsInFile; ++localThreadIndex)
        this->m_numberOfUsedBuckets += inputStream.read<size_t>();
    inputStream.readMemoryRegion(this->m_policyBuckets.m_buckets);
    this->m_afterLastBucket = this->m_policyBuckets.m_buckets + this->m_numberOfBuckets * this->m_policyBuckets.BUCKET_SIZE;
    initializeCounterWindow();
    m_oldBuckets.deinitialize();
    m_numberOfResizeChunks = 0;
    m_nextAvailableResizeChunk = 0;
    m_numberOfResizeChunksToFinish = 0;
}

#endif /* PARALLELHASHTABLEIMPL_H_ */
