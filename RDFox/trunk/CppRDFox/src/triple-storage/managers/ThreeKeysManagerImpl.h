// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef THREEKEYSMANAGERIMPL_H_
#define THREEKEYSMANAGERIMPL_H_

#include "../../util/ComponentStatistics.h"
#include "../../util/InputStream.h"
#include "../../util/OutputStream.h"
#include "ThreeKeysManager.h"

template<class ThreeKeysManagerConfiguration>
always_inline ThreeKeysManager<ThreeKeysManagerConfiguration>::ThreeKeysManager(MemoryManager& memoryManager, TripleListType& tripleList, const Parameters& dataStoreParameters) : m_threeKeysIndex(memoryManager, HASH_TABLE_LOAD_FACTOR, tripleList) {
}

template<class ThreeKeysManagerConfiguration>
always_inline typename ThreeKeysManager<ThreeKeysManagerConfiguration>::TripleListType& ThreeKeysManager<ThreeKeysManagerConfiguration>::getTripleList() {
    return m_threeKeysIndex.getPolicy().getTripleList();
}

template<class ThreeKeysManagerConfiguration>
always_inline const typename ThreeKeysManager<ThreeKeysManagerConfiguration>::TripleListType& ThreeKeysManager<ThreeKeysManagerConfiguration>::getTripleList() const {
    return m_threeKeysIndex.getPolicy().getTripleList();
}

template<class ThreeKeysManagerConfiguration>
always_inline bool ThreeKeysManager<ThreeKeysManagerConfiguration>::initialize(const size_t initialTripleCapacity) {
    return m_threeKeysIndex.initialize(getHashTableSize(initialTripleCapacity));
}

template<class ThreeKeysManagerConfiguration>
always_inline void ThreeKeysManager<ThreeKeysManagerConfiguration>::setNumberOfThreads(const size_t numberOfThreads) {
    m_threeKeysIndex.setNumberOfThreads(numberOfThreads);
}

template<class ThreeKeysManagerConfiguration>
always_inline bool ThreeKeysManager<ThreeKeysManagerConfiguration>::getInsertToken(ThreadContext& threadContext, const ResourceID insertedS, const ResourceID insertedP, const ResourceID insertedO, InsertToken& insertToken, bool& alreadyExists) {
    m_threeKeysIndex.acquireBucket(threadContext, insertToken, insertedS, insertedP, insertedO);
    BucketStatus bucketStatus;
    while ((bucketStatus = m_threeKeysIndex.continueBucketSearch(threadContext, insertToken, insertedS, insertedP, insertedO)) == BUCKET_EMPTY) {
        if (m_threeKeysIndex.getPolicy().startBucketInsertionConditional(insertToken.m_bucket)) {
            alreadyExists = false;
            return true;
        }
    }
    alreadyExists = true;
    return bucketStatus != BUCKET_NOT_CONTAINS;
}

template<class ThreeKeysManagerConfiguration>
always_inline void ThreeKeysManager<ThreeKeysManagerConfiguration>::abortInsertToken(ThreadContext& threadContext, InsertToken& insertToken) {
    m_threeKeysIndex.getPolicy().setTripleIndex(insertToken.m_bucket, INVALID_TUPLE_INDEX);
}

template<class ThreeKeysManagerConfiguration>
always_inline void ThreeKeysManager<ThreeKeysManagerConfiguration>::releaseInsertToken(ThreadContext& threadContext, InsertToken& insertToken) {
    m_threeKeysIndex.releaseBucket(threadContext, insertToken);
}

template<class ThreeKeysManagerConfiguration>
always_inline void ThreeKeysManager<ThreeKeysManagerConfiguration>::updateOnInsert(ThreadContext& threadContext, InsertToken& insertToken, const TupleIndex insertedTripleIndex, const ResourceID insertedS, const ResourceID insertedP, const ResourceID insertedO) {
    m_threeKeysIndex.getPolicy().setTripleIndex(insertToken.m_bucket, insertedTripleIndex);
    m_threeKeysIndex.acknowledgeInsert(threadContext, insertToken);
}

template<class ThreeKeysManagerConfiguration>
always_inline bool ThreeKeysManager<ThreeKeysManagerConfiguration>::insertTriple(ThreadContext& threadContext, const TupleIndex insertedTripleIndex, const ResourceID insertedS, const ResourceID insertedP, const ResourceID insertedO, bool& tripleIndexInserted, TupleIndex& resultingTripleIndex) {
    tripleIndexInserted = false;
    typename ThreeKeysIndexType::BucketDescriptor bucketDescriptor;
    m_threeKeysIndex.acquireBucket(threadContext, bucketDescriptor, insertedS, insertedP, insertedO);
    BucketStatus bucketStatus;
    while ((bucketStatus = m_threeKeysIndex.continueBucketSearch(threadContext, bucketDescriptor, insertedS, insertedP, insertedO)) == BUCKET_EMPTY) {
        if (m_threeKeysIndex.getPolicy().setTripleIndexConditional(bucketDescriptor.m_bucket, INVALID_TUPLE_INDEX, insertedTripleIndex)) {
            m_threeKeysIndex.acknowledgeInsert(threadContext, bucketDescriptor);
            tripleIndexInserted = true;
            resultingTripleIndex = insertedTripleIndex;
            break;
        }
    }
    if (bucketStatus == BUCKET_CONTAINS)
        resultingTripleIndex = bucketDescriptor.m_bucketContents.m_tripleIndex;
    m_threeKeysIndex.releaseBucket(threadContext, bucketDescriptor);
    return bucketStatus != BUCKET_NOT_CONTAINS;
}

template<class ThreeKeysManagerConfiguration>
always_inline TupleIndex ThreeKeysManager<ThreeKeysManagerConfiguration>::getTripleIndex(ThreadContext& threadContext, const ResourceID s, const ResourceID p, const ResourceID o) const {
    typename ThreeKeysIndexType::BucketDescriptor bucketDescriptor;
    m_threeKeysIndex.acquireBucket(threadContext, bucketDescriptor, s, p, o);
    BucketStatus bucketStatus = m_threeKeysIndex.continueBucketSearch(threadContext, bucketDescriptor, s, p, o);
    m_threeKeysIndex.releaseBucket(threadContext, bucketDescriptor);
    return bucketStatus == BUCKET_CONTAINS && (getTripleList().getTripleStatus(bucketDescriptor.m_bucketContents.m_tripleIndex) & TUPLE_STATUS_COMPLETE) == TUPLE_STATUS_COMPLETE ? bucketDescriptor.m_bucketContents.m_tripleIndex : INVALID_TUPLE_INDEX;
}

template<class ThreeKeysManagerConfiguration>
always_inline bool ThreeKeysManager<ThreeKeysManagerConfiguration>::contains(ThreadContext& threadContext, const ResourceID s, const ResourceID p, const ResourceID o, const TupleStatus tupleStatusMask, const TupleStatus tupleStatusExpectedValue) const {
    typename ThreeKeysIndexType::BucketDescriptor bucketDescriptor;
    m_threeKeysIndex.acquireBucket(threadContext, bucketDescriptor, s, p, o);
    BucketStatus bucketStatus = m_threeKeysIndex.continueBucketSearch(threadContext, bucketDescriptor, s, p, o);
    m_threeKeysIndex.releaseBucket(threadContext, bucketDescriptor);
    return bucketStatus == BUCKET_CONTAINS && (getTripleList().getTripleStatus(bucketDescriptor.m_bucketContents.m_tripleIndex) & tupleStatusMask) == tupleStatusExpectedValue;
}

template<class ThreeKeysManagerConfiguration>
always_inline size_t ThreeKeysManager<ThreeKeysManagerConfiguration>::getCountEstimate(ThreadContext& threadContext, const ResourceID s, const ResourceID p, const ResourceID o) const {
    return static_cast<size_t>(contains(threadContext, s, p, o, TUPLE_STATUS_IDB | TUPLE_STATUS_IDB_MERGED, TUPLE_STATUS_IDB));
}

template<class ThreeKeysManagerConfiguration>
always_inline void ThreeKeysManager<ThreeKeysManagerConfiguration>::save(OutputStream& outputStream) const {
    outputStream.writeString("ThreeKeysManager");
    m_threeKeysIndex.save(outputStream);
}

template<class ThreeKeysManagerConfiguration>
always_inline void ThreeKeysManager<ThreeKeysManagerConfiguration>::load(InputStream& inputStream) {
    if (!inputStream.checkNextString("ThreeKeysManager"))
        throw RDF_STORE_EXCEPTION("Invalid input file: cannot load ThreeKeysManager.");
    m_threeKeysIndex.load(inputStream);
}

template<class ThreeKeysManagerConfiguration>
always_inline std::unique_ptr<ComponentStatistics> ThreeKeysManager<ThreeKeysManagerConfiguration>::getComponentStatistics() const {
    std::unique_ptr<ComponentStatistics> result(new ComponentStatistics("ThreeKeysManager"));
    size_t numberOfBuckets = m_threeKeysIndex.getNumberOfBuckets();
    size_t numberOfUsedBuckets = m_threeKeysIndex.getNumberOfUsedBuckets();
    uint64_t size = numberOfBuckets * m_threeKeysIndex.getPolicy().BUCKET_SIZE;
    result->addIntegerItem("Size", size);
    result->addIntegerItem("Total number of buckets", numberOfBuckets);
    result->addIntegerItem("Number of used buckets", numberOfUsedBuckets);
    if (numberOfUsedBuckets != 0)
        result->addFloatingPointItem("Bytes per used bucket", size / static_cast<double>(numberOfUsedBuckets));
    result->addFloatingPointItem("Load factor (%)", (numberOfUsedBuckets * 100.0) / static_cast<double>(numberOfBuckets));
    const size_t tripleCount = getTripleList().getExactTripleCount(TUPLE_STATUS_COMPLETE, TUPLE_STATUS_COMPLETE);
    if (tripleCount != 0)
        result->addFloatingPointItem("Bytes per triple", size / static_cast<double>(tripleCount));
    return result;
}

#endif /* THREEKEYSMANAGERIMPL_H_ */
