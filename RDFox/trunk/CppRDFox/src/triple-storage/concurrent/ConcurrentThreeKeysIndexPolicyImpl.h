// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef CONCURRENTTHREEKEYSINDEXPOLICYIMPL_H_
#define CONCURRENTTHREEKEYSINDEXPOLICYIMPL_H_

#include "ConcurrentThreeKeysIndexPolicy.h"

template<class TripleListType>
always_inline ConcurrentThreeKeysIndexPolicy<TripleListType>::ConcurrentThreeKeysIndexPolicy(TripleListType& tripleList) : m_tripleList(tripleList) {
}
template<class TripleListType>
always_inline TripleListType& ConcurrentThreeKeysIndexPolicy<TripleListType>::getTripleList() {
    return m_tripleList;
}

template<class TripleListType>
always_inline void ConcurrentThreeKeysIndexPolicy<TripleListType>::getBucketContents(const uint8_t* const bucket, BucketContents& bucketContents) {
    do {
        bucketContents.m_tripleIndex = getTripleIndex(bucket);
    } while (bucketContents.m_tripleIndex == IN_INSERTION_TRIPLE_INDEX);
}

template<class TripleListType>
always_inline BucketStatus ConcurrentThreeKeysIndexPolicy<TripleListType>::getBucketContentsStatus(const BucketContents& bucketContents, const size_t valuesHashCode, const ResourceID s, const ResourceID p, const ResourceID o) const {
    if (bucketContents.m_tripleIndex == INVALID_TUPLE_INDEX)
        return BUCKET_EMPTY;
    else {
        ResourceID tripleS;
        ResourceID tripleP;
        ResourceID tripleO;
        m_tripleList.getResourceIDs(bucketContents.m_tripleIndex, tripleS, tripleP, tripleO);
        if (tripleS == s && tripleP == p && tripleO == o)
            return BUCKET_CONTAINS;
        else
            return BUCKET_NOT_CONTAINS;
    }
}

template<class TripleListType>
always_inline bool ConcurrentThreeKeysIndexPolicy<TripleListType>::setBucketContentsIfEmpty(uint8_t* const bucket, const BucketContents& bucketContents) {
    return setTripleIndexConditional(bucket, INVALID_TUPLE_INDEX, bucketContents.m_tripleIndex);
}

template<class TripleListType>
always_inline bool ConcurrentThreeKeysIndexPolicy<TripleListType>::isBucketContentsEmpty(const BucketContents& bucketContents) {
    return bucketContents.m_tripleIndex == INVALID_TUPLE_INDEX;
}

template<class TripleListType>
always_inline size_t ConcurrentThreeKeysIndexPolicy<TripleListType>::getBucketContentsHashCode(const BucketContents& bucketContents) const {
    ResourceID tripleS;
    ResourceID tripleP;
    ResourceID tripleO;
    m_tripleList.getResourceIDs(bucketContents.m_tripleIndex, tripleS, tripleP, tripleO);
    return hashCodeFor(tripleS, tripleP, tripleO);
}

template<class TripleListType>
always_inline size_t ConcurrentThreeKeysIndexPolicy<TripleListType>::hashCodeFor(const ResourceID s, const ResourceID p, const ResourceID o) {
    size_t hash = 0;
    hash += s;
    hash += (hash << 10);
    hash ^= (hash >> 6);

    hash += p;
    hash += (hash << 10);
    hash ^= (hash >> 6);

    hash += o;
    hash += (hash << 10);
    hash ^= (hash >> 6);

    hash += (hash << 3);
    hash ^= (hash >> 11);
    hash += (hash << 15);
    return hash;
}

template<class TripleListType>
always_inline TupleIndex ConcurrentThreeKeysIndexPolicy<TripleListType>::getTripleIndex(const uint8_t* const bucket) {
    return static_cast<TupleIndex>(::atomicRead(*reinterpret_cast<const StoreTripleIndexType*>(bucket)));
}

template<class TripleListType>
always_inline void ConcurrentThreeKeysIndexPolicy<TripleListType>::setTripleIndex(uint8_t* const bucket, const TupleIndex tupleIndex) {
    ::atomicWrite(*reinterpret_cast<StoreTripleIndexType*>(bucket), static_cast<StoreTripleIndexType>(tupleIndex));
}

template<class TripleListType>
always_inline bool ConcurrentThreeKeysIndexPolicy<TripleListType>::setTripleIndexConditional(uint8_t* const bucket, const TupleIndex testTripleIndex, const TupleIndex tupleIndex) {
    return ::atomicConditionalSet(*reinterpret_cast<StoreTripleIndexType*>(bucket), static_cast<StoreTripleIndexType>(testTripleIndex), static_cast<StoreTripleIndexType>(tupleIndex));
}

template<class TripleListType>
always_inline bool ConcurrentThreeKeysIndexPolicy<TripleListType>::startBucketInsertionConditional(uint8_t* const bucket) {
    return setTripleIndexConditional(bucket, INVALID_TUPLE_INDEX, IN_INSERTION_TRIPLE_INDEX);
}

#endif /* CONCURRENTTHREEKEYSINDEXPOLICYIMPL_H_ */
