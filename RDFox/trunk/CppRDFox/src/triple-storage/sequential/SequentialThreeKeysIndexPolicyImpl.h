// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef SEQUENTIALTHREEKEYSINDEXPOLICYIMPL_H_
#define SEQUENTIALTHREEKEYSINDEXPOLICYIMPL_H_

#include "SequentialThreeKeysIndexPolicy.h"

template<class TripleListType>
always_inline SequentialThreeKeysIndexPolicy<TripleListType>::SequentialThreeKeysIndexPolicy(TripleListType& tripleList) : m_tripleList(tripleList) {
}

template<class TripleListType>
always_inline TripleListType& SequentialThreeKeysIndexPolicy<TripleListType>::getTripleList() {
    return m_tripleList;
}

template<class TripleListType>
always_inline void SequentialThreeKeysIndexPolicy<TripleListType>::getBucketContents(const uint8_t* const bucket, BucketContents& bucketContents) {
    bucketContents.m_tripleIndex = getTripleIndex(bucket);
}

template<class TripleListType>
always_inline BucketStatus SequentialThreeKeysIndexPolicy<TripleListType>::getBucketContentsStatus(const BucketContents& bucketContents, const size_t valuesHashCode, const ResourceID s, const ResourceID p, const ResourceID o) const {
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
always_inline bool SequentialThreeKeysIndexPolicy<TripleListType>::setBucketContentsIfEmpty(uint8_t* const bucket, const BucketContents& bucketContents) {
    if (getTripleIndex(bucket) == INVALID_TUPLE_INDEX) {
        setTripleIndex(bucket, bucketContents.m_tripleIndex);
        return true;
    }
    else
        return false;
}

template<class TripleListType>
always_inline bool SequentialThreeKeysIndexPolicy<TripleListType>::isBucketContentsEmpty(const BucketContents& bucketContents) {
    return bucketContents.m_tripleIndex == INVALID_TUPLE_INDEX;
}

template<class TripleListType>
always_inline size_t SequentialThreeKeysIndexPolicy<TripleListType>::getBucketContentsHashCode(const BucketContents& bucketContents) const {
    ResourceID tripleS;
    ResourceID tripleP;
    ResourceID tripleO;
    m_tripleList.getResourceIDs(bucketContents.m_tripleIndex, tripleS, tripleP, tripleO);
    return hashCodeFor(tripleS, tripleP, tripleO);
}

template<class TripleListType>
always_inline size_t SequentialThreeKeysIndexPolicy<TripleListType>::hashCodeFor(const ResourceID s, const ResourceID p, const ResourceID o) {
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
always_inline TupleIndex SequentialThreeKeysIndexPolicy<TripleListType>::getTripleIndex(const uint8_t* const bucket) {
    return ::read6(bucket);
}

template<class TripleListType>
always_inline void SequentialThreeKeysIndexPolicy<TripleListType>::setTripleIndex(uint8_t* const bucket, const TupleIndex tupleIndex) {
    ::write6(bucket, tupleIndex);
}

template<class TripleListType>
always_inline bool SequentialThreeKeysIndexPolicy<TripleListType>::setTripleIndexConditional(uint8_t* const bucket, const TupleIndex testTripleIndex, const TupleIndex tupleIndex) {
    setTripleIndex(bucket, tupleIndex);
    return true;
}

template<class TripleListType>
always_inline bool SequentialThreeKeysIndexPolicy<TripleListType>::startBucketInsertionConditional(uint8_t* const bucket) {
    return true;
}

#endif /* SEQUENTIALTHREEKEYSINDEXPOLICYIMPL_H_ */
