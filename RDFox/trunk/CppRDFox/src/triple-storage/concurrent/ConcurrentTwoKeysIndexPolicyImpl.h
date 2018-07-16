// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef CONCURRENTTWOKEYSINDEXPOLICYIMPL_H_
#define CONCURRENTTWOKEYSINDEXPOLICYIMPL_H_

#include "ConcurrentTwoKeysIndexPolicy.h"

template<class TripleListType, ResourceComponent component1, ResourceComponent component2>
always_inline ConcurrentTwoKeysIndexPolicy<TripleListType, component1, component2>::ConcurrentTwoKeysIndexPolicy(TripleListType& tripleList) : m_tripleList(tripleList) {
}

template<class TripleListType, ResourceComponent component1, ResourceComponent component2>
always_inline TripleListType& ConcurrentTwoKeysIndexPolicy<TripleListType, component1, component2>::getTripleList() {
    return m_tripleList;
}

template<class TripleListType, ResourceComponent component1, ResourceComponent component2>
always_inline void ConcurrentTwoKeysIndexPolicy<TripleListType, component1, component2>::getBucketContents(const uint8_t* const bucket, BucketContents& bucketContents) {
    do {
        bucketContents.m_headTripleIndex = getHeadTripleIndex(bucket);
    } while (bucketContents.m_headTripleIndex == IN_INSERTION_TRIPLE_INDEX);
}

template<class TripleListType, ResourceComponent component1, ResourceComponent component2>
always_inline BucketStatus ConcurrentTwoKeysIndexPolicy<TripleListType, component1, component2>::getBucketContentsStatus(const BucketContents& bucketContents, const size_t valuesHashCode, const ResourceID value1, const ResourceID value2) const {
    if (bucketContents.m_headTripleIndex == INVALID_TUPLE_INDEX)
        return BUCKET_EMPTY;
    else {
        ResourceID tripleValue1;
        ResourceID tripleValue2;
        m_tripleList.getResourceIDs(bucketContents.m_headTripleIndex, component1, component2, tripleValue1, tripleValue2);
        if (tripleValue1 == value1 && tripleValue2 == value2)
            return BUCKET_CONTAINS;
        else
            return BUCKET_NOT_CONTAINS;
    }
}

template<class TripleListType, ResourceComponent component1, ResourceComponent component2>
always_inline bool ConcurrentTwoKeysIndexPolicy<TripleListType, component1, component2>::setBucketContentsIfEmpty(uint8_t* const bucket, const BucketContents& bucketContents) {
    return ::atomicConditionalSet(*reinterpret_cast<StoreTripleIndexType*>(bucket), static_cast<StoreTripleIndexType>(INVALID_TUPLE_INDEX), static_cast<StoreTripleIndexType>(bucketContents.m_headTripleIndex));
}

template<class TripleListType, ResourceComponent component1, ResourceComponent component2>
always_inline bool ConcurrentTwoKeysIndexPolicy<TripleListType, component1, component2>::isBucketContentsEmpty(const BucketContents& bucketContents) {
    return bucketContents.m_headTripleIndex == INVALID_TUPLE_INDEX;
}

template<class TripleListType, ResourceComponent component1, ResourceComponent component2>
always_inline size_t ConcurrentTwoKeysIndexPolicy<TripleListType, component1, component2>::getBucketContentsHashCode(const BucketContents& bucketContents) const {
    ResourceID value1;
    ResourceID value2;
    m_tripleList.getResourceIDs(bucketContents.m_headTripleIndex, component1, component2, value1, value2);
    return hashCodeFor(value1, value2);
}

template<class TripleListType, ResourceComponent component1, ResourceComponent component2>
always_inline size_t ConcurrentTwoKeysIndexPolicy<TripleListType, component1, component2>::hashCodeFor(const ResourceID value1, const ResourceID value2) {
    size_t hash = 0;
    hash += value1;
    hash += (hash << 10);
    hash ^= (hash >> 6);

    hash += value2;
    hash += (hash << 10);
    hash ^= (hash >> 6);

    hash += (hash << 3);
    hash ^= (hash >> 11);
    hash += (hash << 15);
    return hash;
}

template<class TripleListType, ResourceComponent component1, ResourceComponent component2>
always_inline TupleIndex ConcurrentTwoKeysIndexPolicy<TripleListType, component1, component2>::getHeadTripleIndex(const uint8_t* const bucket) {
    return static_cast<TupleIndex>(::atomicRead(*reinterpret_cast<const StoreTripleIndexType*>(bucket)));
}

template<class TripleListType, ResourceComponent component1, ResourceComponent component2>
always_inline void ConcurrentTwoKeysIndexPolicy<TripleListType, component1, component2>::setHeadTripleIndex(uint8_t* const bucket, const TupleIndex headTripleIndex) {
    ::atomicWrite(*reinterpret_cast<StoreTripleIndexType*>(bucket), static_cast<StoreTripleIndexType>(headTripleIndex));
}

template<class TripleListType, ResourceComponent component1, ResourceComponent component2>
always_inline bool ConcurrentTwoKeysIndexPolicy<TripleListType, component1, component2>::startBucketInsertionConditional(uint8_t* const bucket) {
    return ::atomicConditionalSet(*reinterpret_cast<StoreTripleIndexType*>(bucket), static_cast<StoreTripleIndexType>(INVALID_TUPLE_INDEX), static_cast<StoreTripleIndexType>(IN_INSERTION_TRIPLE_INDEX));
}

#endif /* CONCURRENTTWOKEYSINDEXPOLICYIMPL_H_ */
