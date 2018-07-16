// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef CONCURRENTTHREEKEYSINDEXPOLICY_H_
#define CONCURRENTTHREEKEYSINDEXPOLICY_H_

#include "../../util/SequentialHashTable.h"

template<class TripleListType>
class ConcurrentThreeKeysIndexPolicy {

protected:

    TripleListType& m_tripleList;

public:

    typedef typename TripleListType::StoreTripleIndexType StoreTripleIndexType;

    static const size_t BUCKET_SIZE = sizeof(StoreTripleIndexType);

    static const TupleIndex IN_INSERTION_TRIPLE_INDEX = static_cast<StoreTripleIndexType>(-1);

    struct BucketContents {
        TupleIndex m_tripleIndex;
    };

    ConcurrentThreeKeysIndexPolicy(TripleListType& tripleList);

    TripleListType& getTripleList();

    static void getBucketContents(const uint8_t* const bucket, BucketContents& bucketContents);

    BucketStatus getBucketContentsStatus(const BucketContents& bucketContents, const size_t valuesHashCode, const ResourceID s, const ResourceID p, const ResourceID o) const;

    static bool setBucketContentsIfEmpty(uint8_t* const bucket, const BucketContents& bucketContents);

    static bool isBucketContentsEmpty(const BucketContents& bucketContents);

    size_t getBucketContentsHashCode(const BucketContents& bucketContents) const;

    static size_t hashCodeFor(const ResourceID s, const ResourceID p, const ResourceID o);

    static TupleIndex getTripleIndex(const uint8_t* const bucket);

    static void setTripleIndex(uint8_t* const bucket, const TupleIndex tupleIndex);

    static bool setTripleIndexConditional(uint8_t* const bucket, const TupleIndex testTripleIndex, const TupleIndex tupleIndex);

    static bool startBucketInsertionConditional(uint8_t* const bucket);

};

#endif /* CONCURRENTTHREEKEYSINDEXPOLICY_H_ */
