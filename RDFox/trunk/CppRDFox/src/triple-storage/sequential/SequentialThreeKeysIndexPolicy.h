// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef SEQUENTIALTHREEKEYSINDEXPOLICY_H_
#define SEQUENTIALTHREEKEYSINDEXPOLICY_H_

#include "../../util/SequentialHashTable.h"

template<class TripleListType>
class SequentialThreeKeysIndexPolicy {

protected:

    TripleListType& m_tripleList;

public:

    static const size_t BUCKET_SIZE = SEQUENTIAL_TUPLE_INDEX_STORAGE_SIZE;

    struct BucketContents {
        TupleIndex m_tripleIndex;
    };

    SequentialThreeKeysIndexPolicy(TripleListType& tripleList);

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

#endif /* SEQUENTIALTHREEKEYSINDEXPOLICY_H_ */
