// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef CONCURRENTTWOKEYSINDEXPOLICY_H_
#define CONCURRENTTWOKEYSINDEXPOLICY_H_

#include "../../util/SequentialHashTable.h"

template<class TripleListType, ResourceComponent component1, ResourceComponent component2>
class ConcurrentTwoKeysIndexPolicy {

protected:

    TripleListType& m_tripleList;

public:

    typedef typename TripleListType::StoreTripleIndexType StoreTripleIndexType;

    static const size_t BUCKET_SIZE = sizeof(StoreTripleIndexType);

    static const TupleIndex IN_INSERTION_TRIPLE_INDEX = static_cast<StoreTripleIndexType>(-1);

    struct BucketContents {
        TupleIndex m_headTripleIndex;
    };

    ConcurrentTwoKeysIndexPolicy(TripleListType& tripleList);

    TripleListType& getTripleList();

    static void getBucketContents(const uint8_t* const bucket, BucketContents& bucketContents);

    BucketStatus getBucketContentsStatus(const BucketContents& bucketContents, const size_t valuesHashCode, const ResourceID value1, const ResourceID value2) const;

    static bool setBucketContentsIfEmpty(uint8_t* const bucket, const BucketContents& bucketContents);

    static bool isBucketContentsEmpty(const BucketContents& bucketContents);

    size_t getBucketContentsHashCode(const BucketContents& bucketContents) const;

    static size_t hashCodeFor(const ResourceID value1, const ResourceID value2);

    static TupleIndex getHeadTripleIndex(const uint8_t* const bucket);

    static void setHeadTripleIndex(uint8_t* const bucket, const TupleIndex headTripleIndex);

    static bool startBucketInsertionConditional(uint8_t* const bucket);

};

#endif /* CONCURRENTTWOKEYSINDEXPOLICY_H_ */
