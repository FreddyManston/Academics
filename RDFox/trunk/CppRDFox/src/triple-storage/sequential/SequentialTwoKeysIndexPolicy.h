// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef SEQUENTIALTWOKEYSINDEXPOLICY_H_
#define SEQUENTIALTWOKEYSINDEXPOLICY_H_

#include "../../util/SequentialHashTable.h"

template<class TripleListType, ResourceComponent component1, ResourceComponent component2>
class SequentialTwoKeysIndexPolicy {

protected:

    TripleListType& m_tripleList;

public:

    static const size_t BUCKET_SIZE = SEQUENTIAL_TUPLE_INDEX_STORAGE_SIZE;

    struct BucketContents {
        TupleIndex m_headTripleIndex;
    };

    SequentialTwoKeysIndexPolicy(TripleListType& tripleList);

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

#endif /* SEQUENTIALTWOKEYSINDEXPOLICY_H_ */
