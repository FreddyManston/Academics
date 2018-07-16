// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef STRINGPOLICIES_H_
#define STRINGPOLICIES_H_

#include "../Common.h"
#include "DataPool.h"

// AbstractStringPolicy

class AbstractStringPolicy {

protected:

    DataPool& m_dataPool;

public:

    struct BucketContents {
        uint64_t m_chunkIndex;
    };

    always_inline AbstractStringPolicy(DataPool& dataPool) : m_dataPool(dataPool) {
    }

    always_inline BucketStatus getBucketContentsStatus(const BucketContents& bucketContents, const size_t valuesHashCode, const char* lexicalForm) const {
        if (bucketContents.m_chunkIndex == DataPool::INVALID_CHUNK_INDEX)
            return BUCKET_EMPTY;
        else {
            const char* bucketLexicalForm = reinterpret_cast<const char*>(m_dataPool.getDataFor(bucketContents.m_chunkIndex) + sizeof(ResourceID));
            while (*bucketLexicalForm != 0 && *lexicalForm != 0) {
                if (*bucketLexicalForm != *lexicalForm)
                    return BUCKET_NOT_CONTAINS;
                ++bucketLexicalForm;
                ++lexicalForm;
            }
            if (*bucketLexicalForm == *lexicalForm)
                return BUCKET_CONTAINS;
            else
                return BUCKET_NOT_CONTAINS;
        }
    }

    always_inline static bool isBucketContentsEmpty(const BucketContents& bucketContents) {
        return bucketContents.m_chunkIndex == DataPool::INVALID_CHUNK_INDEX;
    }

    always_inline size_t getBucketContentsHashCode(const BucketContents& bucketContents) const {
        return hashCodeFor(reinterpret_cast<const char*>(m_dataPool.getDataFor(bucketContents.m_chunkIndex) + sizeof(ResourceID)));
    }

    always_inline static size_t hashCodeFor(const char* lexicalForm) {
        size_t result = static_cast<size_t>(14695981039346656037ULL);
        uint8_t value;
        while ((value = *(lexicalForm++)) != 0) {
            result ^= static_cast<size_t>(value);
            result *= static_cast<size_t>(1099511628211ULL);
        }
        return result;
    }

};

// SequentialStringPolicy

class SequentialStringPolicy : public AbstractStringPolicy {

public:

    static const bool IS_PARALLEL = false;

    static const size_t BUCKET_SIZE = 6;

    always_inline SequentialStringPolicy(DataPool& dataPool) : AbstractStringPolicy(dataPool) {
    }

    always_inline static void getBucketContents(const uint8_t* const bucket, BucketContents& bucketContents) {
        bucketContents.m_chunkIndex = getChunkIndex(bucket);
    }

    always_inline static bool setBucketContentsIfEmpty(uint8_t* const bucket, const BucketContents& bucketContents) {
        if (getChunkIndex(bucket) == 0) {
            setChunkIndex(bucket, bucketContents.m_chunkIndex);
            return true;
        }
        else
            return false;
    }

    always_inline static uint64_t getChunkIndex(const uint8_t* const bucket) {
        return ::read6(bucket);
    }

    always_inline static void setChunkIndex(uint8_t* const bucket, const uint64_t chunkIndex) {
        ::write6(bucket, chunkIndex);
    }

    always_inline static bool startBucketInsertionConditional(uint8_t* const bucket) {
        return true;
    }

};

// ConcurrentStringPolicy

class ConcurrentStringPolicy : public AbstractStringPolicy {

public:

    static const bool IS_PARALLEL = true;

    static const size_t BUCKET_SIZE = sizeof(uint64_t);

    static const uint64_t IN_INSERTION_CHUNK_INDEX = static_cast<uint64_t>(-1);

    always_inline ConcurrentStringPolicy(DataPool& dataPool) : AbstractStringPolicy(dataPool) {
    }

    always_inline static void getBucketContents(const uint8_t* const bucket, BucketContents& bucketContents) {
        do {
            bucketContents.m_chunkIndex = getChunkIndex(bucket);
        } while (bucketContents.m_chunkIndex == IN_INSERTION_CHUNK_INDEX);
    }

    always_inline static bool setBucketContentsIfEmpty(uint8_t* const bucket, const BucketContents& bucketContents) {
        return ::atomicConditionalSet(*reinterpret_cast<uint64_t*>(bucket), 0, bucketContents.m_chunkIndex);
    }

    always_inline static uint64_t getChunkIndex(const uint8_t* const bucket) {
        return ::atomicRead(*reinterpret_cast<const uint64_t*>(bucket));
    }

    always_inline static void setChunkIndex(uint8_t* const bucket, const uint64_t chunkIndex) {
        ::atomicWrite(*reinterpret_cast<uint64_t*>(bucket), chunkIndex);
    }

    always_inline static bool startBucketInsertionConditional(uint8_t* const bucket) {
        return ::atomicConditionalSet(*reinterpret_cast<uint64_t*>(bucket), 0, IN_INSERTION_CHUNK_INDEX);
    }

};

#endif /* STRINGPOLICIES_H_ */
