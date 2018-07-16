// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef INTERNINGMANAGER_H_
#define INTERNINGMANAGER_H_

#include "../all.h"
#include "../RDFStoreException.h"
#include "SequentialHashTable.h"

class MemoryManager;

template<class T, class Derived>
class InterningManager : private Unmovable {

protected:

    class EqualityMarker {
    };

    class Policy {

    public:

        static const size_t BUCKET_SIZE = sizeof(T*);

        struct BucketContents {
            T* m_object;
        };

        static void getBucketContents(const uint8_t* const bucket, BucketContents& bucketContents);

        template<typename... Args>
        static BucketStatus getBucketContentsStatus(const BucketContents& bucketContents, const size_t valuesHashCode, Args&&... args);

        static BucketStatus getBucketContentsStatus(const BucketContents& bucketContents, const size_t valuesHashCode, const T* const object, EqualityMarker equalityMarker);

        static bool setBucketContentsIfEmpty(uint8_t* const bucket, const BucketContents& bucketContents);

        static bool isBucketContentsEmpty(const BucketContents& bucketContents);

        static size_t getBucketContentsHashCode(const BucketContents& bucketContents);

        template<typename... Args>
        static size_t hashCodeFor(Args&&... args);

        static size_t hashCodeFor(const T* const object, EqualityMarker equalityMarker);

        static void makeBucketEmpty(uint8_t* const bucket);

        static bool startBucketInsertionConditional(uint8_t* const bucket);

        static const T* getObject(const uint8_t* const bucket);

        static void setObject(uint8_t* const bucket, const T* object);

    };

    typedef SequentialHashTable<Policy> HashTableType;

    HashTableType m_elements;

public:

    InterningManager(MemoryManager& memoryManager);

    bool initialize();

    void dispose(const T* const object);

    template<typename... Args>
    T* get(Args&&... args);

    size_t getNumberOfObjects() const;

};

#endif /* INTERNINGMANAGER_H_ */
