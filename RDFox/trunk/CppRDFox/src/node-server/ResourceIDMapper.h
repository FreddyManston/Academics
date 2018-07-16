// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef RESOURCEIDMAPPER_H_
#define RESOURCEIDMAPPER_H_

#include "../Common.h"
#include "../util/MemoryRegion.h"
#include "../util/SequentialHashTable.h"

class MemoryManager;
class ThreadContext;

class ResourceIDMapper : private Unmovable {

protected:

    class Policy {

    public:

        static const size_t BUCKET_SIZE = sizeof(ResourceID) + sizeof(ResourceID);

        struct BucketContents {
            ResourceID m_globalResourceID;
            ResourceID m_localResourceID;
        };

        void getBucketContents(const uint8_t* const bucket, BucketContents& bucketContents);

        BucketStatus getBucketContentsStatus(const BucketContents& bucketContents, const size_t valuesHashCode, const ResourceID globalResourceID);

        bool isBucketContentsEmpty(const BucketContents& bucketContents);

        size_t getBucketContentsHashCode(const BucketContents& bucketContents);

        bool setBucketContentsIfEmpty(uint8_t* const bucket, const BucketContents& bucketContents);

        size_t hashCodeFor(const ResourceID globalResourceID);

        int64_t getGlobalResourceID(const uint8_t* const bucket);

        void setGlobalResourceID(uint8_t* const bucket, const ResourceID globalResourceID);
        
        ResourceID getLocalResourceID(const uint8_t* const bucket);

        void setLocalResourceID(uint8_t* const bucket, const ResourceID localResourceID);

    };

    MemoryRegion<ResourceID> m_localToGlobal;
    mutable SequentialHashTable<Policy> m_globalToLocal;

public:

    ResourceIDMapper(MemoryManager& memoryManager);

    void initialize();

    void addMapping(ThreadContext& threadContext, const ResourceID localResourceID, const ResourceID globalResourceID);

    ResourceID getLocalResourceID(ThreadContext& threadContext, const ResourceID globalResourceID) const;

    ResourceID getGlobalResourceID(const ResourceID localResourceID) const;

};

#endif // RESOURCEIDMAPPER_H_
