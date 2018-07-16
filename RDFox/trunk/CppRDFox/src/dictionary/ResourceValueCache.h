// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef RESOURCEVALUECACHE_H_
#define RESOURCEVALUECACHE_H_

#include "../Common.h"
#include "../util/MemoryRegion.h"
#include "../util/SequentialHashTable.h"

class MemoryManager;
class Dictionary;
class ThreadContext;

class ResourceValueCache : private Unmovable {

protected:

    friend class Policy;

    static const ResourceID CACHED_RESOURCE_ID_MASK = 0x8000000000000000ULL;

    class Policy {

    public:

        MemoryRegion<uint8_t> m_data;

        static const size_t BUCKET_SIZE = sizeof(ResourceID);

        struct BucketContents {
            ResourceID m_resourceID;
        };

        Policy(MemoryManager& memoryManager);
        
        static void getBucketContents(const uint8_t* const bucket, BucketContents& bucketContents);

        BucketStatus getBucketContentsStatus(const BucketContents& bucketContents, const size_t valuesHashCode, const ResourceValue* const resourceValue) const;

        static bool setBucketContentsIfEmpty(uint8_t* const bucket, const BucketContents& bucketContents);

        static bool isBucketContentsEmpty(const BucketContents& bucketContents);

        size_t getBucketContentsHashCode(const BucketContents& bucketContents) const;

        static size_t hashCodeFor(const ResourceValue* const resourceValue);

        static size_t hashCodeFor(const DatatypeID datatypeID, const size_t dataSize, const uint8_t* buffer);

        static ResourceID getResourceID(const uint8_t* const bucket);

        static void setResourceID(uint8_t* const bucket, const ResourceID resourceID);

        void dereference(const ResourceID resourceID, DatatypeID& datatypeID, size_t& dataSize, const uint8_t* & buffer) const;
        
    };

    const Dictionary& m_dictionary;
    size_t m_nextFreeLocation;
    mutable SequentialHashTable<Policy> m_hashTable;

    void initialize();

public:

    ResourceValueCache(const Dictionary& dictionary, MemoryManager& memoryManager);

    ~ResourceValueCache();

    always_inline static bool isPermanentValue(const ResourceID resourceID) {
        return resourceID < CACHED_RESOURCE_ID_MASK;
    }

    always_inline const Dictionary& getDictionary() const {
        return m_dictionary;
    }
    
    void clear();

    bool getResource(const ResourceID resourceID, ResourceValue& resourceValue) const;

    bool getResource(const ResourceID resourceID, ResourceType& resourceType, std::string& lexicalForm, std::string& datatypeIRI) const;

    bool getResource(const ResourceID resourceID, std::string& lexicalForm, DatatypeID& datatypeID) const;

    ResourceID tryResolveResource(ThreadContext& threadContext, const ResourceValue& resourceValue) const;

    ResourceID resolveResource(ThreadContext& threadContext, const ResourceValue& resourceValue);

};

#endif /* RESOURCEVALUECACHE_H_ */
