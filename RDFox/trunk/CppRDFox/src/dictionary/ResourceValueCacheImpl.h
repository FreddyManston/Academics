// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef RESOURCEVALUECACHEIMPL_H_
#define RESOURCEVALUECACHEIMPL_H_

#include "../dictionary/Dictionary.h"
#include "../util/SequentialHashTableImpl.h"
#include "ResourceValueCache.h"

// ResourceValueCache::Policy

always_inline ResourceValueCache::Policy::Policy(MemoryManager& memoryManager) : m_data(memoryManager) {
}

always_inline void ResourceValueCache::Policy::getBucketContents(const uint8_t* const bucket, BucketContents& bucketContents) {
    bucketContents.m_resourceID = getResourceID(bucket);
}

always_inline BucketStatus ResourceValueCache::Policy::getBucketContentsStatus(const BucketContents& bucketContents, const size_t valuesHashCode, const ResourceValue* const resourceValue) const {
    if (bucketContents.m_resourceID == INVALID_RESOURCE_ID)
        return BUCKET_EMPTY;
    else {
        DatatypeID datatypeID;
        size_t dataSize;
        const uint8_t* data;
        dereference(bucketContents.m_resourceID, datatypeID, dataSize, data);
        if (resourceValue->equals(datatypeID, dataSize, data))
            return BUCKET_CONTAINS;
        else
            return BUCKET_NOT_CONTAINS;
    }

}

always_inline bool ResourceValueCache::Policy::setBucketContentsIfEmpty(uint8_t* const bucket, const BucketContents& bucketContents) {
    if (getResourceID(bucket) == INVALID_RESOURCE_ID) {
        setResourceID(bucket, bucketContents.m_resourceID);
        return true;
    }
    else
        return false;
}

always_inline bool ResourceValueCache::Policy::isBucketContentsEmpty(const BucketContents& bucketContents) {
    return bucketContents.m_resourceID == INVALID_RESOURCE_ID;
}

always_inline size_t ResourceValueCache::Policy::getBucketContentsHashCode(const BucketContents& bucketContents) const {
    DatatypeID datatypeID;
    size_t dataSize;
    const uint8_t* data;
    dereference(bucketContents.m_resourceID, datatypeID, dataSize, data);
    return hashCodeFor(datatypeID, dataSize, data);
}

always_inline size_t ResourceValueCache::Policy::hashCodeFor(const ResourceValue* const resourceValue) {
    return resourceValue->hashCode();
}

always_inline size_t ResourceValueCache::Policy::hashCodeFor(const DatatypeID datatypeID, const size_t dataSize, const uint8_t* data) {
    return ResourceValue::hashCodeFor(datatypeID, dataSize, data);
}

always_inline ResourceID ResourceValueCache::Policy::getResourceID(const uint8_t* const bucket) {
    return *reinterpret_cast<const ResourceID*>(bucket);
}

always_inline void ResourceValueCache::Policy::setResourceID(uint8_t* const bucket, const ResourceID resourceID) {
    *reinterpret_cast<ResourceID*>(bucket) = resourceID;
}

always_inline void ResourceValueCache::Policy::dereference(const ResourceID resourceID, DatatypeID& datatypeID, size_t& dataSize, const uint8_t* & data) const {
    const uint8_t* bucketValueData = m_data.getData() + (resourceID & ~ResourceValueCache::CACHED_RESOURCE_ID_MASK);
    dataSize = *reinterpret_cast<const size_t*>(bucketValueData);
    data = bucketValueData + sizeof(size_t);
    datatypeID = *reinterpret_cast<const DatatypeID*>(bucketValueData + sizeof(size_t) + dataSize);
}

// ResourceValueCache

bool ResourceValueCache::getResource(const ResourceID resourceID, ResourceValue& resourceValue) const {
    if (isPermanentValue(resourceID))
        return m_dictionary.getResource(resourceID, resourceValue);
    else if (m_hashTable.getPolicy().m_data.isInitialized()) {
        DatatypeID datatypeID;
        size_t dataSize;
        const uint8_t* data;
        m_hashTable.getPolicy().dereference(resourceID, datatypeID, dataSize, data);
        resourceValue.setDataRaw(datatypeID, data, dataSize);
        return true;
    }
    else
        return false;
}

bool ResourceValueCache::getResource(const ResourceID resourceID, ResourceType& resourceType, std::string& lexicalForm, std::string& datatypeIRI) const {
    if (isPermanentValue(resourceID))
        return m_dictionary.getResource(resourceID, resourceType, lexicalForm, datatypeIRI);
    else if (m_hashTable.getPolicy().m_data.isInitialized()) {
        DatatypeID datatypeID;
        size_t dataSize;
        const uint8_t* data;
        m_hashTable.getPolicy().dereference(resourceID, datatypeID, dataSize, data);
        Dictionary::toLexicalForm(datatypeID, data, dataSize, lexicalForm);
        switch (datatypeID) {
        case D_IRI_REFERENCE:
            resourceType = IRI_REFERENCE;
            datatypeIRI.clear();
            break;
        case D_BLANK_NODE:
            resourceType = BLANK_NODE;
            datatypeIRI.clear();
            break;
        default:
            resourceType = LITERAL;
            datatypeIRI = Dictionary::getDatatypeIRI(datatypeID);
            break;
        }
        return true;
    }
    else
        return false;
}

bool ResourceValueCache::getResource(const ResourceID resourceID, std::string& lexicalForm, DatatypeID& datatypeID) const {
    if (isPermanentValue(resourceID))
        return m_dictionary.getResource(resourceID, lexicalForm, datatypeID);
    else if (m_hashTable.getPolicy().m_data.isInitialized()) {
        size_t dataSize;
        const uint8_t* data;
        m_hashTable.getPolicy().dereference(resourceID, datatypeID, dataSize, data);
        Dictionary::toLexicalForm(datatypeID, data, dataSize, lexicalForm);
        return true;
    }
    else
        return false;
}

#include "../util/Prefixes.h"

ResourceID ResourceValueCache::tryResolveResource(ThreadContext& threadContext, const ResourceValue& resourceValue) const {
    ResourceID resourceID;
    if (resourceValue.isUndefined())
        resourceID = INVALID_RESOURCE_ID;
    else {
        resourceID = m_dictionary.tryResolveResource(threadContext, resourceValue);
        if (resourceID == INVALID_RESOURCE_ID && m_hashTable.getPolicy().m_data.isInitialized()) {
            SequentialHashTable<Policy>::BucketDescriptor bucketDescriptor;
            m_hashTable.acquireBucket(threadContext, bucketDescriptor, &resourceValue);
            if (m_hashTable.continueBucketSearch(threadContext, bucketDescriptor, &resourceValue) == BUCKET_CONTAINS)
                resourceID = bucketDescriptor.m_bucketContents.m_resourceID;
            else
                resourceID = INVALID_RESOURCE_ID;
            m_hashTable.releaseBucket(threadContext, bucketDescriptor);
        }
    }
    return resourceID;
}

ResourceID ResourceValueCache::resolveResource(ThreadContext& threadContext, const ResourceValue& resourceValue) {
    ResourceID resourceID;
    if (resourceValue.isUndefined())
        resourceID = INVALID_RESOURCE_ID;
    else {
        resourceID = m_dictionary.tryResolveResource(threadContext, resourceValue);
        if (resourceID == INVALID_RESOURCE_ID) {
            if (!m_hashTable.getPolicy().m_data.isInitialized())
                initialize();
            const char* error = nullptr;
            SequentialHashTable<Policy>::BucketDescriptor bucketDescriptor;
            m_hashTable.acquireBucket(threadContext, bucketDescriptor, &resourceValue);
            const BucketStatus bucketStatus = m_hashTable.continueBucketSearch(threadContext, bucketDescriptor, &resourceValue);
            if (bucketStatus == BUCKET_EMPTY) {
                const size_t dataStartLocation = ::alignValue<size_t>(m_nextFreeLocation);
                const size_t nextFreeLocation = dataStartLocation + sizeof(size_t) + resourceValue.getDataSize() + sizeof(DatatypeID);
                if (m_hashTable.getPolicy().m_data.ensureEndAtLeast(nextFreeLocation, 0)) {
                    uint8_t* const dataStart = m_hashTable.getPolicy().m_data.getData() + dataStartLocation;
                    *reinterpret_cast<size_t*>(dataStart) = resourceValue.getDataSize();
                    ::memcpy(dataStart + sizeof(size_t), resourceValue.getDataRaw(), resourceValue.getDataSize());
                    *reinterpret_cast<DatatypeID*>(dataStart + sizeof(size_t) + resourceValue.getDataSize()) = resourceValue.getDatatypeID();
                    m_nextFreeLocation = nextFreeLocation;
                    resourceID = dataStartLocation | CACHED_RESOURCE_ID_MASK;
                    m_hashTable.getPolicy().setResourceID(bucketDescriptor.m_bucket, resourceID);
                    m_hashTable.acknowledgeInsert(threadContext, bucketDescriptor);
                }
                else
                    error = "The data pool of the ResourceValueCache is full.";
            }
            else if (bucketStatus == BUCKET_CONTAINS)
                resourceID = bucketDescriptor.m_bucketContents.m_resourceID;
            else
                error = "The hash table of the ResourceValueCache is full.";
            m_hashTable.releaseBucket(threadContext, bucketDescriptor);
            if (error)
                throw RDF_STORE_EXCEPTION(error);
        }
    }
    return resourceID;
}


#endif /* RESOURCEVALUECACHEIMPL_H_ */
