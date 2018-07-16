// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef RESOURCEIDMAPPERIMPL_H_
#define RESOURCEIDMAPPERIMPL_H_

#include "../RDFStoreException.h"
#include "../util/SequentialHashTableImpl.h"
#include "ResourceIDMapper.h"

// ResourceIDMapper::Policy

always_inline void ResourceIDMapper::Policy::getBucketContents(const uint8_t* const bucket, BucketContents& bucketContents) {
    bucketContents.m_globalResourceID = getGlobalResourceID(bucket);
    bucketContents.m_localResourceID = getLocalResourceID(bucket);
}

always_inline BucketStatus ResourceIDMapper::Policy::getBucketContentsStatus(const BucketContents& bucketContents, const size_t valuesHashCode, const ResourceID globalResourceID) {
    if (bucketContents.m_globalResourceID == INVALID_RESOURCE_ID)
        return BUCKET_EMPTY;
    else
        return bucketContents.m_globalResourceID == globalResourceID ? BUCKET_CONTAINS : BUCKET_NOT_CONTAINS;
}

always_inline bool ResourceIDMapper::Policy::isBucketContentsEmpty(const BucketContents& bucketContents) {
    return bucketContents.m_globalResourceID == INVALID_RESOURCE_ID;
}

always_inline size_t ResourceIDMapper::Policy::getBucketContentsHashCode(const BucketContents& bucketContents) {
    return hashCodeFor(bucketContents.m_globalResourceID);
}

always_inline bool ResourceIDMapper::Policy::setBucketContentsIfEmpty(uint8_t* const bucket, const BucketContents& bucketContents) {
    if (getGlobalResourceID(bucket) == INVALID_RESOURCE_ID) {
        setGlobalResourceID(bucket, bucketContents.m_globalResourceID);
        setLocalResourceID(bucket, bucketContents.m_localResourceID);
        return true;
    }
    else
        return false;
}

always_inline size_t ResourceIDMapper::Policy::hashCodeFor(const ResourceID globalResourceID) {
    return globalResourceID * 2654435761;
}

always_inline int64_t ResourceIDMapper::Policy::getGlobalResourceID(const uint8_t* const bucket) {
    return *reinterpret_cast<const ResourceID*>(bucket);
}

always_inline void ResourceIDMapper::Policy::setGlobalResourceID(uint8_t* const bucket, const ResourceID globalResourceID) {
    *reinterpret_cast<ResourceID*>(bucket) = globalResourceID;
}

always_inline ResourceID ResourceIDMapper::Policy::getLocalResourceID(const uint8_t* const bucket) {
    return *reinterpret_cast<const ResourceID*>(bucket + sizeof(ResourceID));
}

always_inline void ResourceIDMapper::Policy::setLocalResourceID(uint8_t* const bucket, const ResourceID localResourceID) {
    *reinterpret_cast<ResourceID*>(bucket + sizeof(ResourceID)) = localResourceID;
}

// ResourceIDMapper

always_inline void ResourceIDMapper::addMapping(ThreadContext& threadContext, const ResourceID localResourceID, const ResourceID globalResourceID) {
    if (!m_localToGlobal.ensureEndAtLeast(localResourceID, 1))
        throw RDF_STORE_EXCEPTION("Out of memory in ResourceIDMapper.");
    m_localToGlobal[localResourceID] = globalResourceID;
    typename SequentialHashTable<Policy>::BucketDescriptor bucketDescriptor;
    m_globalToLocal.acquireBucket(threadContext, bucketDescriptor, globalResourceID);
    const BucketStatus bucketStatus = m_globalToLocal.continueBucketSearch(threadContext, bucketDescriptor, globalResourceID);
    if (bucketStatus == BUCKET_EMPTY) {
        m_globalToLocal.getPolicy().setGlobalResourceID(bucketDescriptor.m_bucket, globalResourceID);
        m_globalToLocal.getPolicy().setLocalResourceID(bucketDescriptor.m_bucket, localResourceID);
        m_globalToLocal.acknowledgeInsert(threadContext, bucketDescriptor);
        m_globalToLocal.releaseBucket(threadContext, bucketDescriptor);
        return;
    }
    m_globalToLocal.releaseBucket(threadContext, bucketDescriptor);
    if (bucketStatus == BUCKET_CONTAINS) {
        std::ostringstream message;
        message << "ResourceIDMapper already contains a mapping for global resource ID " << globalResourceID << "." << std::endl;
        throw RDF_STORE_EXCEPTION(message.str());
    }
    else
        throw RDF_STORE_EXCEPTION("The hash table of the resource ID mapper is full.");
}

always_inline ResourceID ResourceIDMapper::getLocalResourceID(ThreadContext& threadContext, const ResourceID globalResourceID) const {
    typename SequentialHashTable<Policy>::BucketDescriptor bucketDescriptor;
    m_globalToLocal.acquireBucket(threadContext, bucketDescriptor, globalResourceID);
    const BucketStatus bucketStatus = m_globalToLocal.continueBucketSearch(threadContext, bucketDescriptor, globalResourceID);
    m_globalToLocal.releaseBucket(threadContext, bucketDescriptor);
    return bucketStatus == BUCKET_CONTAINS ? bucketDescriptor.m_bucketContents.m_localResourceID : INVALID_RESOURCE_ID;
}

always_inline ResourceID ResourceIDMapper::getGlobalResourceID(const ResourceID localResourceID) const {
    if (m_localToGlobal.isBeforeEnd(localResourceID, 1))
        return m_localToGlobal[localResourceID];
    else
        return INVALID_RESOURCE_ID;
}

#endif // RESOURCEIDMAPPERIMPL_H_
