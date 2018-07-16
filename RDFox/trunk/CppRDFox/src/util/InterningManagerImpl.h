// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef INTERNINGMANAGERIMPL_H_
#define INTERNINGMANAGERIMPL_H_

#include "ThreadContext.h"
#include "SequentialHashTableImpl.h"
#include "InterningManager.h"

class MemoryManager;

// InterningManager::Policy

template<class T, class Derived>
always_inline void InterningManager<T, Derived>::Policy::getBucketContents(const uint8_t* const bucket, BucketContents& bucketContents) {
    bucketContents.m_object = const_cast<T*>(getObject(bucket));
}

template<class T, class Derived>
template<typename... Args>
always_inline BucketStatus InterningManager<T, Derived>::Policy::getBucketContentsStatus(const BucketContents& bucketContents, const size_t valuesHashCode, Args&&... args) {
    if (bucketContents.m_object == nullptr)
        return BUCKET_EMPTY;
    else
        return Derived::isEqual(bucketContents.m_object, valuesHashCode, std::forward<Args>(args)...) ? BUCKET_CONTAINS : BUCKET_NOT_CONTAINS;
}

template<class T, class Derived>
always_inline  BucketStatus InterningManager<T, Derived>::Policy::getBucketContentsStatus(const BucketContents& bucketContents, const size_t valuesHashCode, const T* const object, EqualityMarker equalityMarker) {
    if (bucketContents.m_object == nullptr)
        return BUCKET_EMPTY;
    else
        return bucketContents.m_object == object ? BUCKET_CONTAINS : BUCKET_NOT_CONTAINS;
}

template<class T, class Derived>
always_inline bool InterningManager<T, Derived>::Policy::setBucketContentsIfEmpty(uint8_t* const bucket, const BucketContents& bucketContents) {
    if (getObject(bucket) == nullptr) {
        setObject(bucket, bucketContents.m_object);
        return true;
    }
    else
        return false;
}

template<class T, class Derived>
always_inline bool InterningManager<T, Derived>::Policy::isBucketContentsEmpty(const BucketContents& bucketContents) {
    return bucketContents.m_object == nullptr;
}

template<class T, class Derived>
always_inline size_t InterningManager<T, Derived>::Policy::getBucketContentsHashCode(const BucketContents& bucketContents) {
    return Derived::getObjectHashCode(bucketContents.m_object);
}

template<class T, class Derived>
template<typename... Args>
always_inline size_t InterningManager<T, Derived>::Policy::hashCodeFor(Args&&... args) {
    return Derived::hashCodeFor(std::forward<Args>(args)...);
}

template<class T, class Derived>
always_inline size_t InterningManager<T, Derived>::Policy::hashCodeFor(const T* const object, EqualityMarker equalityMarker) {
    return Derived::getObjectHashCode(object);
}

template<class T, class Derived>
always_inline void InterningManager<T, Derived>::Policy::makeBucketEmpty(uint8_t* const bucket) {
    setObject(bucket, nullptr);
}

template<class T, class Derived>
always_inline bool InterningManager<T, Derived>::Policy::startBucketInsertionConditional(uint8_t* const bucket) {
    return true;
}

template<class T, class Derived>
always_inline const T* InterningManager<T, Derived>::Policy::getObject(const uint8_t* const bucket) {
    return *reinterpret_cast<const T* const*>(bucket);
}

template<class T, class Derived>
always_inline void InterningManager<T, Derived>::Policy::setObject(uint8_t* const bucket, const T* object) {
    *reinterpret_cast<const T**>(bucket) = object;
}

// InterningManager

template<class T, class Derived>
always_inline InterningManager<T, Derived>::InterningManager(MemoryManager& memoryManager) : m_elements(memoryManager, HASH_TABLE_LOAD_FACTOR) {

}

template<class T, class Derived>
always_inline bool InterningManager<T, Derived>::initialize() {
    return m_elements.initialize(HASH_TABLE_INITIAL_SIZE);
}

template<class T, class Derived>
always_inline void InterningManager<T, Derived>::dispose(const T* const object) {
    ThreadContext& threadContext = ThreadContext::getCurrentThreadContext();
    typename HashTableType::BucketDescriptor bucketDescriptor;
    m_elements.acquireBucket(threadContext, bucketDescriptor, object, EqualityMarker());
    const BucketStatus bucketStatus = m_elements.continueBucketSearch(threadContext, bucketDescriptor, object, EqualityMarker());
    assert(bucketStatus == BUCKET_CONTAINS);
    // The following removes the "unused variable" warning from release builds.
    (void)(bucketStatus);
    m_elements.deleteBucket(threadContext, bucketDescriptor);
    m_elements.releaseBucket(threadContext, bucketDescriptor);
}

template<class T, class Derived>
template<typename... Args>
always_inline T* InterningManager<T, Derived>::get(Args&&... args) {
    ThreadContext& threadContext = ThreadContext::getCurrentThreadContext();
    typename HashTableType::BucketDescriptor bucketDescriptor;
    m_elements.acquireBucket(threadContext, bucketDescriptor, std::forward<Args>(args)...);
    BucketStatus bucketStatus;
    while ((bucketStatus = m_elements.continueBucketSearch(threadContext, bucketDescriptor, std::forward<Args>(args)...)) == BUCKET_EMPTY) {
        if (m_elements.getPolicy().startBucketInsertionConditional(bucketDescriptor.m_bucket))
            break;
    }
    if (bucketStatus == BUCKET_EMPTY) {
        bucketDescriptor.m_bucketContents.m_object = static_cast<Derived*>(this)->makeNew(bucketDescriptor.m_hashCode, std::forward<Args>(args)...);
        m_elements.getPolicy().setObject(bucketDescriptor.m_bucket, bucketDescriptor.m_bucketContents.m_object);
        m_elements.acknowledgeInsert(threadContext, bucketDescriptor);
    }
    m_elements.releaseBucket(threadContext, bucketDescriptor);
    if (bucketStatus == BUCKET_NOT_CONTAINS)
        throw RDF_STORE_EXCEPTION("Out of memory.");
    else
        return bucketDescriptor.m_bucketContents.m_object;
}

template<class T, class Derived>
always_inline size_t InterningManager<T, Derived>::getNumberOfObjects() const {
    return m_elements.getNumberOfUsedBuckets();
}

#endif /* INTERNINGMANAGERIMPL_H_ */
