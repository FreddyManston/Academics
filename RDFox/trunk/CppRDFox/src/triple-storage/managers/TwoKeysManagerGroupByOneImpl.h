// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef TWOKEYSMANAGERGROUPBYONEIMPL_H_
#define TWOKEYSMANAGERGROUPBYONEIMPL_H_

#include "../../storage/Parameters.h"
#include "../../util/ComponentStatistics.h"
#include "../../util/InputStream.h"
#include "../../util/OutputStream.h"
#include "../../util/SequentialHashTableImpl.h"
#include "TwoKeysManagerGroupByOne.h"

// TwoKeysManagerGroupByOneProxy::OneKeyIndexProxyPolicy

template<class TwoKeysManager>
always_inline TwoKeysManagerGroupByOneProxy<TwoKeysManager>::OneKeyIndexProxyPolicy::OneKeyIndexProxyPolicy(TripleListType& tripleList) : m_tripleList(tripleList) {
}

template<class TwoKeysManager>
always_inline void TwoKeysManagerGroupByOneProxy<TwoKeysManager>::OneKeyIndexProxyPolicy::getBucketContents(const uint8_t* const bucket, BucketContents& bucketContents) {
    bucketContents.m_headTripleIndex = getHeadTripleIndex(bucket);
}

template<class TwoKeysManager>
always_inline BucketStatus TwoKeysManagerGroupByOneProxy<TwoKeysManager>::OneKeyIndexProxyPolicy::getBucketContentsStatus(const BucketContents& bucketContents, const size_t valuesHashCode, const ResourceID value) const {
    if (bucketContents.m_headTripleIndex == INVALID_TUPLE_INDEX)
        return BUCKET_EMPTY;
    else {
        const ResourceID tripleValue = m_tripleList.getResourceID(bucketContents.m_headTripleIndex, COMPONENT1);
        if (tripleValue == value)
            return BUCKET_CONTAINS;
        else
            return BUCKET_NOT_CONTAINS;
    }
}

template<class TwoKeysManager>
always_inline bool TwoKeysManagerGroupByOneProxy<TwoKeysManager>::OneKeyIndexProxyPolicy::setBucketContentsIfEmpty(uint8_t* const bucket, const BucketContents& bucketContents) {
    if (getHeadTripleIndex(bucket) == INVALID_TUPLE_INDEX) {
        setHeadTripleIndex(bucket, bucketContents.m_headTripleIndex);
        return true;
    }
    else
        return false;
}

template<class TwoKeysManager>
always_inline bool TwoKeysManagerGroupByOneProxy<TwoKeysManager>::OneKeyIndexProxyPolicy::isBucketContentsEmpty(const BucketContents& bucketContents) {
    return bucketContents.m_headTripleIndex == INVALID_TUPLE_INDEX;
}

template<class TwoKeysManager>
always_inline size_t TwoKeysManagerGroupByOneProxy<TwoKeysManager>::OneKeyIndexProxyPolicy::getBucketContentsHashCode(const BucketContents& bucketContents) const {
    const ResourceID value = m_tripleList.getResourceID(bucketContents.m_headTripleIndex, COMPONENT1);
    return hashCodeFor(value);
}

template<class TwoKeysManager>
always_inline size_t TwoKeysManagerGroupByOneProxy<TwoKeysManager>::OneKeyIndexProxyPolicy::hashCodeFor(const ResourceID value) {
    return static_cast<size_t>(value) ^ ((static_cast<size_t>(value) >> 32) | (static_cast<size_t>(value) << 32));
}

template<class TwoKeysManager>
always_inline TupleIndex TwoKeysManagerGroupByOneProxy<TwoKeysManager>::OneKeyIndexProxyPolicy::getHeadTripleIndex(const uint8_t* const bucket) {
    return static_cast<TupleIndex>(*reinterpret_cast<const StoreTripleIndexType*>(bucket));
}

template<class TwoKeysManager>
always_inline void TwoKeysManagerGroupByOneProxy<TwoKeysManager>::OneKeyIndexProxyPolicy::setHeadTripleIndex(uint8_t* const bucket, const TupleIndex tupleIndex) {
    *reinterpret_cast<StoreTripleIndexType*>(bucket) = static_cast<StoreTripleIndexType>(tupleIndex);
}

// TwoKeysManagerGroupByOneProxy

template<class TwoKeysManager>
always_inline TwoKeysManagerGroupByOneProxy<TwoKeysManager>::TwoKeysManagerGroupByOneProxy(TwoKeysManagerType& twoKeysManager, const Parameters& dataStoreParameters) :
    m_proxyArrayThreshold(static_cast<ResourceID>(dataStoreParameters.getNumber("proxyArrayThreshold", 10000, 10000))),
    m_proxyHashTableThreshold(static_cast<size_t>(dataStoreParameters.getNumber("proxyHashTableThreshold", 500, static_cast<size_t>(-1)))),
    m_twoKeysManager(twoKeysManager),
    m_tripleIndexes(twoKeysManager.getMemoryManager()),
    m_oneKeyIndexProxy(twoKeysManager.getMemoryManager(), HASH_TABLE_LOAD_FACTOR, twoKeysManager.getTripleList())
{
}

template<class TwoKeysManager>
always_inline typename TwoKeysManagerGroupByOneProxy<TwoKeysManager>::TripleListType& TwoKeysManagerGroupByOneProxy<TwoKeysManager>::getTripleList() {
    return m_oneKeyIndexProxy.getPolicy().m_tripleList;
}

template<class TwoKeysManager>
always_inline bool TwoKeysManagerGroupByOneProxy<TwoKeysManager>::initialize() {
    return (m_proxyArrayThreshold == 0 ? true : m_tripleIndexes.initialize(m_proxyArrayThreshold)) && (m_proxyHashTableThreshold == static_cast<size_t>(-1) ? true : m_oneKeyIndexProxy.initialize(HASH_TABLE_INITIAL_SIZE));
}

template<class TwoKeysManager>
always_inline bool TwoKeysManagerGroupByOneProxy<TwoKeysManager>::insertTriple(ThreadContext& threadContext, const TupleIndex insertedTripleIndex, const ResourceID insertedS, const ResourceID insertedP, const ResourceID insertedO) {
    bool success = false;
    const ResourceID value = ::getTripleComponent<COMPONENT1>(insertedS, insertedP, insertedO);
    if (value < m_proxyArrayThreshold) {
        if (m_tripleIndexes.ensureEndAtLeast(value, 1)) {
            const TupleIndex currentHeadTripleIndex = static_cast<TupleIndex>(m_tripleIndexes[value]);
            if (currentHeadTripleIndex == INVALID_TUPLE_INDEX) {
                if (m_twoKeysManager.insertTripleInternal(threadContext, value, insertedTripleIndex, insertedS, insertedP, insertedO)) {
                    m_tripleIndexes[value] = static_cast<StoreTripleIndexType>(insertedTripleIndex);
                    success = true;
                }
            }
            else {
                const TupleIndex afterHeadTripleIndex = getTripleList().getNext(currentHeadTripleIndex, COMPONENT1);
                getTripleList().setNext(insertedTripleIndex, COMPONENT1, afterHeadTripleIndex);
                getTripleList().setNext(currentHeadTripleIndex, COMPONENT1, insertedTripleIndex);
                success = true;
            }
        }
    }
    else if (m_proxyHashTableThreshold != static_cast<size_t>(-1) && m_twoKeysManager.m_oneKeyIndex.getTripleCount(value) >= m_proxyHashTableThreshold) {
        typename OneKeyIndexProxyType::BucketDescriptor bucketDescriptor;
        m_oneKeyIndexProxy.acquireBucket(threadContext, bucketDescriptor, value);
        if (m_oneKeyIndexProxy.continueBucketSearch(threadContext, bucketDescriptor, value) == BUCKET_EMPTY) {
            if (m_twoKeysManager.insertTripleInternal(threadContext, value, insertedTripleIndex, insertedS, insertedP, insertedO)) {
                m_oneKeyIndexProxy.getPolicy().setHeadTripleIndex(bucketDescriptor.m_bucket, insertedTripleIndex);
                m_oneKeyIndexProxy.acknowledgeInsert(threadContext, bucketDescriptor);
                success = true;
            }
        }
        else {
            const TupleIndex headTripleIndex = m_oneKeyIndexProxy.getPolicy().getHeadTripleIndex(bucketDescriptor.m_bucket);
            const TupleIndex afterHeadTripleIndex = getTripleList().getNext(headTripleIndex, COMPONENT1);
            getTripleList().setNext(insertedTripleIndex, COMPONENT1, afterHeadTripleIndex);
            getTripleList().setNext(headTripleIndex, COMPONENT1, insertedTripleIndex);
            success = true;
        }
        m_oneKeyIndexProxy.releaseBucket(threadContext, bucketDescriptor);
    }
    else {
        if (m_twoKeysManager.insertTripleInternal(threadContext, value, insertedTripleIndex, insertedS, insertedP, insertedO))
            success = true;
    }
    return success;
}

template<class TwoKeysManager>
always_inline std::unique_ptr<ComponentStatistics> TwoKeysManagerGroupByOneProxy<TwoKeysManager>::getComponentStatistics() const {
    std::stringstream name;
    name << "TwoKeysManagerGroupByOneProxy[" << getResourceComponentName(COMPONENT1) << '.' << getResourceComponentName(COMPONENT2) << ']';
    std::unique_ptr<ComponentStatistics> result(new ComponentStatistics(name.str()));
    const size_t endIndex = m_tripleIndexes.getEndIndex();
    result->addIntegerItem("Total number of entries", endIndex);
    result->addIntegerItem("Size", endIndex * sizeof(StoreTripleIndexType));
    return result;
}

// TwoKeysManagerGroupByOne

template<class TwoKeysManagerConfiguration>
always_inline TwoKeysManagerGroupByOne<TwoKeysManagerConfiguration>::TwoKeysManagerGroupByOne(MemoryManager& memoryManager, TripleListType& tripleList, const Parameters& dataStoreParameters) :
    m_tripleList(tripleList),
    m_oneKeyIndex(memoryManager)
{
}

template<class TwoKeysManagerConfiguration>
always_inline typename TwoKeysManagerGroupByOne<TwoKeysManagerConfiguration>::TripleListType& TwoKeysManagerGroupByOne<TwoKeysManagerConfiguration>::getTripleList() {
    return m_tripleList;
}

template<class TwoKeysManagerConfiguration>
always_inline const typename TwoKeysManagerGroupByOne<TwoKeysManagerConfiguration>::TripleListType& TwoKeysManagerGroupByOne<TwoKeysManagerConfiguration>::getTripleList() const {
    return m_tripleList;
}

template<class TwoKeysManagerConfiguration>
always_inline MemoryManager& TwoKeysManagerGroupByOne<TwoKeysManagerConfiguration>::getMemoryManager() const {
    return m_oneKeyIndex.getMemoryManager();
}

template<class TwoKeysManagerConfiguration>
always_inline bool TwoKeysManagerGroupByOne<TwoKeysManagerConfiguration>::initialize(const size_t initialTripleCapacity, const size_t initialResourceCapacity) {
    return m_oneKeyIndex.initialize() && (initialResourceCapacity == 0 || m_oneKeyIndex.extendToResourceID(static_cast<ResourceID>(initialResourceCapacity + 1)));
}

template<class TwoKeysManagerConfiguration>
always_inline void TwoKeysManagerGroupByOne<TwoKeysManagerConfiguration>::setNumberOfThreads(const size_t numberOfThreads) {
}

template<class TwoKeysManagerConfiguration>
always_inline bool TwoKeysManagerGroupByOne<TwoKeysManagerConfiguration>::insertTriple(ThreadContext& threadContext, const TupleIndex insertedTripleIndex, const ResourceID insertedS, const ResourceID insertedP, const ResourceID insertedO) {
    const ResourceID insertedResourceID = ::getTripleComponent<COMPONENT1>(insertedS, insertedP, insertedO);
    return insertTripleInternal(threadContext, insertedResourceID, insertedTripleIndex, insertedS, insertedP, insertedO);
}

template<class TwoKeysManagerConfiguration>
always_inline bool TwoKeysManagerGroupByOne<TwoKeysManagerConfiguration>::insertTripleInternal(ThreadContext& threadContext, const ResourceID insertedResourceID, const TupleIndex insertedTripleIndex, const ResourceID insertedS, const ResourceID insertedP, const ResourceID insertedO) {
    if (m_oneKeyIndex.extendToResourceID(insertedResourceID)) {
        TupleIndex headTripleIndex;
        do {
            headTripleIndex = m_oneKeyIndex.getHeadTripleIndex(insertedResourceID);
            m_tripleList.setNext(insertedTripleIndex, COMPONENT1, headTripleIndex);
        } while (!m_oneKeyIndex.setHeadTripleIndexConditional(insertedResourceID, headTripleIndex, insertedTripleIndex));
        return true;
    }
    return false;
}

template<class TwoKeysManagerConfiguration>
always_inline TupleIndex TwoKeysManagerGroupByOne<TwoKeysManagerConfiguration>::getFirstTripleIndex1(ThreadContext& threadContext, const ResourceID s, const ResourceID p, const ResourceID o) const {
    return m_oneKeyIndex.getHeadTripleIndex(::getTripleComponent<COMPONENT1>(s, p, o));
}

template<class TwoKeysManagerConfiguration>
always_inline TupleIndex TwoKeysManagerGroupByOne<TwoKeysManagerConfiguration>::getFirstTripleIndex12(ThreadContext& threadContext, const ResourceID s, const ResourceID p, const ResourceID o, ResourceID& compareResourceID, TupleIndex& compareGroupedMask) const {
    compareResourceID = ::getTripleComponent<COMPONENT2>(s, p, o);
    compareGroupedMask = NOT_GROUPED_MASK;
    return m_oneKeyIndex.getHeadTripleIndex(::getTripleComponent<COMPONENT1>(s, p, o));
}

template<class TwoKeysManagerConfiguration>
always_inline TupleIndex TwoKeysManagerGroupByOne<TwoKeysManagerConfiguration>::getFirstTripleIndex13(ThreadContext& threadContext, const ResourceID s, const ResourceID p, const ResourceID o, ResourceID& compareResourceID, TupleIndex& compareGroupedMask) const {
    compareResourceID = ::getTripleComponent<COMPONENT3>(s, p, o);
    compareGroupedMask = NOT_GROUPED_MASK;
    return m_oneKeyIndex.getHeadTripleIndex(::getTripleComponent<COMPONENT1>(s, p, o));
}

template<class TwoKeysManagerConfiguration>
always_inline size_t TwoKeysManagerGroupByOne<TwoKeysManagerConfiguration>::getCountEstimate1(ThreadContext& threadContext, const ResourceID s, const ResourceID p, const ResourceID o) const {
    ResourceID value1 = ::getTripleComponent<COMPONENT1>(s, p, o);
    return m_oneKeyIndex.getTripleCount(value1);
}

template<class TwoKeysManagerConfiguration>
always_inline size_t TwoKeysManagerGroupByOne<TwoKeysManagerConfiguration>::getCountEstimate12(ThreadContext& threadContext, const ResourceID s, const ResourceID p, const ResourceID o) const {
    ResourceID value1 = ::getTripleComponent<COMPONENT1>(s, p, o);
    return m_oneKeyIndex.getTripleCount(value1);
}

template<class TwoKeysManagerConfiguration>
always_inline size_t TwoKeysManagerGroupByOne<TwoKeysManagerConfiguration>::getCountEstimate13(ThreadContext& threadContext, const ResourceID s, const ResourceID p, const ResourceID o) const {
    ResourceID value1 = ::getTripleComponent<COMPONENT1>(s, p, o);
    return m_oneKeyIndex.getTripleCount(value1);
}

template<class TwoKeysManagerConfiguration>
always_inline void TwoKeysManagerGroupByOne<TwoKeysManagerConfiguration>::startUpdatingStatistics() {
    if (!m_oneKeyIndex.clearCounts())
        throw RDF_STORE_EXCEPTION("Cannot clear counts of a one-key index.");
}

template<class TwoKeysManagerConfiguration>
always_inline void TwoKeysManagerGroupByOne<TwoKeysManagerConfiguration>::updateStatisticsFor(const ResourceID s, const ResourceID p, const ResourceID o) {
    ResourceID value1 = ::getTripleComponent<COMPONENT1>(s, p, o);
    m_oneKeyIndex.incrementTripleCount(value1);
}

template<class TwoKeysManagerConfiguration>
always_inline void TwoKeysManagerGroupByOne<TwoKeysManagerConfiguration>::save(OutputStream& outputStream) const {
    std::stringstream name;
    name << "TwoKeysManagerGroupByOne[" << getResourceComponentName(COMPONENT1) << '.' << getResourceComponentName(COMPONENT2) << ']';
    outputStream.writeString(name.str());
    m_oneKeyIndex.save(outputStream);
}

template<class TwoKeysManagerConfiguration>
always_inline void TwoKeysManagerGroupByOne<TwoKeysManagerConfiguration>::load(InputStream& inputStream) {
    std::stringstream name;
    name << "TwoKeysManagerGroupByOne[" << getResourceComponentName(COMPONENT1) << '.' << getResourceComponentName(COMPONENT2) << ']';
    std::string nameString(name.str());
    if (!inputStream.checkNextString(nameString.c_str()))
        throw RDF_STORE_EXCEPTION("Invalid input file: cannot load " + name.str() + ".");
    m_oneKeyIndex.load(inputStream);
}

template<class TwoKeysManagerConfiguration>
always_inline void TwoKeysManagerGroupByOne<TwoKeysManagerConfiguration>::printContents(std::ostream& output) const {
    output << "TwoKeysManagerGroupByOne[" << getResourceComponentName(COMPONENT1) << '.' << getResourceComponentName(COMPONENT2) << ']' << std::endl;
    m_oneKeyIndex.printContents(output);
}

template<class TwoKeysManagerConfiguration>
always_inline std::unique_ptr<ComponentStatistics> TwoKeysManagerGroupByOne<TwoKeysManagerConfiguration>::getComponentStatistics() const {
    std::stringstream name;
    name << "TwoKeysManagerGroupByOne[" << getResourceComponentName(COMPONENT1) << '.' << getResourceComponentName(COMPONENT2) << ']';
    std::unique_ptr<ComponentStatistics> result(new ComponentStatistics(name.str()));
    std::unique_ptr<ComponentStatistics> oneKeyIndexStatistics = m_oneKeyIndex.getComponentStatistics();
    uint64_t aggregateSize = oneKeyIndexStatistics->getItemIntegerValue("Size");
    result->addIntegerItem("Aggregate size", aggregateSize);
    const size_t tripleCount = m_tripleList.getExactTripleCount(TUPLE_STATUS_COMPLETE, TUPLE_STATUS_COMPLETE);
    if (tripleCount != 0)
        result->addFloatingPointItem("Bytes per triple", aggregateSize / static_cast<double>(tripleCount));
    result->addSubcomponent(std::move(oneKeyIndexStatistics));
    return result;
}

#endif /* TWOKEYSMANAGERGROUPBYONEIMPL_H_ */
