// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef TWOKEYSMANAGERGROUPBYTWOIMPL_H_
#define TWOKEYSMANAGERGROUPBYTWOIMPL_H_

#include "../../storage/Parameters.h"
#include "../../util/ComponentStatistics.h"
#include "../../util/InputStream.h"
#include "../../util/OutputStream.h"
#include "../../util/SequentialHashTableImpl.h"
#include "TwoKeysManagerGroupByTwo.h"

// TwoKeysManagerGroupByTwoProxy::TwoKeysIndexProxyPolicy

template<class TwoKeysManager>
always_inline TwoKeysManagerGroupByTwoProxy<TwoKeysManager>::TwoKeysIndexProxyPolicy::TwoKeysIndexProxyPolicy(TripleListType& tripleList) : m_tripleList(tripleList) {
}

template<class TwoKeysManager>
always_inline void TwoKeysManagerGroupByTwoProxy<TwoKeysManager>::TwoKeysIndexProxyPolicy::getBucketContents(const uint8_t* const bucket, BucketContents& bucketContents) {
    bucketContents.m_headTripleIndex = getHeadTripleIndex(bucket);
}

template<class TwoKeysManager>
always_inline BucketStatus TwoKeysManagerGroupByTwoProxy<TwoKeysManager>::TwoKeysIndexProxyPolicy::getBucketContentsStatus(const BucketContents& bucketContents, const size_t valuesHashCode, const ResourceID value1, const ResourceID value2) const {
    if (bucketContents.m_headTripleIndex == INVALID_TUPLE_INDEX)
        return BUCKET_EMPTY;
    else {
        ResourceID tripleValue1;
        ResourceID tripleValue2;
        m_tripleList.getResourceIDs(bucketContents.m_headTripleIndex, COMPONENT1, COMPONENT2, tripleValue1, tripleValue2);
        if (tripleValue1 == value1 && tripleValue2 == value2)
            return BUCKET_CONTAINS;
        else
            return BUCKET_NOT_CONTAINS;
    }
}

template<class TwoKeysManager>
always_inline bool TwoKeysManagerGroupByTwoProxy<TwoKeysManager>::TwoKeysIndexProxyPolicy::setBucketContentsIfEmpty(uint8_t* const bucket, const BucketContents& bucketContents) {
    if (getHeadTripleIndex(bucket) == INVALID_TUPLE_INDEX) {
        setHeadTripleIndex(bucket, bucketContents.m_headTripleIndex);
        return true;
    }
    else
        return false;
}

template<class TwoKeysManager>
always_inline bool TwoKeysManagerGroupByTwoProxy<TwoKeysManager>::TwoKeysIndexProxyPolicy::isBucketContentsEmpty(const BucketContents& bucketContents) {
    return bucketContents.m_headTripleIndex == INVALID_TUPLE_INDEX;
}

template<class TwoKeysManager>
always_inline size_t TwoKeysManagerGroupByTwoProxy<TwoKeysManager>::TwoKeysIndexProxyPolicy::getBucketContentsHashCode(const BucketContents& bucketContents) const {
    ResourceID value1;
    ResourceID value2;
    m_tripleList.getResourceIDs(bucketContents.m_headTripleIndex, COMPONENT1, COMPONENT2, value1, value2);
    return hashCodeFor(value1, value2);
}

template<class TwoKeysManager>
always_inline size_t TwoKeysManagerGroupByTwoProxy<TwoKeysManager>::TwoKeysIndexProxyPolicy::hashCodeFor(const ResourceID value1, const ResourceID value2) {
    size_t hash = 0;
    hash += value1;
    hash += (hash << 10);
    hash ^= (hash >> 6);

    hash += value2;
    hash += (hash << 10);
    hash ^= (hash >> 6);

    hash += (hash << 3);
    hash ^= (hash >> 11);
    hash += (hash << 15);
    return hash;
}

template<class TwoKeysManager>
always_inline TupleIndex TwoKeysManagerGroupByTwoProxy<TwoKeysManager>::TwoKeysIndexProxyPolicy::getHeadTripleIndex(const uint8_t* const bucket) {
    return static_cast<TupleIndex>(*reinterpret_cast<const StoreTripleIndexType*>(bucket));
}

template<class TwoKeysManager>
always_inline void TwoKeysManagerGroupByTwoProxy<TwoKeysManager>::TwoKeysIndexProxyPolicy::setHeadTripleIndex(uint8_t* const bucket, const TupleIndex tupleIndex) {
    *reinterpret_cast<StoreTripleIndexType*>(bucket) = static_cast<StoreTripleIndexType>(tupleIndex);
}

// TwoKeysManagerGroupByTwoProxy

template<class TwoKeysManager>
always_inline TwoKeysManagerGroupByTwoProxy<TwoKeysManager>::TwoKeysManagerGroupByTwoProxy(TwoKeysManagerType& twoKeysManager, const Parameters& dataStoreParameters) :
    m_proxyArrayThreshold(static_cast<ResourceID>(dataStoreParameters.getNumber("proxyArrayThreshold", 10000, 10000))),
    m_proxyHashTableThreshold(static_cast<size_t>(dataStoreParameters.getNumber("proxyHashTableThreshold", 500, static_cast<size_t>(-1)))),
    m_twoKeysManager(twoKeysManager),
    m_rdfTypeTripleIndexes(twoKeysManager.getMemoryManager()),
    m_twoKeysIndexProxy(twoKeysManager.getMemoryManager(), HASH_TABLE_LOAD_FACTOR, twoKeysManager.getTripleList())
{
}

template<class TwoKeysManager>
always_inline typename TwoKeysManagerGroupByTwoProxy<TwoKeysManager>::TripleListType& TwoKeysManagerGroupByTwoProxy<TwoKeysManager>::getTripleList() {
    return m_twoKeysIndexProxy.getPolicy().m_tripleList;
}

template<class TwoKeysManager>
always_inline bool TwoKeysManagerGroupByTwoProxy<TwoKeysManager>::initialize() {
    return (USES_RDF_TYPE_TRIPLE_INDEXES && m_proxyArrayThreshold != 0 ? m_rdfTypeTripleIndexes.initialize(m_proxyArrayThreshold) : true) && (m_proxyHashTableThreshold == static_cast<size_t>(-1) ? true : m_twoKeysIndexProxy.initialize(HASH_TABLE_INITIAL_SIZE));
}

template<class TwoKeysManager>
always_inline bool TwoKeysManagerGroupByTwoProxy<TwoKeysManager>::insertTriple(ThreadContext& threadContext, const TupleIndex insertedTripleIndex, const ResourceID insertedS, const ResourceID insertedP, const ResourceID insertedO) {
    bool success = false;
    const ResourceID value1 = ::getTripleComponent<COMPONENT1>(insertedS, insertedP, insertedO);
    const ResourceID value2 = ::getTripleComponent<COMPONENT2>(insertedS, insertedP, insertedO);
    if (USES_RDF_TYPE_TRIPLE_INDEXES && insertedP == RDF_TYPE_ID && insertedO < m_proxyArrayThreshold) {
        if (m_rdfTypeTripleIndexes.ensureEndAtLeast(insertedO, 1)) {
            const TupleIndex currentHeadTripleIndex = static_cast<TupleIndex>(m_rdfTypeTripleIndexes[insertedO]);
            if (currentHeadTripleIndex == INVALID_TUPLE_INDEX) {
                bool groupAlreadyExists;
                const size_t hashCode = m_twoKeysIndexProxy.getPolicy().hashCodeFor(value1, value2);
                if (m_twoKeysManager.insertTripleInternal(threadContext, value1, value2, hashCode, insertedTripleIndex, insertedS, insertedP, insertedO, groupAlreadyExists)) {
                    m_rdfTypeTripleIndexes[insertedO] = static_cast<StoreTripleIndexType>(insertedTripleIndex);
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
    else if (m_proxyHashTableThreshold != static_cast<size_t>(-1) && m_twoKeysManager.m_oneKeyIndex.getTripleCount(value1) >= m_proxyHashTableThreshold) {
        typename TwoKeysIndexProxyType::BucketDescriptor bucketDescriptor;
        m_twoKeysIndexProxy.acquireBucket(threadContext, bucketDescriptor, value1, value2);
        const BucketStatus bucketStatus = m_twoKeysIndexProxy.continueBucketSearch(threadContext, bucketDescriptor, value1, value2);
        if (bucketStatus == BUCKET_EMPTY) {
            bool groupAlreadyExists;
            if (m_twoKeysManager.insertTripleInternal(threadContext, value1, value2, bucketDescriptor.m_hashCode, insertedTripleIndex, insertedS, insertedP, insertedO, groupAlreadyExists)) {
                if (groupAlreadyExists) {
                    m_twoKeysIndexProxy.getPolicy().setHeadTripleIndex(bucketDescriptor.m_bucket, insertedTripleIndex);
                    m_twoKeysIndexProxy.acknowledgeInsert(threadContext, bucketDescriptor);
                }
                success = true;
            }
        }
        else if (bucketStatus == BUCKET_CONTAINS) {
            const TupleIndex headTripleIndex = m_twoKeysIndexProxy.getPolicy().getHeadTripleIndex(bucketDescriptor.m_bucket);
            const TupleIndex afterHeadTripleIndex = getTripleList().getNext(headTripleIndex, COMPONENT1);
            getTripleList().setNext(insertedTripleIndex, COMPONENT1, afterHeadTripleIndex);
            getTripleList().setNext(headTripleIndex, COMPONENT1, insertedTripleIndex);
            success = true;
        }
        m_twoKeysIndexProxy.releaseBucket(threadContext, bucketDescriptor);
    }
    else {
        bool groupAlreadyExists;
        const size_t hashCode = m_twoKeysIndexProxy.getPolicy().hashCodeFor(value1, value2);
        success = m_twoKeysManager.insertTripleInternal(threadContext, value1, value2, hashCode, insertedTripleIndex, insertedS, insertedP, insertedO, groupAlreadyExists);
    }
    return success;
}

template<class TwoKeysManager>
always_inline std::unique_ptr<ComponentStatistics> TwoKeysManagerGroupByTwoProxy<TwoKeysManager>::getComponentStatistics() const {
    std::stringstream name;
    name << "TwoKeysManagerGroupByTwoProxy[" << getResourceComponentName(COMPONENT1) << '.' << getResourceComponentName(COMPONENT2) << ']';
    std::unique_ptr<ComponentStatistics> result(new ComponentStatistics(name.str()));
    const size_t twoKeysIndexProxyNumberOfBuckets = m_twoKeysIndexProxy.getNumberOfBuckets();
    const size_t twoKeysIndexProxyNumberOfUsedBuckets = m_twoKeysIndexProxy.getNumberOfUsedBuckets();
    const size_t twoKeysIndexProxySize = twoKeysIndexProxyNumberOfBuckets * m_twoKeysIndexProxy.getPolicy().BUCKET_SIZE;
    result->addIntegerItem("Two keys index -- size", twoKeysIndexProxySize);
    result->addIntegerItem("Two keys index -- total buckets", twoKeysIndexProxyNumberOfBuckets);
    result->addIntegerItem("Two keys index -- used buckets", twoKeysIndexProxyNumberOfUsedBuckets);
    result->addFloatingPointItem("Two keys index -- load factor (%)", (twoKeysIndexProxyNumberOfUsedBuckets * 100.0) / static_cast<double>(twoKeysIndexProxyNumberOfBuckets));
    result->addIntegerItem("Size", twoKeysIndexProxySize);
    return result;
}

// TwoKeysManagerGroupByTwo

template<class TwoKeysManagerConfiguration>
always_inline TwoKeysManagerGroupByTwo<TwoKeysManagerConfiguration>::TwoKeysManagerGroupByTwo(MemoryManager& memoryManager, TripleListType& tripleList, const Parameters& dataStoreParameters) :
    m_oneKeyIndex(memoryManager),
    m_twoKeysIndex(memoryManager, HASH_TABLE_LOAD_FACTOR, tripleList)
{
}

template<class TwoKeysManagerConfiguration>
always_inline typename TwoKeysManagerGroupByTwo<TwoKeysManagerConfiguration>::TripleListType& TwoKeysManagerGroupByTwo<TwoKeysManagerConfiguration>::getTripleList() {
    return m_twoKeysIndex.getPolicy().getTripleList();
}

template<class TwoKeysManagerConfiguration>
always_inline const typename TwoKeysManagerGroupByTwo<TwoKeysManagerConfiguration>::TripleListType& TwoKeysManagerGroupByTwo<TwoKeysManagerConfiguration>::getTripleList() const {
    return m_twoKeysIndex.getPolicy().getTripleList();
}

template<class TwoKeysManagerConfiguration>
always_inline MemoryManager& TwoKeysManagerGroupByTwo<TwoKeysManagerConfiguration>::getMemoryManager() const {
    return m_oneKeyIndex.getMemoryManager();
}

template<class TwoKeysManagerConfiguration>
always_inline bool TwoKeysManagerGroupByTwo<TwoKeysManagerConfiguration>::initialize(const size_t initialTripleCapacity, const size_t initialResourceCapacity) {
    return m_oneKeyIndex.initialize() && (initialResourceCapacity == 0 || m_oneKeyIndex.extendToResourceID(static_cast<ResourceID>(initialResourceCapacity) + 1)) && m_twoKeysIndex.initialize(::getHashTableSize(initialTripleCapacity));
}

template<class TwoKeysManagerConfiguration>
always_inline void TwoKeysManagerGroupByTwo<TwoKeysManagerConfiguration>::setNumberOfThreads(const size_t numberOfThreads) {
    m_twoKeysIndex.setNumberOfThreads(numberOfThreads);
}

template<class TwoKeysManagerConfiguration>
always_inline bool TwoKeysManagerGroupByTwo<TwoKeysManagerConfiguration>::insertTriple(ThreadContext& threadContext, const TupleIndex insertedTripleIndex, const ResourceID insertedS, const ResourceID insertedP, const ResourceID insertedO) {
    const ResourceID value1 = ::getTripleComponent<COMPONENT1>(insertedS, insertedP, insertedO);
    const ResourceID value2 = ::getTripleComponent<COMPONENT2>(insertedS, insertedP, insertedO);
    const size_t hashCode = m_twoKeysIndex.getPolicy().hashCodeFor(value1, value2);
    bool groupAlreadyExists;
    return insertTripleInternal(threadContext, value1, value2, hashCode, insertedTripleIndex, insertedS, insertedP, insertedO, groupAlreadyExists);
}

template<class TwoKeysManagerConfiguration>
always_inline bool TwoKeysManagerGroupByTwo<TwoKeysManagerConfiguration>::insertTripleInternal(ThreadContext& threadContext, const ResourceID value1, const ResourceID value2, const size_t hashCode, const TupleIndex insertedTripleIndex, const ResourceID insertedS, const ResourceID insertedP, const ResourceID insertedO, bool& groupAlreadyExists) {
    bool success = false;
    if (m_oneKeyIndex.extendToResourceID(value1)) {
        typename TwoKeysIndexType::BucketDescriptor bucketDescriptor;
        bucketDescriptor.m_hashCode = hashCode;
        m_twoKeysIndex.acquireBucketNoHash(threadContext, bucketDescriptor);
        BucketStatus bucketStatus;
        while ((bucketStatus = m_twoKeysIndex.continueBucketSearch(threadContext, bucketDescriptor, value1, value2)) == BUCKET_EMPTY) {
            if (m_twoKeysIndex.getPolicy().startBucketInsertionConditional(bucketDescriptor.m_bucket))
                break;
        }
        if (bucketStatus != BUCKET_NOT_CONTAINS) {
            if (bucketStatus == BUCKET_EMPTY) {
                TupleIndex previousGlobalHeadTripleIndex;
                do {
                    previousGlobalHeadTripleIndex = m_oneKeyIndex.getHeadTripleIndex(value1);
                    getTripleList().setNext(insertedTripleIndex, COMPONENT1, previousGlobalHeadTripleIndex);
                } while (!m_oneKeyIndex.setHeadTripleIndexConditional(value1, previousGlobalHeadTripleIndex, insertedTripleIndex));
                m_twoKeysIndex.getPolicy().setHeadTripleIndex(bucketDescriptor.m_bucket, insertedTripleIndex);
                m_twoKeysIndex.acknowledgeInsert(threadContext, bucketDescriptor);
                groupAlreadyExists = false;
            }
            else {
                TupleIndex headNextTripleIndex;
                do {
                    headNextTripleIndex = getTripleList().getNext(bucketDescriptor.m_bucketContents.m_headTripleIndex, COMPONENT1);
                    getTripleList().setNext(insertedTripleIndex, COMPONENT1, headNextTripleIndex);
                } while (!getTripleList().setNextConditional(bucketDescriptor.m_bucketContents.m_headTripleIndex, COMPONENT1, headNextTripleIndex, insertedTripleIndex));
                groupAlreadyExists = true;
            }
            success = true;
        }
        m_twoKeysIndex.releaseBucket(threadContext, bucketDescriptor);
    }
    return success;
}

template<class TwoKeysManagerConfiguration>
always_inline TupleIndex TwoKeysManagerGroupByTwo<TwoKeysManagerConfiguration>::getFirstTripleIndex1(ThreadContext& threadContext, const ResourceID s, const ResourceID p, const ResourceID o) const {
    return m_oneKeyIndex.getHeadTripleIndex(::getTripleComponent<COMPONENT1>(s, p, o));
}

template<class TwoKeysManagerConfiguration>
always_inline TupleIndex TwoKeysManagerGroupByTwo<TwoKeysManagerConfiguration>::getFirstTripleIndex12(ThreadContext& threadContext, const ResourceID s, const ResourceID p, const ResourceID o, ResourceID& compareResourceID, TupleIndex& compareGroupedMask) const {
    const ResourceID value1 = ::getTripleComponent<COMPONENT1>(s, p, o);
    compareResourceID = ::getTripleComponent<COMPONENT2>(s, p, o);
    typename TwoKeysIndexType::BucketDescriptor bucketDescriptor;
    m_twoKeysIndex.acquireBucket(threadContext, bucketDescriptor, value1, compareResourceID);
    BucketStatus bucketStatus = m_twoKeysIndex.continueBucketSearch(threadContext, bucketDescriptor, value1, compareResourceID);
    m_twoKeysIndex.releaseBucket(threadContext, bucketDescriptor);
    compareGroupedMask = GROUPED_MASK;
    return bucketStatus == BUCKET_CONTAINS ? bucketDescriptor.m_bucketContents.m_headTripleIndex : INVALID_TUPLE_INDEX;
}

template<class TwoKeysManagerConfiguration>
always_inline TupleIndex TwoKeysManagerGroupByTwo<TwoKeysManagerConfiguration>::getFirstTripleIndex13(ThreadContext& threadContext, const ResourceID s, const ResourceID p, const ResourceID o, ResourceID& compareResourceID, TupleIndex& compareGroupedMask) const {
    compareResourceID = ::getTripleComponent<COMPONENT3>(s, p, o);
    compareGroupedMask = NOT_GROUPED_MASK;
    return m_oneKeyIndex.getHeadTripleIndex(::getTripleComponent<COMPONENT1>(s, p, o));
}

template<class TwoKeysManagerConfiguration>
always_inline size_t TwoKeysManagerGroupByTwo<TwoKeysManagerConfiguration>::getCountEstimate1(ThreadContext& threadContext, const ResourceID s, const ResourceID p, const ResourceID o) const {
    const ResourceID value1 = ::getTripleComponent<COMPONENT1>(s, p, o);
    return m_oneKeyIndex.getTripleCount(value1);
}

template<class TwoKeysManagerConfiguration>
always_inline size_t TwoKeysManagerGroupByTwo<TwoKeysManagerConfiguration>::getCountEstimate12(ThreadContext& threadContext, const ResourceID s, const ResourceID p, const ResourceID o) const {
    const ResourceID value1 = ::getTripleComponent<COMPONENT1>(s, p, o);
    const ResourceID value2 = ::getTripleComponent<COMPONENT2>(s, p, o);
    typename TwoKeysIndexType::BucketDescriptor bucketDescriptor;
    m_twoKeysIndex.acquireBucket(threadContext, bucketDescriptor, value1, value2);
    BucketStatus bucketStatus = m_twoKeysIndex.continueBucketSearch(threadContext, bucketDescriptor, value1, value2);
    m_twoKeysIndex.releaseBucket(threadContext, bucketDescriptor);
    size_t tripleCount = 0;
    if (bucketStatus == BUCKET_CONTAINS) {
        TupleIndex currentTripleIndex = bucketDescriptor.m_bucketContents.m_headTripleIndex;
        while (currentTripleIndex != INVALID_TUPLE_INDEX && getTripleList().getResourceID(currentTripleIndex, COMPONENT1) == value1 && getTripleList().getResourceID(currentTripleIndex, COMPONENT2) == value2) {
            currentTripleIndex = getTripleList().getNext(currentTripleIndex, COMPONENT1);
            ++tripleCount;
        }
    }
    return tripleCount;
}

template<class TwoKeysManagerConfiguration>
always_inline size_t TwoKeysManagerGroupByTwo<TwoKeysManagerConfiguration>::getCountEstimate13(ThreadContext& threadContext, const ResourceID s, const ResourceID p, const ResourceID o) const {
    const ResourceID value1 = ::getTripleComponent<COMPONENT1>(s, p, o);
    return m_oneKeyIndex.getTripleCount(value1);
}

template<class TwoKeysManagerConfiguration>
always_inline void TwoKeysManagerGroupByTwo<TwoKeysManagerConfiguration>::startUpdatingStatistics() {
    if (!m_oneKeyIndex.clearCounts())
        throw RDF_STORE_EXCEPTION("Cannot clear counts of a one-key index.");
}

template<class TwoKeysManagerConfiguration>
always_inline void TwoKeysManagerGroupByTwo<TwoKeysManagerConfiguration>::updateStatisticsFor(const ResourceID s, const ResourceID p, const ResourceID o) {
    ResourceID value1 = ::getTripleComponent<COMPONENT1>(s, p, o);
    m_oneKeyIndex.incrementTripleCount(value1);
}

template<class TwoKeysManagerConfiguration>
always_inline void TwoKeysManagerGroupByTwo<TwoKeysManagerConfiguration>::save(OutputStream& outputStream) const {
    std::stringstream name;
    name << "TwoKeysManagerGroupByTwo[" << getResourceComponentName(COMPONENT1) << '.' << getResourceComponentName(COMPONENT2) << ']';
    outputStream.writeString(name.str());
    m_oneKeyIndex.save(outputStream);
    m_twoKeysIndex.save(outputStream);
}

template<class TwoKeysManagerConfiguration>
always_inline void TwoKeysManagerGroupByTwo<TwoKeysManagerConfiguration>::load(InputStream& inputStream) {
    std::stringstream name;
    name << "TwoKeysManagerGroupByTwo[" << getResourceComponentName(COMPONENT1) << '.' << getResourceComponentName(COMPONENT2) << ']';
    std::string nameString(name.str());
    if (!inputStream.checkNextString(nameString.c_str()))
        throw RDF_STORE_EXCEPTION("Invalid input file: cannot load " + name.str() + ".");
    m_oneKeyIndex.load(inputStream);
    m_twoKeysIndex.load(inputStream);
}

template<class TwoKeysManagerConfiguration>
always_inline void TwoKeysManagerGroupByTwo<TwoKeysManagerConfiguration>::printContents(std::ostream& output) const {
    output << "TwoKeysManagerGroupByTwo[" << getResourceComponentName(COMPONENT1) << '.' << getResourceComponentName(COMPONENT2) << ']' << std::endl;
    m_oneKeyIndex.printContents(output);
}

template<class TwoKeysManagerConfiguration>
always_inline std::unique_ptr<ComponentStatistics> TwoKeysManagerGroupByTwo<TwoKeysManagerConfiguration>::getComponentStatistics() const {
    std::stringstream name;
    name << "TwoKeysManagerGroupByTwo[" << getResourceComponentName(COMPONENT1) << '.' << getResourceComponentName(COMPONENT2) << ']';
    std::unique_ptr<ComponentStatistics> result(new ComponentStatistics(name.str()));
    std::unique_ptr<ComponentStatistics> oneKeyIndexStatistics = m_oneKeyIndex.getComponentStatistics();
    const size_t tripleCount = getTripleList().getExactTripleCount(TUPLE_STATUS_COMPLETE, TUPLE_STATUS_COMPLETE);
    const size_t twoKeysIndexNumberOfBuckets = m_twoKeysIndex.getNumberOfBuckets();
    const size_t twoKeysIndexNumberOfUsedBuckets = m_twoKeysIndex.getNumberOfUsedBuckets();
    const size_t twoKeysIndexSize = twoKeysIndexNumberOfBuckets * m_twoKeysIndex.getPolicy().BUCKET_SIZE;
    const uint64_t aggregateSize = oneKeyIndexStatistics->getItemIntegerValue("Size") + twoKeysIndexSize;
    result->addIntegerItem("Two keys index -- size", twoKeysIndexSize);
    result->addIntegerItem("Two keys index -- total buckets", twoKeysIndexNumberOfBuckets);
    result->addIntegerItem("Two keys index -- used buckets", twoKeysIndexNumberOfUsedBuckets);
    result->addFloatingPointItem("Two keys index -- load factor (%)", (twoKeysIndexNumberOfUsedBuckets * 100.0) / static_cast<double>(twoKeysIndexNumberOfBuckets));
    if (tripleCount != 0)
        result->addFloatingPointItem("Two keys index -- used buckets / number of triples (%)", (twoKeysIndexNumberOfUsedBuckets * 100.0) / static_cast<double>(tripleCount));
    result->addIntegerItem("Aggregate size", aggregateSize);
    if (tripleCount != 0)
        result->addFloatingPointItem("Bytes per triple", aggregateSize / static_cast<double>(tripleCount));
    result->addSubcomponent(std::move(oneKeyIndexStatistics));
    return result;
}

#endif /* TWOKEYSMANAGERGROUPBYTWOIMPL_H_ */
