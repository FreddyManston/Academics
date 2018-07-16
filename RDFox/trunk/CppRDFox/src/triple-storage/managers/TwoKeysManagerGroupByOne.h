// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef TWOKEYSMANAGERGROUPBYONE_H_
#define TWOKEYSMANAGERGROUPBYONE_H_

#include "../../util/MemoryRegion.h"
#include "../../util/SequentialHashTable.h"

class MemoryManager;
class ComponentStatistics;
class Parameters;
class InputStream;
class OutputStream;

// TwoKeysManagerGroupByOneProxy

template<class TwoKeysManager>
class TwoKeysManagerGroupByOneProxy {

public:

    typedef TwoKeysManager TwoKeysManagerType;
    typedef TwoKeysManagerGroupByOneProxy<TwoKeysManager> TwoKeysManagerProxyType;
    typedef typename TwoKeysManagerType::TripleListType TripleListType;
    typedef typename TripleListType::StoreTripleIndexType StoreTripleIndexType;
    static const ResourceComponent COMPONENT1 = TwoKeysManagerType::COMPONENT1;
    static const ResourceComponent COMPONENT2 = TwoKeysManagerType::COMPONENT2;
    static const ResourceComponent COMPONENT3 = TwoKeysManagerType::COMPONENT3;

    friend TwoKeysManagerType;

    class OneKeyIndexProxyPolicy {

    public:

        TripleListType& m_tripleList;

        static const size_t BUCKET_SIZE = sizeof(StoreTripleIndexType);

        struct BucketContents {
            TupleIndex m_headTripleIndex;
        };

        OneKeyIndexProxyPolicy(TripleListType& tripleList);

        static void getBucketContents(const uint8_t* const bucket, BucketContents& bucketContents);

        BucketStatus getBucketContentsStatus(const BucketContents& bucketContents, const size_t valuesHashCode, const ResourceID value) const;

        static bool setBucketContentsIfEmpty(uint8_t* const bucket, const BucketContents& bucketContents);

        static bool isBucketContentsEmpty(const BucketContents& bucketContents);

        size_t getBucketContentsHashCode(const BucketContents& bucketContents) const;

        static size_t hashCodeFor(const ResourceID value);

        static TupleIndex getHeadTripleIndex(const uint8_t* const bucket);

        static void setHeadTripleIndex(uint8_t* const bucket, const TupleIndex tupleIndex);
        
    };
    
    typedef SequentialHashTable<OneKeyIndexProxyPolicy> OneKeyIndexProxyType;

protected:

    const ResourceID m_proxyArrayThreshold;
    const size_t m_proxyHashTableThreshold;
    TwoKeysManagerType& m_twoKeysManager;
    MemoryRegion<StoreTripleIndexType> m_tripleIndexes;
    OneKeyIndexProxyType m_oneKeyIndexProxy;

public:

    TwoKeysManagerGroupByOneProxy(TwoKeysManagerType& twoKeysManager, const Parameters& dataStoreParameters);

    TripleListType& getTripleList();

    bool initialize();

    bool insertTriple(ThreadContext& threadContext, const TupleIndex insertedTripleIndex, const ResourceID insertedS, const ResourceID insertedP, const ResourceID insertedO);

    std::unique_ptr<ComponentStatistics> getComponentStatistics() const;

};

// TwoKeysManagerGroupByOne

template<class TwoKeysManagerConfiguration>
class TwoKeysManagerGroupByOne : private Unmovable {

public:

    typedef TwoKeysManagerGroupByOne<TwoKeysManagerConfiguration> TwoKeysManagerType;
    typedef TwoKeysManagerGroupByOneProxy<TwoKeysManagerType> TwoKeysManagerProxyType;
    typedef typename TwoKeysManagerConfiguration::TripleListType TripleListType;
    typedef typename TwoKeysManagerConfiguration::OneKeyIndexType OneKeyIndexType;
    static const ResourceComponent COMPONENT1 = TwoKeysManagerConfiguration::COMPONENT1;
    static const ResourceComponent COMPONENT2 = TwoKeysManagerConfiguration::COMPONENT2;
    static const ResourceComponent COMPONENT3 = TwoKeysManagerConfiguration::COMPONENT3;

    friend class TwoKeysManagerGroupByOneProxy<TwoKeysManagerType>;

protected:

    TripleListType& m_tripleList;
    OneKeyIndexType m_oneKeyIndex;

public:

    TwoKeysManagerGroupByOne(MemoryManager& memoryManager, TripleListType& tripleList, const Parameters& dataStoreParameters);

    TripleListType& getTripleList();

    const TripleListType& getTripleList() const;

    MemoryManager& getMemoryManager() const;

    bool initialize(const size_t initialTripleCapacity, const size_t initialResourceCapacity);

    void setNumberOfThreads(const size_t numberOfThreads);

    bool insertTriple(ThreadContext& threadContext, const TupleIndex insertedTripleIndex, const ResourceID insertedS, const ResourceID insertedP, const ResourceID insertedO);

    bool insertTripleInternal(ThreadContext& threadContext, const ResourceID insertedResourceID, const TupleIndex insertedTripleIndex, const ResourceID insertedS, const ResourceID insertedP, const ResourceID insertedO);

    TupleIndex getFirstTripleIndex1(ThreadContext& threadContext, const ResourceID s, const ResourceID p, const ResourceID o) const;

    TupleIndex getFirstTripleIndex12(ThreadContext& threadContext, const ResourceID s, const ResourceID p, const ResourceID o, ResourceID& compareResourceID, TupleIndex& compareGroupedMask) const;

    TupleIndex getFirstTripleIndex13(ThreadContext& threadContext, const ResourceID s, const ResourceID p, const ResourceID o, ResourceID& compareResourceID, TupleIndex& compareGroupedMask) const;

    size_t getCountEstimate1(ThreadContext& threadContext, const ResourceID s, const ResourceID p, const ResourceID o) const;

    size_t getCountEstimate12(ThreadContext& threadContext, const ResourceID s, const ResourceID p, const ResourceID o) const;

    size_t getCountEstimate13(ThreadContext& threadContext, const ResourceID s, const ResourceID p, const ResourceID o) const;

    void startUpdatingStatistics();

    void updateStatisticsFor(const ResourceID s, const ResourceID p, const ResourceID o);

    void save(OutputStream& outputStream) const;

    void load(InputStream& inputStream);

    void printContents(std::ostream& output) const;

    std::unique_ptr<ComponentStatistics> getComponentStatistics() const;

};

#endif /* TWOKEYSMANAGERGROUPBYONE_H_ */
