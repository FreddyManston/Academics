// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef TWOKEYSMANAGERGROUPBYTWO_H_
#define TWOKEYSMANAGERGROUPBYTWO_H_

#include "../../Common.h"
#include "../../util/SequentialHashTable.h"

class MemoryManager;
class ComponentStatistics;
class Parameters;
class InputStream;
class OutputStream;

// TwoKeysManagerGroupByTwoProxy

template<class TwoKeysManager>
class TwoKeysManagerGroupByTwoProxy {

public:

    typedef TwoKeysManager TwoKeysManagerType;
    typedef TwoKeysManagerGroupByTwoProxy<TwoKeysManager> TwoKeysManagerProxyType;
    typedef typename TwoKeysManagerType::TripleListType TripleListType;
    typedef typename TripleListType::StoreTripleIndexType StoreTripleIndexType;
    static const ResourceComponent COMPONENT1 = TwoKeysManagerType::COMPONENT1;
    static const ResourceComponent COMPONENT2 = TwoKeysManagerType::COMPONENT2;
    static const ResourceComponent COMPONENT3 = TwoKeysManagerType::COMPONENT3;
    static const bool USES_RDF_TYPE_TRIPLE_INDEXES = (COMPONENT1 == RC_P && COMPONENT2 == RC_O) || (COMPONENT1 == RC_O && COMPONENT2 == RC_P);

    friend TwoKeysManagerType;

    class TwoKeysIndexProxyPolicy {

    public:

        TripleListType& m_tripleList;

        static const size_t BUCKET_SIZE = sizeof(StoreTripleIndexType);

        struct BucketContents {
            TupleIndex m_headTripleIndex;
        };

        TwoKeysIndexProxyPolicy(TripleListType& tripleList);

        static void getBucketContents(const uint8_t* const bucket, BucketContents& bucketContents);

        BucketStatus getBucketContentsStatus(const BucketContents& bucketContents, const size_t valuesHashCode, const ResourceID value1, const ResourceID value2) const;

        static bool setBucketContentsIfEmpty(uint8_t* const bucket, const BucketContents& bucketContents);

        static bool isBucketContentsEmpty(const BucketContents& bucketContents);

        size_t getBucketContentsHashCode(const BucketContents& bucketContents) const;

        static size_t hashCodeFor(const ResourceID value1, const ResourceID value2);

        static TupleIndex getHeadTripleIndex(const uint8_t* const bucket);

        static void setHeadTripleIndex(uint8_t* const bucket, const TupleIndex tupleIndex);

    };

    typedef SequentialHashTable<TwoKeysIndexProxyPolicy> TwoKeysIndexProxyType;

protected:

    const ResourceID m_proxyArrayThreshold;
    const size_t m_proxyHashTableThreshold;
    TwoKeysManagerType& m_twoKeysManager;
    MemoryRegion<StoreTripleIndexType> m_rdfTypeTripleIndexes;
    TwoKeysIndexProxyType m_twoKeysIndexProxy;

public:

    TwoKeysManagerGroupByTwoProxy(TwoKeysManagerType& twoKeysManager, const Parameters& dataStoreParameters);

    TripleListType& getTripleList();

    bool initialize();

    bool insertTriple(ThreadContext& threadContext, const TupleIndex insertedTripleIndex, const ResourceID insertedS, const ResourceID insertedP, const ResourceID insertedO);

    std::unique_ptr<ComponentStatistics> getComponentStatistics() const;

};

// TwoKeysManagerGroupByTwo

template<class TwoKeysManagerConfiguration>
class TwoKeysManagerGroupByTwo : private Unmovable {

public:

    typedef TwoKeysManagerGroupByTwo<TwoKeysManagerConfiguration> TwoKeysManagerType;
    typedef TwoKeysManagerGroupByTwoProxy<TwoKeysManagerType> TwoKeysManagerProxyType;
    typedef typename TwoKeysManagerConfiguration::TripleListType TripleListType;
    typedef typename TwoKeysManagerConfiguration::OneKeyIndexType OneKeyIndexType;
    typedef typename TwoKeysManagerConfiguration::TwoKeysIndexPolicyType TwoKeysIndexPolicyType;
    typedef typename TwoKeysManagerConfiguration::TwoKeysIndexType TwoKeysIndexType;
    static const ResourceComponent COMPONENT1 = TwoKeysManagerConfiguration::COMPONENT1;
    static const ResourceComponent COMPONENT2 = TwoKeysManagerConfiguration::COMPONENT2;
    static const ResourceComponent COMPONENT3 = TwoKeysManagerConfiguration::COMPONENT3;

    friend class TwoKeysManagerGroupByTwoProxy<TwoKeysManagerType>;

protected:

    OneKeyIndexType m_oneKeyIndex;
    mutable TwoKeysIndexType m_twoKeysIndex;

public:

    TwoKeysManagerGroupByTwo(MemoryManager& memoryManager, TripleListType& tripleList, const Parameters& dataStoreParameters);

    TripleListType& getTripleList();

    const TripleListType& getTripleList() const;

    MemoryManager& getMemoryManager() const;

    bool initialize(const size_t initialTripleCapacity, const size_t initialResourceCapacity);

    void setNumberOfThreads(const size_t numberOfThreads);

    bool insertTriple(ThreadContext& threadContext, const TupleIndex insertedTripleIndex, const ResourceID insertedS, const ResourceID insertedP, const ResourceID insertedO);

    bool insertTripleInternal(ThreadContext& threadContext, const ResourceID value1, const ResourceID value2, const size_t hashCode, const TupleIndex insertedTripleIndex, const ResourceID insertedS, const ResourceID insertedP, const ResourceID insertedO, bool& groupAlreadyExists);

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

#endif /* TWOKEYSMANAGERGROUPBYTWO_H_ */
