// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef THREEKEYSMANAGER_H_
#define THREEKEYSMANAGER_H_

#include "../../Common.h"
#include "../TripleTableIterator.h"

class MemoryManager;
class ComponentStatistics;
class Parameters;
class InputStream;
class OutputStream;

template<class ThreeKeysManagerConfiguration>
class ThreeKeysManager : private Unmovable {

public:

    typedef typename ThreeKeysManagerConfiguration::TripleListType TripleListType;
    typedef typename ThreeKeysManagerConfiguration::ThreeKeysIndexPolicyType ThreeKeysIndexPolicyType;
    typedef typename ThreeKeysManagerConfiguration::ThreeKeysIndexType ThreeKeysIndexType;

protected:

    mutable ThreeKeysIndexType m_threeKeysIndex;

public:

    struct InsertToken : public ThreeKeysIndexType::BucketDescriptor {
    };

    ThreeKeysManager(MemoryManager& memoryManager, TripleListType& tripleList, const Parameters& dataStoreParameters);

    TripleListType& getTripleList();

    const TripleListType& getTripleList() const;

    bool initialize(const size_t initialTripleCapacity);

    void setNumberOfThreads(const size_t numberOfThreads);

    bool getInsertToken(ThreadContext& threadContext, const ResourceID insertedS, const ResourceID insertedP, const ResourceID insertedO, InsertToken& insertToken, bool& alreadyExists);

    void abortInsertToken(ThreadContext& threadContext, InsertToken& insertToken);

    void releaseInsertToken(ThreadContext& threadContext, InsertToken& insertToken);

    void updateOnInsert(ThreadContext& threadContext, InsertToken& insertToken, const TupleIndex insertedTripleIndex, const ResourceID insertedS, const ResourceID insertedP, const ResourceID insertedO);

    bool insertTriple(ThreadContext& threadContext, const TupleIndex insertedTripleIndex, const ResourceID insertedS, const ResourceID insertedP, const ResourceID insertedO, bool& tripleIndexInserted, TupleIndex& resultingTripleIndex);

    TupleIndex getTripleIndex(ThreadContext& threadContext, const ResourceID s, const ResourceID p, const ResourceID o) const;

    bool contains(ThreadContext& threadContext, const ResourceID s, const ResourceID p, const ResourceID o, const TupleStatus tupleStatusMask, const TupleStatus tupleStatusExpectedValue) const;

    size_t getCountEstimate(ThreadContext& threadContext, const ResourceID s, const ResourceID p, const ResourceID o) const;

    void save(OutputStream& outputStream) const;

    void load(InputStream& inputStream);

    std::unique_ptr<ComponentStatistics> getComponentStatistics() const;

};

#endif /* THREEKEYSMANAGER_H_ */
