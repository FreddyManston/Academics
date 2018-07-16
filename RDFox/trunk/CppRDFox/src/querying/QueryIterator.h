// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef QUERYITERATOR_H_
#define QUERYITERATOR_H_

#include "../dictionary/ResourceValueCache.h"
#include "../storage/TupleIterator.h"

class ValuationCache;
class DataStore;
class EqualityManager;

// QueryIterator

class QueryIterator : public TupleIterator {

public:

    QueryIterator(TupleIteratorMonitor* const tupleIteratorMonitor, std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const ArgumentIndexSet& allInputArguments, const ArgumentIndexSet& surelyBoundInputArguments, const ArgumentIndexSet& allArguments, const ArgumentIndexSet& surelyBoundArguments);

    QueryIterator(const QueryIterator& other, CloneReplacements& cloneReplacements);

    virtual const ResourceValueCache& getResourceValueCache() const = 0;

};

// QueryIteratorImpl

template<bool callMonitor>
class QueryIteratorImpl : public QueryIterator {

protected:

    ResourceValueCache& m_resourceValueCache;
    const std::unique_ptr<ResourceValueCache> m_resourceValueCacheOwner;
    const EqualityManager& m_equalityManager;
    const std::unique_ptr<std::vector<ResourceID> > m_argumentsBufferOwner;
    const size_t m_numberOfConstantsToNormalize;
    const std::unique_ptr<TupleIterator> m_tupleIterator;
    std::vector<ValuationCache*> m_valuationCaches;

    void loadValuationCaches(const TupleIterator* tupleIterator);
    
public:

    QueryIteratorImpl(TupleIteratorMonitor* const tupleIteratorMonitor, const DataStore& dataStore, ResourceValueCache& resourceValueCache, std::unique_ptr<ResourceValueCache> resourceValueCacheOwner, std::vector<ResourceID>& argumentsBuffer, std::unique_ptr<std::vector<ResourceID> > argumentsBufferOwner, const size_t numberOfConstantsToNormalize, const std::vector<ArgumentIndex>& argumentIndexes, std::unique_ptr<TupleIterator> tupleIterator);

    QueryIteratorImpl(const QueryIteratorImpl<callMonitor>& other, CloneReplacements& cloneReplacements);

    virtual const ResourceValueCache& getResourceValueCache() const;

    virtual const char* getName() const;

    virtual size_t getNumberOfChildIterators() const;

    virtual const TupleIterator& getChildIterator(const size_t childIteratorIndex) const;

    virtual std::unique_ptr<TupleIterator> clone(CloneReplacements& cloneReplacements) const;

    virtual size_t open(ThreadContext& threadContext);

    virtual size_t open();

    virtual size_t advance();

    virtual bool getCurrentTupleInfo(TupleIndex& tupleIndex, TupleStatus& tupleStatus) const;

    virtual TupleIndex getCurrentTupleIndex() const;

};

always_inline std::unique_ptr<QueryIterator> newQueryIterator(TupleIteratorMonitor* const tupleIteratorMonitor, const DataStore& dataStore, ResourceValueCache& resourceValueCache, std::unique_ptr<ResourceValueCache> resourceValueCacheOwner, std::vector<ResourceID>& argumentsBuffer, std::unique_ptr<std::vector<ResourceID> > argumentsBufferOwner, const size_t numberOfConstantsToNormalize, const std::vector<ArgumentIndex>& argumentIndexes, std::unique_ptr<TupleIterator> tupleIterator) {
    if (tupleIteratorMonitor)
        return std::unique_ptr<QueryIterator>(new QueryIteratorImpl<true>(tupleIteratorMonitor, dataStore, resourceValueCache, std::move(resourceValueCacheOwner), argumentsBuffer, std::move(argumentsBufferOwner), numberOfConstantsToNormalize, argumentIndexes, std::move(tupleIterator)));
    else
        return std::unique_ptr<QueryIterator>(new QueryIteratorImpl<false>(tupleIteratorMonitor, dataStore, resourceValueCache, std::move(resourceValueCacheOwner), argumentsBuffer, std::move(argumentsBufferOwner), numberOfConstantsToNormalize, argumentIndexes, std::move(tupleIterator)));
}

#endif /* QUERYITERATOR_H_ */
