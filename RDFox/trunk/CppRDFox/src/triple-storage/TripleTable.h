// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef TRIPLETABLE_H_
#define TRIPLETABLE_H_

#include "../Common.h"
#include "../storage/TupleTable.h"
#include "../util/LockFreeQueue.h"
#include "TripleTableIterator.h"
#include "TripleTableProxy.h"

template<class TT>
struct TripleTableTraits {
    static const char* const TYPE_NAME;
};

class MemoryManager;
class ComponentStatistics;
class TupleIteratorMonitor;
class Parameters;
class InputStream;
class OutputStream;

template<class TripleTableConfiguration>
class TripleTable : public TupleTable {

public:

    typedef typename TripleTableConfiguration::TripleListType TripleListType;
    typedef typename TripleTableConfiguration::TwoKeysManager1Type TwoKeysManager1Type;
    typedef typename TripleTableConfiguration::TwoKeysManager2Type TwoKeysManager2Type;
    typedef typename TripleTableConfiguration::TwoKeysManager3Type TwoKeysManager3Type;
    typedef typename TripleTableConfiguration::ThreeKeysManagerType ThreeKeysManagerType;
    typedef TripleTable<TripleTableConfiguration> TripleTableType;
    typedef TripleTableProxy<TripleTableType> TripleTableProxyType;

    struct QP0HandlerType {
        static const TripleTableIteratorType ITERATOR_TYPE = ITERATE_LIST;
        static const ResourceComponent ITERATE_NEXT_ON = static_cast<ResourceComponent>(-1);
        static const ResourceComponent ITERATE_COMPARE_WITH = static_cast<ResourceComponent>(-1);

        static always_inline TupleIndex getFirstTripleIndex(ThreadContext& threadContext, const TripleTableType& tripleTable, const ResourceID s, const ResourceID p, const ResourceID o, ResourceID& compareResourceID, TupleIndex& compareGroupedMask) {
            return tripleTable.m_tripleList.getFirstTripleIndex();
        }

        static always_inline size_t getCountEstimate(ThreadContext& threadContext, const TripleTableType& tripleTable, ResourceID s, ResourceID p, ResourceID o) {
            return tripleTable.m_tripleList.getApproximateTripleCount();
        }
    };
    typedef typename TripleTableConfiguration::QP1HandlerType QP1HandlerType;
    typedef typename TripleTableConfiguration::QP2HandlerType QP2HandlerType;
    typedef typename TripleTableConfiguration::QP3HandlerType QP3HandlerType;
    typedef typename TripleTableConfiguration::QP4HandlerType QP4HandlerType;
    typedef typename TripleTableConfiguration::QP5HandlerType QP5HandlerType;
    typedef typename TripleTableConfiguration::QP6HandlerType QP6HandlerType;
    struct QP7HandlerType {
        static const TripleTableIteratorType ITERATOR_TYPE = SINGLETON;
        static const ResourceComponent ITERATE_NEXT_ON = static_cast<ResourceComponent>(-1);
        static const ResourceComponent ITERATE_COMPARE_WITH = static_cast<ResourceComponent>(-1);

        static always_inline TupleIndex getFirstTripleIndex(ThreadContext& threadContext, const TripleTableType& tripleTable, const ResourceID s, const ResourceID p, const ResourceID o, ResourceID& compareResourceID, TupleIndex& compareGroupedMask) {
            return tripleTable.m_threeKeysManager.getTripleIndex(threadContext, s, p, o);
        }

        static always_inline size_t getCountEstimate(ThreadContext& threadContext, const TripleTableType& tripleTable, ResourceID s, ResourceID p, ResourceID o) {
            return tripleTable.m_threeKeysManager.getCountEstimate(threadContext, s, p, o);
        }
    };

    const Parameters& m_dataStoreParameters;
    TripleListType m_tripleList;
    TwoKeysManager1Type m_twoKeysManager1;
    TwoKeysManager2Type m_twoKeysManager2;
    TwoKeysManager3Type m_twoKeysManager3;
    ThreeKeysManagerType m_threeKeysManager;
    LockFreeQueue<TupleIndex> m_triplesScheduledForAddition;
    LockFreeQueue<TupleIndex> m_triplesScheduledForDeletion;

    template<class FT, bool callMonitor>
    std::unique_ptr<TupleIterator> createTupleIteratorInternal(std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const ArgumentIndexSet& allInputArguments, const ArgumentIndexSet& surelyBoundInputArguments, const FT& filter, TupleIteratorMonitor* const tupleIteratorMonitor) const;

    TripleTable(MemoryManager& memoryManager, const Parameters& dataStoreParameters);

    static const char* getTypeName();

    virtual std::string getPredicateName() const;

    virtual size_t getArity() const;

    virtual size_t getTupleCount(const TupleStatus tupleStatusMask, const TupleStatus tupleStatusExpectedValue) const;

    virtual bool supportsProxy() const;

    virtual std::unique_ptr<TupleTableProxy> createTupleTableProxy(const size_t windowSize);

    virtual std::pair<bool, TupleIndex> addTuple(ThreadContext& threadContext, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const TupleStatus deleteTupleStatus, const TupleStatus addTupleStatus);

    virtual std::pair<bool, TupleIndex> addTuple(const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const TupleStatus deleteTupleStatus, const TupleStatus addTupleStatus);

    virtual bool addTupleStatus(const TupleIndex tupleIndex, const TupleStatus tupleStatus);

    virtual bool deleteTupleStatus(const TupleIndex tupleIndex, const TupleStatus tupleStatus);

    virtual bool deleteAddTupleStatus(const TupleIndex tupleIndex, const TupleStatus deleteMask, const TupleStatus deleteExpected, const TupleStatus deleteTupleStatus, const TupleStatus addMask, const TupleStatus addExpected, const TupleStatus addTupleStatus);

    virtual TupleIndex getTupleIndex(ThreadContext& threadContext, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) const;

    virtual TupleIndex getTupleIndex(const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) const;

    virtual bool containsTuple(ThreadContext& threadContext, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const TupleStatus tupleStatusMask, const TupleStatus tupleStatusExpectedValue) const;

    virtual bool containsTuple(const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const TupleStatus tupleStatusMask, const TupleStatus tupleStatusExpectedValue) const;

    virtual TupleIndex getFirstFreeTupleIndex() const;

    virtual TupleStatus getTupleStatus(const TupleIndex tupleIndex) const;

    virtual TupleStatus getStatusAndTupleIfComplete(const TupleIndex tupleIndex, std::vector<ResourceID>& tupleBuffer) const;

    virtual TupleStatus getStatusAndTuple(const TupleIndex tupleIndex, std::vector<ResourceID>& tupleBuffer) const;

    virtual std::unique_ptr<TupleIterator> createTupleIterator(std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const ArgumentIndexSet& allInputArguments, const ArgumentIndexSet& surelyBoundInputArguments, const TupleStatus tupleStatusMask = (TUPLE_STATUS_COMPLETE | TUPLE_STATUS_IDB | TUPLE_STATUS_IDB_MERGED), const TupleStatus tupleStatusExpectedValue = (TUPLE_STATUS_COMPLETE | TUPLE_STATUS_IDB), TupleIteratorMonitor* const tupleIteratorMonitor = 0) const;

    virtual std::unique_ptr<TupleIterator> createTupleIterator(std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const ArgumentIndexSet& allInputArguments, const ArgumentIndexSet& surelyBoundInputArguments, const TupleFilter* & tupleFilter, const void* const tupleFilterContext, TupleIteratorMonitor* const tupleIteratorMonitor = nullptr) const;

    virtual size_t getCountEstimate(ThreadContext& threadContext, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const ArgumentIndexSet& allInputArguments) const;

    virtual size_t getCountEstimate(const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const ArgumentIndexSet& allInputArguments) const;

    virtual bool scheduleForAddition(ThreadContext& threadContext, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes);

    virtual bool scheduleForAddition(const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes);

    virtual const LockFreeQueue<TupleIndex>& getTupleIndexesScheduledForAddition() const;

    virtual bool scheduleForDeletion(ThreadContext& threadContext, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes);

    virtual bool scheduleForDeletion(const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes);

    virtual const LockFreeQueue<TupleIndex>& getTupleIndexesScheduledForDeletion() const;

    virtual std::unique_ptr<ComponentStatistics> getComponentStatistics() const;

    void initialize(const size_t initialTripleCapacity, const size_t initialResourceCapacity);

    void reindex(const bool dropIDB, const size_t initialTripleCapacity, const size_t initialResourceCapacity);

    void setNumberOfThreads(const size_t numberOfThreads);
    
    void makeFactsExplicit();

    void saveFormatted(OutputStream& outputStream) const;

    void loadFormatted(InputStream& inputStream);

    void saveUnformatted(OutputStream& outputStream) const;

    void loadUnformatted(InputStream& inputStream, const size_t numberOfThreads);

    void printContents(std::ostream& output, const bool printTriples, const bool printManager1, const bool printManager2, const bool printManager3) const;

    void updateStatistics();

    __ALIGNED(TripleTable<TripleTableConfiguration>)

};

#endif /* TRIPLETABLE_H_ */
