// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef TUPLETABLE_H_
#define TUPLETABLE_H_

#include "../Common.h"
#include "ArgumentIndexSet.h"
#include "TupleReceiver.h"
#include "TupleTableProxy.h"
#include "TupleIterator.h"

template<class E>
class LockFreeQueue;
class ComponentStatistics;
class File;
class TupleIteratorMonitor;
class TupleFilter;

class TupleTable : public TupleReceiver {

public:

    virtual ~TupleTable() {
    }

    virtual std::string getPredicateName() const = 0;

    virtual size_t getArity() const = 0;

    virtual size_t getTupleCount(const TupleStatus tupleStatusMask, const TupleStatus tupleStatusExpectedValue) const = 0;

    virtual std::unique_ptr<TupleIterator> createTupleIterator(std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const ArgumentIndexSet& allInputArguments, const ArgumentIndexSet& surelyBoundInputArguments, const TupleStatus tupleStatusMask = (TUPLE_STATUS_COMPLETE | TUPLE_STATUS_IDB | TUPLE_STATUS_IDB_MERGED), const TupleStatus tupleStatusExpectedValue = (TUPLE_STATUS_COMPLETE | TUPLE_STATUS_IDB), TupleIteratorMonitor* const tupleIteratorMonitor = nullptr) const = 0;

    virtual std::unique_ptr<TupleIterator> createTupleIterator(std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const ArgumentIndexSet& allInputArguments, const ArgumentIndexSet& surelyBoundInputArguments, const TupleFilter* & tupleFilter, const void* const tupleFilterContext, TupleIteratorMonitor* const tupleIteratorMonitor = nullptr) const = 0;

    virtual size_t getCountEstimate(ThreadContext& threadContext, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const ArgumentIndexSet& allInputArguments) const = 0;

    virtual size_t getCountEstimate(const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const ArgumentIndexSet& allInputArguments) const = 0;

    virtual bool supportsProxy() const = 0;

    virtual std::unique_ptr<TupleTableProxy> createTupleTableProxy(const size_t windowSize) = 0;

    virtual bool addTupleStatus(const TupleIndex tupleIndex, const TupleStatus tupleStatus) = 0;

    virtual bool deleteTupleStatus(const TupleIndex tupleIndex, const TupleStatus tupleStatus) = 0;

    virtual bool deleteAddTupleStatus(const TupleIndex tupleIndex, const TupleStatus deleteMask, const TupleStatus deleteExpected, const TupleStatus deleteTupleStatus, const TupleStatus addMask, const TupleStatus addExpected, const TupleStatus addTupleStatus) = 0;

    virtual TupleIndex getTupleIndex(ThreadContext& threadContext, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) const = 0;

    virtual TupleIndex getTupleIndex(const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) const = 0;

    virtual bool containsTuple(ThreadContext& threadContext, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const TupleStatus tupleStatusMask, const TupleStatus tupleStatusExpectedValue) const = 0;

    virtual bool containsTuple(const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const TupleStatus tupleStatusMask, const TupleStatus tupleStatusExpectedValue) const = 0;

    virtual TupleIndex getFirstFreeTupleIndex() const = 0;

    virtual TupleStatus getTupleStatus(const TupleIndex tupleIndex) const = 0;

    virtual TupleStatus getStatusAndTuple(const TupleIndex tupleIndex, std::vector<ResourceID>& tupleBuffer) const = 0;

    virtual TupleStatus getStatusAndTupleIfComplete(const TupleIndex tupleIndex, std::vector<ResourceID>& tupleBuffer) const = 0;

    virtual bool scheduleForAddition(ThreadContext& threadContext, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) = 0;

    virtual bool scheduleForAddition(const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) = 0;

    virtual const LockFreeQueue<TupleIndex>& getTupleIndexesScheduledForAddition() const = 0;

    virtual bool scheduleForDeletion(ThreadContext& threadContext, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) = 0;

    virtual bool scheduleForDeletion(const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) = 0;

    virtual const LockFreeQueue<TupleIndex>& getTupleIndexesScheduledForDeletion() const = 0;

    virtual void printContents(std::ostream& output, const bool printTriples, const bool printManager1, const bool printManager2, const bool printManager3) const = 0;

    virtual std::unique_ptr<ComponentStatistics> getComponentStatistics() const = 0;

};

#endif /* TUPLETABLE_H_ */
