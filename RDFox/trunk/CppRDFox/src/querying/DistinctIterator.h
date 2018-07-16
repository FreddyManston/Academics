// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef DISTINCTITERATOR_H_
#define DISTINCTITERATOR_H_

#include "../storage/TupleIterator.h"
#include "ValuationCache.h"

class MemoryManager;

template<bool callMonitor>
class DistinctIterator : public TupleIterator {

protected:

    ValuationCache m_valuationCache;
    const std::unique_ptr<TupleIterator> m_tupleIterator;
    std::vector<ArgumentIndex> m_surelyBoundInputArgumentIndexes;
    std::vector<ResourceID> m_onlyPossiblyBoundInputValues;
    std::vector<ArgumentIndex> m_onlyPossiblyBoundInputArgumentIndexes;
    std::vector<size_t> m_onlyPossiblyBoundInputOffsets;
    std::vector<ArgumentIndex> m_outputArgumentIndexes;
    ResourceID* m_argumentsBufferFirstElement;
    ArgumentIndex* m_surelyBoundInputArgumentIndexesFirstElement;
    ArgumentIndex* m_outputArgumentIndexesFirstElement;
    OutputValuation* m_currentOutputValuation;

public:

    DistinctIterator(TupleIteratorMonitor* const tupleIteratorMonitor, MemoryManager& memoryManager, std::unique_ptr<TupleIterator> tupleIterator);

    DistinctIterator(const DistinctIterator<callMonitor>& other, CloneReplacements& cloneReplacements);

    always_inline ValuationCache& getValuationCache() const {
        return const_cast<ValuationCache&>(static_cast<const ValuationCache&>(m_valuationCache));
    }

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

always_inline std::unique_ptr<TupleIterator> newDistinctIterator(TupleIteratorMonitor* const tupleIteratorMonitor, MemoryManager& memoryManager, std::unique_ptr<TupleIterator> tupleIterator) {
    if (tupleIteratorMonitor)
        return std::unique_ptr<TupleIterator>(new DistinctIterator<true>(tupleIteratorMonitor, memoryManager, std::move(tupleIterator)));
    else
        return std::unique_ptr<TupleIterator>(new DistinctIterator<false>(tupleIteratorMonitor, memoryManager, std::move(tupleIterator)));
}

#endif /* DISTINCTITERATOR_H_ */
