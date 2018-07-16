// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef FILTERTUPLEITERATOR_H_
#define FILTERTUPLEITERATOR_H_

#include "../storage/TupleIterator.h"

class ThreadContext;
class BuiltinExpressionEvaluator;

template<bool callMonitor>
class FilterTupleIterator : public TupleIterator {

protected:

    std::unique_ptr<BuiltinExpressionEvaluator> m_builtinExpressionEvaluator;

public:

    FilterTupleIterator(std::unique_ptr<BuiltinExpressionEvaluator> builtinExpressionEvaluator, TupleIteratorMonitor* const tupleIteratorMonitor, std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const ArgumentIndexSet& allInputArguments, const ArgumentIndexSet& surelyBoundInputArguments);

    FilterTupleIterator(const FilterTupleIterator<callMonitor>& other, CloneReplacements& cloneReplacements);

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

always_inline std::unique_ptr<TupleIterator> newFilterTupleIterator(std::unique_ptr<BuiltinExpressionEvaluator> builtinExpressionEvaluator, TupleIteratorMonitor* const tupleIteratorMonitor, std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const ArgumentIndexSet& allInputArguments, const ArgumentIndexSet& surelyBoundInputArguments) {
    if (tupleIteratorMonitor)
        return std::unique_ptr<TupleIterator>(new FilterTupleIterator<true>(std::move(builtinExpressionEvaluator), tupleIteratorMonitor, argumentsBuffer, argumentIndexes, allInputArguments, surelyBoundInputArguments));
    else
        return std::unique_ptr<TupleIterator>(new FilterTupleIterator<false>(std::move(builtinExpressionEvaluator), tupleIteratorMonitor, argumentsBuffer, argumentIndexes, allInputArguments, surelyBoundInputArguments));
}

#endif /* FILTERTUPLEITERATOR_H_ */
