// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef BINDTUPLEITERATOR_H_
#define BINDTUPLEITERATOR_H_

#include "../storage/TupleIterator.h"

class ThreadContext;
class BuiltinExpressionEvaluator;

enum BindResultType { BIND_RESULT_ALWAYS_PRODUCE, BIND_RESULT_ALWAYS_COMPARE, BIND_RESULT_PRODUCE_OR_COMPARE };

template<bool callMonitor, class RT, BindResultType bindResultType>
class BindTupleIterator : public TupleIterator {

public:

    typedef RT ResolverType;

protected:

    ResolverType& m_resolver;
    std::unique_ptr<BuiltinExpressionEvaluator> m_builtinExpressionEvaluator;
    const ArgumentIndex m_firstArgumentIndex;

public:

    BindTupleIterator(ResolverType& resolver, std::unique_ptr<BuiltinExpressionEvaluator> builtinExpressionEvaluator, TupleIteratorMonitor* const tupleIteratorMonitor, std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const ArgumentIndexSet& allInputArguments, const ArgumentIndexSet& surelyBoundInputArguments);

    BindTupleIterator(const BindTupleIterator<callMonitor, RT, bindResultType>& other, CloneReplacements& cloneReplacements);

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

template<class RT>
always_inline std::unique_ptr<TupleIterator> newBindTupleIterator(RT& resolver, std::unique_ptr<BuiltinExpressionEvaluator> builtinExpressionEvaluator, TupleIteratorMonitor* const tupleIteratorMonitor, std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const ArgumentIndexSet& allInputArguments, const ArgumentIndexSet& surelyBoundInputArguments) {
    const ArgumentIndex firstArgumentIndex = argumentIndexes[0];
    if (surelyBoundInputArguments.contains(firstArgumentIndex)) {
        if (tupleIteratorMonitor)
            return std::unique_ptr<TupleIterator>(new BindTupleIterator<true, RT, BIND_RESULT_ALWAYS_COMPARE>(resolver, std::move(builtinExpressionEvaluator), tupleIteratorMonitor, argumentsBuffer, argumentIndexes, allInputArguments, surelyBoundInputArguments));
        else
            return std::unique_ptr<TupleIterator>(new BindTupleIterator<false, RT, BIND_RESULT_ALWAYS_COMPARE>(resolver, std::move(builtinExpressionEvaluator), tupleIteratorMonitor, argumentsBuffer, argumentIndexes, allInputArguments, surelyBoundInputArguments));
    }
    else if (allInputArguments.contains(firstArgumentIndex)) {
        if (tupleIteratorMonitor)
            return std::unique_ptr<TupleIterator>(new BindTupleIterator<true, RT, BIND_RESULT_PRODUCE_OR_COMPARE>(resolver, std::move(builtinExpressionEvaluator), tupleIteratorMonitor, argumentsBuffer, argumentIndexes, allInputArguments, surelyBoundInputArguments));
        else
            return std::unique_ptr<TupleIterator>(new BindTupleIterator<false, RT, BIND_RESULT_PRODUCE_OR_COMPARE>(resolver, std::move(builtinExpressionEvaluator), tupleIteratorMonitor, argumentsBuffer, argumentIndexes, allInputArguments, surelyBoundInputArguments));
    }
    else {
        if (tupleIteratorMonitor)
            return std::unique_ptr<TupleIterator>(new BindTupleIterator<true, RT, BIND_RESULT_ALWAYS_PRODUCE>(resolver, std::move(builtinExpressionEvaluator), tupleIteratorMonitor, argumentsBuffer, argumentIndexes, allInputArguments, surelyBoundInputArguments));
        else
            return std::unique_ptr<TupleIterator>(new BindTupleIterator<false, RT, BIND_RESULT_ALWAYS_PRODUCE>(resolver, std::move(builtinExpressionEvaluator), tupleIteratorMonitor, argumentsBuffer, argumentIndexes, allInputArguments, surelyBoundInputArguments));
    }
}

#endif /* BINDTUPLEITERATOR_H_ */
