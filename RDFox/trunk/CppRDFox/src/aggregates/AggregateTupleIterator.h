// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef AGGREGATETUPLEITERATOR_H_
#define AGGREGATETUPLEITERATOR_H_

#include "../Common.h"
#include "../storage/TupleIterator.h"
#include "AggregateFunctionEvaluator.h"

class ThreadContext;
class BuiltinExpressionEvaluator;

typedef std::pair<std::unique_ptr<AggregateFunctionEvaluator>, unique_ptr_vector<BuiltinExpressionEvaluator> > AggregateFunctionCallInfo;

template<bool callMonitor, class RT>
class AggregateTupleIterator : public TupleIterator {

public:

    typedef RT ResolverType;

protected:

    enum ResultType { RESULT_ALWAYS_PRODUCE, RESULT_ALWAYS_COMPARE, RESULT_PRODUCE_OR_COMPARE };

    struct Call {

        ArgumentIndex m_resultArgumentIndex;
        ResultType m_resultType;
        std::unique_ptr<AggregateFunctionEvaluator> m_aggregateFunctionEvaluator;
        unique_ptr_vector<BuiltinExpressionEvaluator> m_argumentExpressionEvaluators;
        std::vector<ResourceValue> m_argumentValues;
        ResourceValue m_resultValue;

        Call(const ArgumentIndex resultArgumentIndex, const ResultType resultType, std::unique_ptr<AggregateFunctionEvaluator> aggregateFunctionEvaluator, unique_ptr_vector<BuiltinExpressionEvaluator> argumentExpressionEvaluators);

        Call(const Call& other, CloneReplacements& cloneReplacements);

        Call(const Call& other) = delete;

        Call(Call&& other);

        Call& operator=(const Call& other) = delete;

        Call& operator=(Call&& other);

        void open(ThreadContext& threadContext);

        void accummulate(ThreadContext& threadContext, const size_t multiplicity);

        bool finish(ThreadContext& threadContext, ResolverType& resolver, std::vector<ResourceID>& argumentsBuffer);

    };

    ResolverType& m_resolver;
    std::unique_ptr<TupleIterator> m_tupleIterator;
    std::vector<Call> m_calls;

public:

    AggregateTupleIterator(ResolverType& resolver, TupleIteratorMonitor* const tupleIteratorMonitor, const std::vector<ArgumentIndex>& argumentIndexes, const ArgumentIndexSet& allInputArguments, const ArgumentIndexSet& surelyBoundInputArguments, std::unique_ptr<TupleIterator> tupleIterator, std::vector<AggregateFunctionCallInfo>& aggregateFunctionCallInfos);

    AggregateTupleIterator(const AggregateTupleIterator<callMonitor, RT>& other, CloneReplacements& cloneReplacements);

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
always_inline std::unique_ptr<TupleIterator> newAggregateTupleIterator(RT& resolver, TupleIteratorMonitor* const tupleIteratorMonitor, const std::vector<ArgumentIndex>& argumentIndexes, const ArgumentIndexSet& allInputArguments, const ArgumentIndexSet& surelyBoundInputArguments, std::unique_ptr<TupleIterator> tupleIterator, std::vector<AggregateFunctionCallInfo>& aggregateFunctionCallInfos) {
    if (tupleIteratorMonitor)
        return std::unique_ptr<TupleIterator>(new AggregateTupleIterator<true, RT>(resolver, tupleIteratorMonitor, argumentIndexes, allInputArguments, surelyBoundInputArguments, std::move(tupleIterator), aggregateFunctionCallInfos));
    else
        return std::unique_ptr<TupleIterator>(new AggregateTupleIterator<false, RT>(resolver, tupleIteratorMonitor, argumentIndexes, allInputArguments, surelyBoundInputArguments, std::move(tupleIterator), aggregateFunctionCallInfos));
}

#endif /* AGGREGATETUPLEITERATOR_H_ */
