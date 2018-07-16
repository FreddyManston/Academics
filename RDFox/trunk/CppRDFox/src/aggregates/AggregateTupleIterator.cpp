// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../dictionary/Dictionary.h"
#include "../dictionary/ResourceValueCache.h"
#include "../storage/TupleIteratorMonitor.h"
#include "../util/ThreadContext.h"
#include "../builtins/BuiltinExpressionEvaluator.h"
#include "AggregateFunctionEvaluator.h"
#include "AggregateTupleIterator.h"

// AggregateTupleIterator::Call

template<bool callMonitor, class RT>
always_inline AggregateTupleIterator<callMonitor, RT>::Call::Call(const ArgumentIndex resultArgumentIndex, const ResultType resultType, std::unique_ptr<AggregateFunctionEvaluator> aggregateFunctionEvaluator, unique_ptr_vector<BuiltinExpressionEvaluator> argumentExpressionEvaluators) :
    m_resultArgumentIndex(resultArgumentIndex),
    m_resultType(resultType),
    m_aggregateFunctionEvaluator(std::move(aggregateFunctionEvaluator)),
    m_argumentExpressionEvaluators(std::move(argumentExpressionEvaluators)),
    m_argumentValues(m_argumentExpressionEvaluators.size()),
    m_resultValue()
{
}

template<bool callMonitor, class RT>
always_inline AggregateTupleIterator<callMonitor, RT>::Call::Call(const Call& other, CloneReplacements& cloneReplacements) :
    m_resultArgumentIndex(other.m_resultArgumentIndex),
    m_resultType(other.m_resultType),
    m_aggregateFunctionEvaluator(other.m_aggregateFunctionEvaluator->clone(cloneReplacements)),
    m_argumentExpressionEvaluators(),
    m_argumentValues(other.m_argumentValues),
    m_resultValue(other.m_resultValue)
{
    for (auto iterator = other.m_argumentExpressionEvaluators.begin(); iterator != other.m_argumentExpressionEvaluators.end(); ++iterator)
        m_argumentExpressionEvaluators.push_back((*iterator)->clone(cloneReplacements));
}

template<bool callMonitor, class RT>
always_inline AggregateTupleIterator<callMonitor, RT>::Call::Call(Call&& other) :
    m_resultArgumentIndex(other.m_resultArgumentIndex),
    m_resultType(other.m_resultType),
    m_aggregateFunctionEvaluator(std::move(other.m_aggregateFunctionEvaluator)),
    m_argumentExpressionEvaluators(std::move(other.m_argumentExpressionEvaluators)),
    m_argumentValues(std::move(other.m_argumentValues)),
    m_resultValue(std::move(other.m_resultValue))
{
}

template<bool callMonitor, class RT>
always_inline typename AggregateTupleIterator<callMonitor, RT>::Call& AggregateTupleIterator<callMonitor, RT>::Call::operator=(Call&& other) {
    m_resultArgumentIndex = other.m_resultArgumentIndex;
    m_resultType = other.m_resultType;
    m_aggregateFunctionEvaluator = std::move(other.m_aggregateFunctionEvaluator);
    m_argumentExpressionEvaluators = std::move(other.m_argumentExpressionEvaluators);
    m_argumentValues = std::move(other.m_argumentValues);
    m_resultValue = std::move(other.m_resultValue);
    return *this;
}

template<bool callMonitor, class RT>
always_inline void AggregateTupleIterator<callMonitor, RT>::Call::open(ThreadContext& threadContext) {
    m_aggregateFunctionEvaluator->open(threadContext);
}

template<bool callMonitor, class RT>
always_inline void AggregateTupleIterator<callMonitor, RT>::Call::accummulate(ThreadContext& threadContext, const size_t multiplicity) {
    auto resultIterator = m_argumentValues.begin();
    for (auto argumentIterator = m_argumentExpressionEvaluators.begin(); argumentIterator != m_argumentExpressionEvaluators.end(); ++argumentIterator) {
        if ((*argumentIterator)->evaluate(threadContext, *resultIterator))
            return;
        ++resultIterator;
    }
    m_aggregateFunctionEvaluator->accummulate(threadContext, m_argumentValues, multiplicity);
}

template<bool callMonitor, class RT>
always_inline bool AggregateTupleIterator<callMonitor, RT>::Call::finish(ThreadContext& threadContext, ResolverType& resolver, std::vector<ResourceID>& argumentsBuffer) {
    if (m_aggregateFunctionEvaluator->finish(threadContext, m_resultValue))
        return true;
    switch (m_resultType) {
    case RESULT_ALWAYS_PRODUCE:
        argumentsBuffer[m_resultArgumentIndex] = resolver.resolveResource(threadContext, m_resultValue);
        return false;
    case RESULT_ALWAYS_COMPARE:
        return resolver.tryResolveResource(threadContext, m_resultValue) != argumentsBuffer[m_resultArgumentIndex];
    case RESULT_PRODUCE_OR_COMPARE:
        if (argumentsBuffer[m_resultArgumentIndex] == INVALID_RESOURCE_ID) {
            argumentsBuffer[m_resultArgumentIndex] = resolver.resolveResource(threadContext, m_resultValue);
            return false;
        }
        else
            return resolver.tryResolveResource(threadContext, m_resultValue) != argumentsBuffer[m_resultArgumentIndex];
    default:
        UNREACHABLE;
    }
}

// AggregateTupleIterator

template<bool callMonitor, class RT>
AggregateTupleIterator<callMonitor, RT>::AggregateTupleIterator(ResolverType& resolver, TupleIteratorMonitor* const tupleIteratorMonitor, const std::vector<ArgumentIndex>& argumentIndexes, const ArgumentIndexSet& allInputArguments, const ArgumentIndexSet& surelyBoundInputArguments, std::unique_ptr<TupleIterator> tupleIterator, std::vector<AggregateFunctionCallInfo>& aggregateFunctionCallInfos) :
    TupleIterator(tupleIteratorMonitor, tupleIterator->getArgumentsBuffer(), argumentIndexes, allInputArguments, surelyBoundInputArguments, allInputArguments, surelyBoundInputArguments),
    m_resolver(resolver),
    m_tupleIterator(std::move(tupleIterator)),
    m_calls()
{
    auto resultArgumentIndexIterator = m_argumentIndexes.end() - aggregateFunctionCallInfos.size();
    for (auto aggregateFunctionCallInfoIterator = aggregateFunctionCallInfos.begin(); aggregateFunctionCallInfoIterator != aggregateFunctionCallInfos.end(); ++aggregateFunctionCallInfoIterator) {
        const ArgumentIndex resultArgumentIndex = *resultArgumentIndexIterator;
        ResultType resultType;
        if (surelyBoundInputArguments.contains(resultArgumentIndex))
            resultType = RESULT_ALWAYS_COMPARE;
        else if (allInputArguments.contains(resultArgumentIndex))
            resultType = RESULT_PRODUCE_OR_COMPARE;
        else
            resultType = RESULT_ALWAYS_PRODUCE;
        m_calls.emplace_back(*resultArgumentIndexIterator, resultType, std::move(aggregateFunctionCallInfoIterator->first), std::move(aggregateFunctionCallInfoIterator->second));
        ++resultArgumentIndexIterator;
    }
}

template<bool callMonitor, class RT>
AggregateTupleIterator<callMonitor, RT>::AggregateTupleIterator(const AggregateTupleIterator<callMonitor, RT>& other, CloneReplacements& cloneReplacements) :
    TupleIterator(other, cloneReplacements),
    m_resolver(*cloneReplacements.getReplacement(&other.m_resolver)),
    m_tupleIterator(other.m_tupleIterator->clone(cloneReplacements)),
    m_calls()
{
    for (auto iterator = other.m_calls.begin(); iterator != other.m_calls.end(); ++iterator)
        m_calls.emplace_back(*iterator, cloneReplacements);
}

template<bool callMonitor, class RT>
const char* AggregateTupleIterator<callMonitor, RT>::getName() const {
    return "AggregateTupleIterator";
}

template<bool callMonitor, class RT>
size_t AggregateTupleIterator<callMonitor, RT>::getNumberOfChildIterators() const {
    return 1;
}

template<bool callMonitor, class RT>
const TupleIterator& AggregateTupleIterator<callMonitor, RT>::getChildIterator(const size_t childIteratorIndex) const {
    switch (childIteratorIndex) {
    case 0:
        return *m_tupleIterator;
    default:
        throw RDF_STORE_EXCEPTION("Invalid child iterator index.");
    }
}

template<bool callMonitor, class RT>
std::unique_ptr<TupleIterator> AggregateTupleIterator<callMonitor, RT>::clone(CloneReplacements& cloneReplacements) const {
    return std::unique_ptr<TupleIterator>(new AggregateTupleIterator<callMonitor, RT>(*this, cloneReplacements));
}

template<bool callMonitor, class RT>
size_t AggregateTupleIterator<callMonitor, RT>::open(ThreadContext& threadContext) {
    for (auto iterator = m_calls.begin(); iterator != m_calls.end(); ++iterator)
        iterator->open(threadContext);
    for (size_t multiplicity = m_tupleIterator->open(threadContext); multiplicity != 0; multiplicity = m_tupleIterator->advance())
        for (auto iterator = m_calls.begin(); iterator != m_calls.end(); ++iterator)
            iterator->accummulate(threadContext, multiplicity);
    for (auto iterator = m_calls.begin(); iterator != m_calls.end(); ++iterator)
        if (iterator->finish(threadContext, m_resolver, m_argumentsBuffer)) {
            if (callMonitor)
                m_tupleIteratorMonitor->iteratorOpened(*this, 0);
            return 0;
        }
    if (callMonitor)
        m_tupleIteratorMonitor->iteratorOpened(*this, 1);
    return 1;
}

template<bool callMonitor, class RT>
size_t AggregateTupleIterator<callMonitor, RT>::open() {
    return AggregateTupleIterator<callMonitor, RT>::open(ThreadContext::getCurrentThreadContext());
}

template<bool callMonitor, class RT>
size_t AggregateTupleIterator<callMonitor, RT>::advance() {
    if (callMonitor)
        m_tupleIteratorMonitor->iteratorAdvanced(*this, 0);
    return 0;
}

template<bool callMonitor, class RT>
bool AggregateTupleIterator<callMonitor, RT>::getCurrentTupleInfo(TupleIndex& tupleIndex, TupleStatus& tupleStatus) const {
    return false;
}

template<bool callMonitor, class RT>
TupleIndex AggregateTupleIterator<callMonitor, RT>::getCurrentTupleIndex() const {
    return INVALID_TUPLE_INDEX;
}

template class AggregateTupleIterator<false, Dictionary>;
template class AggregateTupleIterator<true, Dictionary>;
template class AggregateTupleIterator<false, ResourceValueCache>;
template class AggregateTupleIterator<true, ResourceValueCache>;
