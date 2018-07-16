// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../dictionary/Dictionary.h"
#include "../dictionary/ResourceValueCache.h"
#include "../storage/TupleIteratorMonitor.h"
#include "../util/ThreadContext.h"
#include "BuiltinExpressionEvaluator.h"
#include "BindTupleIterator.h"

template<bool callMonitor, class RT, BindResultType bindResultType>
BindTupleIterator<callMonitor, RT, bindResultType>::BindTupleIterator(ResolverType& resolver, std::unique_ptr<BuiltinExpressionEvaluator> builtinExpressionEvaluator, TupleIteratorMonitor* const tupleIteratorMonitor, std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const ArgumentIndexSet& allInputArguments, const ArgumentIndexSet& surelyBoundInputArguments) :
    TupleIterator(tupleIteratorMonitor, argumentsBuffer, argumentIndexes, allInputArguments, surelyBoundInputArguments, allInputArguments, surelyBoundInputArguments),
    m_resolver(resolver),
    m_builtinExpressionEvaluator(std::move(builtinExpressionEvaluator)),
    m_firstArgumentIndex(m_argumentIndexes[0])
{
    m_allArguments.add(m_firstArgumentIndex);
    m_surelyBoundArguments.add(m_firstArgumentIndex);
}

template<bool callMonitor, class RT, BindResultType bindResultType>
BindTupleIterator<callMonitor, RT, bindResultType>::BindTupleIterator(const BindTupleIterator<callMonitor, RT, bindResultType>& other, CloneReplacements& cloneReplacements) :
    TupleIterator(other, cloneReplacements),
    m_resolver(*cloneReplacements.getReplacement(&other.m_resolver)),
    m_builtinExpressionEvaluator(other.m_builtinExpressionEvaluator->clone(cloneReplacements)),
    m_firstArgumentIndex(other.m_firstArgumentIndex)
{
}

template<bool callMonitor, class RT, BindResultType bindResultType>
const char* BindTupleIterator<callMonitor, RT, bindResultType>::getName() const {
    return "BindTupleIterator";
}

template<bool callMonitor, class RT, BindResultType bindResultType>
size_t BindTupleIterator<callMonitor, RT, bindResultType>::getNumberOfChildIterators() const {
    return 0;
}

template<bool callMonitor, class RT, BindResultType bindResultType>
const TupleIterator& BindTupleIterator<callMonitor, RT, bindResultType>::getChildIterator(const size_t childIteratorIndex) const {
    throw RDF_STORE_EXCEPTION("Invalid child iterator index.");
}

template<bool callMonitor, class RT, BindResultType bindResultType>
std::unique_ptr<TupleIterator> BindTupleIterator<callMonitor, RT, bindResultType>::clone(CloneReplacements& cloneReplacements) const {
    return std::unique_ptr<TupleIterator>(new BindTupleIterator<callMonitor, RT, bindResultType>(*this, cloneReplacements));
}

template<bool callMonitor, class RT, BindResultType bindResultType>
size_t BindTupleIterator<callMonitor, RT, bindResultType>::open(ThreadContext& threadContext) {
    ResourceValue expressionValue;
    size_t multiplicity;
    if (!m_builtinExpressionEvaluator->evaluate(threadContext, expressionValue)) {
        switch (bindResultType) {
        case BIND_RESULT_ALWAYS_PRODUCE:
            m_argumentsBuffer[m_firstArgumentIndex] = m_resolver.resolveResource(threadContext, expressionValue);
            multiplicity = 1;
            break;
        case BIND_RESULT_ALWAYS_COMPARE:
            multiplicity = m_resolver.tryResolveResource(threadContext, expressionValue) == m_argumentsBuffer[m_firstArgumentIndex] ? 1 : 0;
            break;
        case BIND_RESULT_PRODUCE_OR_COMPARE:
            if (m_argumentsBuffer[m_firstArgumentIndex] == INVALID_RESOURCE_ID) {
                m_argumentsBuffer[m_firstArgumentIndex] = m_resolver.resolveResource(threadContext, expressionValue);
                multiplicity = 1;
            }
            else
                multiplicity = m_resolver.tryResolveResource(threadContext, expressionValue) == m_argumentsBuffer[m_firstArgumentIndex] ? 1 : 0;
            break;
        }
    }
    else
        multiplicity = 0;
    if (callMonitor)
        m_tupleIteratorMonitor->iteratorOpened(*this, multiplicity);
    return multiplicity;
}

template<bool callMonitor, class RT, BindResultType bindResultType>
size_t BindTupleIterator<callMonitor, RT, bindResultType>::open() {
    return BindTupleIterator<callMonitor, RT, bindResultType>::open(ThreadContext::getCurrentThreadContext());
}

template<bool callMonitor, class RT, BindResultType bindResultType>
size_t BindTupleIterator<callMonitor, RT, bindResultType>::advance() {
    if (callMonitor)
        m_tupleIteratorMonitor->iteratorAdvanced(*this, 0);
    return 0;
}

template<bool callMonitor, class RT, BindResultType bindResultType>
bool BindTupleIterator<callMonitor, RT, bindResultType>::getCurrentTupleInfo(TupleIndex& tupleIndex, TupleStatus& tupleStatus) const {
    return false;
}

template<bool callMonitor, class RT, BindResultType bindResultType>
TupleIndex BindTupleIterator<callMonitor, RT, bindResultType>::getCurrentTupleIndex() const {
    return INVALID_TUPLE_INDEX;
}

template class BindTupleIterator<false, Dictionary, BIND_RESULT_ALWAYS_PRODUCE>;
template class BindTupleIterator<false, Dictionary, BIND_RESULT_ALWAYS_COMPARE>;
template class BindTupleIterator<false, Dictionary, BIND_RESULT_PRODUCE_OR_COMPARE>;
template class BindTupleIterator<true, Dictionary, BIND_RESULT_ALWAYS_PRODUCE>;
template class BindTupleIterator<true, Dictionary, BIND_RESULT_ALWAYS_COMPARE>;
template class BindTupleIterator<true, Dictionary, BIND_RESULT_PRODUCE_OR_COMPARE>;
template class BindTupleIterator<false, ResourceValueCache, BIND_RESULT_ALWAYS_PRODUCE>;
template class BindTupleIterator<false, ResourceValueCache, BIND_RESULT_ALWAYS_COMPARE>;
template class BindTupleIterator<false, ResourceValueCache, BIND_RESULT_PRODUCE_OR_COMPARE>;
template class BindTupleIterator<true, ResourceValueCache, BIND_RESULT_ALWAYS_PRODUCE>;
template class BindTupleIterator<true, ResourceValueCache, BIND_RESULT_ALWAYS_COMPARE>;
template class BindTupleIterator<true, ResourceValueCache, BIND_RESULT_PRODUCE_OR_COMPARE>;
