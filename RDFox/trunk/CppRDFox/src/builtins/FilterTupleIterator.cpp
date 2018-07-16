// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../dictionary/Dictionary.h"
#include "../storage/TupleIteratorMonitor.h"
#include "../util/ThreadContext.h"
#include "BuiltinExpressionEvaluator.h"
#include "FilterTupleIterator.h"

template<bool callMonitor>
FilterTupleIterator<callMonitor>::FilterTupleIterator(std::unique_ptr<BuiltinExpressionEvaluator> builtinExpressionEvaluator, TupleIteratorMonitor* const tupleIteratorMonitor, std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const ArgumentIndexSet& allInputArguments, const ArgumentIndexSet& surelyBoundInputArguments) :
    TupleIterator(tupleIteratorMonitor, argumentsBuffer, argumentIndexes, allInputArguments, surelyBoundInputArguments, allInputArguments, surelyBoundInputArguments),
    m_builtinExpressionEvaluator(std::move(builtinExpressionEvaluator))
{
}

template<bool callMonitor>
FilterTupleIterator<callMonitor>::FilterTupleIterator(const FilterTupleIterator<callMonitor>& other, CloneReplacements& cloneReplacements) :
    TupleIterator(other, cloneReplacements),
    m_builtinExpressionEvaluator(other.m_builtinExpressionEvaluator->clone(cloneReplacements))
{
}

template<bool callMonitor>
const char* FilterTupleIterator<callMonitor>::getName() const {
    return "FilterTupleIterator";
}

template<bool callMonitor>
size_t FilterTupleIterator<callMonitor>::getNumberOfChildIterators() const {
    return 0;
}

template<bool callMonitor>
const TupleIterator& FilterTupleIterator<callMonitor>::getChildIterator(const size_t childIteratorIndex) const {
    throw RDF_STORE_EXCEPTION("Invalid child iterator index.");
}

template<bool callMonitor>
std::unique_ptr<TupleIterator> FilterTupleIterator<callMonitor>::clone(CloneReplacements& cloneReplacements) const {
    return std::unique_ptr<TupleIterator>(new FilterTupleIterator(*this, cloneReplacements));
}

template<bool callMonitor>
size_t FilterTupleIterator<callMonitor>::open(ThreadContext& threadContext) {
    ResourceValue expressionValue;
    if (!m_builtinExpressionEvaluator->evaluate(threadContext, expressionValue) && Dictionary::getEffectiveBooleanValue(expressionValue) == EBV_TRUE) {
        if (callMonitor)
            m_tupleIteratorMonitor->iteratorOpened(*this, 1);
        return 1;
    }
    else {
        if (callMonitor)
            m_tupleIteratorMonitor->iteratorOpened(*this, 0);
        return 0;
    }
}

template<bool callMonitor>
size_t FilterTupleIterator<callMonitor>::open() {
    return FilterTupleIterator::open(ThreadContext::getCurrentThreadContext());
}

template<bool callMonitor>
size_t FilterTupleIterator<callMonitor>::advance() {
    if (callMonitor)
        m_tupleIteratorMonitor->iteratorAdvanced(*this, 1);
    return 0;
}

template<bool callMonitor>
bool FilterTupleIterator<callMonitor>::getCurrentTupleInfo(TupleIndex& tupleIndex, TupleStatus& tupleStatus) const {
    return false;
}

template<bool callMonitor>
TupleIndex FilterTupleIterator<callMonitor>::getCurrentTupleIndex() const {
    return INVALID_TUPLE_INDEX;
}

template class FilterTupleIterator<false>;
template class FilterTupleIterator<true>;
