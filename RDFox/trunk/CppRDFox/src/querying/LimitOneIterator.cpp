// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../RDFStoreException.h"
#include "../util/ThreadContext.h"
#include "../storage/TupleIteratorMonitor.h"
#include "LimitOneIterator.h"

template<bool callMonitor>
LimitOneIterator<callMonitor>::LimitOneIterator(TupleIteratorMonitor* const tupleIteratorMonitor, std::unique_ptr<TupleIterator> tupleIterator) :
    TupleIterator(tupleIteratorMonitor, tupleIterator->getArgumentsBuffer(), tupleIterator->getArgumentIndexes(), tupleIterator->getAllInputArguments(), tupleIterator->getSurelyBoundInputArguments(), tupleIterator->getAllArguments(), tupleIterator->getSurelyBoundArguments()),
    m_tupleIterator(std::move(tupleIterator))
{
    eliminateArgumentIndexesRedundancy();
}

template<bool callMonitor>
LimitOneIterator<callMonitor>::LimitOneIterator(const LimitOneIterator<callMonitor>& other, CloneReplacements& cloneReplacements) :
    TupleIterator(other, cloneReplacements),
    m_tupleIterator(other.m_tupleIterator->clone(cloneReplacements))
{
}

template<bool callMonitor>
const char* LimitOneIterator<callMonitor>::getName() const {
    return "LimitOneIterator";
}

template<bool callMonitor>
size_t LimitOneIterator<callMonitor>::getNumberOfChildIterators() const {
    return 1;
}

template<bool callMonitor>
const TupleIterator& LimitOneIterator<callMonitor>::getChildIterator(const size_t childIteratorIndex) const {
    switch (childIteratorIndex) {
    case 0:
        return *m_tupleIterator;
    default:
        throw RDF_STORE_EXCEPTION("Invalid child iterator index.");
    }
}

template<bool callMonitor>
std::unique_ptr<TupleIterator> LimitOneIterator<callMonitor>::clone(CloneReplacements& cloneReplacements) const {
    return std::unique_ptr<TupleIterator>(new LimitOneIterator<callMonitor>(*this, cloneReplacements));
}

template<bool callMonitor>
always_inline size_t LimitOneIterator<callMonitor>::open(ThreadContext& threadContext) {
    size_t multiplicity = m_tupleIterator->open(threadContext);
    if (callMonitor)
        m_tupleIteratorMonitor->iteratorOpened(*this, multiplicity);
    return multiplicity;
}

template<bool callMonitor>
size_t LimitOneIterator<callMonitor>::open() {
    return LimitOneIterator<callMonitor>::open(ThreadContext::getCurrentThreadContext());
}

template<bool callMonitor>
size_t LimitOneIterator<callMonitor>::advance() {
    if (callMonitor)
        m_tupleIteratorMonitor->iteratorAdvanced(*this, 0);
    return 0;
}

template<bool callMonitor>
bool LimitOneIterator<callMonitor>::getCurrentTupleInfo(TupleIndex& tupleIndex, TupleStatus& tupleStatus) const {
    return m_tupleIterator->getCurrentTupleInfo(tupleIndex, tupleStatus);
}

template<bool callMonitor>
TupleIndex LimitOneIterator<callMonitor>::getCurrentTupleIndex() const {
    return m_tupleIterator->getCurrentTupleIndex();
}

template class LimitOneIterator<false>;
template class LimitOneIterator<true>;
