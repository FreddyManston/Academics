// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../RDFStoreException.h"
#include "../util/ThreadContext.h"
#include "../storage/TupleIteratorMonitor.h"
#include "NegationIterator.h"

template<bool callMonitor>
NegationIterator<callMonitor>::NegationIterator(TupleIteratorMonitor* const tupleIteratorMonitor, std::unique_ptr<TupleIterator> tupleIterator) :
    TupleIterator(tupleIteratorMonitor, tupleIterator->getArgumentsBuffer(), tupleIterator->getArgumentIndexes(), tupleIterator->getAllInputArguments(), tupleIterator->getSurelyBoundInputArguments(), tupleIterator->getAllArguments(), tupleIterator->getSurelyBoundArguments()),
    m_tupleIterator(std::move(tupleIterator))
{
}

template<bool callMonitor>
NegationIterator<callMonitor>::NegationIterator(const NegationIterator& other, CloneReplacements& cloneReplacements) :
    TupleIterator(other, cloneReplacements),
    m_tupleIterator(other.m_tupleIterator->clone(cloneReplacements))
{
}

template<bool callMonitor>
const char* NegationIterator<callMonitor>::getName() const {
    return "NegationIterator";
}

template<bool callMonitor>
size_t NegationIterator<callMonitor>::getNumberOfChildIterators() const {
    return 1;
}

template<bool callMonitor>
const TupleIterator& NegationIterator<callMonitor>::getChildIterator(const size_t childIteratorIndex) const {
    switch (childIteratorIndex) {
    case 0:
        return *m_tupleIterator;
    default:
        throw RDF_STORE_EXCEPTION("Invalid child iterator index.");
    }
}

template<bool callMonitor>
std::unique_ptr<TupleIterator> NegationIterator<callMonitor>::clone(CloneReplacements& cloneReplacements) const {
    return std::unique_ptr<TupleIterator>(new NegationIterator(*this, cloneReplacements));
}

template<bool callMonitor>
size_t NegationIterator<callMonitor>::open(ThreadContext& threadContext) {
    const size_t multiplicity = (m_tupleIterator->open(threadContext) == 0 ? 1 : 0);
    if (callMonitor)
        m_tupleIteratorMonitor->iteratorOpened(*this, multiplicity);
    return multiplicity;
}

template<bool callMonitor>
size_t NegationIterator<callMonitor>::open() {
    return NegationIterator::open(ThreadContext::getCurrentThreadContext());
}

template<bool callMonitor>
size_t NegationIterator<callMonitor>::advance() {
    if (callMonitor)
        m_tupleIteratorMonitor->iteratorAdvanced(*this, 0);
    return 0;
}

template<bool callMonitor>
bool NegationIterator<callMonitor>::getCurrentTupleInfo(TupleIndex& tupleIndex, TupleStatus& tupleStatus) const {
    return false;
}

template<bool callMonitor>
TupleIndex NegationIterator<callMonitor>::getCurrentTupleIndex() const {
    return INVALID_TUPLE_INDEX;
}

template class NegationIterator<false>;
template class NegationIterator<true>;
