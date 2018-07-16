// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../RDFStoreException.h"
#include "../util/ThreadContext.h"
#include "../dictionary/Dictionary.h"
#include "../storage/TupleTable.h"
#include "../storage/TupleIteratorMonitor.h"
#include "../storage/DataStore.h"
#include "EmptyTupleIterator.h"

template<bool callMonitor>
EmptyTupleIterator<callMonitor>::EmptyTupleIterator(TupleIteratorMonitor* const tupleIteratorMonitor, std::vector<ResourceID>& argumentsBuffer, const ArgumentIndexSet& allInputArguments, const ArgumentIndexSet& surelyBoundInputArguments) :
    TupleIterator(tupleIteratorMonitor, argumentsBuffer, allInputArguments, surelyBoundInputArguments, allInputArguments, surelyBoundInputArguments)
{
}

template<bool callMonitor>
EmptyTupleIterator<callMonitor>::EmptyTupleIterator(const EmptyTupleIterator<callMonitor>& other, CloneReplacements& cloneReplacements) : TupleIterator(other, cloneReplacements) {
}

template<bool callMonitor>
const char* EmptyTupleIterator<callMonitor>::getName() const {
    return "EmptyTupleIterator";
}

template<bool callMonitor>
size_t EmptyTupleIterator<callMonitor>::getNumberOfChildIterators() const {
    return 0;
}

template<bool callMonitor>
const TupleIterator& EmptyTupleIterator<callMonitor>::getChildIterator(const size_t childIteratorIndex) const {
    throw RDF_STORE_EXCEPTION("Invalid child iterator index.");
}

template<bool callMonitor>
std::unique_ptr<TupleIterator> EmptyTupleIterator<callMonitor>::clone(CloneReplacements& cloneReplacements) const {
    return std::unique_ptr<TupleIterator>(new EmptyTupleIterator(*this, cloneReplacements));
}

template<bool callMonitor>
always_inline size_t EmptyTupleIterator<callMonitor>::open(ThreadContext& threadContext) {
    if (callMonitor)
        m_tupleIteratorMonitor->iteratorOpened(*this, 1);
    return 1;
}

template<bool callMonitor>
size_t EmptyTupleIterator<callMonitor>::open() {
    if (callMonitor)
        m_tupleIteratorMonitor->iteratorOpened(*this, 1);
    return 1;
}

template<bool callMonitor>
size_t EmptyTupleIterator<callMonitor>::advance() {
    if (callMonitor)
        m_tupleIteratorMonitor->iteratorAdvanced(*this, 0);
    return 0;
}

template<bool callMonitor>
bool EmptyTupleIterator<callMonitor>::getCurrentTupleInfo(TupleIndex& tupleIndex, TupleStatus& tupleStatus) const {
    return false;
}

template<bool callMonitor>
TupleIndex EmptyTupleIterator<callMonitor>::getCurrentTupleIndex() const {
    return INVALID_TUPLE_INDEX;
}

template class EmptyTupleIterator<false>;
template class EmptyTupleIterator<true>;
