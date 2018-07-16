// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../util/ThreadContext.h"
#include "../storage/TupleIteratorMonitor.h"
#include "DifferenceIterator.h"

template<bool callMonitor>
DifferenceIterator<callMonitor>::DifferenceIterator(TupleIteratorMonitor* const tupleIteratorMonitor, unique_ptr_vector<TupleIterator> childIterators) :
    TupleIterator(tupleIteratorMonitor, childIterators[0]->getArgumentsBuffer(), childIterators[0]->getArgumentIndexes(), childIterators[0]->getAllInputArguments(), childIterators[0]->getSurelyBoundInputArguments(), childIterators[0]->getAllArguments(), childIterators[0]->getSurelyBoundArguments()),
    m_childIterators(std::move(childIterators)),
    m_threadContext(0),
    m_notSurelyBoundInputVariables()
{
    assert(!m_childIterators.empty());
    for (std::vector<ArgumentIndex>::iterator iterator = m_argumentIndexes.begin(); iterator != m_argumentIndexes.end(); ++iterator)
        if (!m_surelyBoundInputArguments.contains(*iterator))
            m_notSurelyBoundInputVariables.push_back(std::make_pair(*iterator, INVALID_RESOURCE_ID));
    m_notSurelyBoundInputVariables.shrink_to_fit();
}

template<bool callMonitor>
DifferenceIterator<callMonitor>::DifferenceIterator(const DifferenceIterator<callMonitor>& other, CloneReplacements& cloneReplacements) :
    TupleIterator(other, cloneReplacements),
    m_childIterators(),
    m_threadContext(0),
    m_notSurelyBoundInputVariables(other.m_notSurelyBoundInputVariables)
{
    for (unique_ptr_vector<TupleIterator>::const_iterator iterator = other.m_childIterators.begin(); iterator != other.m_childIterators.end(); ++iterator)
        m_childIterators.push_back((*iterator)->clone(cloneReplacements));
}

template<bool callMonitor>
const char* DifferenceIterator<callMonitor>::getName() const {
    return "DifferenceIterator";
}

template<bool callMonitor>
size_t DifferenceIterator<callMonitor>::getNumberOfChildIterators() const {
    return m_childIterators.size();
}

template<bool callMonitor>
const TupleIterator& DifferenceIterator<callMonitor>::getChildIterator(const size_t childIteratorIndex) const {
    return *m_childIterators[childIteratorIndex];
}

template<bool callMonitor>
std::unique_ptr<TupleIterator> DifferenceIterator<callMonitor>::clone(CloneReplacements& cloneReplacements) const {
    return std::unique_ptr<TupleIterator>(new DifferenceIterator<callMonitor>(*this, cloneReplacements));
}

template<bool callMonitor>
always_inline size_t DifferenceIterator<callMonitor>::ensureOnTuple(size_t multiplicity) {
    while (multiplicity != 0) {
        for (std::vector<std::pair<ArgumentIndex, ResourceID> >::iterator iterator = m_notSurelyBoundInputVariables.begin(); iterator != m_notSurelyBoundInputVariables.end(); ++iterator)
            iterator->second = m_argumentsBuffer[iterator->first];
        bool noExtensionSeen = true;
        for (unique_ptr_vector<TupleIterator>::iterator iterator = m_childIterators.begin() + 1; noExtensionSeen && iterator != m_childIterators.end(); ++iterator) {
            if ((*iterator)->open(*m_threadContext) != 0)
                noExtensionSeen = false;
            for (std::vector<std::pair<ArgumentIndex, ResourceID> >::iterator iterator = m_notSurelyBoundInputVariables.begin(); iterator != m_notSurelyBoundInputVariables.end(); ++iterator)
                m_argumentsBuffer[iterator->first] = iterator->second;
        }
        if (noExtensionSeen)
            return multiplicity;
        multiplicity = m_childIterators[0]->advance();
    }
    return 0;
}

template<bool callMonitor>
size_t DifferenceIterator<callMonitor>::open(ThreadContext& threadContext) {
    m_threadContext = &threadContext;
    const size_t tupleMultiplicity = ensureOnTuple(m_childIterators[0]->open(*m_threadContext));
    if (callMonitor)
        m_tupleIteratorMonitor->iteratorOpened(*this, tupleMultiplicity);
    return tupleMultiplicity;
}

template<bool callMonitor>
size_t DifferenceIterator<callMonitor>::open() {
    return DifferenceIterator<callMonitor>::open(ThreadContext::getCurrentThreadContext());
}

template<bool callMonitor>
size_t DifferenceIterator<callMonitor>::advance() {
    const size_t tupleMultiplicity = ensureOnTuple(m_childIterators[0]->advance());
    if (callMonitor)
        m_tupleIteratorMonitor->iteratorAdvanced(*this, tupleMultiplicity);
    return tupleMultiplicity;
}

template<bool callMonitor>
bool DifferenceIterator<callMonitor>::getCurrentTupleInfo(TupleIndex& tupleIndex, TupleStatus& tupleStatus) const {
    return m_childIterators[0]->getCurrentTupleInfo(tupleIndex, tupleStatus);
}

template<bool callMonitor>
TupleIndex DifferenceIterator<callMonitor>::getCurrentTupleIndex() const {
    return m_childIterators[0]->getCurrentTupleIndex();
}

template class DifferenceIterator<false>;
template class DifferenceIterator<true>;
