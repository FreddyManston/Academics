// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../RDFStoreException.h"
#include "../util/ThreadContext.h"
#include "../storage/TupleIteratorMonitor.h"
#include "ValuationCacheImpl.h"
#include "DistinctIterator.h"

template<class T>
always_inline T* firstElementPointer(std::vector<T>& vector) {
    if (vector.empty())
        return 0;
    else
        return &vector[0];
}

template<bool callMonitor>
DistinctIterator<callMonitor>::DistinctIterator(TupleIteratorMonitor* const tupleIteratorMonitor, MemoryManager& memoryManager, std::unique_ptr<TupleIterator> tupleIterator) :
    TupleIterator(tupleIteratorMonitor, tupleIterator->getArgumentsBuffer(), tupleIterator->getArgumentIndexes(), tupleIterator->getAllInputArguments(), tupleIterator->getSurelyBoundInputArguments(), tupleIterator->getAllArguments(), tupleIterator->getSurelyBoundArguments()),
    m_valuationCache(memoryManager, static_cast<uint16_t>(m_surelyBoundInputArguments.size()), static_cast<uint16_t>(m_allArguments.size()) - static_cast<uint16_t>(m_surelyBoundInputArguments.size())),
    m_tupleIterator(std::move(tupleIterator)),
    m_surelyBoundInputArgumentIndexes(),
    m_onlyPossiblyBoundInputValues(),
    m_onlyPossiblyBoundInputArgumentIndexes(),
    m_onlyPossiblyBoundInputOffsets(),
    m_outputArgumentIndexes(),
    m_argumentsBufferFirstElement(0),
    m_surelyBoundInputArgumentIndexesFirstElement(0),
    m_outputArgumentIndexesFirstElement(0),
    m_currentOutputValuation(0)
{
    eliminateArgumentIndexesRedundancy();
    for (std::vector<ArgumentIndex>::iterator iterator = m_argumentIndexes.begin(); iterator != m_argumentIndexes.end(); ++iterator) {
        if (m_surelyBoundInputArguments.contains(*iterator))
            m_surelyBoundInputArgumentIndexes.push_back(*iterator);
        else if (m_allInputArguments.contains(*iterator)) {
            m_onlyPossiblyBoundInputValues.push_back(INVALID_RESOURCE_ID);
            m_onlyPossiblyBoundInputArgumentIndexes.push_back(*iterator);
            m_onlyPossiblyBoundInputOffsets.push_back(m_outputArgumentIndexes.size());
            m_outputArgumentIndexes.push_back(*iterator);
        }
        else if (m_allArguments.contains(*iterator))
            m_outputArgumentIndexes.push_back(*iterator);
    }
    m_argumentsBufferFirstElement = ::firstElementPointer(m_argumentsBuffer);
    m_surelyBoundInputArgumentIndexesFirstElement = ::firstElementPointer(m_surelyBoundInputArgumentIndexes);
    m_outputArgumentIndexesFirstElement = ::firstElementPointer(m_outputArgumentIndexes);
}

template<bool callMonitor>
DistinctIterator<callMonitor>::DistinctIterator(const DistinctIterator<callMonitor>& other, CloneReplacements& cloneReplacements) :
    TupleIterator(other, cloneReplacements),
    m_valuationCache(other.m_valuationCache.getMemoryManager(), other.m_valuationCache.m_inputValuationManager.getInputValuationSize(), other.m_valuationCache.m_outputValuationManager.getOutputValuationSize()),
    m_tupleIterator(other.m_tupleIterator->clone(cloneReplacements)),
    m_surelyBoundInputArgumentIndexes(other.m_surelyBoundInputArgumentIndexes),
    m_onlyPossiblyBoundInputValues(other.m_onlyPossiblyBoundInputValues),
    m_onlyPossiblyBoundInputArgumentIndexes(other.m_onlyPossiblyBoundInputArgumentIndexes),
    m_onlyPossiblyBoundInputOffsets(other.m_onlyPossiblyBoundInputOffsets),
    m_outputArgumentIndexes(other.m_outputArgumentIndexes),
    m_argumentsBufferFirstElement(::firstElementPointer(m_argumentsBuffer)),
    m_surelyBoundInputArgumentIndexesFirstElement(::firstElementPointer(m_surelyBoundInputArgumentIndexes)),
    m_outputArgumentIndexesFirstElement(::firstElementPointer(m_outputArgumentIndexes)),
    m_currentOutputValuation(other.m_currentOutputValuation)
{
}

template<bool callMonitor>
const char* DistinctIterator<callMonitor>::getName() const {
    return "DistinctIterator";
}

template<bool callMonitor>
size_t DistinctIterator<callMonitor>::getNumberOfChildIterators() const {
    return 1;
}

template<bool callMonitor>
const TupleIterator& DistinctIterator<callMonitor>::getChildIterator(const size_t childIteratorIndex) const {
    switch (childIteratorIndex) {
    case 0:
        return *m_tupleIterator;
    default:
        throw RDF_STORE_EXCEPTION("Invalid child iterator index.");
    }
}

template<bool callMonitor>
std::unique_ptr<TupleIterator> DistinctIterator<callMonitor>::clone(CloneReplacements& cloneReplacements) const {
    return std::unique_ptr<TupleIterator>(new DistinctIterator<callMonitor>(*this, cloneReplacements));
}

template<bool callMonitor>
size_t DistinctIterator<callMonitor>::open(ThreadContext& threadContext) {
    const size_t numberOfOnlyPossiblyBoundInputValues = m_onlyPossiblyBoundInputValues.size();
    for (size_t index = 0; index < numberOfOnlyPossiblyBoundInputValues; ++index)
        m_onlyPossiblyBoundInputValues[index] = m_argumentsBuffer[m_onlyPossiblyBoundInputArgumentIndexes[index]];
    bool alreadyExistsInput;
    InputValuation* inputValuation = m_valuationCache.m_inputValuationManager.resolveInputValuation(m_argumentsBufferFirstElement, m_surelyBoundInputArgumentIndexesFirstElement, alreadyExistsInput);
    if (!alreadyExistsInput) {
        size_t multiplicity = m_tupleIterator->open(threadContext);
        while (multiplicity != 0) {
            bool alreadyExistsOutput;
            m_valuationCache.m_outputValuationManager.resolveOutputValuation(inputValuation, m_argumentsBufferFirstElement, m_outputArgumentIndexesFirstElement, multiplicity, alreadyExistsOutput);
            multiplicity = m_tupleIterator->advance();
        }
    }
    m_currentOutputValuation = inputValuation->getFirstOutputValuation();
    while (m_currentOutputValuation && !m_currentOutputValuation->valuesMatch(m_onlyPossiblyBoundInputValues.begin(), m_onlyPossiblyBoundInputOffsets.begin(), m_onlyPossiblyBoundInputOffsets.end()))
        m_currentOutputValuation = m_currentOutputValuation->getNextOutputValuation();
    size_t multiplicity;
    if (m_currentOutputValuation) {
        multiplicity = m_currentOutputValuation->getMultiplicity();
        m_valuationCache.m_outputValuationManager.loadOutputValuation(m_currentOutputValuation, m_argumentsBufferFirstElement, m_outputArgumentIndexesFirstElement);
    }
    else
        multiplicity = 0;
    if (callMonitor)
        m_tupleIteratorMonitor->iteratorOpened(*this, multiplicity);
    return multiplicity;
}

template<bool callMonitor>
size_t DistinctIterator<callMonitor>::open() {
    return DistinctIterator<callMonitor>::open(ThreadContext::getCurrentThreadContext());
}

template<bool callMonitor>
size_t DistinctIterator<callMonitor>::advance() {
    m_currentOutputValuation = m_currentOutputValuation->getNextOutputValuation();
    while (m_currentOutputValuation && !m_currentOutputValuation->valuesMatch(m_onlyPossiblyBoundInputValues.begin(), m_onlyPossiblyBoundInputOffsets.begin(), m_onlyPossiblyBoundInputOffsets.end()))
        m_currentOutputValuation = m_currentOutputValuation->getNextOutputValuation();
    size_t multiplicity;
    if (m_currentOutputValuation) {
        multiplicity = m_currentOutputValuation->getMultiplicity();
        m_valuationCache.m_outputValuationManager.loadOutputValuation(m_currentOutputValuation, m_argumentsBufferFirstElement, m_outputArgumentIndexesFirstElement);
    }
    else
        multiplicity = 0;
    if (callMonitor)
        m_tupleIteratorMonitor->iteratorAdvanced(*this, multiplicity);
    return multiplicity;
}

template<bool callMonitor>
bool DistinctIterator<callMonitor>::getCurrentTupleInfo(TupleIndex& tupleIndex, TupleStatus& tupleStatus) const {
    return false;
}

template<bool callMonitor>
TupleIndex DistinctIterator<callMonitor>::getCurrentTupleIndex() const {
    return INVALID_TUPLE_INDEX;
}

template class DistinctIterator<false>;
template class DistinctIterator<true>;
