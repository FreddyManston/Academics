// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../equality/EqualityManager.h"
#include "../util/ThreadContext.h"
#include "../storage/DataStore.h"
#include "../storage/TupleIteratorMonitor.h"
#include "UnionIterator.h"

// UnionIterator::Step

template<bool callMonitor, bool multiplyProjectedEquality>
always_inline UnionIterator<callMonitor, multiplyProjectedEquality>::Step::Step(std::unique_ptr<TupleIterator> tupleIterator, const ArgumentIndexSet& allArguments, const ArgumentIndexSet& variableArguments) :
    m_tupleIterator(std::move(tupleIterator)),
    m_projectedArgumentIndexes()
{
    const std::vector<ArgumentIndex> stepArgumentIndexes = m_tupleIterator->getArgumentIndexes();
    for (std::vector<ArgumentIndex>::const_iterator iterator = stepArgumentIndexes.begin(); iterator != stepArgumentIndexes.end(); ++iterator)
        if (!allArguments.contains(*iterator) && variableArguments.contains(*iterator))
            m_projectedArgumentIndexes.push_back(*iterator);
}

template<bool callMonitor, bool multiplyProjectedEquality>
always_inline UnionIterator<callMonitor, multiplyProjectedEquality>::Step::Step(std::unique_ptr<TupleIterator> tupleIterator, const std::vector<ArgumentIndex>& projectedArgumentIndexes) :
    m_tupleIterator(std::move(tupleIterator)),
    m_projectedArgumentIndexes(projectedArgumentIndexes)
{
}

template<bool callMonitor, bool multiplyProjectedEquality>
always_inline UnionIterator<callMonitor, multiplyProjectedEquality>::Step::Step(Step&& other) :
    m_tupleIterator(std::move(other.m_tupleIterator)),
    m_projectedArgumentIndexes(std::move(other.m_projectedArgumentIndexes))
{
}

template<bool callMonitor, bool multiplyProjectedEquality>
always_inline typename UnionIterator<callMonitor, multiplyProjectedEquality>::Step& UnionIterator<callMonitor, multiplyProjectedEquality>::Step::operator=(Step&& other) {
    m_tupleIterator = std::move(other.m_tupleIterator);
    m_projectedArgumentIndexes = std::move(other.m_projectedArgumentIndexes);
    return *this;
}

// UnionIterator

template<bool callMonitor, bool multiplyProjectedEquality>
UnionIterator<callMonitor, multiplyProjectedEquality>::UnionIterator(TupleIteratorMonitor* const tupleIteratorMonitor, const DataStore& dataStore, std::vector<ResourceID>& argumentsBuffer, const ArgumentIndexSet& possiblyBoundInputArguments, const ArgumentIndexSet& surelyBoundInputArguments, const ArgumentIndexSet& allArguments, const ArgumentIndexSet& surelyBoundArguments, const ArgumentIndexSet& variableArguments, unique_ptr_vector<TupleIterator> childIterators) :
    TupleIterator(tupleIteratorMonitor, argumentsBuffer, possiblyBoundInputArguments, surelyBoundInputArguments, allArguments, surelyBoundArguments),
    m_equalityManager(dataStore.getEqualityManager()),
    m_possiblyButNotSurelyBoundInputVariables(),
    m_unboundInputVariablesInCurrentCall(),
    m_outputVariables(),
    m_steps(),
    m_threadContext(0)
{
    for (ArgumentIndexSet::const_iterator iterator = variableArguments.begin(); iterator != variableArguments.end(); ++iterator) {
        if (possiblyBoundInputArguments.contains(*iterator)) {
            if (!surelyBoundInputArguments.contains(*iterator))
                m_possiblyButNotSurelyBoundInputVariables.push_back(*iterator);
        }
        else
            m_outputVariables.push_back(*iterator);
    }
    m_possiblyButNotSurelyBoundInputVariables.shrink_to_fit();
    m_unboundInputVariablesInCurrentCall.reserve(m_possiblyButNotSurelyBoundInputVariables.size());
    m_outputVariables.shrink_to_fit();
    for (unique_ptr_vector<TupleIterator>::iterator iterator = childIterators.begin(); iterator != childIterators.end(); ++iterator)
        m_steps.emplace_back(std::move(*iterator), m_allArguments, variableArguments);
    m_firstStep = &m_steps[0];
    m_currentStep = &m_steps[m_steps.size() - 1] + 1;
    m_afterLastStep = m_currentStep;
}

template<bool callMonitor, bool multiplyProjectedEquality>
UnionIterator<callMonitor, multiplyProjectedEquality>::UnionIterator(const UnionIterator<callMonitor, multiplyProjectedEquality>& other, CloneReplacements& cloneReplacements) :
    TupleIterator(other, cloneReplacements),
    m_equalityManager(other.m_equalityManager),
    m_possiblyButNotSurelyBoundInputVariables(other.m_possiblyButNotSurelyBoundInputVariables),
    m_unboundInputVariablesInCurrentCall(),
    m_outputVariables(other.m_outputVariables),
    m_steps(),
    m_threadContext(0)
{
    m_unboundInputVariablesInCurrentCall.reserve(m_possiblyButNotSurelyBoundInputVariables.size());
    for (typename std::vector<Step>::const_iterator iterator = other.m_steps.begin(); iterator != other.m_steps.end(); ++iterator)
        m_steps.emplace_back(iterator->m_tupleIterator->clone(cloneReplacements), iterator->m_projectedArgumentIndexes);
    m_firstStep = &m_steps[0];
    m_currentStep = &m_steps[m_steps.size() - 1] + 1;
    m_afterLastStep = m_currentStep;
}

template<bool callMonitor, bool multiplyProjectedEquality>
const char* UnionIterator<callMonitor, multiplyProjectedEquality>::getName() const {
    return "UnionIterator";
}

template<bool callMonitor, bool multiplyProjectedEquality>
size_t UnionIterator<callMonitor, multiplyProjectedEquality>::getNumberOfChildIterators() const {
    return m_steps.size();
}

template<bool callMonitor, bool multiplyProjectedEquality>
const TupleIterator& UnionIterator<callMonitor, multiplyProjectedEquality>::getChildIterator(const size_t childIteratorIndex) const {
    return *m_steps[childIteratorIndex].m_tupleIterator;
}

template<bool callMonitor, bool multiplyProjectedEquality>
std::unique_ptr<TupleIterator> UnionIterator<callMonitor, multiplyProjectedEquality>::clone(CloneReplacements& cloneReplacements) const {
    return std::unique_ptr<TupleIterator>(new UnionIterator<callMonitor, multiplyProjectedEquality>(*this, cloneReplacements));
}

template<bool callMonitor, bool multiplyProjectedEquality>
always_inline size_t UnionIterator<callMonitor, multiplyProjectedEquality>::openCurrentTupleIterator() {
    while (m_currentStep != m_afterLastStep) {
        for (std::vector<ArgumentIndex>::iterator iterator = m_unboundInputVariablesInCurrentCall.begin(); iterator != m_unboundInputVariablesInCurrentCall.end(); ++iterator)
            m_argumentsBuffer[*iterator] = INVALID_RESOURCE_ID;
        for (std::vector<ArgumentIndex>::iterator iterator = m_outputVariables.begin(); iterator != m_outputVariables.end(); ++iterator)
            m_argumentsBuffer[*iterator] = INVALID_RESOURCE_ID;
        const size_t tupleMultiplicity = m_currentStep->m_tupleIterator->open(*m_threadContext);
        if (tupleMultiplicity != 0) {
            if (multiplyProjectedEquality)
                return m_equalityManager.multiplyByEquivalenceClassSizes(tupleMultiplicity, m_argumentsBuffer, m_currentStep->m_projectedArgumentIndexes);
            else
                return tupleMultiplicity;
        }
        ++m_currentStep;
    }
    return 0;
}

template<bool callMonitor, bool multiplyProjectedEquality>
always_inline size_t UnionIterator<callMonitor, multiplyProjectedEquality>::advanceCurrentTupleIterator() {
    const size_t tupleMultiplicity = m_currentStep->m_tupleIterator->advance();
    if (multiplyProjectedEquality)
        return m_equalityManager.multiplyByEquivalenceClassSizes(tupleMultiplicity, m_argumentsBuffer, m_currentStep->m_projectedArgumentIndexes);
    else
        return tupleMultiplicity;
}

template<bool callMonitor, bool multiplyProjectedEquality>
size_t UnionIterator<callMonitor, multiplyProjectedEquality>::open(ThreadContext& threadContext) {
    m_threadContext = &threadContext;
    m_unboundInputVariablesInCurrentCall.clear();
    for (std::vector<ArgumentIndex>::iterator iterator = m_possiblyButNotSurelyBoundInputVariables.begin(); iterator != m_possiblyButNotSurelyBoundInputVariables.end(); ++iterator)
        if (m_argumentsBuffer[*iterator] == INVALID_RESOURCE_ID)
            m_unboundInputVariablesInCurrentCall.push_back(*iterator);
    m_currentStep = m_firstStep;
    const size_t tupleMultiplicity = openCurrentTupleIterator();
    if (callMonitor)
        m_tupleIteratorMonitor->iteratorOpened(*this, tupleMultiplicity);
    return tupleMultiplicity;
}

template<bool callMonitor, bool multiplyProjectedEquality>
size_t UnionIterator<callMonitor, multiplyProjectedEquality>::open() {
    return UnionIterator<callMonitor, multiplyProjectedEquality>::open(ThreadContext::getCurrentThreadContext());
}

template<bool callMonitor, bool multiplyProjectedEquality>
size_t UnionIterator<callMonitor, multiplyProjectedEquality>::advance() {
    size_t tupleMultiplicity = advanceCurrentTupleIterator();
    if (tupleMultiplicity == 0) {
        ++m_currentStep;
        tupleMultiplicity = openCurrentTupleIterator();
    }
    if (callMonitor)
        m_tupleIteratorMonitor->iteratorAdvanced(*this, tupleMultiplicity);
    return tupleMultiplicity;
}

template<bool callMonitor, bool multiplyProjectedEquality>
bool UnionIterator<callMonitor, multiplyProjectedEquality>::getCurrentTupleInfo(TupleIndex& tupleIndex, TupleStatus& tupleStatus) const {
    return m_currentStep->m_tupleIterator->getCurrentTupleInfo(tupleIndex, tupleStatus);
}

template<bool callMonitor, bool multiplyProjectedEquality>
TupleIndex UnionIterator<callMonitor, multiplyProjectedEquality>::getCurrentTupleIndex() const {
    return m_currentStep->m_tupleIterator->getCurrentTupleIndex();
}

template class UnionIterator<false, false>;
template class UnionIterator<false, true>;
template class UnionIterator<true, false>;
template class UnionIterator<true, true>;
