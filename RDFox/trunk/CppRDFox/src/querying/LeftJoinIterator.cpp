// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../util/ThreadContext.h"
#include "../storage/DataStore.h"
#include "../storage/TupleIteratorMonitor.h"
#include "../equality/EqualityManager.h"
#include "LeftJoinIterator.h"

// LeftJoinIterator::Step

template<bool callMonitor, CardinalityType cardinalityType>
always_inline LeftJoinIterator<callMonitor, cardinalityType>::Step::Step(std::unique_ptr<TupleIterator> tupleIterator) :
    m_tupleIterator(std::move(tupleIterator)),
    m_multiplicityBefore(1),
    m_outputIndexes(),
    m_noCurrentTuple(false)
{
    const std::vector<ArgumentIndex>& argumentIndexes = m_tupleIterator->getArgumentIndexes();
    const ArgumentIndexSet& allArguments = m_tupleIterator->getAllArguments();
    const ArgumentIndexSet& inputArguments = m_tupleIterator->getAllInputArguments();
    for (std::vector<ArgumentIndex>::const_iterator iterator = argumentIndexes.begin(); iterator != argumentIndexes.end(); ++iterator)
        if (allArguments.contains(*iterator) && !inputArguments.contains(*iterator))
            m_outputIndexes.push_back(*iterator);
    m_outputIndexes.shrink_to_fit();
}

template<bool callMonitor, CardinalityType cardinalityType>
always_inline LeftJoinIterator<callMonitor, cardinalityType>::Step::Step(Step&& other) :
    m_tupleIterator(std::move(other.m_tupleIterator)),
    m_multiplicityBefore(other.m_multiplicityBefore),
    m_outputIndexes(std::move(other.m_outputIndexes)),
    m_noCurrentTuple(other.m_noCurrentTuple)
{
}

template<bool callMonitor, CardinalityType cardinalityType>
always_inline typename LeftJoinIterator<callMonitor, cardinalityType>::Step& LeftJoinIterator<callMonitor, cardinalityType>::Step::operator=(Step&& other) {
    m_tupleIterator = std::move(other.m_tupleIterator);
    m_multiplicityBefore = other.m_multiplicityBefore;
    m_outputIndexes = std::move(other.m_outputIndexes);
    m_noCurrentTuple = other.m_noCurrentTuple;
    return *this;
}

template<bool callMonitor, CardinalityType cardinalityType>
always_inline size_t LeftJoinIterator<callMonitor, cardinalityType>::Step::open(ThreadContext& threadContext, std::vector<ResourceID>& argumentsBuffer) {
    const size_t multiplicity = m_tupleIterator->open(threadContext);
    if (multiplicity == 0) {
        m_noCurrentTuple = true;
        invalidateOutput(argumentsBuffer);
        return 1;
    }
    else {
        m_noCurrentTuple = false;
        return multiplicity;
    }
}

template<bool callMonitor, CardinalityType cardinalityType>
always_inline size_t LeftJoinIterator<callMonitor, cardinalityType>::Step::openFirst(ThreadContext& threadContext) {
    const size_t multiplicity = m_tupleIterator->open(threadContext);
    m_noCurrentTuple = (multiplicity == 0);
    return multiplicity;
}

template<bool callMonitor, CardinalityType cardinalityType>
always_inline size_t LeftJoinIterator<callMonitor, cardinalityType>::Step::advance() {
    if (m_noCurrentTuple)
        return 0;
    else
        return m_tupleIterator->advance();
}

template<bool callMonitor, CardinalityType cardinalityType>
always_inline void LeftJoinIterator<callMonitor, cardinalityType>::Step::invalidateOutput(std::vector<ResourceID>& argumentsBuffer) {
    if (!m_outputIndexes.empty())
        for (std::vector<ArgumentIndex>::iterator iterator = m_outputIndexes.begin(); iterator != m_outputIndexes.end(); ++iterator)
            argumentsBuffer[*iterator] = INVALID_RESOURCE_ID;
}

// LeftJoinIterator

template<bool callMonitor, CardinalityType cardinalityType>
always_inline size_t LeftJoinIterator<callMonitor, cardinalityType>::moveToNext(Step* currentStep, size_t currentStepTupleMultiplicity) {
    const Step* const firstStep = m_firstStep;
    const Step* const lastStep = m_lastStep;
    while (currentStepTupleMultiplicity == 0 && currentStep > firstStep) {
        --currentStep;
        currentStepTupleMultiplicity = currentStep->advance();
    }
    if (currentStepTupleMultiplicity != 0) {
        while (currentStep < lastStep) {
            if (cardinalityType == CARDINALITY_NOT_EXACT)
                ++currentStep;
            else {
                const size_t multiplicitySoFar = currentStep->m_multiplicityBefore * currentStepTupleMultiplicity;
                ++currentStep;
                currentStep->m_multiplicityBefore = multiplicitySoFar;
            }
            currentStepTupleMultiplicity = currentStep->open(*m_threadContext, m_argumentsBuffer);
        }
    }
    if (cardinalityType == CARDINALITY_EXACT_WITH_EQUALITY)
        return m_equalityManager.multiplyByEquivalenceClassSizes(currentStep->m_multiplicityBefore * currentStepTupleMultiplicity, m_argumentsBuffer, m_projectedArgumentIndexes);
    else
        return currentStepTupleMultiplicity;
}

template<bool callMonitor, CardinalityType cardinalityType>
LeftJoinIterator<callMonitor, cardinalityType>::LeftJoinIterator(TupleIteratorMonitor* const tupleIteratorMonitor, const DataStore& dataStore, std::vector<ResourceID>& argumentsBuffer, const ArgumentIndexSet& possiblyBoundInputArguments, const ArgumentIndexSet& surelyBoundInputArguments, const ArgumentIndexSet& allArguments, const ArgumentIndexSet& surelyBoundArguments, const ArgumentIndexSet& variableArguments, unique_ptr_vector<TupleIterator> childIterators) :
    TupleIterator(tupleIteratorMonitor, argumentsBuffer, possiblyBoundInputArguments, surelyBoundInputArguments, allArguments, surelyBoundArguments, 0),
    m_equalityManager(dataStore.getEqualityManager()),
    m_steps(),
    m_threadContext(0),
    m_projectedArgumentIndexes()
{
    assert(!childIterators.empty());
    ArgumentIndexSet projectedVariables;
    ArgumentIndexSet visitedArgumentIndexes;
    std::vector<ArgumentIndex> allArgumentIndexes;
    for (size_t index = 0; index < childIterators.size(); ++index) {
        std::unique_ptr<TupleIterator>& tupleIterator = childIterators[index];
        const std::vector<ArgumentIndex>& tupleIteratorArgumentIndexes = tupleIterator->getArgumentIndexes();
        for (std::vector<ArgumentIndex>::const_iterator iterator = tupleIteratorArgumentIndexes.begin(); iterator != tupleIteratorArgumentIndexes.end(); ++iterator) {
            if (!visitedArgumentIndexes.contains(*iterator)) {
                allArgumentIndexes.push_back(*iterator);
                if (m_allArguments.contains(*iterator))
                    m_argumentIndexes.push_back(*iterator);
                visitedArgumentIndexes.add(*iterator);
            }
        }
        projectedVariables.unionWith(tupleIterator->getAllArguments());
        m_steps.emplace_back(std::move(tupleIterator));
    }
    projectedVariables.intersectWith(variableArguments);
    projectedVariables.removeAll(m_allArguments);
    for (std::vector<ArgumentIndex>::iterator iterator = allArgumentIndexes.begin(); iterator != allArgumentIndexes.end(); ++iterator)
        if (projectedVariables.contains(*iterator))
            m_projectedArgumentIndexes.push_back(*iterator);
    m_projectedArgumentIndexes.shrink_to_fit();
    m_firstStep = &m_steps[0];
    m_lastStep = &m_steps[m_steps.size() - 1];
}

template<bool callMonitor, CardinalityType cardinalityType>
LeftJoinIterator<callMonitor, cardinalityType>::LeftJoinIterator(const LeftJoinIterator<callMonitor, cardinalityType>& other, CloneReplacements& cloneReplacements) :
    TupleIterator(other, cloneReplacements),
    m_equalityManager(other.m_equalityManager),
    m_steps(),
    m_threadContext(0),
    m_projectedArgumentIndexes(other.m_projectedArgumentIndexes)
{
    for (typename std::vector<Step>::const_iterator iterator = other.m_steps.begin(); iterator != other.m_steps.end(); ++iterator)
        m_steps.emplace_back(iterator->m_tupleIterator->clone(cloneReplacements));
    m_firstStep = &m_steps[0];
    m_lastStep = &m_steps[m_steps.size() - 1];
}

template<bool callMonitor, CardinalityType cardinalityType>
const char* LeftJoinIterator<callMonitor, cardinalityType>::getName() const {
    return "LeftJoinIterator";
}

template<bool callMonitor, CardinalityType cardinalityType>
size_t LeftJoinIterator<callMonitor, cardinalityType>::getNumberOfChildIterators() const {
    return m_steps.size();
}

template<bool callMonitor, CardinalityType cardinalityType>
const TupleIterator& LeftJoinIterator<callMonitor, cardinalityType>::getChildIterator(const size_t childIteratorIndex) const {
    return *m_steps[childIteratorIndex].m_tupleIterator;
}

template<bool callMonitor, CardinalityType cardinalityType>
std::unique_ptr<TupleIterator> LeftJoinIterator<callMonitor, cardinalityType>::clone(CloneReplacements& cloneReplacements) const {
    return std::unique_ptr<TupleIterator>(new LeftJoinIterator<callMonitor, cardinalityType>(*this, cloneReplacements));
}

template<bool callMonitor, CardinalityType cardinalityType>
size_t LeftJoinIterator<callMonitor, cardinalityType>::open(ThreadContext& threadContext) {
    m_threadContext = &threadContext;
    const size_t tupleMultiplicity = moveToNext(m_firstStep, m_firstStep->openFirst(*m_threadContext));
    if (callMonitor)
        m_tupleIteratorMonitor->iteratorOpened(*this, tupleMultiplicity);
    return tupleMultiplicity;
}

template<bool callMonitor, CardinalityType cardinalityType>
size_t LeftJoinIterator<callMonitor, cardinalityType>::open() {
    return LeftJoinIterator<callMonitor, cardinalityType>::open(ThreadContext::getCurrentThreadContext());
}

template<bool callMonitor, CardinalityType cardinalityType>
size_t LeftJoinIterator<callMonitor, cardinalityType>::advance() {
    const size_t tupleMultiplicity = moveToNext(m_lastStep, m_lastStep->advance());
    if (callMonitor)
        m_tupleIteratorMonitor->iteratorAdvanced(*this, tupleMultiplicity);
    return tupleMultiplicity;
}

template<bool callMonitor, CardinalityType cardinalityType>
bool LeftJoinIterator<callMonitor, cardinalityType>::getCurrentTupleInfo(TupleIndex& tupleIndex, TupleStatus& tupleStatus) const {
    return false;
}

template<bool callMonitor, CardinalityType cardinalityType>
TupleIndex LeftJoinIterator<callMonitor, cardinalityType>::getCurrentTupleIndex() const {
    return INVALID_TUPLE_INDEX;
}

template class LeftJoinIterator<false, CARDINALITY_NOT_EXACT>;
template class LeftJoinIterator<false, CARDINALITY_EXACT_NO_EQUALITY>;
template class LeftJoinIterator<false, CARDINALITY_EXACT_WITH_EQUALITY>;
template class LeftJoinIterator<true, CARDINALITY_NOT_EXACT>;
template class LeftJoinIterator<true, CARDINALITY_EXACT_NO_EQUALITY>;
template class LeftJoinIterator<true, CARDINALITY_EXACT_WITH_EQUALITY>;
