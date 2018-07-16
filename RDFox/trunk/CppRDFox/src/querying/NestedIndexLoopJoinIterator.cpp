// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../util/ThreadContext.h"
#include "../storage/DataStore.h"
#include "../storage/TupleIteratorMonitor.h"
#include "../equality/EqualityManager.h"
#include "NestedIndexLoopJoinIterator.h"

// NestedIndexLoopJoinIterator::Step

template<bool callMonitor, CardinalityType cardinalityType>
always_inline NestedIndexLoopJoinIterator<callMonitor, cardinalityType>::Step::Step(std::unique_ptr<TupleIterator> tupleIterator) :
    m_tupleIterator(std::move(tupleIterator)),
    m_multiplicityBefore(1)
{
}

template<bool callMonitor, CardinalityType cardinalityType>
always_inline NestedIndexLoopJoinIterator<callMonitor, cardinalityType>::Step::Step(Step&& other) :
    m_tupleIterator(std::move(other.m_tupleIterator)),
    m_multiplicityBefore(other.m_multiplicityBefore)
{
}

template<bool callMonitor, CardinalityType cardinalityType>
always_inline typename NestedIndexLoopJoinIterator<callMonitor, cardinalityType>::Step& NestedIndexLoopJoinIterator<callMonitor, cardinalityType>::Step::operator=(Step&& other) {
    m_tupleIterator = std::move(other.m_tupleIterator);
    m_multiplicityBefore = other.m_multiplicityBefore;
    return *this;
}

// NestedIndexLoopJoinIterator

template<bool callMonitor, CardinalityType cardinalityType>
always_inline size_t NestedIndexLoopJoinIterator<callMonitor, cardinalityType>::moveToNext(ThreadContext& threadContext, Step* currentStep, size_t currentStepTupleMultiplicity) {
    const Step* const firstStep = m_firstStep;
    const Step* const lastStep = m_lastStep;
    while (true) {
        if (currentStepTupleMultiplicity == 0) {
            if (currentStep == firstStep)
                return 0;
            --currentStep;
            currentStepTupleMultiplicity = currentStep->m_tupleIterator->advance();
        }
        else {
            if (cardinalityType == CARDINALITY_NOT_EXACT) {
                if (currentStep == lastStep)
                    return 1;
                ++currentStep;
            }
            else {
                const size_t multiplicitySoFar = currentStep->m_multiplicityBefore * currentStepTupleMultiplicity;
                if (currentStep == lastStep) {
                    if (cardinalityType == CARDINALITY_EXACT_WITH_EQUALITY)
                        return m_equalityManager.multiplyByEquivalenceClassSizes(multiplicitySoFar, m_argumentsBuffer, m_projectedArgumentIndexes);
                    else
                        return multiplicitySoFar;
                }
                ++currentStep;
                currentStep->m_multiplicityBefore = multiplicitySoFar;
            }
            currentStepTupleMultiplicity = currentStep->m_tupleIterator->open(threadContext);
        }
    }
}

template<bool callMonitor, CardinalityType cardinalityType>
NestedIndexLoopJoinIterator<callMonitor, cardinalityType>::NestedIndexLoopJoinIterator(TupleIteratorMonitor* const tupleIteratorMonitor, const DataStore& dataStore, std::vector<ResourceID>& argumentsBuffer, const ArgumentIndexSet& possiblyBoundInputArguments, const ArgumentIndexSet& surelyBoundInputArguments, const ArgumentIndexSet& allArguments, const ArgumentIndexSet& surelyBoundArguments, const ArgumentIndexSet& variableArguments, unique_ptr_vector<TupleIterator> childIterators) :
    TupleIterator(tupleIteratorMonitor, argumentsBuffer, possiblyBoundInputArguments, surelyBoundInputArguments, allArguments, surelyBoundArguments, 0),
    m_equalityManager(dataStore.getEqualityManager()),
    m_steps(),
    m_threadContext(0),
    m_projectedArgumentIndexes()
{
    assert(!childIterators.empty());
    for (unique_ptr_vector<TupleIterator>::iterator iterator = childIterators.begin(); iterator != childIterators.end(); ++iterator)
        m_steps.emplace_back(std::move(*iterator));
    m_firstStep = &m_steps[0];
    m_lastStep = &m_steps[m_steps.size() - 1];
    // Deal with projected arguments
    ArgumentIndexSet projectedVariables;
    ArgumentIndexSet visitedArgumentIndexes;
    std::vector<ArgumentIndex> allArgumentIndexes;
    for (auto iterator = this->m_steps.begin(); iterator != this->m_steps.end(); ++iterator) {
        TupleIterator& tupleIterator = *iterator->m_tupleIterator;
        const std::vector<ArgumentIndex>& tupleIteratorArgumentIndexes = tupleIterator.getArgumentIndexes();
        for (std::vector<ArgumentIndex>::const_iterator iterator = tupleIteratorArgumentIndexes.begin(); iterator != tupleIteratorArgumentIndexes.end(); ++iterator) {
            if (!visitedArgumentIndexes.contains(*iterator)) {
                allArgumentIndexes.push_back(*iterator);
                if (m_allArguments.contains(*iterator))
                    m_argumentIndexes.push_back(*iterator);
                visitedArgumentIndexes.add(*iterator);
            }
        }
        projectedVariables.unionWith(tupleIterator.getAllArguments());
    }
    projectedVariables.intersectWith(variableArguments);
    projectedVariables.removeAll(m_allArguments);
    for (std::vector<ArgumentIndex>::iterator iterator = allArgumentIndexes.begin(); iterator != allArgumentIndexes.end(); ++iterator)
        if (projectedVariables.contains(*iterator))
            m_projectedArgumentIndexes.push_back(*iterator);
    m_projectedArgumentIndexes.shrink_to_fit();
}

template<bool callMonitor, CardinalityType cardinalityType>
NestedIndexLoopJoinIterator<callMonitor, cardinalityType>::NestedIndexLoopJoinIterator(const NestedIndexLoopJoinIterator<callMonitor, cardinalityType>& other, CloneReplacements& cloneReplacements) :
    TupleIterator(other, cloneReplacements),
    m_equalityManager(other.m_equalityManager),
    m_steps(),
    m_threadContext(0),
    m_projectedArgumentIndexes(other.m_projectedArgumentIndexes)
{
    for (typename std::vector<Step>::const_iterator iterator = other.m_steps.begin(); iterator != other.m_steps.end(); ++iterator) {
        std::unique_ptr<TupleIterator> tupleIterator(iterator->m_tupleIterator->clone(cloneReplacements));
        m_steps.emplace_back(std::move(tupleIterator));
    }
    m_firstStep = &m_steps[0];
    m_lastStep = &m_steps[m_steps.size() - 1];
}

template<bool callMonitor, CardinalityType cardinalityType>
const char* NestedIndexLoopJoinIterator<callMonitor, cardinalityType>::getName() const {
    return "NestedIndexLoopJoinIterator";
}

template<bool callMonitor, CardinalityType cardinalityType>
size_t NestedIndexLoopJoinIterator<callMonitor, cardinalityType>::getNumberOfChildIterators() const {
    return this->m_steps.size();
}

template<bool callMonitor, CardinalityType cardinalityType>
const TupleIterator& NestedIndexLoopJoinIterator<callMonitor, cardinalityType>::getChildIterator(const size_t childIteratorIndex) const {
    return *this->m_steps[childIteratorIndex].m_tupleIterator;
}

template<bool callMonitor, CardinalityType cardinalityType>
std::unique_ptr<TupleIterator> NestedIndexLoopJoinIterator<callMonitor, cardinalityType>::clone(CloneReplacements& cloneReplacements) const {
    return std::unique_ptr<TupleIterator>(new NestedIndexLoopJoinIterator<callMonitor, cardinalityType>(*this, cloneReplacements));
}

template<bool callMonitor, CardinalityType cardinalityType>
size_t NestedIndexLoopJoinIterator<callMonitor, cardinalityType>::open(ThreadContext& threadContext) {
    m_threadContext = &threadContext;
    const size_t tupleMultiplicity = moveToNext(threadContext, m_firstStep, m_firstStep->m_tupleIterator->open(threadContext));
    if (callMonitor)
        m_tupleIteratorMonitor->iteratorOpened(*this, tupleMultiplicity);
    return tupleMultiplicity;
}

template<bool callMonitor, CardinalityType cardinalityType>
size_t NestedIndexLoopJoinIterator<callMonitor, cardinalityType>::open() {
    return NestedIndexLoopJoinIterator<callMonitor, cardinalityType>::open(ThreadContext::getCurrentThreadContext());
}

template<bool callMonitor, CardinalityType cardinalityType>
size_t NestedIndexLoopJoinIterator<callMonitor, cardinalityType>::advance() {
    const size_t tupleMultiplicity = moveToNext(*m_threadContext, m_lastStep, m_lastStep->m_tupleIterator->advance());
    if (callMonitor)
        m_tupleIteratorMonitor->iteratorAdvanced(*this, tupleMultiplicity);
    return tupleMultiplicity;
}

template<bool callMonitor, CardinalityType cardinalityType>
bool NestedIndexLoopJoinIterator<callMonitor, cardinalityType>::getCurrentTupleInfo(TupleIndex& tupleIndex, TupleStatus& tupleStatus) const {
    return false;
}

template<bool callMonitor, CardinalityType cardinalityType>
TupleIndex NestedIndexLoopJoinIterator<callMonitor, cardinalityType>::getCurrentTupleIndex() const {
    return INVALID_TUPLE_INDEX;
}

template class NestedIndexLoopJoinIterator<false, CARDINALITY_NOT_EXACT>;
template class NestedIndexLoopJoinIterator<false, CARDINALITY_EXACT_NO_EQUALITY>;
template class NestedIndexLoopJoinIterator<false, CARDINALITY_EXACT_WITH_EQUALITY>;
template class NestedIndexLoopJoinIterator<true, CARDINALITY_NOT_EXACT>;
template class NestedIndexLoopJoinIterator<true, CARDINALITY_EXACT_NO_EQUALITY>;
template class NestedIndexLoopJoinIterator<true, CARDINALITY_EXACT_WITH_EQUALITY>;
