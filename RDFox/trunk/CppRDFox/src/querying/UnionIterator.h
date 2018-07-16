// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef UNIONITERATOR_H_
#define UNIONITERATOR_H_

#include "../storage/TupleIterator.h"

class DataStore;
class EqualityManager;

template<bool callMonitor, bool multiplyProjectedEquality>
class UnionIterator : public TupleIterator {

protected:

    struct Step {
        std::unique_ptr<TupleIterator> m_tupleIterator;
        std::vector<ArgumentIndex> m_projectedArgumentIndexes;

        Step(std::unique_ptr<TupleIterator> tupleIterator, const ArgumentIndexSet& allArguments, const ArgumentIndexSet& variableArguments);

        Step(std::unique_ptr<TupleIterator>, const std::vector<ArgumentIndex>& projectedArgumentIndexes);

        Step(const Step& other) = delete;

        Step(Step&& other);

        Step& operator=(const Step& other) = delete;

        Step& operator=(Step&& other);

    };

    const EqualityManager& m_equalityManager;
    std::vector<ArgumentIndex> m_possiblyButNotSurelyBoundInputVariables;
    std::vector<ArgumentIndex> m_unboundInputVariablesInCurrentCall;
    std::vector<ArgumentIndex> m_outputVariables;
    std::vector<Step> m_steps;
    Step* m_firstStep;
    Step* m_currentStep;
    Step* m_afterLastStep;
    ThreadContext* m_threadContext;

    size_t openCurrentTupleIterator();

    size_t advanceCurrentTupleIterator();

public:

    UnionIterator(TupleIteratorMonitor* const tupleIteratorMonitor, const DataStore& dataStore, std::vector<ResourceID>& argumentsBuffer, const ArgumentIndexSet& possiblyBoundInputArguments, const ArgumentIndexSet& surelyBoundInputArguments, const ArgumentIndexSet& allArguments, const ArgumentIndexSet& surelyBoundArguments, const ArgumentIndexSet& variableArguments, unique_ptr_vector<TupleIterator> childIterators);

    UnionIterator(const UnionIterator<callMonitor, multiplyProjectedEquality>& other, CloneReplacements& cloneReplacements);

    virtual const char* getName() const;

    virtual size_t getNumberOfChildIterators() const;

    virtual const TupleIterator& getChildIterator(const size_t childIteratorIndex) const;

    virtual std::unique_ptr<TupleIterator> clone(CloneReplacements& cloneReplacements) const;

    virtual size_t open(ThreadContext& threadContext);

    virtual size_t open();

    virtual size_t advance();

    virtual bool getCurrentTupleInfo(TupleIndex& tupleIndex, TupleStatus& tupleStatus) const;

    virtual TupleIndex getCurrentTupleIndex() const;

};


always_inline std::unique_ptr<TupleIterator> newUnionIterator(TupleIteratorMonitor* const tupleIteratorMonitor, const CardinalityType cardinalityType, const DataStore& dataStore, std::vector<ResourceID>& argumentsBuffer, const ArgumentIndexSet& possiblyBoundInputArguments, const ArgumentIndexSet& surelyBoundInputArguments, const ArgumentIndexSet& allArguments, const ArgumentIndexSet& surelyBoundArguments, const ArgumentIndexSet& variableArguments, unique_ptr_vector<TupleIterator> childIterators) {
    if (tupleIteratorMonitor) {
        if (cardinalityType == CARDINALITY_EXACT_WITH_EQUALITY)
            return std::unique_ptr<TupleIterator>(new UnionIterator<true, true>(tupleIteratorMonitor, dataStore, argumentsBuffer, possiblyBoundInputArguments, surelyBoundInputArguments, allArguments, surelyBoundArguments, variableArguments, std::move(childIterators)));
        else
            return std::unique_ptr<TupleIterator>(new UnionIterator<true, false>(tupleIteratorMonitor, dataStore, argumentsBuffer, possiblyBoundInputArguments, surelyBoundInputArguments, allArguments, surelyBoundArguments, variableArguments, std::move(childIterators)));
    }
    else {
        if (cardinalityType == CARDINALITY_EXACT_WITH_EQUALITY)
            return std::unique_ptr<TupleIterator>(new UnionIterator<false, true>(tupleIteratorMonitor, dataStore, argumentsBuffer, possiblyBoundInputArguments, surelyBoundInputArguments, allArguments, surelyBoundArguments, variableArguments, std::move(childIterators)));
        else
            return std::unique_ptr<TupleIterator>(new UnionIterator<false, false>(tupleIteratorMonitor, dataStore, argumentsBuffer, possiblyBoundInputArguments, surelyBoundInputArguments, allArguments, surelyBoundArguments, variableArguments, std::move(childIterators)));
    }
}

#endif /* UNIONITERATOR_H_ */
