// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef LEFTJOINITERATOR_H_
#define LEFTJOINITERATOR_H_

#include "../storage/TupleIterator.h"

class ArgumentIndexSet;
class EqualityManager;

template<bool callMonitor, CardinalityType cardinalityType>
class LeftJoinIterator : public TupleIterator {

protected:

    struct Step {
        std::unique_ptr<TupleIterator> m_tupleIterator;
        size_t m_multiplicityBefore;
        std::vector<ArgumentIndex> m_outputIndexes;
        bool m_noCurrentTuple;

        Step(std::unique_ptr<TupleIterator> tupleIterator);

        Step(const Step& other) = delete;

        Step(Step&& other);

        Step& operator=(const Step& other) = delete;

        Step& operator=(Step&& other);

        size_t open(ThreadContext& threadContext, std::vector<ResourceID>& argumentsBuffer);

        size_t openFirst(ThreadContext& threadContext);

        size_t advance();

        void invalidateOutput(std::vector<ResourceID>& argumentsBuffer);

    };

    const EqualityManager& m_equalityManager;
    std::vector<Step> m_steps;
    Step* m_firstStep;
    Step* m_lastStep;
    ThreadContext* m_threadContext;
    std::vector<ArgumentIndex> m_projectedArgumentIndexes;

    size_t moveToNext(Step* currentStep, size_t currentStepTupleMultiplicity);

public:

    LeftJoinIterator(TupleIteratorMonitor* const tupleIteratorMonitor, const DataStore& dataStore, std::vector<ResourceID>& argumentsBuffer, const ArgumentIndexSet& possiblyBoundInputArguments, const ArgumentIndexSet& surelyBoundInputArguments, const ArgumentIndexSet& allArguments, const ArgumentIndexSet& surelyBoundArguments, const ArgumentIndexSet& variableArguments, unique_ptr_vector<TupleIterator> childIterators);

    LeftJoinIterator(const LeftJoinIterator<callMonitor, cardinalityType>& other, CloneReplacements& cloneReplacements);

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

always_inline std::unique_ptr<TupleIterator> newLeftJoinIterator(TupleIteratorMonitor* const tupleIteratorMonitor, const CardinalityType cardinalityType, const DataStore& dataStore, std::vector<ResourceID>& argumentsBuffer, const ArgumentIndexSet& possiblyBoundInputArguments, const ArgumentIndexSet& surelyBoundInputArguments, const ArgumentIndexSet& allArguments, const ArgumentIndexSet& surelyBoundArguments, const ArgumentIndexSet& variableArguments, unique_ptr_vector<TupleIterator> childIterators) {
    switch (cardinalityType) {
    case CARDINALITY_NOT_EXACT:
        if (tupleIteratorMonitor)
            return std::unique_ptr<TupleIterator>(new LeftJoinIterator<true, CARDINALITY_NOT_EXACT>(tupleIteratorMonitor, dataStore, argumentsBuffer, possiblyBoundInputArguments, surelyBoundInputArguments, allArguments, surelyBoundArguments, variableArguments, std::move(childIterators)));
        else
            return std::unique_ptr<TupleIterator>(new LeftJoinIterator<false, CARDINALITY_NOT_EXACT>(tupleIteratorMonitor, dataStore, argumentsBuffer, possiblyBoundInputArguments, surelyBoundInputArguments, allArguments, surelyBoundArguments, variableArguments, std::move(childIterators)));
    case CARDINALITY_EXACT_NO_EQUALITY:
        if (tupleIteratorMonitor)
            return std::unique_ptr<TupleIterator>(new LeftJoinIterator<true, CARDINALITY_EXACT_NO_EQUALITY>(tupleIteratorMonitor, dataStore, argumentsBuffer, possiblyBoundInputArguments, surelyBoundInputArguments, allArguments, surelyBoundArguments, variableArguments, std::move(childIterators)));
        else
            return std::unique_ptr<TupleIterator>(new LeftJoinIterator<false, CARDINALITY_EXACT_NO_EQUALITY>(tupleIteratorMonitor, dataStore, argumentsBuffer, possiblyBoundInputArguments, surelyBoundInputArguments, allArguments, surelyBoundArguments, variableArguments, std::move(childIterators)));
    case CARDINALITY_EXACT_WITH_EQUALITY:
        if (tupleIteratorMonitor)
            return std::unique_ptr<TupleIterator>(new LeftJoinIterator<true, CARDINALITY_EXACT_WITH_EQUALITY>(tupleIteratorMonitor, dataStore, argumentsBuffer, possiblyBoundInputArguments, surelyBoundInputArguments, allArguments, surelyBoundArguments, variableArguments, std::move(childIterators)));
        else
            return std::unique_ptr<TupleIterator>(new LeftJoinIterator<false, CARDINALITY_EXACT_WITH_EQUALITY>(tupleIteratorMonitor, dataStore, argumentsBuffer, possiblyBoundInputArguments, surelyBoundInputArguments, allArguments, surelyBoundArguments, variableArguments, std::move(childIterators)));
    default:
        UNREACHABLE;
    }
}

#endif /* LEFTJOINITERATOR_H_ */
