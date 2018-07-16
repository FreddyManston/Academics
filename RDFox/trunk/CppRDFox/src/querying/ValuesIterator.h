// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef VALUESITERATOR_H_
#define VALUESITERATOR_H_

#include "../storage/TupleIterator.h"

template<bool callMonitor>
class ValuesIterator : public TupleIterator {

protected:

    const size_t m_dataRowSize;
    const size_t m_numberOfDataRows;
    const std::vector<ArgumentIndex> m_data;
    const size_t m_numberOfInputValues;
    const size_t m_numberOfOutputValues;
    std::vector<ResourceID> m_currentInputValues;
    std::vector<ArgumentIndex> m_inputArgumentIndexes;
    std::vector<size_t> m_inputArgumentRowPositions;
    std::vector<ArgumentIndex> m_outputArgumentIndexes;
    std::vector<size_t> m_outputArgumentRowPositions;
    size_t m_currentRowIndex;
    std::vector<ArgumentIndex>::const_iterator m_currentRow;

    bool currentRowMatches();

    size_t findNextMatchingRow();

public:

    ValuesIterator(TupleIteratorMonitor* const tupleIteratorMonitor, std::vector<ResourceID>& argumentsBuffer, const ArgumentIndexSet& possiblyBoundInputArguments, const ArgumentIndexSet& surelyBoundInputArguments, const std::vector<ArgumentIndex>& argumentIndexes, const size_t numberOfDataRows, const std::vector<ArgumentIndex>& data);

    ValuesIterator(const ValuesIterator<callMonitor>& other, CloneReplacements& cloneReplacements);

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

always_inline std::unique_ptr<TupleIterator> newValuesIterator(TupleIteratorMonitor* const tupleIteratorMonitor, std::vector<ResourceID>& argumentsBuffer, const ArgumentIndexSet& possiblyBoundInputArguments, const ArgumentIndexSet& surelyBoundInputArguments, const std::vector<ArgumentIndex>& argumentIndexes, const size_t numberOfDataRows, const std::vector<ArgumentIndex>& data) {
    if (tupleIteratorMonitor)
        return std::unique_ptr<TupleIterator>(new ValuesIterator<true>(tupleIteratorMonitor, argumentsBuffer, possiblyBoundInputArguments, surelyBoundInputArguments, argumentIndexes, numberOfDataRows, data));
    else
        return std::unique_ptr<TupleIterator>(new ValuesIterator<false>(tupleIteratorMonitor, argumentsBuffer, possiblyBoundInputArguments, surelyBoundInputArguments, argumentIndexes, numberOfDataRows, data));
}

#endif /* VALUESITERATOR_H_ */
