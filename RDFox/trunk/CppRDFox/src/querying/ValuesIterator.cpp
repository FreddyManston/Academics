    // RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../RDFStoreException.h"
#include "../util/ThreadContext.h"
#include "../storage/TupleIteratorMonitor.h"
#include "ValuesIterator.h"

template<bool callMonitor>
ValuesIterator<callMonitor>::ValuesIterator(TupleIteratorMonitor* const tupleIteratorMonitor, std::vector<ResourceID>& argumentsBuffer, const ArgumentIndexSet& possiblyBoundInputArguments, const ArgumentIndexSet& surelyBoundInputArguments, const std::vector<ArgumentIndex>& argumentIndexes, const size_t numberOfDataRows, const std::vector<ArgumentIndex>& data) :
    TupleIterator(tupleIteratorMonitor, argumentsBuffer, argumentIndexes, possiblyBoundInputArguments, surelyBoundInputArguments),
    m_dataRowSize(argumentIndexes.size()),
    m_numberOfDataRows(numberOfDataRows),
    m_data(data),
    m_numberOfInputValues(m_allInputArguments.size()),
    m_numberOfOutputValues(m_argumentIndexes.size() - m_numberOfInputValues),
    m_currentInputValues(m_numberOfInputValues, INVALID_RESOURCE_ID),
    m_inputArgumentIndexes(),
    m_inputArgumentRowPositions(),
    m_outputArgumentIndexes(),
    m_outputArgumentRowPositions(),
    m_currentRowIndex(0),
    m_currentRow(m_data.begin())
{
    for (ArgumentIndexSet::iterator iterator = m_allInputArguments.begin(); iterator != m_allInputArguments.end(); ++iterator) {
        const ArgumentIndex argumentIndex = *iterator;
        m_inputArgumentIndexes.push_back(argumentIndex);
        m_inputArgumentRowPositions.push_back(std::find(m_argumentIndexes.begin(), m_argumentIndexes.end(), argumentIndex) - m_argumentIndexes.begin());
    }
    for (std::vector<ArgumentIndex>::iterator iterator = m_argumentIndexes.begin(); iterator != m_argumentIndexes.end(); ++iterator) {
        const ArgumentIndex argumentIndex = *iterator;
        m_allArguments.add(argumentIndex);
        m_surelyBoundArguments.add(argumentIndex);
        if (!m_allInputArguments.contains(argumentIndex)) {
            m_outputArgumentIndexes.push_back(argumentIndex);
            m_outputArgumentRowPositions.push_back(std::find(m_argumentIndexes.begin(), m_argumentIndexes.end(), argumentIndex) - m_argumentIndexes.begin());
        }
    }
    for (std::vector<ArgumentIndex>::const_iterator currentDataRowStart = m_data.begin(); currentDataRowStart != m_data.end(); currentDataRowStart += m_dataRowSize) {
        for (size_t elementIndex = 0; elementIndex < m_dataRowSize; ++elementIndex) {
            if (m_argumentsBuffer[*(currentDataRowStart + elementIndex)] == INVALID_RESOURCE_ID) {
                const ArgumentIndex argumentIndex = m_argumentIndexes[elementIndex];
                if (!m_surelyBoundInputArguments.contains(argumentIndex))
                    m_surelyBoundArguments.remove(argumentIndex);
            }
        }
    }
    m_inputArgumentIndexes.shrink_to_fit();
    m_inputArgumentRowPositions.shrink_to_fit();
    m_outputArgumentIndexes.shrink_to_fit();
    m_outputArgumentRowPositions.shrink_to_fit();
    m_allArguments.shrinkToFit();
    m_surelyBoundArguments.shrinkToFit();
}

template<bool callMonitor>
ValuesIterator<callMonitor>::ValuesIterator(const ValuesIterator<callMonitor>& other, CloneReplacements& cloneReplacements) :
    TupleIterator(other, cloneReplacements),
    m_dataRowSize(other.m_dataRowSize),
    m_numberOfDataRows(other.m_numberOfDataRows),
    m_data(other.m_data),
    m_numberOfInputValues(other.m_numberOfInputValues),
    m_numberOfOutputValues(other.m_numberOfOutputValues),
    m_currentInputValues(other.m_currentInputValues),
    m_inputArgumentIndexes(other.m_inputArgumentIndexes),
    m_inputArgumentRowPositions(other.m_inputArgumentRowPositions),
    m_outputArgumentIndexes(other.m_outputArgumentIndexes),
    m_outputArgumentRowPositions(other.m_outputArgumentRowPositions),
    m_currentRowIndex(0),
    m_currentRow(m_data.begin())
{
}

template<bool callMonitor>
const char* ValuesIterator<callMonitor>::getName() const {
    return "ValuesIterator";
}

template<bool callMonitor>
size_t ValuesIterator<callMonitor>::getNumberOfChildIterators() const {
    return 0;
}

template<bool callMonitor>
const TupleIterator& ValuesIterator<callMonitor>::getChildIterator(const size_t childIteratorIndex) const {
    throw RDF_STORE_EXCEPTION("Invalid child iterator index.");
}

template<bool callMonitor>
std::unique_ptr<TupleIterator> ValuesIterator<callMonitor>::clone(CloneReplacements& cloneReplacements) const {
    return std::unique_ptr<TupleIterator>(new ValuesIterator<callMonitor>(*this, cloneReplacements));
}

template<bool callMonitor>
always_inline bool ValuesIterator<callMonitor>::currentRowMatches() {
    for (size_t index = 0; index < m_numberOfInputValues; ++index) {
        const ResourceID inputValue = m_currentInputValues[index];
        const ResourceID rowValue = m_argumentsBuffer[*(m_currentRow + m_inputArgumentRowPositions[index])];
        if (inputValue != INVALID_RESOURCE_ID && rowValue != INVALID_RESOURCE_ID && inputValue != rowValue)
            return false;
    }
    return true;
}

template<bool callMonitor>
always_inline size_t ValuesIterator<callMonitor>::findNextMatchingRow() {
    while (m_currentRowIndex < m_numberOfDataRows) {
        if (currentRowMatches()) {
            for (size_t index = 0; index < m_numberOfInputValues; ++index) {
                const ResourceID rowValue = m_argumentsBuffer[*(m_currentRow + m_inputArgumentRowPositions[index])];
                if (rowValue != INVALID_RESOURCE_ID)
                    m_argumentsBuffer[m_inputArgumentIndexes[index]] = rowValue;
            }
            for (size_t index = 0; index < m_numberOfOutputValues; ++index) {
                const ResourceID rowValue = m_argumentsBuffer[*(m_currentRow + m_outputArgumentRowPositions[index])];
                m_argumentsBuffer[m_outputArgumentIndexes[index]] = rowValue;
            }
            return 1;
        }
        ++m_currentRowIndex;
        m_currentRow += m_dataRowSize;
    }
    return 0;
}

template<bool callMonitor>
size_t ValuesIterator<callMonitor>::open(ThreadContext& threadContext) {
    for (size_t index = 0; index < m_numberOfInputValues; ++index)
        m_currentInputValues[index] = m_argumentsBuffer[m_inputArgumentIndexes[index]];
    m_currentRowIndex = 0;
    m_currentRow = m_data.begin();
    const size_t multiplicity = findNextMatchingRow();
    if (callMonitor)
        m_tupleIteratorMonitor->iteratorOpened(*this, multiplicity);
    return multiplicity;
}

template<bool callMonitor>
size_t ValuesIterator<callMonitor>::open() {
    return ValuesIterator<callMonitor>::open(ThreadContext::getCurrentThreadContext());
}

template<bool callMonitor>
size_t ValuesIterator<callMonitor>::advance() {
    ++m_currentRowIndex;
    m_currentRow += m_argumentIndexes.size();
    const size_t multiplicity = findNextMatchingRow();
    if (callMonitor)
        m_tupleIteratorMonitor->iteratorAdvanced(*this, multiplicity);
    return multiplicity;
}

template<bool callMonitor>
bool ValuesIterator<callMonitor>::getCurrentTupleInfo(TupleIndex& tupleIndex, TupleStatus& tupleStatus) const {
    return false;
}

template<bool callMonitor>
TupleIndex ValuesIterator<callMonitor>::getCurrentTupleIndex() const {
    return INVALID_TUPLE_INDEX;
}

template class ValuesIterator<false>;
template class ValuesIterator<true>;
