// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "AbstractDeletionTaskImpl.h"

// AbstractDeletionTask

void AbstractDeletionTask::doInitialize() {
    if (m_componentLevel == static_cast<size_t>(-1))
        m_afterLastDeletedInPreviousLevels = 0;
    else {
        m_afterLastDeletedInPreviousLevels = m_incrementalReasoningState.getDeleteList().getFirstFreePosition();
        m_incrementalReasoningState.getDeleteList().appendUnprocessed(m_incrementalReasoningState.getInitiallyDeletedList(m_componentLevel));
    }
    m_incrementalReasoningState.getDeleteList().resetDequeuePosition();
    // If there are rules with negation, we need to reset the added list so that we can process A\D.
    if (m_datalogEngine.getRuleIndex().hasRulesWithNegation(m_componentLevel))
        m_incrementalReasoningState.getAddedList().resetDequeuePosition();
    if (!m_reflexiveSameAsStatus.initializeLarge())
        throw RDF_STORE_EXCEPTION("Cannot initialize the set for tracking the status of reflexivity tuples.");
}

AbstractDeletionTask::AbstractDeletionTask(DatalogEngine& datalogEngine, IncrementalMonitor* const incrementalMonitor, IncrementalReasoningState& incrementalReasoningState, const size_t componentLevel) :
    ReasoningTask(datalogEngine),
    m_incrementalMonitor(incrementalMonitor),
    m_incrementalReasoningState(incrementalReasoningState),
    m_componentLevel(componentLevel),
    m_reflexiveSameAsStatus(datalogEngine.getDataStore().getMemoryManager()),
    m_afterLastDeletedInPreviousLevels(0)
{
}
