// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../util/Vocabulary.h"
#include "../storage/DataStore.h"
#include "../storage/TupleTable.h"
#include "DatalogEngine.h"
#include "DatalogEngineWorker.h"
#include "UpdateEqualityManagerTask.h"
#include "IncrementalReasoningState.h"
#include "IncrementalMonitor.h"

// UpdateEqualityManagerTask

template<bool callMonitor>
always_inline std::unique_ptr<ReasoningTaskWorker> UpdateEqualityManagerTask::doCreateWorker1(DatalogEngineWorker& datalogEngineWorker) {
    if (isMultithreaded())
        return doCreateWorker2<callMonitor, true>(datalogEngineWorker);
    else
        return doCreateWorker2<callMonitor, false>(datalogEngineWorker);
}

template<bool callMonitor, bool multithreaded>
always_inline std::unique_ptr<ReasoningTaskWorker> UpdateEqualityManagerTask::doCreateWorker2(DatalogEngineWorker& datalogEngineWorker) {
    if (m_updateType == COPY_CLASSES)
        return std::unique_ptr<ReasoningTaskWorker>(new UpdateEqualityManagerTaskWorker<callMonitor, multithreaded, COPY_CLASSES>(*this, datalogEngineWorker));
    else if (m_updateType == UNREPRESENT)
        return std::unique_ptr<ReasoningTaskWorker>(new UpdateEqualityManagerTaskWorker<callMonitor, multithreaded, UNREPRESENT>(*this, datalogEngineWorker));
    else
        return std::unique_ptr<ReasoningTaskWorker>(new UpdateEqualityManagerTaskWorker<callMonitor, multithreaded, BREAK_EQUALS>(*this, datalogEngineWorker));
}

std::unique_ptr<ReasoningTaskWorker> UpdateEqualityManagerTask::doCreateWorker(DatalogEngineWorker& datalogEngineWorker) {
    if (m_incrementalMonitor == nullptr)
        return doCreateWorker1<false>(datalogEngineWorker);
    else
        return doCreateWorker1<true>(datalogEngineWorker);
}

void UpdateEqualityManagerTask::doInitialize() {
    m_incrementalReasoningState.getDeleteList().resetDequeuePosition();
}

UpdateEqualityManagerTask::UpdateEqualityManagerTask(DatalogEngine& datalogEngine, IncrementalMonitor* const incrementalMonitor, IncrementalReasoningState& incrementalReasoningState, const UpdateType updateType) :
    ReasoningTask(datalogEngine),
    m_incrementalMonitor(incrementalMonitor),
    m_incrementalReasoningState(incrementalReasoningState),
    m_updateType(updateType)
{
}

// UpdateEqualityManagerTaskWorker

template<bool callMonitor, bool multithreaded, UpdateEqualityManagerTask::UpdateType updateType>
UpdateEqualityManagerTaskWorker<callMonitor, multithreaded, updateType>::UpdateEqualityManagerTaskWorker(UpdateEqualityManagerTask& updateEqualityManagerTask, DatalogEngineWorker& datalogEngineWorker) :
    m_updateEqualityManagerTask(updateEqualityManagerTask),
    m_incrementalMonitor(m_updateEqualityManagerTask.m_incrementalMonitor),
    m_workerIndex(datalogEngineWorker.getWorkerIndex()),
    m_tripleTable(m_updateEqualityManagerTask.getDatalogEngine().getDataStore().getTupleTable("internal$rdf")),
    m_incrementalReasoningState(m_updateEqualityManagerTask.m_incrementalReasoningState),
    m_equalityManager(const_cast<EqualityManager&>(m_updateEqualityManagerTask.getDatalogEngine().getDataStore().getEqualityManager())),
    m_provingEqualityManager(m_updateEqualityManagerTask.getDatalogEngine().getRuleIndex().getProvingEqualityManager()),
    m_taskRunning(1),
    m_currentTupleBuffer(3, INVALID_RESOURCE_ID)
{
}

template<bool callMonitor, bool multithreaded, UpdateEqualityManagerTask::UpdateType updateType>
void UpdateEqualityManagerTaskWorker<callMonitor, multithreaded, updateType>::run(ThreadContext& threadContext) {
    if (callMonitor)
        m_incrementalMonitor->updateEqualityManagerStarted(m_workerIndex);
    TupleIndex tupleIndex;
    while (::atomicRead(m_taskRunning) && (tupleIndex = m_incrementalReasoningState.getDeleteList().template dequeue<multithreaded>()) != INVALID_TUPLE_INDEX) {
        m_tripleTable.getStatusAndTuple(tupleIndex, m_currentTupleBuffer);
        if (m_currentTupleBuffer[1] == OWL_SAME_AS_ID) {
            const ResourceID resourceID = m_currentTupleBuffer[0];
            switch (updateType) {
            case UpdateEqualityManagerTask::COPY_CLASSES:
                // The monitor must be called before the actual change so that it has the chance to analyze the equivalence classes.
                if (callMonitor)
                    m_incrementalMonitor->equivalenceClassCopied(m_workerIndex, resourceID, m_provingEqualityManager, m_equalityManager);
                m_equalityManager.copyEquivalenceClass(resourceID, m_provingEqualityManager);
                break;
            case UpdateEqualityManagerTask::UNREPRESENT:
                m_equalityManager.unrepresent(resourceID);
                break;
            case UpdateEqualityManagerTask::BREAK_EQUALS:
                m_equalityManager.breakEquals(resourceID);
                break;
            }
        }
    }
    if (callMonitor)
        m_incrementalMonitor->updateEqualityManagerFinished(m_workerIndex);
}

template<bool callMonitor, bool multithreaded, UpdateEqualityManagerTask::UpdateType updateType>
void UpdateEqualityManagerTaskWorker<callMonitor, multithreaded, updateType>::stop() {
    ::atomicWrite(m_taskRunning, 0);
}
