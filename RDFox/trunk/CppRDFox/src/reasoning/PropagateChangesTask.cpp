// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../util/Vocabulary.h"
#include "../storage/DataStore.h"
#include "../storage/TupleTable.h"
#include "DatalogEngine.h"
#include "DatalogEngineWorker.h"
#include "PropagateChangesTask.h"
#include "IncrementalMonitor.h"
#include "IncrementalReasoningState.h"

// PropagateChangesTask

template<bool callMonitor>
always_inline std::unique_ptr<ReasoningTaskWorker> PropagateChangesTask::doCreateWorker1(DatalogEngineWorker& datalogEngineWorker) {
    if (m_optimizeEquality)
        return doCreateWorker2<callMonitor, true>(datalogEngineWorker);
    else
        return doCreateWorker2<callMonitor, false>(datalogEngineWorker);
}

template<bool callMonitor, bool optimizeEquality>
always_inline std::unique_ptr<ReasoningTaskWorker> PropagateChangesTask::doCreateWorker2(DatalogEngineWorker& datalogEngineWorker) {
    if (isMultithreaded())
        return std::unique_ptr<ReasoningTaskWorker>(new PropagateChangesTaskWorker<callMonitor, optimizeEquality, true>(*this, datalogEngineWorker));
    else
        return std::unique_ptr<ReasoningTaskWorker>(new PropagateChangesTaskWorker<callMonitor, optimizeEquality, false>(*this, datalogEngineWorker));
}

std::unique_ptr<ReasoningTaskWorker> PropagateChangesTask::doCreateWorker(DatalogEngineWorker& datalogEngineWorker) {
    if (m_incrementalMonitor == nullptr)
        return doCreateWorker1<false>(datalogEngineWorker);
    else
        return doCreateWorker1<true>(datalogEngineWorker);
}

void PropagateChangesTask::doInitialize() {
    m_incrementalReasoningState.getDeleteList().resetDequeuePosition();
    m_incrementalReasoningState.getProvedList().resetDequeuePosition();
    m_incrementalReasoningState.getAddedList().resetDequeuePosition();
}

PropagateChangesTask::PropagateChangesTask(DatalogEngine& datalogEngine, IncrementalMonitor* const incrementalMonitor, IncrementalReasoningState& incrementalReasoningState, const bool byLevels) :
    ReasoningTask(datalogEngine),
    m_optimizeEquality(datalogEngine.getDataStore().getEqualityAxiomatizationType() != EQUALITY_AXIOMATIZATION_OFF),
    m_byLevels(byLevels),
    m_incrementalMonitor(incrementalMonitor),
    m_incrementalReasoningState(incrementalReasoningState)
{
}

// PropagateChangesTaskWorker

template<bool callMonitor, bool optimizeEquality, bool multithreaded>
PropagateChangesTaskWorker<callMonitor, optimizeEquality, multithreaded>::PropagateChangesTaskWorker(PropagateChangesTask& propagateChangesTask, DatalogEngineWorker& datalogEngineWorker) :
    m_propagateChangesTask(propagateChangesTask),
    m_incrementalMonitor(m_propagateChangesTask.m_incrementalMonitor),
    m_workerIndex(datalogEngineWorker.getWorkerIndex()),
    m_tripleTable(m_propagateChangesTask.getDatalogEngine().getDataStore().getTupleTable("internal$rdf")),
    m_equalityManager(const_cast<EqualityManager&>(m_propagateChangesTask.getDatalogEngine().getDataStore().getEqualityManager())),
    m_provingEqualityManager(m_propagateChangesTask.getDatalogEngine().getRuleIndex().getProvingEqualityManager()),
    m_incrementalReasoningState(m_propagateChangesTask.m_incrementalReasoningState),
    m_taskRunning(1)
{
}

template<bool callMonitor, bool optimizeEquality, bool multithreaded>
void PropagateChangesTaskWorker<callMonitor, optimizeEquality, multithreaded>::run(ThreadContext& threadContext) {
    TupleIndex tupleIndex;
    std::vector<ResourceID> currentTupleBuffer1(3, INVALID_RESOURCE_ID);
    std::vector<ResourceID> currentTupleBuffer2(3, INVALID_RESOURCE_ID);
    std::vector<ArgumentIndex> currentTupleArgumentIndexes;
    currentTupleArgumentIndexes.push_back(0);
    currentTupleArgumentIndexes.push_back(1);
    currentTupleArgumentIndexes.push_back(2);
    const size_t maxComponentLevel = m_incrementalReasoningState.getMaxComponentLevel();
    for (size_t componentLevel = 0; componentLevel <= maxComponentLevel; ++componentLevel) {
        if (callMonitor)
            m_incrementalMonitor->propagateDeletedProvedStarted(m_workerIndex, m_propagateChangesTask.m_byLevels ? componentLevel : static_cast<size_t>(-1));
        const uint64_t deleteListEnd = m_incrementalReasoningState.getDeleteListEnd(componentLevel);
        while (::atomicRead(m_taskRunning) && (tupleIndex = m_incrementalReasoningState.getDeleteList().template dequeue<multithreaded>(deleteListEnd)) != INVALID_TUPLE_INDEX) {
            if ((m_incrementalReasoningState.getGlobalFlags(tupleIndex) & (GF_DELETED | GF_ADDED)) == GF_DELETED) {
                if (m_tripleTable.deleteTupleStatus(tupleIndex, TUPLE_STATUS_IDB | TUPLE_STATUS_IDB_MERGED)) {
                    if (callMonitor) {
                        m_tripleTable.getStatusAndTuple(tupleIndex, currentTupleBuffer1);
                        m_incrementalMonitor->tupleDeleted(m_workerIndex, currentTupleBuffer1, currentTupleArgumentIndexes, true);
                    }
                }
                else {
                    if (callMonitor) {
                        m_tripleTable.getStatusAndTuple(tupleIndex, currentTupleBuffer1);
                        m_incrementalMonitor->tupleDeleted(m_workerIndex, currentTupleBuffer1, currentTupleArgumentIndexes, false);
                    }
                }
            }
        }
        if (optimizeEquality) {
            while (::atomicRead(m_taskRunning) && (tupleIndex = m_incrementalReasoningState.getProvedList().template dequeue<multithreaded>()) != INVALID_TUPLE_INDEX) {
                if (((m_incrementalReasoningState.getCurrentLevelFlags(tupleIndex) & LF_PROVED) == LF_PROVED) && ((m_incrementalReasoningState.getGlobalFlags(tupleIndex) & GF_ADDED_MERGED) == 0)) {
                    m_tripleTable.getStatusAndTuple(tupleIndex, currentTupleBuffer1);
                    if (m_equalityManager.normalize(currentTupleBuffer1, currentTupleBuffer2, currentTupleArgumentIndexes)) {
                        if (m_tripleTable.addTuple(threadContext, currentTupleBuffer2, currentTupleArgumentIndexes, TUPLE_STATUS_IDB_MERGED, TUPLE_STATUS_IDB).first) {
                            if (callMonitor)
                                m_incrementalMonitor->tupleAdded(m_workerIndex, currentTupleBuffer2, currentTupleArgumentIndexes, true);
                        }
                        else {
                            if (callMonitor)
                                m_incrementalMonitor->tupleAdded(m_workerIndex, currentTupleBuffer2, currentTupleArgumentIndexes, false);
                        }
                    }
                    else {
                        if (m_tripleTable.deleteAddTupleStatus(tupleIndex, 0, 0, TUPLE_STATUS_IDB_MERGED, 0, 0, TUPLE_STATUS_IDB)) {
                            if (callMonitor)
                                m_incrementalMonitor->tupleAdded(m_workerIndex, currentTupleBuffer1, currentTupleArgumentIndexes, true);
                        }
                        else {
                            if (callMonitor)
                                m_incrementalMonitor->tupleAdded(m_workerIndex, currentTupleBuffer1, currentTupleArgumentIndexes, false);
                        }
                    }
                }
            }
        }
        const uint64_t addedListEnd = m_incrementalReasoningState.getAddedListEnd(componentLevel);
        while (::atomicRead(m_taskRunning) && (tupleIndex = m_incrementalReasoningState.getAddedList().template dequeue<multithreaded>(addedListEnd)) != INVALID_TUPLE_INDEX) {
            if ((m_incrementalReasoningState.getGlobalFlags(tupleIndex) & (GF_DELETED | GF_ADDED | GF_ADDED_MERGED)) == GF_ADDED) {
                if (m_tripleTable.deleteAddTupleStatus(tupleIndex, 0, 0, TUPLE_STATUS_IDB_MERGED, 0, 0, TUPLE_STATUS_IDB)) {
                    if (callMonitor) {
                        m_tripleTable.getStatusAndTuple(tupleIndex, currentTupleBuffer1);
                        m_incrementalMonitor->tupleAdded(m_workerIndex, currentTupleBuffer1, currentTupleArgumentIndexes, true);
                    }
                }
                else {
                    if (callMonitor) {
                        m_tripleTable.getStatusAndTuple(tupleIndex, currentTupleBuffer1);
                        m_incrementalMonitor->tupleAdded(m_workerIndex, currentTupleBuffer1, currentTupleArgumentIndexes, false);
                    }
                }
            }
        }
        if (callMonitor)
            m_incrementalMonitor->propagateDeletedProvedFinished(m_workerIndex);
    }
}

template<bool callMonitor, bool optimizeEquality, bool multithreaded>
void PropagateChangesTaskWorker<callMonitor, optimizeEquality, multithreaded>::stop() {
    ::atomicWrite(m_taskRunning, 0);
}
