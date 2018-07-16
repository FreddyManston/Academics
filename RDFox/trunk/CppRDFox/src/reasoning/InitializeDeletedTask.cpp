// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../util/LockFreeQueue.h"
#include "../equality/EqualityManager.h"
#include "../storage/DataStore.h"
#include "../storage/TupleTable.h"
#include "RuleIndexImpl.h"
#include "DatalogEngine.h"
#include "DatalogEngineWorker.h"
#include "InitializeDeletedTask.h"
#include "IncrementalReasoningState.h"

// InitializeDeletedTask

template<bool optimizeEquality>
always_inline std::unique_ptr<ReasoningTaskWorker> InitializeDeletedTask::doCreateWorker1(DatalogEngineWorker& datalogEngineWorker) {
    if (m_checkComponentLevel) {
        if (!optimizeEquality)
            return doCreateWorker2<optimizeEquality, true>(datalogEngineWorker);
        else
            UNREACHABLE;
    }
    else
        return doCreateWorker2<optimizeEquality, false>(datalogEngineWorker);
}

template<bool optimizeEquality, bool checkComponentLevel>
always_inline std::unique_ptr<ReasoningTaskWorker> InitializeDeletedTask::doCreateWorker2(DatalogEngineWorker& datalogEngineWorker) {
    if (isMultithreaded())
        return std::unique_ptr<ReasoningTaskWorker>(new InitializeDeletedTaskWorker<optimizeEquality, checkComponentLevel, true>(*this, datalogEngineWorker));
    else
        return std::unique_ptr<ReasoningTaskWorker>(new InitializeDeletedTaskWorker<optimizeEquality, checkComponentLevel, false>(*this, datalogEngineWorker));
}

std::unique_ptr<ReasoningTaskWorker> InitializeDeletedTask::doCreateWorker(DatalogEngineWorker& datalogEngineWorker) {
    if (m_datalogEngine.getDataStore().getEqualityAxiomatizationType() != EQUALITY_AXIOMATIZATION_OFF)
        return doCreateWorker1<true>(datalogEngineWorker);
    else
        return doCreateWorker1<false>(datalogEngineWorker);
}

void InitializeDeletedTask::doInitialize() {
    m_edbDeleteList.resetDequeuePosition();
}

InitializeDeletedTask::InitializeDeletedTask(DatalogEngine& datalogEngine, IncrementalReasoningState& incrementalReasoningState, LockFreeQueue<TupleIndex>& edbDeleteList, const bool checkComponentLevel) :
    ReasoningTask(datalogEngine),
    m_incrementalReasoningState(incrementalReasoningState),
    m_edbDeleteList(edbDeleteList),
    m_checkComponentLevel(checkComponentLevel)
{
}

// InitializeDeletedTaskWorker

template<bool optimizeEquality, bool checkComponentLevel, bool multithreaded>
InitializeDeletedTaskWorker<optimizeEquality, checkComponentLevel, multithreaded>::InitializeDeletedTaskWorker(InitializeDeletedTask& initializeDeletedTask, DatalogEngineWorker& datalogEngineWorker) :
    m_initializeDeletedTask(initializeDeletedTask),
    m_equalityManager(m_initializeDeletedTask.getDatalogEngine().getDataStore().getEqualityManager()),
    m_tripleTable(m_initializeDeletedTask.getDatalogEngine().getDataStore().getTupleTable("internal$rdf")),
    m_ruleIndex(m_initializeDeletedTask.getDatalogEngine().getRuleIndex()),
    m_incrementalReasoningState(m_initializeDeletedTask.m_incrementalReasoningState),
    m_edbDeleteList(m_initializeDeletedTask.m_edbDeleteList),
    m_taskRunning(1),
    m_currentTupleBuffer(3, INVALID_RESOURCE_ID),
    m_currentTupleArgumentIndexes()
{
    m_currentTupleArgumentIndexes.push_back(0);
    m_currentTupleArgumentIndexes.push_back(1);
    m_currentTupleArgumentIndexes.push_back(2);
}

template<bool optimizeEquality, bool checkComponentLevel, bool multithreaded>
void InitializeDeletedTaskWorker<optimizeEquality, checkComponentLevel, multithreaded>::run(ThreadContext& threadContext) {
    TupleIndex tupleIndex;
    while (::atomicRead(m_taskRunning) && (tupleIndex = m_edbDeleteList.dequeue<multithreaded>()) != INVALID_TUPLE_INDEX) {
        if (optimizeEquality || checkComponentLevel) {
            const TupleStatus tupleStatus = m_tripleTable.getStatusAndTuple(tupleIndex, m_currentTupleBuffer);
            if ((tupleStatus & TUPLE_STATUS_EDB_DEL) != 0) {
                // Since tripleIndex can be modified in the code below, we must delete the flags right away.
                m_tripleTable.deleteTupleStatus(tupleIndex, TUPLE_STATUS_EDB_DEL | TUPLE_STATUS_EDB);
                if ((tupleStatus & TUPLE_STATUS_EDB) != 0) {
                    if (optimizeEquality && m_equalityManager.normalize(m_currentTupleBuffer, m_currentTupleArgumentIndexes))
                        tupleIndex = m_tripleTable.addTuple(threadContext, m_currentTupleBuffer, m_currentTupleArgumentIndexes, 0, 0).second;
                    if (m_incrementalReasoningState.addGlobalFlags<multithreaded>(tupleIndex, GF_DELETED_NEW)) {
                        if (checkComponentLevel) {
                            const size_t componentLevel = m_ruleIndex.getComponentLevel(m_currentTupleBuffer, m_currentTupleArgumentIndexes);
                            m_incrementalReasoningState.getInitiallyDeletedList(componentLevel).template enqueue<multithreaded>(tupleIndex);
                        }
                        else
                            m_incrementalReasoningState.getDeleteList().template enqueue<multithreaded>(tupleIndex);
                    }
                }
            }
        }
        else {
            const TupleStatus tupleStatus = m_tripleTable.getTupleStatus(tupleIndex);
            if ((tupleStatus & TUPLE_STATUS_EDB_DEL) != 0) {
                m_tripleTable.deleteTupleStatus(tupleIndex, TUPLE_STATUS_EDB_DEL | TUPLE_STATUS_EDB);
                if ((tupleStatus & TUPLE_STATUS_EDB) != 0 && m_incrementalReasoningState.addGlobalFlags<multithreaded>(tupleIndex, GF_DELETED_NEW))
                    m_incrementalReasoningState.getDeleteList().template enqueue<multithreaded>(tupleIndex);
            }
        }
    }
}

template<bool optimizeEquality, bool checkComponentLevel, bool multithreaded>
void InitializeDeletedTaskWorker<optimizeEquality, checkComponentLevel, multithreaded>::stop() {
    ::atomicWrite(m_taskRunning, 0);
}
