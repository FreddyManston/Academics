// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../util/LockFreeQueue.h"
#include "../storage/DataStore.h"
#include "../storage/TupleTable.h"
#include "RuleIndexImpl.h"
#include "DatalogEngine.h"
#include "DatalogEngineWorker.h"
#include "InitializeInsertedTask.h"
#include "IncrementalReasoningState.h"

// InitializeInsertedTask

template<bool checkComponentLevel>
always_inline std::unique_ptr<ReasoningTaskWorker> InitializeInsertedTask::doCreateWorker1(DatalogEngineWorker& datalogEngineWorker) {
    if (isMultithreaded())
        return std::unique_ptr<ReasoningTaskWorker>(new InitializeInsertedTaskWorker<checkComponentLevel, true>(*this, datalogEngineWorker));
    else
        return std::unique_ptr<ReasoningTaskWorker>(new InitializeInsertedTaskWorker<checkComponentLevel, false>(*this, datalogEngineWorker));
}

std::unique_ptr<ReasoningTaskWorker> InitializeInsertedTask::doCreateWorker(DatalogEngineWorker& datalogEngineWorker) {
    if (m_checkComponentLevel)
        return doCreateWorker1<true>(datalogEngineWorker);
    else
        return doCreateWorker1<false>(datalogEngineWorker);
}

void InitializeInsertedTask::doInitialize() {
    m_edbInsertList.resetDequeuePosition();
}

InitializeInsertedTask::InitializeInsertedTask(DatalogEngine& datalogEngine, IncrementalReasoningState& incrementalReasoningState, LockFreeQueue<TupleIndex>& edbInsertList, const bool checkComponentLevel) :
    ReasoningTask(datalogEngine),
    m_incrementalReasoningState(incrementalReasoningState),
    m_edbInsertList(edbInsertList),
    m_checkComponentLevel(checkComponentLevel)
{
}

// InitializeInsertedTaskWorker

template<bool checkComponentLevel, bool multithreaded>
InitializeInsertedTaskWorker<checkComponentLevel, multithreaded>::InitializeInsertedTaskWorker(InitializeInsertedTask& initializeInsertedTask, DatalogEngineWorker& datalogEngineWorker) :
    m_initializeInsertedTask(initializeInsertedTask),
    m_tripleTable(m_initializeInsertedTask.getDatalogEngine().getDataStore().getTupleTable("internal$rdf")),
    m_ruleIndex(m_initializeInsertedTask.getDatalogEngine().getRuleIndex()),
    m_incrementalReasoningState(m_initializeInsertedTask.m_incrementalReasoningState),
    m_edbInsertList(m_initializeInsertedTask.m_edbInsertList),
    m_taskRunning(1),
    m_currentTupleBuffer(3, INVALID_RESOURCE_ID),
    m_currentTupleArgumentIndexes()
{
    m_currentTupleArgumentIndexes.push_back(0);
    m_currentTupleArgumentIndexes.push_back(1);
    m_currentTupleArgumentIndexes.push_back(2);
}

template<bool checkComponentLevel, bool multithreaded>
void InitializeInsertedTaskWorker<checkComponentLevel, multithreaded>::run(ThreadContext& threadContext) {
    TupleIndex tupleIndex;
    while (::atomicRead(m_taskRunning) && (tupleIndex = m_edbInsertList.dequeue<multithreaded>()) != INVALID_TUPLE_INDEX) {
        if (m_tripleTable.deleteTupleStatus(tupleIndex, TUPLE_STATUS_EDB_INS) && m_tripleTable.addTupleStatus(tupleIndex, TUPLE_STATUS_EDB) && m_incrementalReasoningState.addGlobalFlags<multithreaded>(tupleIndex, GF_ADDED_NEW)) {
            if (checkComponentLevel) {
                m_tripleTable.getStatusAndTuple(tupleIndex, m_currentTupleBuffer);
                const size_t componentLevel = m_ruleIndex.getComponentLevel(m_currentTupleBuffer, m_currentTupleArgumentIndexes);
                m_incrementalReasoningState.getInitiallyAddedList(componentLevel).template enqueue<multithreaded>(tupleIndex);
            }
            else
                m_incrementalReasoningState.getAddedList().template enqueue<multithreaded>(tupleIndex);
        }
    }
}

template<bool checkComponentLevel, bool multithreaded>
void InitializeInsertedTaskWorker<checkComponentLevel, multithreaded>::stop() {
    ::atomicWrite(m_taskRunning, 0);
}
