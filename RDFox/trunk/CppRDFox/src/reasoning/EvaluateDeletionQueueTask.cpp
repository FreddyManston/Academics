// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../storage/DataStore.h"
#include "../storage/TupleTable.h"
#include "RuleIndexImpl.h"
#include "DatalogEngine.h"
#include "DatalogEngineWorker.h"
#include "EvaluateDeletionQueueTask.h"
#include "IncrementalMonitor.h"
#include "IncrementalReasoningState.h"

// EvaluateDeletionQueueTask

template<bool callMonitor>
always_inline std::unique_ptr<ReasoningTaskWorker> EvaluateDeletionQueueTask::doCreateWorker1(DatalogEngineWorker& datalogEngineWorker) {
    if (m_checkComponentLevel)
        return doCreateWorker2<callMonitor, true>(datalogEngineWorker);
    else
        return doCreateWorker2<callMonitor, false>(datalogEngineWorker);
}

template<bool callMonitor, bool checkComponentLevel>
always_inline std::unique_ptr<ReasoningTaskWorker> EvaluateDeletionQueueTask::doCreateWorker2(DatalogEngineWorker& datalogEngineWorker) {
    if (m_datalogEngine.getDataStore().getNumberOfThreads())
        return std::unique_ptr<ReasoningTaskWorker>(new EvaluateDeletionQueueTaskWorker<callMonitor, checkComponentLevel, true>(*this, datalogEngineWorker));
    else
        return std::unique_ptr<ReasoningTaskWorker>(new EvaluateDeletionQueueTaskWorker<callMonitor, checkComponentLevel, false>(*this, datalogEngineWorker));
}

std::unique_ptr<ReasoningTaskWorker> EvaluateDeletionQueueTask::doCreateWorker(DatalogEngineWorker& datalogEngineWorker) {
    if (m_incrementalMonitor == nullptr)
        return doCreateWorker1<false>(datalogEngineWorker);
    else
        return doCreateWorker1<true>(datalogEngineWorker);
}

void EvaluateDeletionQueueTask::doInitialize() {
    if (!m_ruleQueue.initializeLarge())
        throw RDF_STORE_EXCEPTION("Cannot initialize rule queue.");
    m_datalogEngine.getRuleIndex().enqueueDeletedRules(m_ruleQueue);
}

EvaluateDeletionQueueTask::EvaluateDeletionQueueTask(DatalogEngine& datalogEngine, IncrementalMonitor* const incrementalMonitor, IncrementalReasoningState& incrementalReasoningState, const bool checkComponentLevel) :
    ReasoningTask(datalogEngine),
    m_incrementalMonitor(incrementalMonitor),
    m_incrementalReasoningState(incrementalReasoningState),
    m_checkComponentLevel(checkComponentLevel),
    m_ruleQueue(m_datalogEngine.getDataStore().getMemoryManager())
{
}

// EvaluateDeletionQueueTaskWorker

template<bool callMonitor, bool checkComponentLevel, bool multithreaded>
EvaluateDeletionQueueTaskWorker<callMonitor, checkComponentLevel, multithreaded>::EvaluateDeletionQueueTaskWorker(EvaluateDeletionQueueTask& evaluateDeletionQueueTask, DatalogEngineWorker& datalogEngineWorker) :
    m_evaluateDeletionQueueTask(evaluateDeletionQueueTask),
    m_incrementalMonitor(m_evaluateDeletionQueueTask.m_incrementalMonitor),
    m_workerIndex(datalogEngineWorker.getWorkerIndex()),
    m_ruleQueue(m_evaluateDeletionQueueTask.m_ruleQueue),
    m_tripleTable(m_evaluateDeletionQueueTask.getDatalogEngine().getDataStore().getTupleTable("internal$rdf")),
    m_ruleIndex(m_evaluateDeletionQueueTask.getDatalogEngine().getRuleIndex()),
    m_incrementalReasoningState(m_evaluateDeletionQueueTask.m_incrementalReasoningState),
    m_taskRunning(1)
{
}

template<bool callMonitor, bool checkComponentLevel, bool multithreaded>
always_inline void EvaluateDeletionQueueTaskWorker<callMonitor, checkComponentLevel, multithreaded>::deriveTuple(ThreadContext& threadContext, const HeadAtomInfo& headAtomInfo, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) {
    const TupleIndex tupleIndex = m_tripleTable.getTupleIndex(threadContext, argumentsBuffer, argumentIndexes);
    if (tupleIndex == INVALID_TUPLE_INDEX)
       throw RDF_STORE_EXCEPTION("Error during rule deletion: the state of the store does not match the rule set.");
    if (m_incrementalReasoningState.addGlobalFlags<multithreaded>(tupleIndex, GF_DELETED_NEW)) {
        if (checkComponentLevel)
            m_incrementalReasoningState.getInitiallyDeletedList(headAtomInfo.getComponentLevel()).template enqueue<multithreaded>(tupleIndex);
        else
            m_incrementalReasoningState.getDeleteList().template enqueue<multithreaded>(tupleIndex);
        if (callMonitor)
            m_incrementalMonitor->tupleDerived(m_workerIndex, argumentsBuffer, argumentIndexes, true, true);
    }
    else {
        if (callMonitor)
            m_incrementalMonitor->tupleDerived(m_workerIndex, argumentsBuffer, argumentIndexes, true, false);
    }
}

template<bool callMonitor, bool checkComponentLevel, bool multithreaded>
void EvaluateDeletionQueueTaskWorker<callMonitor, checkComponentLevel, multithreaded>::run(ThreadContext& threadContext) {
    auto filters = m_ruleIndex.template setTupleIteratorFilters<MAIN_TUPLE_ITERATOR_FILTER>(*this, [](EvaluateDeletionQueueTaskWorkerType& target, const TupleIndex tupleIndex, const TupleStatus tupleStatus) {
        return (tupleStatus & (TUPLE_STATUS_IDB | TUPLE_STATUS_IDB_MERGED)) == TUPLE_STATUS_IDB;
    });

    RuleInfo* ruleInfo;
    while (::atomicRead(m_taskRunning) && (ruleInfo = m_ruleQueue.template dequeue<multithreaded>()) != nullptr) {
        if (callMonitor)
            m_incrementalMonitor->deletedRuleEvaluationStarted(m_workerIndex, *ruleInfo);
        ruleInfo->template evaluateRuleMain<false, callMonitor>(threadContext, this->m_workerIndex, static_cast<size_t>(-1), m_incrementalMonitor,
            [this](ThreadContext& threadContext, const HeadAtomInfo& headAtomInfo, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) {
                this->deriveTuple(threadContext, headAtomInfo, argumentsBuffer, argumentIndexes);
            }
        );
        if (callMonitor)
            m_incrementalMonitor->deletedRuleEvaluationFinished(m_workerIndex);
    }
}

template<bool callMonitor, bool checkComponentLevel, bool multithreaded>
void EvaluateDeletionQueueTaskWorker<callMonitor, checkComponentLevel, multithreaded>::stop() {
    ::atomicWrite(m_taskRunning, 0);
}
