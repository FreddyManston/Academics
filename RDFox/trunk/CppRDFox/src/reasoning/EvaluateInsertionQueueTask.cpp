// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../storage/DataStore.h"
#include "../storage/TupleTable.h"
#include "../util/LockFreeQueue.h"
#include "RuleIndexImpl.h"
#include "DatalogEngine.h"
#include "DatalogEngineWorker.h"
#include "EvaluateInsertionQueueTask.h"
#include "IncrementalMonitor.h"
#include "IncrementalReasoningState.h"

// EvaluateInsertionQueueTask

template<bool callMonitor>
always_inline std::unique_ptr<ReasoningTaskWorker> EvaluateInsertionQueueTask::doCreateWorker1(DatalogEngineWorker& datalogEngineWorker) {
    if (m_componentLevel == static_cast<size_t>(-1))
        return doCreateWorker2<callMonitor, false>(datalogEngineWorker);
    else
        return doCreateWorker2<callMonitor, true>(datalogEngineWorker);
}

template<bool callMonitor, bool checkComponentLevel>
always_inline std::unique_ptr<ReasoningTaskWorker> EvaluateInsertionQueueTask::doCreateWorker2(DatalogEngineWorker& datalogEngineWorker) {
    if (m_datalogEngine.getDataStore().getEqualityAxiomatizationType() != EQUALITY_AXIOMATIZATION_OFF) {
        if (!checkComponentLevel)
            return doCreateWorker3<callMonitor, checkComponentLevel, true>(datalogEngineWorker);
        else
            UNREACHABLE;
    }
    else
        return doCreateWorker3<callMonitor, checkComponentLevel, false>(datalogEngineWorker);
}

template<bool callMonitor, bool checkComponentLevel, bool optimizeEquality>
always_inline std::unique_ptr<ReasoningTaskWorker> EvaluateInsertionQueueTask::doCreateWorker3(DatalogEngineWorker& datalogEngineWorker) {
    if (isMultithreaded())
        return std::unique_ptr<ReasoningTaskWorker>(new EvaluateInsertionQueueTaskWorker<callMonitor, checkComponentLevel, optimizeEquality, true>(*this, datalogEngineWorker));
    else
        return std::unique_ptr<ReasoningTaskWorker>(new EvaluateInsertionQueueTaskWorker<callMonitor, checkComponentLevel, optimizeEquality, false>(*this, datalogEngineWorker));
}

std::unique_ptr<ReasoningTaskWorker> EvaluateInsertionQueueTask::doCreateWorker(DatalogEngineWorker& datalogEngineWorker) {
    if (m_incrementalMonitor == nullptr)
        return doCreateWorker1<false>(datalogEngineWorker);
    else
        return doCreateWorker1<true>(datalogEngineWorker);
}

void EvaluateInsertionQueueTask::doInitialize() {
    m_ruleQueue.resetDequeuePosition();
}

EvaluateInsertionQueueTask::EvaluateInsertionQueueTask(DatalogEngine& datalogEngine, IncrementalMonitor* const incrementalMonitor, IncrementalReasoningState& incrementalReasoningState, LockFreeQueue<RuleInfo*>& ruleQueue, const size_t componentLevel) :
    ReasoningTask(datalogEngine),
    m_incrementalMonitor(incrementalMonitor),
    m_incrementalReasoningState(incrementalReasoningState),
    m_ruleQueue(ruleQueue),
    m_componentLevel(componentLevel)
{
}

// EvaluateInsertionQueueTaskWorker

template<bool callMonitor, bool checkComponentLevel, bool optimizeEquality, bool multithreaded>
EvaluateInsertionQueueTaskWorker<callMonitor, checkComponentLevel, optimizeEquality, multithreaded>::EvaluateInsertionQueueTaskWorker(EvaluateInsertionQueueTask& evaluateInsertionQueueTask, DatalogEngineWorker& datalogEngineWorker) :
    m_evaluateInsertionQueueTask(evaluateInsertionQueueTask),
    m_incrementalMonitor(m_evaluateInsertionQueueTask.m_incrementalMonitor),
    m_workerIndex(datalogEngineWorker.getWorkerIndex()),
    m_tripleTable(m_evaluateInsertionQueueTask.getDatalogEngine().getDataStore().getTupleTable("internal$rdf")),
    m_ruleIndex(m_evaluateInsertionQueueTask.getDatalogEngine().getRuleIndex()),
    m_incrementalReasoningState(m_evaluateInsertionQueueTask.m_incrementalReasoningState),
    m_ruleQueue(m_evaluateInsertionQueueTask.m_ruleQueue),
    m_taskRunning(1)
{
}

template<bool callMonitor, bool checkComponentLevel, bool optimizeEquality, bool multithreaded>
always_inline void EvaluateInsertionQueueTaskWorker<callMonitor, checkComponentLevel, optimizeEquality, multithreaded>::deriveTuple(ThreadContext& threadContext, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) {
    const TupleIndex tupleIndex = m_tripleTable.addTuple(threadContext, argumentsBuffer, argumentIndexes, 0, 0).second;
    if (m_incrementalReasoningState.addGlobalFlags<multithreaded>(tupleIndex, GF_ADDED_NEW)) {
        m_incrementalReasoningState.getAddedList().template enqueue<multithreaded>(tupleIndex);
        if (callMonitor)
            m_incrementalMonitor->tupleDerived(m_workerIndex, argumentsBuffer, argumentIndexes, true, true);
    }
    else {
        if (callMonitor)
            m_incrementalMonitor->tupleDerived(m_workerIndex, argumentsBuffer, argumentIndexes, true, false);
    }
}

template<bool callMonitor, bool checkComponentLevel, bool optimizeEquality, bool multithreaded>
void EvaluateInsertionQueueTaskWorker<callMonitor, checkComponentLevel, optimizeEquality, multithreaded>::run(ThreadContext& threadContext) {
    auto filters = m_ruleIndex.template setTupleIteratorFilters<MAIN_TUPLE_ITERATOR_FILTER>(*this, [](EvaluateInsertionQueueTaskWorkerType& target, const TupleIndex tupleIndex, const TupleStatus tupleStatus) {
        const TupleFlags tupleFlags = target.m_incrementalReasoningState.getGlobalFlags(tupleIndex);
        return
            (((tupleStatus & (TUPLE_STATUS_IDB | TUPLE_STATUS_IDB_MERGED)) == TUPLE_STATUS_IDB) && ((tupleFlags & (GF_DELETED | GF_ADDED_MERGED)) == 0)) ||
            ((tupleFlags & (GF_ADDED | GF_ADDED_MERGED)) == GF_ADDED) ||
            (optimizeEquality && ((target.m_incrementalReasoningState.getCurrentLevelFlags(tupleIndex) & (LF_PROVED | LF_PROVED_MERGED)) == LF_PROVED) && ((tupleFlags & GF_ADDED_MERGED) == 0));
    });

    const size_t componentLevel = m_evaluateInsertionQueueTask.m_componentLevel;
    RuleInfo* ruleInfo;
    while (::atomicRead(m_taskRunning) && (ruleInfo = m_ruleQueue.template dequeue<multithreaded>()) != nullptr) {
        if (!checkComponentLevel || (ruleInfo->isInComponentLevelFilter(componentLevel))) {
            if (callMonitor)
                m_incrementalMonitor->addedRuleEvaluationStarted(m_workerIndex, *ruleInfo);
            ruleInfo->template evaluateRuleMain<checkComponentLevel, callMonitor>(threadContext, m_workerIndex, componentLevel, m_incrementalMonitor,
                [this](ThreadContext& threadContext, const HeadAtomInfo& headAtomInfo, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) {
                    this->deriveTuple(threadContext, argumentsBuffer, argumentIndexes);
                }
            );
            if (callMonitor)
                m_incrementalMonitor->addedRuleEvaluationFinished(m_workerIndex);
        }
    }
}

template<bool callMonitor, bool checkComponentLevel, bool optimizeEquality, bool multithreaded>
void EvaluateInsertionQueueTaskWorker<callMonitor, checkComponentLevel, optimizeEquality, multithreaded>::stop() {
    ::atomicWrite(m_taskRunning, 0);
}
