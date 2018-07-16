// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../equality/EqualityManager.h"
#include "../storage/DataStore.h"
#include "../storage/TupleIterator.h"
#include "../storage/TupleTable.h"
#include "RuleIndexImpl.h"
#include "DatalogEngine.h"
#include "DatalogEngineWorker.h"
#include "DRedRederivationTask.h"
#include "IncrementalMonitor.h"
#include "IncrementalReasoningState.h"

// DRedRederivationTask

template<bool callMonitor>
always_inline std::unique_ptr<ReasoningTaskWorker> DRedRederivationTask::doCreateWorker1(DatalogEngineWorker& datalogEngineWorker) {
    if (m_datalogEngine.getDataStore().getEqualityAxiomatizationType() != EQUALITY_AXIOMATIZATION_OFF)
        return doCreateWorker2<callMonitor, true>(datalogEngineWorker);
    else
        return doCreateWorker2<callMonitor, false>(datalogEngineWorker);
}

template<bool callMonitor, bool optimizeEquality>
always_inline std::unique_ptr<ReasoningTaskWorker> DRedRederivationTask::doCreateWorker2(DatalogEngineWorker& datalogEngineWorker) {
    if (m_datalogEngine.getRuleIndex().hasRules(m_componentLevel))
        return doCreateWorker3<callMonitor, optimizeEquality, true>(datalogEngineWorker);
    else
        return doCreateWorker3<callMonitor, optimizeEquality, false>(datalogEngineWorker);
}

template<bool callMonitor, bool optimizeEquality, bool hasRules>
always_inline std::unique_ptr<ReasoningTaskWorker> DRedRederivationTask::doCreateWorker3(DatalogEngineWorker& datalogEngineWorker) {
    if (isMultithreaded())
        return std::unique_ptr<ReasoningTaskWorker>(new DRedRederivationTaskWorker<callMonitor, optimizeEquality, hasRules, true>(*this, datalogEngineWorker));
    else
        return std::unique_ptr<ReasoningTaskWorker>(new DRedRederivationTaskWorker<callMonitor, optimizeEquality, hasRules, false>(*this, datalogEngineWorker));
}

std::unique_ptr<ReasoningTaskWorker> DRedRederivationTask::doCreateWorker(DatalogEngineWorker& datalogEngineWorker) {
    if (m_incrementalMonitor == nullptr)
        return doCreateWorker1<false>(datalogEngineWorker);
    else
        return doCreateWorker1<true>(datalogEngineWorker);
}

void DRedRederivationTask::doInitialize() {
    m_incrementalReasoningState.getDeleteList().resetDequeuePosition(m_componentLevel == 0 || m_componentLevel == static_cast<size_t>(-1) ? 0 : m_incrementalReasoningState.getDeleteListEnd(m_componentLevel - 1));
}

DRedRederivationTask::DRedRederivationTask(DatalogEngine& datalogEngine, IncrementalMonitor* const incrementalMonitor, IncrementalReasoningState& incrementalReasoningState, const size_t componentLevel) :
    ReasoningTask(datalogEngine),
    m_incrementalMonitor(incrementalMonitor),
    m_incrementalReasoningState(incrementalReasoningState),
    m_componentLevel(componentLevel)
{
}

// DRedRederivationTaskWorker

template<bool callMonitor, bool optimizeEquality, bool hasRules, bool multithreaded>
DRedRederivationTaskWorker<callMonitor, optimizeEquality, hasRules, multithreaded>::DRedRederivationTaskWorker(DRedRederivationTask& dredRederivationTask, DatalogEngineWorker& datalogEngineWorker) :
    m_dredRederivationTask(dredRederivationTask),
    m_incrementalMonitor(m_dredRederivationTask.m_incrementalMonitor),
    m_equalityManager(m_dredRederivationTask.getDatalogEngine().getDataStore().getEqualityManager()),
    m_workerIndex(datalogEngineWorker.getWorkerIndex()),
    m_tripleTable(m_dredRederivationTask.getDatalogEngine().getDataStore().getTupleTable("internal$rdf")),
    m_ruleIndex(m_dredRederivationTask.getDatalogEngine().getRuleIndex()),
    m_incrementalReasoningState(m_dredRederivationTask.m_incrementalReasoningState),
    m_taskRunning(1),
    m_currentTupleBuffer1(3, INVALID_RESOURCE_ID),
    m_currentTupleBuffer2(3, INVALID_RESOURCE_ID),
    m_currentTupleBuffer3(3, INVALID_RESOURCE_ID),
    m_currentTupleArgumentIndexes()
{
    m_currentTupleArgumentIndexes.push_back(0);
    m_currentTupleArgumentIndexes.push_back(1);
    m_currentTupleArgumentIndexes.push_back(2);
    ArgumentIndexSet allInputArguments;
    for (ArgumentIndex argumentIndex = 0; argumentIndex < 3; ++argumentIndex) {
        allInputArguments.clear();
        allInputArguments.add(argumentIndex);
        m_queriesForSameAsDerivation[argumentIndex] = m_tripleTable.createTupleIterator(m_currentTupleBuffer3, m_currentTupleArgumentIndexes, allInputArguments, allInputArguments);
    }
}

template<bool callMonitor, bool optimizeEquality, bool hasRules, bool multithreaded>
always_inline size_t DRedRederivationTaskWorker<callMonitor, optimizeEquality, hasRules, multithreaded>::getWorkerIndex() {
    return m_workerIndex;
}

template<bool callMonitor, bool optimizeEquality, bool hasRules, bool multithreaded>
always_inline bool DRedRederivationTaskWorker<callMonitor, optimizeEquality, hasRules, multithreaded>::isSupportingAtomPositive(DRedRederivationTaskWorkerType& target, const TupleIndex tupleIndex, const TupleStatus tupleStatus) {
    return
        ((tupleStatus & (TUPLE_STATUS_IDB | TUPLE_STATUS_IDB_MERGED)) == TUPLE_STATUS_IDB) &&
        (target.m_incrementalReasoningState.getGlobalFlags(tupleIndex) & (GF_DELETED | GF_ADDED)) != GF_DELETED;
}

template<bool callMonitor, bool optimizeEquality, bool hasRules, bool multithreaded>
always_inline bool DRedRederivationTaskWorker<callMonitor, optimizeEquality, hasRules, multithreaded>::containsResource(ThreadContext& threadContext, const ResourceID resourceID, const ArgumentIndex argumentIndex) {
    TupleIterator& tupleIterator = *m_queriesForSameAsDerivation[argumentIndex];
    m_currentTupleBuffer3[argumentIndex] = resourceID;
    TupleIndex tupleIndex;
    TupleStatus tupleStatus;
    size_t multiplicity = tupleIterator.open(threadContext);
    while (multiplicity != 0 && tupleIterator.getCurrentTupleInfo(tupleIndex, tupleStatus) && !isSupportingAtomPositive(*this, tupleIndex, tupleStatus))
        multiplicity = tupleIterator.advance();
    if (multiplicity != 0) {
        if (callMonitor) {
            m_currentTupleBuffer2[0] = m_currentTupleBuffer2[2] = resourceID;
            m_currentTupleBuffer2[1] = OWL_SAME_AS_ID;
            m_incrementalMonitor->backwardReflexiveSameAsRuleInstanceStarted(m_workerIndex, m_currentTupleBuffer2, m_currentTupleArgumentIndexes, m_currentTupleBuffer3, m_currentTupleArgumentIndexes);
            m_incrementalMonitor->tupleOptimized(m_workerIndex, m_currentTupleBuffer2, m_currentTupleArgumentIndexes);
            m_incrementalMonitor->backwardReflexiveSameAsRuleInstanceFinished(m_workerIndex);
        }
        return true;
    }
    else
        return false;
}

template<bool callMonitor, bool optimizeEquality, bool hasRules, bool multithreaded>
always_inline bool DRedRederivationTaskWorker<callMonitor, optimizeEquality, hasRules, multithreaded>::isRederived(ThreadContext& threadContext, const std::vector<ResourceID>& currentTupleBuffer) {
    if (hasRules) {
        for (size_t indexingPatternNumber = 0; indexingPatternNumber < 8; ++indexingPatternNumber) {
            for (HeadAtomInfo* headAtomInfo = m_ruleIndex.getMatchingHeadAtomInfos(threadContext, currentTupleBuffer, m_currentTupleArgumentIndexes, indexingPatternNumber); headAtomInfo != nullptr; headAtomInfo = headAtomInfo->getNextMatchingHeadAtomInfo(currentTupleBuffer, m_currentTupleArgumentIndexes)) {
                bool isRecursive;
                if (callMonitor) {
                    isRecursive = (m_dredRederivationTask.m_componentLevel == static_cast<size_t>(-1) ? headAtomInfo->isRecursive<false>() : headAtomInfo->isRecursive<true>());
                    if (isRecursive)
                        m_incrementalMonitor->backwardRecursiveRuleStarted(m_workerIndex, *headAtomInfo);
                    else
                        m_incrementalMonitor->backwardNonrecursiveRuleStarted(m_workerIndex, *headAtomInfo);
                }
                SupportingFactsEvaluator& supportingFactsEvaluator = headAtomInfo->getSupportingFactsEvaluatorPrototype(currentTupleBuffer, m_currentTupleArgumentIndexes);
                const bool result = (supportingFactsEvaluator.open(threadContext) != 0);
                if (callMonitor) {
                    if (result) {
                        if (isRecursive) {
                            m_incrementalMonitor->backwardRecursiveRuleInstanceStarted(m_workerIndex, *headAtomInfo, supportingFactsEvaluator);
                            m_incrementalMonitor->backwardRecursiveRuleInstanceFinished(m_workerIndex);
                        }
                        else
                            m_incrementalMonitor->backwardNonrecursiveInstanceMatched(m_workerIndex, *headAtomInfo, supportingFactsEvaluator);
                    }
                    if (isRecursive)
                        m_incrementalMonitor->backwardRecursiveRuleFinished(m_workerIndex);
                    else
                        m_incrementalMonitor->backwardNonrecursiveRuleFinished(m_workerIndex);
                }
                if (result)
                    return true;
            }
        }
    }
    return false;
}

template<bool callMonitor, bool optimizeEquality, bool hasRules, bool multithreaded>
void DRedRederivationTaskWorker<callMonitor, optimizeEquality, hasRules, multithreaded>::run(ThreadContext& threadContext) {
    auto filters = this->m_ruleIndex.template setTupleIteratorFilters<SUPPORTING_FACTS_TUPLE_ITERATOR_FILTER>(*this,
        [](DRedRederivationTaskWorkerType& target, const TupleIndex tupleIndex, const TupleStatus tupleStatus) {
            return
                ((tupleStatus & (TUPLE_STATUS_IDB | TUPLE_STATUS_IDB_MERGED)) == TUPLE_STATUS_IDB) &&
                (target.m_incrementalReasoningState.getGlobalFlags(tupleIndex) & (GF_DELETED | GF_ADDED)) != GF_DELETED;
        },
        [](DRedRederivationTaskWorkerType& target, const TupleIndex tupleIndex, const TupleStatus tupleStatus) {
            return
                ((tupleStatus & (TUPLE_STATUS_IDB | TUPLE_STATUS_IDB_MERGED)) == TUPLE_STATUS_IDB) ||
                ((target.m_incrementalReasoningState.getGlobalFlags(tupleIndex) & (GF_ADDED | GF_ADDED_MERGED)) == GF_ADDED);
        }
    );

    TupleIndex tupleIndex;
    while (::atomicRead(m_taskRunning) && (tupleIndex = m_incrementalReasoningState.getDeleteList().template dequeue<multithreaded>()) != INVALID_TUPLE_INDEX) {
        const TupleStatus tupleStatus = m_tripleTable.getStatusAndTuple(tupleIndex, m_currentTupleBuffer1);
        if (callMonitor)
            m_incrementalMonitor->checkingProvabilityStarted(m_workerIndex, m_currentTupleBuffer1, m_currentTupleArgumentIndexes, true);
        if (optimizeEquality) {
            for (m_currentTupleBuffer2[0] = m_currentTupleBuffer1[0]; m_currentTupleBuffer2[0] != INVALID_RESOURCE_ID; m_currentTupleBuffer2[0] = m_equalityManager.getNextEqual(m_currentTupleBuffer2[0])) {
                for (m_currentTupleBuffer2[1] = m_currentTupleBuffer1[1]; m_currentTupleBuffer2[1] != INVALID_RESOURCE_ID; m_currentTupleBuffer2[1] = m_equalityManager.getNextEqual(m_currentTupleBuffer2[1])) {
                    for (m_currentTupleBuffer2[2] = m_currentTupleBuffer1[2]; m_currentTupleBuffer2[2] != INVALID_RESOURCE_ID; m_currentTupleBuffer2[2] = m_equalityManager.getNextEqual(m_currentTupleBuffer2[2])) {
                        TupleIndex denormalizedTupleIndex;
                        if (m_currentTupleBuffer1[0] == m_currentTupleBuffer2[0] && m_currentTupleBuffer1[1] == m_currentTupleBuffer2[1] && m_currentTupleBuffer1[2] == m_currentTupleBuffer2[2])
                            denormalizedTupleIndex = tupleIndex;
                        else
                            denormalizedTupleIndex = m_tripleTable.getTupleIndex(threadContext, m_currentTupleBuffer2, m_currentTupleArgumentIndexes);
                        const bool fromEDB = denormalizedTupleIndex != INVALID_TUPLE_INDEX && ((m_tripleTable.getTupleStatus(denormalizedTupleIndex) & TUPLE_STATUS_EDB) == TUPLE_STATUS_EDB);
                        const bool fromRederived = !fromEDB && isRederived(threadContext, m_currentTupleBuffer2);
                        if (fromEDB || fromRederived) {
                            if (denormalizedTupleIndex == INVALID_TUPLE_INDEX)
                                denormalizedTupleIndex = m_tripleTable.addTuple(threadContext, m_currentTupleBuffer2, m_currentTupleArgumentIndexes, 0, 0).second;
                            if (m_incrementalReasoningState.addGlobalFlags<multithreaded>(denormalizedTupleIndex, GF_ADDED_NEW))
                                m_incrementalReasoningState.getAddedList().template enqueue<multithreaded>(denormalizedTupleIndex);
                        }
                    }
                }
            }
            if (m_currentTupleBuffer1[1] == OWL_SAME_AS_ID) {
                assert(m_currentTupleBuffer1[0] == m_currentTupleBuffer1[2]);
                const ResourceID resourceID = m_currentTupleBuffer1[0];
                for (ResourceID denormalizedResourceID = resourceID; denormalizedResourceID != INVALID_RESOURCE_ID; denormalizedResourceID = m_equalityManager.getNextEqual(denormalizedResourceID)) {
                    for (ArgumentIndex argumentIndex = 0; argumentIndex < 3; ++argumentIndex) {
                        if (containsResource(threadContext, denormalizedResourceID, argumentIndex)) {
                            m_currentTupleBuffer3[0] = m_currentTupleBuffer3[2] = denormalizedResourceID;
                            m_currentTupleBuffer3[1] = OWL_SAME_AS_ID;
                            const TupleIndex denormalizedTupleIndex = m_tripleTable.addTuple(threadContext, m_currentTupleBuffer3, m_currentTupleArgumentIndexes, 0, 0).second;
                            if (m_incrementalReasoningState.addGlobalFlags<multithreaded>(denormalizedTupleIndex, GF_ADDED_NEW))
                                m_incrementalReasoningState.getAddedList().template enqueue<multithreaded>(denormalizedTupleIndex);
                            break;
                        }
                    }
                }
            }
        }
        else {
            if ((tupleStatus & TUPLE_STATUS_EDB) == TUPLE_STATUS_EDB || isRederived(threadContext, m_currentTupleBuffer1)) {
                if (m_incrementalReasoningState.addGlobalFlags<multithreaded>(tupleIndex, GF_ADDED_NEW))
                    m_incrementalReasoningState.getAddedList().template enqueue<multithreaded>(tupleIndex);
            }
        }
        if (callMonitor)
            m_incrementalMonitor->checkingProvabilityFinished(m_workerIndex);
    }
}

template<bool callMonitor, bool optimizeEquality, bool hasRules, bool multithreaded>
void DRedRederivationTaskWorker<callMonitor, optimizeEquality, hasRules, multithreaded>::stop() {
    ::atomicWrite(m_taskRunning, 0);
}
