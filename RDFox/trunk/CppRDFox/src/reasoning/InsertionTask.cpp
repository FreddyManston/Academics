// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "AbstractMaterializationTaskImpl.h"
#include "InsertionTask.h"
#include "IncrementalReasoningState.h"
#include "IncrementalMonitor.h"

// InsertionTask

template<bool callMonitor>
always_inline std::unique_ptr<ReasoningTaskWorker> InsertionTask::doCreateWorker1(DatalogEngineWorker& datalogEngineWorker) {
    switch (m_datalogEngine.getDataStore().getEqualityAxiomatizationType()) {
    case EQUALITY_AXIOMATIZATION_NO_UNA:
        return doCreateWorker2<callMonitor, REASONING_MODE_EQUALITY_NO_UNA>(datalogEngineWorker);
    case EQUALITY_AXIOMATIZATION_UNA:
        return doCreateWorker2<callMonitor, REASONING_MODE_EQUALITY_UNA>(datalogEngineWorker);
    case EQUALITY_AXIOMATIZATION_OFF:
        if (m_componentLevel == static_cast<size_t>(-1))
            return doCreateWorker2<callMonitor, REASONING_MODE_NO_EQUALITY_NO_LEVELS>(datalogEngineWorker);
        else
            return doCreateWorker2<callMonitor, REASONING_MODE_NO_EQUALITY_BY_LEVELS>(datalogEngineWorker);
    default:
        UNREACHABLE;
    }
}

template<bool callMonitor, ReasoningModeType reasoningMode>
always_inline std::unique_ptr<ReasoningTaskWorker> InsertionTask::doCreateWorker2(DatalogEngineWorker& datalogEngineWorker) {
    if (m_datalogEngine.getRuleIndex().hasRecursiveRules(m_componentLevel))
        return doCreateWorker3<callMonitor, reasoningMode, true>(datalogEngineWorker);
    else
        return doCreateWorker3<callMonitor, reasoningMode, false>(datalogEngineWorker);
}

template<bool callMonitor, ReasoningModeType reasoningMode, bool hasRecursiveRules>
always_inline std::unique_ptr<ReasoningTaskWorker> InsertionTask::doCreateWorker3(DatalogEngineWorker& datalogEngineWorker) {
    if (isMultithreaded())
        return std::unique_ptr<ReasoningTaskWorker>(new InsertionTaskWorker<callMonitor, reasoningMode, hasRecursiveRules, true>(*this, datalogEngineWorker));
    else
        return std::unique_ptr<ReasoningTaskWorker>(new InsertionTaskWorker<callMonitor, reasoningMode, hasRecursiveRules, false>(*this, datalogEngineWorker));
}

std::unique_ptr<ReasoningTaskWorker> InsertionTask::doCreateWorker(DatalogEngineWorker& datalogEngineWorker) {
    if (m_materializationMonitor == 0)
        return doCreateWorker1<false>(datalogEngineWorker);
    else
        return doCreateWorker1<true>(datalogEngineWorker);
}

void InsertionTask::doInitialize() {
    AbstractMaterializationTask::doInitialize();
    // The following test is needed because, when rules are added, the maximum component level changes; however, the new initially added lists are all empty, so the following is safe.
    if (m_componentLevel != static_cast<size_t>(-1))
        m_incrementalReasoningState.getAddedList().appendUnprocessed(m_incrementalReasoningState.getInitiallyAddedList(m_componentLevel));
    m_incrementalReasoningState.getAddedList().resetDequeuePosition();
    // If there are rules with negation, we need to reset the deleted list so that we can process D\A.
    if (m_datalogEngine.getRuleIndex().hasRulesWithNegation(m_componentLevel))
        m_incrementalReasoningState.getDeleteList().resetDequeuePosition();
}

InsertionTask::InsertionTask(DatalogEngine& datalogEngine, IncrementalMonitor* const incrementalMonitor, IncrementalReasoningState& incrementalReasoningState, const size_t componentLevel) :
    AbstractMaterializationTask(datalogEngine, incrementalMonitor, componentLevel),
    m_incrementalReasoningState(incrementalReasoningState)
{
}

// InsertionMaterializationTaskWorker

template<bool callMonitor, ReasoningModeType reasoningMode, bool hasRecursiveRules, bool multithreaded>
InsertionTaskWorker<callMonitor, reasoningMode, hasRecursiveRules, multithreaded>::InsertionTaskWorker(InsertionTask& insertionTask, DatalogEngineWorker& datalogEngineWorker) :
    AbstractMaterializationTaskWorker<InsertionTaskWorker<callMonitor, reasoningMode, hasRecursiveRules, multithreaded> >(insertionTask, datalogEngineWorker),
    m_insertionTask(insertionTask),
    m_incrementalReasoningState(m_insertionTask.m_incrementalReasoningState)
{
}

template<bool callMonitor, ReasoningModeType reasoningMode, bool hasRecursiveRules, bool multithreaded>
always_inline bool InsertionTaskWorker<callMonitor, reasoningMode, hasRecursiveRules, multithreaded>::isInIDA(InsertionTaskWorkerType& target, const TupleIndex tupleIndex, const TupleStatus tupleStatus) {
    const TupleFlags tupleFlags = target.m_incrementalReasoningState.getGlobalFlags(tupleIndex);
    return
        (((tupleStatus & (TUPLE_STATUS_IDB | TUPLE_STATUS_IDB_MERGED)) == TUPLE_STATUS_IDB) && ((tupleFlags & (GF_DELETED | GF_ADDED_MERGED)) == 0)) ||
        ((tupleFlags & (GF_ADDED | GF_ADDED_MERGED)) == GF_ADDED) ||
        (::isEqualityReasoningMode(reasoningMode) && ((target.m_incrementalReasoningState.getCurrentLevelFlags(tupleIndex) & (LF_PROVED | LF_PROVED_MERGED)) == LF_PROVED) && ((tupleFlags & GF_ADDED_MERGED) == 0));
}

template<bool callMonitor, ReasoningModeType reasoningMode, bool hasRecursiveRules, bool multithreaded>
always_inline bool InsertionTaskWorker<callMonitor, reasoningMode, hasRecursiveRules, multithreaded>::isActiveAtomRewrite(InsertionTaskWorkerType& target, const TupleIndex tupleIndex, const TupleStatus tupleStatus) {
    return isInIDA(target, tupleIndex, tupleStatus);
}

template<bool callMonitor, ReasoningModeType reasoningMode, bool hasRecursiveRules, bool multithreaded>
always_inline void InsertionTaskWorker<callMonitor, reasoningMode, hasRecursiveRules, multithreaded>::startRuleReevaluation() {
}

template<bool callMonitor, ReasoningModeType reasoningMode, bool hasRecursiveRules, bool multithreaded>
always_inline bool InsertionTaskWorker<callMonitor, reasoningMode, hasRecursiveRules, multithreaded>::markMerged(const TupleIndex tupleIndex) {
    // The order is here important for parallelisation. Essentially, we ensure that GF_ADDED_MERGED means that a tuple has been merged during insertion.
    // This must be done before removing the tuple from the actual database so that we can test this flag in isIDA.
    const bool result = m_incrementalReasoningState.addGlobalFlags<multithreaded>(tupleIndex, GF_ADDED_MERGED);
    this->m_tripleTable.deleteAddTupleStatus(tupleIndex, 0, 0, 0, TUPLE_STATUS_IDB, TUPLE_STATUS_IDB, TUPLE_STATUS_IDB_MERGED);
    return result;
}

template<bool callMonitor, ReasoningModeType reasoningMode, bool hasRecursiveRules, bool multithreaded>
always_inline bool InsertionTaskWorker<callMonitor, reasoningMode, hasRecursiveRules, multithreaded>::addTuple(ThreadContext& threadContext, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) {
    const TupleIndex tupleIndex = this->m_currentTupleReceiver->addTuple(threadContext, argumentsBuffer, argumentIndexes, 0, 0).second;
    if (m_incrementalReasoningState.addGlobalFlags<multithreaded>(tupleIndex, GF_ADDED_NEW)) {
        m_incrementalReasoningState.getAddedList().template enqueue<s_multithreaded>(tupleIndex);
        return true;
    }
    else
        return false;
}

template<bool callMonitor, ReasoningModeType reasoningMode, bool hasRecursiveRules, bool multithreaded>
always_inline void InsertionTaskWorker<callMonitor, reasoningMode, hasRecursiveRules, multithreaded>::applyPreviousLevelRulesPositive(ThreadContext& threadContext) {
    auto filter1 = this->m_ruleIndex.template setBodyLiteralInfoFilter<MAIN_POSITIVE_BEFORE_PIVOT>(*this, [](InsertionTaskWorkerType& target, const BodyLiteralInfo& bodyLiteralInfo, const TupleIndex tupleIndex, const TupleStatus tupleStatus) {
        // Condition is [(I\D)+A] \ [A\D] = I\(D\A).
        return
            ((tupleStatus & (TUPLE_STATUS_IDB | TUPLE_STATUS_IDB_MERGED)) == TUPLE_STATUS_IDB) &&
            ((target.m_incrementalReasoningState.getGlobalFlags(tupleIndex) & (GF_ADDED | GF_DELETED)) != GF_DELETED);
    });
    auto filter2 = this->m_ruleIndex.template setBodyLiteralInfoFilter<MAIN_POSITIVE_AFTER_PIVOT>(*this, [](InsertionTaskWorkerType& target, const BodyLiteralInfo& bodyLiteralInfo, const TupleIndex tupleIndex, const TupleStatus tupleStatus) {
        // Condition is (I\D)+A.
        return
            isInIDA(target, tupleIndex, tupleStatus);
    });
    // Pivot atoms are positive, and all negative atoms are "after" all positive atoms; therefore, we just need the "after pivot" filter for negative atoms here.
    auto filter3 = this->m_ruleIndex.template setBodyLiteralInfoFilter<MAIN_NEGATIVE_AT_PIVOT_OR_AFTER_PIVOT>(*this, [](InsertionTaskWorkerType& target, const BodyLiteralInfo& bodyLiteralInfo, const TupleIndex tupleIndex, const TupleStatus tupleStatus) {
        // Condition is (I\D)+A.
        return
            isInIDA(target, tupleIndex, tupleStatus);
    });

    const uint64_t afterLastAddedInPreviousLevels = m_incrementalReasoningState.getAddedListEnd(this->m_componentLevel - 1);
    while (::atomicRead(this->m_taskRunning) && (this->m_currentTupleIndex = m_incrementalReasoningState.getAddedList().template dequeue<s_multithreaded>(afterLastAddedInPreviousLevels)) != INVALID_TUPLE_INDEX) {
        if ((m_incrementalReasoningState.getGlobalFlags(this->m_currentTupleIndex) & (GF_ADDED | GF_ADDED_MERGED | GF_DELETED)) == GF_ADDED) {
            this->m_tripleTable.getStatusAndTuple(this->m_currentTupleIndex, this->m_currentTupleBuffer1);
            if (callMonitor)
                this->m_materializationMonitor->currentTupleExtracted(this->m_workerIndex, this->m_currentTupleBuffer1, this->m_currentTupleArgumentIndexes);
            this->m_ruleIndex.template applyRulesToPositiveLiteralMain<reasoningMode == REASONING_MODE_NO_EQUALITY_BY_LEVELS ? ALL_IN_COMPONENT : ALL_COMPONENTS, callMonitor>(threadContext, this->m_workerIndex, this->m_currentTupleBuffer1, this->m_currentTupleArgumentIndexes, this->m_componentLevel, this->m_materializationMonitor,
                [this](ThreadContext& threadContext, const HeadAtomInfo& headAtomInfo, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) {
                    this->deriveTuple(threadContext, argumentsBuffer, argumentIndexes);
                }
            );
            if (callMonitor)
                this->m_materializationMonitor->currentTupleProcessed(this->m_workerIndex);
        }
    }
}

template<bool callMonitor, ReasoningModeType reasoningMode, bool hasRecursiveRules, bool multithreaded>
always_inline void InsertionTaskWorker<callMonitor, reasoningMode, hasRecursiveRules, multithreaded>::applyPreviousLevelRulesNegative(ThreadContext& threadContext) {
    // Pivot atoms are negative, and all negative atoms are "after" all positive atoms; therefore, we just need the "before pivot" filter for positive atoms here.
    auto filter1 = this->m_ruleIndex.template setBodyLiteralInfoFilter<MAIN_POSITIVE_BEFORE_PIVOT>(*this, [](InsertionTaskWorkerType& target, const BodyLiteralInfo& bodyLiteralInfo, const TupleIndex tupleIndex, const TupleStatus tupleStatus) {
        // Condition is [(I\D)+A] \ [A\D] = I\(D\A).
        return
            ((tupleStatus & (TUPLE_STATUS_IDB | TUPLE_STATUS_IDB_MERGED)) == TUPLE_STATUS_IDB) &&
            ((target.m_incrementalReasoningState.getGlobalFlags(tupleIndex) & (GF_ADDED | GF_DELETED)) != GF_DELETED);
    });
    auto filter2 = this->m_ruleIndex.template setBodyLiteralInfoFilter<MAIN_NEGATIVE_SINGLE_ATOM_BEFORE_PIVOT>(*this, [](InsertionTaskWorkerType& target, const BodyLiteralInfo& bodyLiteralInfo, const TupleIndex tupleIndex, const TupleStatus tupleStatus) {
        // Condition is I + A.
        return
            ((tupleStatus & (TUPLE_STATUS_IDB | TUPLE_STATUS_IDB_MERGED)) == TUPLE_STATUS_IDB) ||
            ((target.m_incrementalReasoningState.getGlobalFlags(tupleIndex) & (GF_ADDED | GF_ADDED_MERGED)) == GF_ADDED);
    });
    auto filter3 = this->m_ruleIndex.template setBodyLiteralInfoFilter<MAIN_NEGATIVE_MULTIPLE_ATOMS_BEFORE_PIVOT>(*this, [](InsertionTaskWorkerType& target, const BodyLiteralInfo& bodyLiteralInfo, const TupleIndex tupleIndex, const TupleStatus tupleStatus) {
        // This check does not ensure the nonrepetition property! Nonrepetition would be ensured by checking on the last atom in the conjunction whether
        // - all conjuncts are in (I\D)+A, or
        // - all conjuncts are in I and at least one conjunct is in D\A.
        // Implementing this test is a pain because it is nonlocal, so we don't bother. Instead, we just check whether the atom is in (I\D)+A, which ensures correctness but not the nonrepetition property.
        return
            isInIDA(target, tupleIndex, tupleStatus);
    });
    auto filter4 = this->m_ruleIndex.template setBodyLiteralInfoFilter<MAIN_NEGATIVE_AT_PIVOT_OR_AFTER_PIVOT>(*this, [](InsertionTaskWorkerType& target, const BodyLiteralInfo& bodyLiteralInfo, const TupleIndex tupleIndex, const TupleStatus tupleStatus) {
        // Condition is (I\D)+A.
        return
            isInIDA(target, tupleIndex, tupleStatus);
    });
    auto filter5 = this->m_ruleIndex.template setUnderlyingLiteralInfoFilter<UNDERLYING_BEFORE_PIVOT>(*this, [](InsertionTaskWorkerType& target, const TupleIndex tupleIndex, const TupleStatus tupleStatus) {
        // The pivot is in D\A, so atoms before pivot are in I\(D\A).
        return
            ((tupleStatus & (TUPLE_STATUS_IDB | TUPLE_STATUS_IDB_MERGED)) == TUPLE_STATUS_IDB) &&
            ((target.m_incrementalReasoningState.getGlobalFlags(tupleIndex) & (GF_ADDED | GF_DELETED)) != GF_DELETED);
    });
    auto filter6 = this->m_ruleIndex.template setUnderlyingLiteralInfoFilter<UNDERLYING_AFTER_PIVOT>(*this, [](InsertionTaskWorkerType& target, const TupleIndex tupleIndex, const TupleStatus tupleStatus) {
        // The pivot is in D\A, so atoms before pivot are in I.
        return
            ((tupleStatus & (TUPLE_STATUS_IDB | TUPLE_STATUS_IDB_MERGED)) == TUPLE_STATUS_IDB);
    });

    while (::atomicRead(this->m_taskRunning) && (this->m_currentTupleIndex = m_incrementalReasoningState.getDeleteList().template dequeue<s_multithreaded>()) != INVALID_TUPLE_INDEX) {
        if ((m_incrementalReasoningState.getGlobalFlags(this->m_currentTupleIndex) & (GF_ADDED | GF_ADDED_MERGED | GF_DELETED)) == GF_DELETED) {
            this->m_tripleTable.getStatusAndTuple(this->m_currentTupleIndex, this->m_currentTupleBuffer1);
            if (callMonitor)
                this->m_materializationMonitor->currentTupleExtracted(this->m_workerIndex, this->m_currentTupleBuffer1, this->m_currentTupleArgumentIndexes);
            this->m_ruleIndex.template applyRulesToUnderlyingNegationLiteralMain<reasoningMode == REASONING_MODE_NO_EQUALITY_BY_LEVELS ? ALL_IN_COMPONENT : ALL_COMPONENTS, callMonitor>(threadContext, this->m_workerIndex, this->m_currentTupleBuffer1, this->m_currentTupleArgumentIndexes, this->m_componentLevel, this->m_materializationMonitor,
                [this](ThreadContext& threadContext, const HeadAtomInfo& headAtomInfo, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) {
                    this->deriveTuple(threadContext, argumentsBuffer, argumentIndexes);
                }
            );
            if (callMonitor)
                this->m_materializationMonitor->currentTupleProcessed(this->m_workerIndex);
        }
    }
}

template<bool callMonitor, ReasoningModeType reasoningMode, bool hasRecursiveRules, bool multithreaded>
always_inline bool InsertionTaskWorker<callMonitor, reasoningMode, hasRecursiveRules, multithreaded>::tryApplyRecursiveRules(ThreadContext& threadContext) {
    this->m_currentTupleIndex = m_incrementalReasoningState.getAddedList().template dequeue<s_multithreaded>();
    if (this->m_currentTupleIndex != INVALID_TUPLE_INDEX && !isInIDA(*this, this->m_currentTupleIndex, this->m_tripleTable.getStatusAndTuple(this->m_currentTupleIndex, this->m_currentTupleBuffer1))) {
        if (callMonitor)
            this->m_materializationMonitor->currentTupleExtracted(this->m_workerIndex, this->m_currentTupleBuffer1, this->m_currentTupleArgumentIndexes);
        if (::isEqualityReasoningMode(reasoningMode) && this->m_equalityManager.normalize(this->m_currentTupleBuffer1, this->m_currentTupleArgumentIndexes)) {
            markMerged(this->m_currentTupleIndex);
            if (addTuple(threadContext, this->m_currentTupleBuffer1, this->m_currentTupleArgumentIndexes)) {
                if (callMonitor)
                    this->m_materializationMonitor->currentTupleNormalized(this->m_workerIndex, this->m_currentTupleBuffer1, this->m_currentTupleArgumentIndexes, true);
            }
            else {
                if (callMonitor)
                    this->m_materializationMonitor->currentTupleNormalized(this->m_workerIndex, this->m_currentTupleBuffer1, this->m_currentTupleArgumentIndexes, false);
            }
        }
        else {
            if (m_incrementalReasoningState.addGlobalFlags<multithreaded>(this->m_currentTupleIndex, GF_ADDED)) {
                if (callMonitor)
                    static_cast<IncrementalMonitor*>(this->m_materializationMonitor)->insertedTupleAddedToIDB(this->m_workerIndex, this->m_currentTupleBuffer1, this->m_currentTupleArgumentIndexes, true);
            }
            else {
                if (callMonitor)
                    static_cast<IncrementalMonitor*>(this->m_materializationMonitor)->insertedTupleAddedToIDB(this->m_workerIndex, this->m_currentTupleBuffer1, this->m_currentTupleArgumentIndexes, false);
            }
            if (::isEqualityReasoningMode(reasoningMode) && this->m_currentTupleBuffer1[1] == OWL_SAME_AS_ID && this->m_currentTupleBuffer1[0] != this->m_currentTupleBuffer1[2])
                this->rewrite(threadContext, this->m_currentTupleBuffer1[0], this->m_currentTupleBuffer1[2]);
            else {
                if (::isEqualityReasoningMode(reasoningMode)) {
                    this->m_currentTupleBuffer2[1] = OWL_SAME_AS_ID;
                    for (size_t positionIndex = 0; positionIndex < 3; ++positionIndex) {
                        const ResourceID resourceID = this->m_currentTupleBuffer1[positionIndex];
                        if (this->m_hasReflexiveSameAs.template add<s_multithreaded>(resourceID)) {
                            this->m_currentTupleBuffer2[0] = this->m_currentTupleBuffer2[2] = resourceID;
                            if (addTuple(threadContext, this->m_currentTupleBuffer2, this->m_currentTupleArgumentIndexes)) {
                                if (callMonitor)
                                    this->m_materializationMonitor->reflexiveSameAsTupleDerived(this->m_workerIndex, resourceID, true);
                            }
                            else {
                                if (callMonitor)
                                    this->m_materializationMonitor->reflexiveSameAsTupleDerived(this->m_workerIndex, resourceID, false);
                            }
                        }
                    }
                }
                if (hasRecursiveRules)
                    this->m_ruleIndex.template applyRulesToPositiveLiteralMain<reasoningMode == REASONING_MODE_NO_EQUALITY_BY_LEVELS ? ALL_IN_COMPONENT : ALL_COMPONENTS, callMonitor>(threadContext, this->m_workerIndex, this->m_currentTupleBuffer1, this->m_currentTupleArgumentIndexes, this->m_componentLevel, this->m_materializationMonitor,
                        [this](ThreadContext& threadContext, const HeadAtomInfo& headAtomInfo, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) {
                            this->deriveTuple(threadContext, argumentsBuffer, argumentIndexes);
                        }
                    );
            }
        }
        if (callMonitor)
            this->m_materializationMonitor->currentTupleProcessed(this->m_workerIndex);
        return true;
    }
    return false;
}

template<bool callMonitor, ReasoningModeType reasoningMode, bool hasRecursiveRules, bool multithreaded>
always_inline bool InsertionTaskWorker<callMonitor, reasoningMode, hasRecursiveRules, multithreaded>::hasAllocatedWork() {
    return false;
}

template<bool callMonitor, ReasoningModeType reasoningMode, bool hasRecursiveRules, bool multithreaded>
always_inline bool InsertionTaskWorker<callMonitor, reasoningMode, hasRecursiveRules, multithreaded>::canAllocateMoreWork() {
    return this->m_ruleQueue.peekDequeue() != nullptr || this->m_mergedConstants.peekDequeue() != INVALID_RESOURCE_ID || m_incrementalReasoningState.getAddedList().peekDequeue() != INVALID_TUPLE_INDEX;
}

template<bool callMonitor, ReasoningModeType reasoningMode, bool hasRecursiveRules, bool multithreaded>
void InsertionTaskWorker<callMonitor, reasoningMode, hasRecursiveRules, multithreaded>::run(ThreadContext& threadContext) {
    if (callMonitor)
        static_cast<IncrementalMonitor*>(this->m_materializationMonitor)->insertionPreviousLevelsStarted(this->m_workerIndex);
    // Evalaute rules from the previous component.
    if (this->m_componentLevel != static_cast<size_t>(-1) && this->m_componentLevel > 0 && this->m_ruleIndex.hasRules(this->m_componentLevel)) {
        this->applyPreviousLevelRulesPositive(threadContext);
        if (this->m_ruleIndex.hasRulesWithNegation(this->m_componentLevel))
            this->applyPreviousLevelRulesNegative(threadContext);
    }
    if (callMonitor)
        static_cast<IncrementalMonitor*>(this->m_materializationMonitor)->insertionRecursiveStarted(this->m_workerIndex);
    // Then evaluate all remaining rules.
    auto filter1 = this->m_ruleIndex.template setBodyLiteralInfoFilter<MAIN_POSITIVE_BEFORE_PIVOT>(*this, [](InsertionTaskWorkerType& target, const BodyLiteralInfo& bodyLiteralInfo, const TupleIndex tupleIndex, const TupleStatus tupleStatus) {
        // Condition is (I\D)+A but without the current atom, which plays the role of \Delta_A.
        return
            (target.m_currentTupleIndex != tupleIndex) &&
            isInIDA(target, tupleIndex, tupleStatus);
    });
    auto filter2 = this->m_ruleIndex.template setBodyLiteralInfoFilter<MAIN_POSITIVE_AFTER_PIVOT>(*this, [](InsertionTaskWorkerType& target, const BodyLiteralInfo& bodyLiteralInfo, const TupleIndex tupleIndex, const TupleStatus tupleStatus) {
        // Condition is (I\D)+A.
        return
            isInIDA(target, tupleIndex, tupleStatus);
    });
    // Pivot atoms are positive, and all negative atoms are "after" all positive atoms; therefore, we just need the "after pivot" filter for negative atoms here.
    auto filter3 = this->m_ruleIndex.template setBodyLiteralInfoFilter<MAIN_NEGATIVE_AT_PIVOT_OR_AFTER_PIVOT>(*this, [](InsertionTaskWorkerType& target, const BodyLiteralInfo& bodyLiteralInfo, const TupleIndex tupleIndex, const TupleStatus tupleStatus) {
        // Condition is (I\D)+A.
        return
            isInIDA(target, tupleIndex, tupleStatus);
    });
    auto filters4 = this->m_ruleIndex.template setTupleIteratorFilters<MAIN_TUPLE_ITERATOR_FILTER>(*this, [](InsertionTaskWorkerType& target, const TupleIndex tupleIndex, const TupleStatus tupleStatus) {
        // Condition is (I\D)+A.
        return
            isInIDA(target, tupleIndex, tupleStatus);
    });
    this->applyRecursiveRules(threadContext);
    if (callMonitor)
        static_cast<IncrementalMonitor*>(this->m_materializationMonitor)->insertionFinished(this->m_workerIndex);
}
