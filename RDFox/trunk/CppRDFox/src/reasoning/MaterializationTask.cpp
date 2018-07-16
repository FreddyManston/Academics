// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "AbstractMaterializationTaskImpl.h"
#include "MaterializationTask.h"

// MaterializationTask

template<bool callMonitor>
always_inline std::unique_ptr<ReasoningTaskWorker> MaterializationTask::doCreateWorker1(DatalogEngineWorker& datalogEngineWorker) {
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
always_inline std::unique_ptr<ReasoningTaskWorker> MaterializationTask::doCreateWorker2(DatalogEngineWorker& datalogEngineWorker) {
    if (isMultithreaded())
        return std::unique_ptr<ReasoningTaskWorker>(new MaterializationTaskWorker<callMonitor, reasoningMode, true>(*this, datalogEngineWorker));
    else
        return std::unique_ptr<ReasoningTaskWorker>(new MaterializationTaskWorker<callMonitor, reasoningMode, false>(*this, datalogEngineWorker));
}

std::unique_ptr<ReasoningTaskWorker> MaterializationTask::doCreateWorker(DatalogEngineWorker& datalogEngineWorker) {
    if (m_materializationMonitor == 0)
        return doCreateWorker1<false>(datalogEngineWorker);
    else
        return doCreateWorker1<true>(datalogEngineWorker);
}

void MaterializationTask::doInitialize() {
    AbstractMaterializationTask::doInitialize();
    m_firstUnreservedTupleIndex = 1;
    RuleIndex& ruleIndex = m_datalogEngine.getRuleIndex();
    if (ruleIndex.hasPivotlessRules(m_componentLevel)) {
        if (!m_ruleQueue.initializeLarge())
            throw RDF_STORE_EXCEPTION("Cannot initialize rule queue.");
        if (m_componentLevel == static_cast<size_t>(-1))
            ruleIndex.enqueueRulesWithoutPositivePivot<false>(m_componentLevel, m_ruleQueue);
        else
            ruleIndex.enqueueRulesWithoutPositivePivot<true>(m_componentLevel, m_ruleQueue);
    }
}

MaterializationTask::MaterializationTask(DatalogEngine& datalogEngine, MaterializationMonitor* const materializationMonitor, const size_t componentLevel) :
    AbstractMaterializationTask(datalogEngine, materializationMonitor, componentLevel),
    m_firstUnreservedTupleIndex(INVALID_TUPLE_INDEX)
{
}

// MaterializationTaskWorker

template<bool callMonitor, ReasoningModeType reasoningMode, bool multithreaded>
MaterializationTaskWorker<callMonitor, reasoningMode, multithreaded>::MaterializationTaskWorker(MaterializationTask& materializationTask, DatalogEngineWorker& datalogEngineWorker) :
    AbstractMaterializationTaskWorker<MaterializationTaskWorker<callMonitor, reasoningMode, multithreaded> >(materializationTask, datalogEngineWorker),
    m_firstUnreservedTupleIndex(materializationTask.m_firstUnreservedTupleIndex),
    m_afterCurrentReadWindowTupleIndex(INVALID_TUPLE_INDEX),
    m_ruleReevaluationLimitTupleIndex(INVALID_TUPLE_INDEX)
{
}

template<bool callMonitor, ReasoningModeType reasoningMode, bool multithreaded>
always_inline bool MaterializationTaskWorker<callMonitor, reasoningMode, multithreaded>::isActiveAtomRewrite(MaterializationTaskWorkerType& target, const TupleIndex tupleIndex, const TupleStatus tupleStatus) {
    return (tupleStatus & (TUPLE_STATUS_IDB | TUPLE_STATUS_IDB_MERGED)) == TUPLE_STATUS_IDB;
}

template<bool callMonitor, ReasoningModeType reasoningMode, bool multithreaded>
always_inline void MaterializationTaskWorker<callMonitor, reasoningMode, multithreaded>::startRuleReevaluation() {
    m_ruleReevaluationLimitTupleIndex = ::atomicRead(m_firstUnreservedTupleIndex);
}

template<bool callMonitor, ReasoningModeType reasoningMode, bool multithreaded>
always_inline bool MaterializationTaskWorker<callMonitor, reasoningMode, multithreaded>::markMerged(const TupleIndex tupleIndex) {
    return this->m_tripleTable.deleteAddTupleStatus(tupleIndex, 0, 0, 0, TUPLE_STATUS_IDB, TUPLE_STATUS_IDB, TUPLE_STATUS_IDB_MERGED);
}

template<bool callMonitor, ReasoningModeType reasoningMode, bool multithreaded>
always_inline bool MaterializationTaskWorker<callMonitor, reasoningMode, multithreaded>::addTuple(ThreadContext& threadContext, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) {
    return this->m_currentTupleReceiver->addTuple(threadContext, argumentsBuffer, argumentIndexes, 0, TUPLE_STATUS_IDB).first;
}

template<bool callMonitor, ReasoningModeType reasoningMode, bool multithreaded>
always_inline bool MaterializationTaskWorker<callMonitor, reasoningMode, multithreaded>::tryApplyRecursiveRules(ThreadContext& threadContext) {
    // If the current tuple is outside the current read window, get another read window.
    if (this->m_currentTupleIndex >= m_afterCurrentReadWindowTupleIndex) {
        if (this->m_currentTupleReceiverIsProxy) {
            if (multithreaded) {
                do {
                    this->m_currentTupleIndex = ::atomicRead(m_firstUnreservedTupleIndex);
                    m_afterCurrentReadWindowTupleIndex = this->m_currentTupleIndex + PROXY_WINDOW_SIZE;
                } while (!::atomicConditionalSet(m_firstUnreservedTupleIndex, this->m_currentTupleIndex, m_afterCurrentReadWindowTupleIndex));
            }
            else {
                this->m_currentTupleIndex = m_firstUnreservedTupleIndex;
                m_firstUnreservedTupleIndex += PROXY_WINDOW_SIZE;
                m_afterCurrentReadWindowTupleIndex = m_firstUnreservedTupleIndex;
            }
        }
        else {
            if (multithreaded) {
                m_afterCurrentReadWindowTupleIndex = ::atomicIncrement(m_firstUnreservedTupleIndex);
                this->m_currentTupleIndex = m_afterCurrentReadWindowTupleIndex - 1;
            }
            else {
                this->m_currentTupleIndex = m_firstUnreservedTupleIndex++;
                m_afterCurrentReadWindowTupleIndex = m_firstUnreservedTupleIndex;
            }
        }
    }
    // If we are using the proxy, we may need to move the writing window if someone's reading window is inside this worker's writing window.
    if (this->m_currentTupleReceiverIsProxy) {
        const TupleIndex workersProxyWindowStartTupleIndex = this->m_tripleTableProxy->getFirstReservedTupleIndex();
        if (::atomicRead(m_firstUnreservedTupleIndex) > workersProxyWindowStartTupleIndex) {
            // If the move substantial? Noe thet first free tuple index should always be after this proxy's start index.
            assert(this->m_tripleTable.getFirstFreeTupleIndex() >= workersProxyWindowStartTupleIndex);
            const bool isMoveSubstantial = (this->m_tripleTable.getFirstFreeTupleIndex() >= workersProxyWindowStartTupleIndex + 3 * PROXY_WINDOW_SIZE * this->m_materializationTask.getTotalNumberOfWorkers()) ;
            // Are we in our own window?
            const bool isInOwnWriteWindow = (m_afterCurrentReadWindowTupleIndex >= workersProxyWindowStartTupleIndex);
            // Invalidate the window so as to unlock the other threads. This also moves the window
            // to the end of the list (in an empty state) so that we never have to deal with the
            // possibility of an empty window.
            this->m_tripleTableProxy->invalidateRemainingBuffer(threadContext);
            // If the move was not substantial or we are in our own window, then turn off the proxy.
            if (isInOwnWriteWindow || !isMoveSubstantial) {
                this->m_currentTupleReceiver = &this->m_tripleTable;
                this->m_currentTupleReceiverIsProxy = false;
                this->m_materializationTask.proxyDeactivated();
            }
        }
    }
    // Normal code path is to return a complete tuple; hence, we check whether we are outside
    // the triple table only if we get an incomplete tuple, as this saves time in common cases.
    TupleStatus tupleStatus = this->m_tripleTable.getStatusAndTupleIfComplete(this->m_currentTupleIndex, this->m_currentTupleBuffer1);
    if ((tupleStatus & TUPLE_STATUS_COMPLETE) != TUPLE_STATUS_COMPLETE) {
        if (this->m_currentTupleIndex >= this->m_tripleTable.getFirstFreeTupleIndex())
            return false;
        do {
            tupleStatus = this->m_tripleTable.getStatusAndTupleIfComplete(this->m_currentTupleIndex, this->m_currentTupleBuffer1);
        } while ((tupleStatus & TUPLE_STATUS_COMPLETE) != TUPLE_STATUS_COMPLETE);
    }
    // Wait until all tuples before the current one have been written. This must terminate eventually
    // because other threads will see that we are waiting. As an optimization, if there are no active
    // proxies, we know that all tuples are added to the end of the list so we can skip the loop.
    this->m_materializationTask.waitForAllBeforeWritten(this->m_currentTupleIndex);
    // Finally process the current tuple.
    if (this->m_currentTupleBuffer1[0] != INVALID_RESOURCE_ID && (tupleStatus & (TUPLE_STATUS_IDB | TUPLE_STATUS_IDB_MERGED)) == TUPLE_STATUS_IDB) {
        if (callMonitor)
            this->m_materializationMonitor->currentTupleExtracted(this->m_workerIndex, this->m_currentTupleBuffer1, this->m_currentTupleArgumentIndexes);
        if (::isEqualityReasoningMode(reasoningMode) && this->m_equalityManager.normalize(this->m_currentTupleBuffer1, this->m_currentTupleArgumentIndexes)) {
            this->markMerged(this->m_currentTupleIndex);
            if (addTuple(threadContext, this->m_currentTupleBuffer1, this->m_currentTupleArgumentIndexes)) {
                if (callMonitor)
                    this->m_materializationMonitor->currentTupleNormalized(this->m_workerIndex, this->m_currentTupleBuffer1, this->m_currentTupleArgumentIndexes, true);
            }
            else {
                if (callMonitor)
                    this->m_materializationMonitor->currentTupleNormalized(this->m_workerIndex, this->m_currentTupleBuffer1, this->m_currentTupleArgumentIndexes, false);
            }
        }
        else if (::isEqualityReasoningMode(reasoningMode) && this->m_currentTupleBuffer1[1] == OWL_SAME_AS_ID && this->m_currentTupleBuffer1[0] != this->m_currentTupleBuffer1[2])
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
            this->m_ruleIndex.template applyRulesToPositiveLiteralMain<reasoningMode == REASONING_MODE_NO_EQUALITY_BY_LEVELS ? WITH_PIVOT_IN_COMPONENT : ALL_COMPONENTS, callMonitor>(threadContext,this->m_workerIndex, this->m_currentTupleBuffer1, this->m_currentTupleArgumentIndexes, this->m_componentLevel, this->m_materializationMonitor,
                [this](ThreadContext& threadContext, const HeadAtomInfo& headAtomInfo, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) {
                    this->deriveTuple(threadContext, argumentsBuffer, argumentIndexes);
                }
            );
        }
        if (callMonitor)
            this->m_materializationMonitor->currentTupleProcessed(this->m_workerIndex);
    }
    ++this->m_currentTupleIndex;
    return true;
}

template<bool callMonitor, ReasoningModeType reasoningMode, bool multithreaded>
always_inline bool MaterializationTaskWorker<callMonitor, reasoningMode, multithreaded>::hasAllocatedWork() {
    return this->m_currentTupleIndex < this->m_tripleTable.getFirstFreeTupleIndex();
}

template<bool callMonitor, ReasoningModeType reasoningMode, bool multithreaded>
always_inline bool MaterializationTaskWorker<callMonitor, reasoningMode, multithreaded>::canAllocateMoreWork() {
    return this->m_ruleQueue.peekDequeue() != nullptr || this->m_mergedConstants.peekDequeue() != INVALID_RESOURCE_ID || ::atomicRead(m_firstUnreservedTupleIndex) < this->m_tripleTable.getFirstFreeTupleIndex();
}

template<bool callMonitor, ReasoningModeType reasoningMode, bool multithreaded>
void MaterializationTaskWorker<callMonitor, reasoningMode, multithreaded>::run(ThreadContext& threadContext) {
    if (callMonitor)
        this->m_materializationMonitor->materializationStarted(this->m_workerIndex);
    // Evalaute the pivotless rules first. The rules are enqueued in MaterializationTask::doInitialize(), and that is so because this operation
    // must be done on just one thread (whereas run() is called for each worker).
    if (this->m_ruleIndex.hasPivotlessRules(this->m_componentLevel)) {
        auto filters = this->m_ruleIndex.template setTupleIteratorFilters<MAIN_TUPLE_ITERATOR_FILTER>(*this, [](MaterializationTaskWorkerType& target, const TupleIndex tupleIndex, const TupleStatus tupleStatus) {
            return ((tupleStatus & (TUPLE_STATUS_IDB | TUPLE_STATUS_IDB_MERGED)) == TUPLE_STATUS_IDB);
        });

        RuleInfo* ruleInfo;
        while (::atomicRead(this->m_taskRunning) && (ruleInfo = this->m_ruleQueue.template dequeue<multithreaded>()) != nullptr) {
            if (callMonitor)
                this->m_materializationMonitor->pivotlessRuleEvaluationStarted(this->m_workerIndex, *ruleInfo);
            ruleInfo->template evaluateRuleMain<reasoningMode == REASONING_MODE_NO_EQUALITY_BY_LEVELS, callMonitor>(threadContext, this->m_workerIndex, this->m_componentLevel, this->m_materializationMonitor,
                [this](ThreadContext& threadContext, const HeadAtomInfo& headAtomInfo, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) {
                    this->deriveTuple(threadContext, argumentsBuffer, argumentIndexes);
                }
            );
            if (callMonitor)
                this->m_materializationMonitor->pivotlessRuleEvaluationFinished(this->m_workerIndex);
        }
    }
    // Then evaluate all remaining rules.
    auto filter1 = this->m_ruleIndex.template setBodyLiteralInfoFilter<MAIN_POSITIVE_BEFORE_PIVOT>(*this, [](MaterializationTaskWorkerType& target, const BodyLiteralInfo& bodyLiteralInfo, const TupleIndex tupleIndex, const TupleStatus tupleStatus) {
        return
            ((tupleStatus & (TUPLE_STATUS_IDB | TUPLE_STATUS_IDB_MERGED)) == TUPLE_STATUS_IDB) &&
            ((reasoningMode == REASONING_MODE_NO_EQUALITY_BY_LEVELS && bodyLiteralInfo.getComponentLevel() != target.m_componentLevel) || (tupleIndex < target.m_currentTupleIndex));
    });
    auto filter2 = this->m_ruleIndex.template setBodyLiteralInfoFilter<MAIN_POSITIVE_AFTER_PIVOT>(*this, [](MaterializationTaskWorkerType& target, const BodyLiteralInfo& bodyLiteralInfo, const TupleIndex tupleIndex, const TupleStatus tupleStatus) {
        return
            ((tupleStatus & (TUPLE_STATUS_IDB | TUPLE_STATUS_IDB_MERGED)) == TUPLE_STATUS_IDB) &&
            ((reasoningMode == REASONING_MODE_NO_EQUALITY_BY_LEVELS && bodyLiteralInfo.getComponentLevel() != target.m_componentLevel) || (tupleIndex <= target.m_currentTupleIndex));
    });
    // Pivot atoms are positive, and all negative atoms are "after" all positive atoms; therefore, we just need the "after pivot" filter for negative atoms here.
    auto filter3 = this->m_ruleIndex.template setBodyLiteralInfoFilter<MAIN_NEGATIVE_AT_PIVOT_OR_AFTER_PIVOT>(*this, [](MaterializationTaskWorkerType& target, const BodyLiteralInfo& bodyLiteralInfo, const TupleIndex tupleIndex, const TupleStatus tupleStatus) {
        return
            ((tupleStatus & (TUPLE_STATUS_IDB | TUPLE_STATUS_IDB_MERGED)) == TUPLE_STATUS_IDB);
    });
    auto filters4 = this->m_ruleIndex.template setTupleIteratorFilters<MAIN_TUPLE_ITERATOR_FILTER>(*this, [](MaterializationTaskWorkerType& target, const TupleIndex tupleIndex, const TupleStatus tupleStatus) {
        return
            ((tupleStatus & (TUPLE_STATUS_IDB | TUPLE_STATUS_IDB_MERGED)) == TUPLE_STATUS_IDB) &&
            (tupleIndex <= target.m_ruleReevaluationLimitTupleIndex);
    });
    this->applyRecursiveRules(threadContext);
    if (callMonitor)
        this->m_materializationMonitor->materializationFinished(this->m_workerIndex);
}
