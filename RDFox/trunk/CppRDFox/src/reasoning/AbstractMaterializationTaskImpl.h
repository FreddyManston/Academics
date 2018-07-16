// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef ABSTRACTMATERIALIZATIONTASKIMPL_H_
#define ABSTRACTMATERIALIZATIONTASKIMPL_H_

#include "../util/Vocabulary.h"
#include "../dictionary/Dictionary.h"
#include "../equality/EqualityManager.h"
#include "../storage/DataStore.h"
#include "../storage/TupleTable.h"
#include "RuleIndexImpl.h"
#include "DatalogEngine.h"
#include "DatalogEngineWorker.h"
#include "AbstractMaterializationTask.h"
#include "MaterializationMonitor.h"

// AbstractMaterializationTask

always_inline bool AbstractMaterializationTask::hasActiveProxies() const {
    return ::atomicRead(m_numberOfActiveProxies) > 0;
}

always_inline void AbstractMaterializationTask::waitForAllBeforeWritten(const TupleIndex currentTupleIndex) {
    if (hasActiveProxies()) {
        while (currentTupleIndex >= ::atomicRead(m_lowestWriteTupleIndex)) {
            TupleIndex newLowestWriteTupleIndex = m_tripleTable.getFirstFreeTupleIndex();
            for (std::vector<TupleTableProxy*>::iterator iterator = m_proxies.begin(); iterator != m_proxies.end(); ++iterator)
                newLowestWriteTupleIndex = (*iterator)->getLowerWriteTupleIndex(newLowestWriteTupleIndex);
            ::atomicWrite(m_lowestWriteTupleIndex, newLowestWriteTupleIndex);
        }
    }
}

always_inline void AbstractMaterializationTask::proxyDeactivated() {
    ::atomicDecrement(m_numberOfActiveProxies);
}

// AbstractMaterializationTaskWorker

template<class Derived>
AbstractMaterializationTaskWorker<Derived>::AbstractMaterializationTaskWorker(AbstractMaterializationTask& abstractMaterializationTask, DatalogEngineWorker& datalogEngineWorker) :
    m_materializationTask(abstractMaterializationTask),
    m_materializationMonitor(abstractMaterializationTask.m_materializationMonitor),
    m_componentLevel(abstractMaterializationTask.m_componentLevel),
    m_workerIndex(datalogEngineWorker.getWorkerIndex()),
    m_condition(),
    m_dictionary(m_materializationTask.getDatalogEngine().getDataStore().getDictionary()),
    m_equalityManager(const_cast<EqualityManager&>(m_materializationTask.getDatalogEngine().getDataStore().getEqualityManager())),
    m_tripleTable(m_materializationTask.getDatalogEngine().getDataStore().getTupleTable("internal$rdf")),
    m_ruleIndex(m_materializationTask.getDatalogEngine().getRuleIndex()),
    m_mergedConstants(m_materializationTask.m_mergedConstants),
    m_ruleQueue(m_materializationTask.m_ruleQueue),
    m_hasReflexiveSameAs(m_materializationTask.m_hasReflexiveSameAs),
    m_taskRunning(m_materializationTask.m_taskRunning),
    m_currentTupleIndex(INVALID_TUPLE_INDEX),
    m_currentTupleBuffer1(3, INVALID_RESOURCE_ID),
    m_currentTupleBuffer2(3, INVALID_RESOURCE_ID),
    m_currentTupleArgumentIndexes(),
    m_tripleTableProxy(),
    m_currentTupleReceiver(0),
    m_currentTupleReceiverIsProxy(false)
{
    m_currentTupleArgumentIndexes.push_back(0);
    m_currentTupleArgumentIndexes.push_back(1);
    m_currentTupleArgumentIndexes.push_back(2);
    ArgumentIndexSet allInputArguments;
    for (ArgumentIndex argumentIndex = 0; argumentIndex < 3; ++argumentIndex) {
        allInputArguments.clear();
        allInputArguments.add(argumentIndex);
        m_queriesForNormalizationSPO[argumentIndex] = m_tripleTable.createTupleIterator(m_currentTupleBuffer1, m_currentTupleArgumentIndexes, allInputArguments, allInputArguments, TUPLE_STATUS_COMPLETE, TUPLE_STATUS_COMPLETE);
    }
    // The proxy needs to be created before all workers start because checking whether a reservation is valid
    // actually involves touching proxies of all workers. Hence, if some proxy had not been initialized
    // but one of the workers checks whether reservation is valid, we'll get a null pointer exception.
    if (m_tripleTable.supportsProxy()) {
        m_tripleTableProxy = m_tripleTable.createTupleTableProxy(PROXY_WINDOW_SIZE);
        this->m_materializationTask.m_proxies.push_back(m_tripleTableProxy.get());
        m_tripleTableProxy->initialize();
        this->m_currentTupleReceiver = m_tripleTableProxy.get();
        m_currentTupleReceiverIsProxy = true;
        ++m_materializationTask.m_numberOfActiveProxies;
    }
    else {
        this->m_currentTupleReceiver = &this->m_tripleTable;
        m_currentTupleReceiverIsProxy = false;
    }
}

template<class Derived>
always_inline void AbstractMaterializationTaskWorker<Derived>::rewrite(ThreadContext& threadContext, const ResourceID resourceID1, const ResourceID resourceID2) {
    const DatatypeID resource1DatatypeID = m_dictionary.getDatatypeID(resourceID1);
    const DatatypeID resource2DatatypeID = m_dictionary.getDatatypeID(resourceID2);
    ResourceID sourceID;
    ResourceID targetID;
    bool clash;
    if (resource1DatatypeID == D_BLANK_NODE && resource2DatatypeID != D_BLANK_NODE) {
        sourceID = resourceID1;
        targetID = resourceID2;
        clash = false;
    }
    else if (resource1DatatypeID != D_BLANK_NODE && resource2DatatypeID == D_BLANK_NODE) {
        sourceID = resourceID2;
        targetID = resourceID1;
        clash = false;
    }
    else {
        // At this point, either both or none of the resources are blank nodes.
        // *DatatypeID > D_BLANK_NODE means that the resource is a literal; hence, if either resource is a literal,
        // then the other resource is not a blank node and so we've got a clash. In addition, we've got a clash if
        // we're using UNA and both resources are IRI references.
        clash = (resource1DatatypeID > D_BLANK_NODE) || (resource2DatatypeID > D_BLANK_NODE) || (Derived::s_reasoningMode == REASONING_MODE_EQUALITY_UNA && resource1DatatypeID == D_IRI_REFERENCE && resource2DatatypeID == D_IRI_REFERENCE);
        if (resourceID1 < resourceID2) {
            sourceID = resourceID2;
            targetID = resourceID1;
        }
        else {
            sourceID = resourceID1;
            targetID = resourceID2;
        }
    }
    if (m_equalityManager.merge<Derived::s_multithreaded>(sourceID, targetID)) {
        m_mergedConstants.enqueue<Derived::s_multithreaded>(sourceID);
        if (Derived::s_callMonitor)
            m_materializationMonitor->constantMerged(m_workerIndex, sourceID, targetID, true);
    }
    else {
        if (Derived::s_callMonitor)
            m_materializationMonitor->constantMerged(m_workerIndex, sourceID, targetID, false);
    }
    if (clash) {
        m_currentTupleBuffer2[0] = targetID;
        m_currentTupleBuffer2[1] = m_equalityManager.normalize(m_dictionary.resolveResource(RDF_TYPE, D_IRI_REFERENCE));
        m_currentTupleBuffer2[2] = m_equalityManager.normalize(m_dictionary.resolveResource(OWL_NOTHING, D_IRI_REFERENCE));
        static_cast<Derived*>(this)->addTuple(threadContext, m_currentTupleBuffer2, m_currentTupleArgumentIndexes);
    }
}

template<class Derived>
always_inline void AbstractMaterializationTaskWorker<Derived>::deriveTuple(ThreadContext& threadContext, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) {
    if (!::isEqualityReasoningMode(Derived::s_reasoningMode) || m_equalityManager.isNormal(argumentsBuffer, argumentIndexes)) {
        if (static_cast<Derived*>(this)->addTuple(threadContext, argumentsBuffer, argumentIndexes)) {
            notifyWaitingWorkers(true);
            if (Derived::s_callMonitor)
                m_materializationMonitor->tupleDerived(m_workerIndex, argumentsBuffer, argumentIndexes, true, true);
            if (::isEqualityReasoningMode(Derived::s_reasoningMode) && argumentsBuffer[argumentIndexes[1]] == OWL_SAME_AS_ID && argumentsBuffer[argumentIndexes[0]] != argumentsBuffer[argumentIndexes[2]])
                rewrite(threadContext, argumentsBuffer[argumentIndexes[0]], argumentsBuffer[argumentIndexes[2]]);
        }
        else {
            if (Derived::s_callMonitor)
                m_materializationMonitor->tupleDerived(m_workerIndex, argumentsBuffer, argumentIndexes, true, false);
        }
    }
    else {
        if (Derived::s_callMonitor)
            m_materializationMonitor->tupleDerived(m_workerIndex, argumentsBuffer, argumentIndexes, false, false);
    }
}

template<class Derived>
always_inline bool AbstractMaterializationTaskWorker<Derived>::tryReevaluateRule(ThreadContext& threadContext) {
    RuleInfo* ruleInfo;
    if (::isEqualityReasoningMode(Derived::s_reasoningMode) && (ruleInfo = m_ruleQueue.template dequeue<Derived::s_multithreaded>()) != nullptr) {
        if (Derived::s_callMonitor)
            m_materializationMonitor->ruleReevaluationStarted(m_workerIndex, *ruleInfo);
        ruleInfo->template evaluateRuleMain<Derived::s_reasoningMode == REASONING_MODE_NO_EQUALITY_BY_LEVELS, Derived::s_callMonitor>(threadContext, m_workerIndex, m_componentLevel, m_materializationMonitor,
            [this](ThreadContext& threadContext, const HeadAtomInfo& headAtomInfo, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) {
                deriveTuple(threadContext, argumentsBuffer, argumentIndexes);
            }
        );
        if (Derived::s_callMonitor)
            m_materializationMonitor->ruleReevaluationFinished(m_workerIndex);
        return true;
    }
    else
        return false;
}

template<class Derived>
always_inline bool AbstractMaterializationTaskWorker<Derived>::tryNormalizeConstants(ThreadContext& threadContext) {
    if (::isEqualityReasoningMode(Derived::s_reasoningMode)) {
        const ResourceID mergedID = m_mergedConstants.dequeue<Derived::s_multithreaded>();
        if (mergedID != INVALID_RESOURCE_ID) {
            if (Derived::s_callMonitor)
                m_materializationMonitor->normalizeConstantStarted(m_workerIndex, mergedID);
            bool notifyFactsDerived = false;
            for (size_t positionIndex = 0; positionIndex < 3; ++positionIndex) {
                TupleIterator& tupleIterator = *m_queriesForNormalizationSPO[positionIndex];
                m_currentTupleBuffer1[positionIndex] = mergedID;
                size_t multiplicity = tupleIterator.open();
                while (multiplicity != 0) {
                    TupleIndex tupleIndex;
                    TupleStatus tupleStatus;
                    // The query selects all triples containing mergedID, so we need to filter the triples accordingly
                    if (tupleIterator.getCurrentTupleInfo(tupleIndex, tupleStatus) && static_cast<Derived*>(this)->isActiveAtomRewrite(static_cast<Derived&>(*this), tupleIndex, tupleStatus)) {
                        // We must copy the tuple; otherwise, the following call to normalize() could override the value of mergedID, which would break the iterator.
                        if (m_equalityManager.normalize(m_currentTupleBuffer1, m_currentTupleBuffer2, m_currentTupleArgumentIndexes) && static_cast<Derived*>(this)->markMerged(tupleIterator.getCurrentTupleIndex()) && static_cast<Derived*>(this)->addTuple(threadContext, m_currentTupleBuffer2, m_currentTupleArgumentIndexes)) {
                            notifyFactsDerived = true;
                            if (Derived::s_callMonitor)
                                m_materializationMonitor->tupleNormalized(m_workerIndex, m_currentTupleBuffer1, m_currentTupleArgumentIndexes, m_currentTupleBuffer2, m_currentTupleArgumentIndexes, true);
                        }
                        else {
                            if (Derived::s_callMonitor)
                                m_materializationMonitor->tupleNormalized(m_workerIndex, m_currentTupleBuffer1, m_currentTupleArgumentIndexes, m_currentTupleBuffer2, m_currentTupleArgumentIndexes, false);
                        }
                    }
                    multiplicity = tupleIterator.advance();
                }
            }
            if (notifyFactsDerived)
                notifyWaitingWorkers(true);
            if (Derived::s_callMonitor)
                m_materializationMonitor->normalizeConstantFinished(m_workerIndex);
            return true;
        }
    }
    return false;
}

template<class Derived>
always_inline bool AbstractMaterializationTaskWorker<Derived>::noWorkerHasAllocatedWork() {
    for (unique_ptr_vector<ReasoningTaskWorker>::iterator iterator = m_materializationTask.m_workers.begin(); iterator != m_materializationTask.m_workers.end(); ++iterator)
        if (static_cast<Derived*>(iterator->get())->hasAllocatedWork())
            return false;
    return true;
}

template<class Derived>
always_inline void AbstractMaterializationTaskWorker<Derived>::notifyWaitingWorkers(const bool onlyThoseWithWork) {
    if (::atomicRead(m_materializationTask.m_numberOfWaitingWorkers) > 0) {
        const bool unlockAll = !onlyThoseWithWork || static_cast<Derived*>(this)->canAllocateMoreWork();
        for (unique_ptr_vector<ReasoningTaskWorker>::iterator iterator = m_materializationTask.m_workers.begin(); iterator != m_materializationTask.m_workers.end(); ++iterator) {
            if (unlockAll || static_cast<Derived*>(iterator->get())->hasAllocatedWork())
                static_cast<Derived*>(iterator->get())->m_condition.signalOne();
        }
    }
}

template<class Derived>
always_inline bool AbstractMaterializationTaskWorker<Derived>::enqueueRulesToReevaluate(uint64_t& nextMergedConstantPosition) {
    static_cast<Derived*>(this)->startRuleReevaluation();
    if (!m_ruleQueue.initializeLarge())
        throw RDF_STORE_EXCEPTION("Cannot initialize rule queue.");
    while (nextMergedConstantPosition < m_mergedConstants.getFirstFreePosition()) {
        const ResourceID mergedID = m_mergedConstants[nextMergedConstantPosition];
        m_ruleIndex.enqueueRulesToReevaluateMain<false>(mergedID, m_componentLevel, m_ruleQueue);
        ++nextMergedConstantPosition;
    }
    return m_ruleQueue.peekDequeue() != nullptr;
}

template<class Derived>
always_inline void AbstractMaterializationTaskWorker<Derived>::applyRecursiveRules(ThreadContext& threadContext) {
    uint64_t nextMergedConstantPosition = 0;
    while (::atomicRead(m_taskRunning)) {
        while (::atomicRead(m_taskRunning) && tryReevaluateRule(threadContext)) {
        }
        while (::atomicRead(m_taskRunning) && (tryNormalizeConstants(threadContext) || static_cast<Derived*>(this)->tryApplyRecursiveRules(threadContext))) {
        }
        MutexHolder mutexHolder(m_materializationTask.m_mutex);
        if (::atomicRead(m_taskRunning) && !static_cast<Derived*>(this)->hasAllocatedWork() && !static_cast<Derived*>(this)->canAllocateMoreWork()) {
            ::atomicIncrement(m_materializationTask.m_numberOfWaitingWorkers);
            if (m_materializationTask.m_numberOfWaitingWorkers == m_materializationTask.getTotalNumberOfWorkers()) {
                if (::isEqualityReasoningMode(Derived::s_reasoningMode) && enqueueRulesToReevaluate(nextMergedConstantPosition)) {
                    if (this->m_tripleTable.supportsProxy()) {
                        for (unique_ptr_vector<ReasoningTaskWorker>::iterator iterator = this->m_materializationTask.m_workers.begin(); iterator != this->m_materializationTask.m_workers.end(); ++iterator) {
                            Derived* worker = static_cast<Derived*>(iterator->get());
                            worker->m_currentTupleReceiver = worker->m_tripleTableProxy.get();
                            worker->m_currentTupleReceiverIsProxy = true;
                        }
                        m_materializationTask.m_numberOfActiveProxies = this->m_materializationTask.m_workers.size();
                    }
                }
                else if (noWorkerHasAllocatedWork())
                    ::atomicWrite(m_taskRunning, 0);
                notifyWaitingWorkers(m_taskRunning != 0);
            }
            else
                m_condition.wait(m_materializationTask.m_mutex);
            ::atomicDecrement(m_materializationTask.m_numberOfWaitingWorkers);
        }
    }
    if (m_tripleTable.supportsProxy()) {
        // The following line is redundant during materialization, but it is important for incremental addition.
        m_tripleTableProxy->invalidateRemainingBuffer(threadContext);
    }
}

template<class Derived>
void AbstractMaterializationTaskWorker<Derived>::stop() {
    MutexHolder mutexHolder(m_materializationTask.m_mutex);
    ::atomicWrite(m_taskRunning, 0);
    m_condition.signalOne();
}

#endif /* ABSTRACTMATERIALIZATIONTASKIMPL_H_ */
