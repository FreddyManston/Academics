// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef ABSTRACTMATERIALIZATIONTASK_H_
#define ABSTRACTMATERIALIZATIONTASK_H_

#include "../Common.h"
#include "../storage/TupleTableProxy.h"
#include "../util/Mutex.h"
#include "../util/Condition.h"
#include "../util/LockFreeQueue.h"
#include "../util/LockFreeBitSet.h"
#include "ReasoningTask.h"

class DatalogEngine;
class DatalogEngineWorker;
class Dictionary;
class EqualityManager;
class ThreadContext;
class MaterializationMonitor;
class HeadAtomInfo;
class RuleInfo;

// AbstractMaterializationTask

class AbstractMaterializationTask : public ReasoningTask {

protected:

    template<class Derived>
    friend class AbstractMaterializationTaskWorker;

    MaterializationMonitor* const m_materializationMonitor;
    const size_t m_componentLevel;
    Mutex m_mutex;
    TupleTable& m_tripleTable;
    std::vector<TupleTableProxy*> m_proxies;
    LockFreeQueue<ResourceID> m_mergedConstants;
    LockFreeQueue<RuleInfo*> m_ruleQueue;
    LockFreeBitSet m_hasReflexiveSameAs;
    aligned_int32_t m_taskRunning;
    aligned_size_t m_numberOfWaitingWorkers;
    aligned_size_t m_numberOfActiveProxies;
    aligned_TupleIndex m_lowestWriteTupleIndex;

    virtual void doInitialize();

public:

    AbstractMaterializationTask(DatalogEngine& datalogEngine, MaterializationMonitor* const materializationMonitor, const size_t componentLevel);

    bool hasActiveProxies() const;

    void waitForAllBeforeWritten(const TupleIndex currentTupleIndex);

    void proxyDeactivated();

};

// AbstractMaterializationTaskWorker

template<class Derived>
class AbstractMaterializationTaskWorker : public ReasoningTaskWorker {

protected:

    AbstractMaterializationTask& m_materializationTask;
    MaterializationMonitor* const m_materializationMonitor;
    const size_t m_componentLevel;
    const size_t m_workerIndex;
    Condition m_condition;
    Dictionary& m_dictionary;
    EqualityManager& m_equalityManager;
    TupleTable& m_tripleTable;
    RuleIndex& m_ruleIndex;
    LockFreeQueue<ResourceID>& m_mergedConstants;
    LockFreeQueue<RuleInfo*>& m_ruleQueue;
    LockFreeBitSet& m_hasReflexiveSameAs;
    int32_t& m_taskRunning;
    std::unique_ptr<TupleIterator> m_queriesForNormalizationSPO[3];
    TupleIndex m_currentTupleIndex;
    TupleIndex m_afterLastReservedTupleIndex;
    std::vector<ResourceID> m_currentTupleBuffer1;
    std::vector<ResourceID> m_currentTupleBuffer2;
    std::vector<ArgumentIndex> m_currentTupleArgumentIndexes;
    std::unique_ptr<TupleTableProxy> m_tripleTableProxy;
    TupleReceiver* m_currentTupleReceiver;
    bool m_currentTupleReceiverIsProxy;

public:

    AbstractMaterializationTaskWorker(AbstractMaterializationTask& abstractMaterializationTask, DatalogEngineWorker& datalogEngineWorker);

    always_inline size_t getWorkerIndex() {
        return m_workerIndex;
    }

    void rewrite(ThreadContext& threadContext, const ResourceID resourceID1, const ResourceID resourceID2);

    void deriveTuple(ThreadContext& threadContext, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes);

    bool tryReevaluateRule(ThreadContext& threadContext);

    bool tryNormalizeConstants(ThreadContext& threadContext);

    bool noWorkerHasAllocatedWork();

    void notifyWaitingWorkers(const bool onlyThoseWithWork);

    bool enqueueRulesToReevaluate(uint64_t& nextMergedConstantPosition);

    void applyRecursiveRules(ThreadContext& threadContext);

    virtual void stop();

};

#endif /* ABSTRACTMATERIALIZATIONTASK_H_ */
