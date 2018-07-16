// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef ABSTRACTDELETIONTASK_H_
#define ABSTRACTDELETIONTASK_H_

#include "../Common.h"
#include "../util/LockFreeBitSet.h"
#include "ReasoningTask.h"

class EqualityManager;
class DatalogEngine;
class DatalogEngineWorker;
class ThreadContext;
class TupleIterator;
class IncrementalMonitor;
class BodyLiteralInfo;
class HeadAtomInfo;
class RuleInfo;
class IncrementalReasoningState;

// AbstractDeletionTask

class AbstractDeletionTask : public ReasoningTask {

protected:

    template<class Derived>
    friend class AbstractDeletionTaskWorker;

    IncrementalMonitor* const m_incrementalMonitor;
    IncrementalReasoningState& m_incrementalReasoningState;
    const size_t m_componentLevel;
    LockFreeBitSet m_reflexiveSameAsStatus;
    uint64_t m_afterLastDeletedInPreviousLevels;

    virtual void doInitialize();

public:

    AbstractDeletionTask(DatalogEngine& datalogEngine, IncrementalMonitor* const incrementalMonitor, IncrementalReasoningState& incrementalReasoningState, const size_t componentLevel);

};

// AbstractDeletionTaskWorker

template<class Derived>
class AbstractDeletionTaskWorker : public ReasoningTaskWorker {

protected:

    typedef AbstractDeletionTaskWorker<Derived> AbstractDeletionTaskWorkerType;

    AbstractDeletionTask& m_deletionTask;
    IncrementalMonitor* const m_incrementalMonitor;
    const size_t m_workerIndex;
    DataStore& m_dataStore;
    const EqualityManager& m_equalityManager;
    TupleTable& m_tripleTable;
    RuleIndex& m_ruleIndex;
    IncrementalReasoningState& m_incrementalReasoningState;
    LockFreeBitSet& m_reflexiveSameAsStatus;
    int32_t m_taskRunning;
    std::unique_ptr<TupleIterator> m_queriesForDeletionPropagationViaReplacementSPO[3];
    TupleIndex m_currentTupleIndex;
    std::vector<ResourceID> m_currentTupleBuffer1;
    std::vector<ResourceID> m_currentTupleBuffer2;
    std::vector<ResourceID> m_currentTupleBuffer3;
    std::vector<ArgumentIndex> m_currentTupleArgumentIndexes;
    const size_t m_componentLevel;

    template<uint8_t reflexivityType>
    bool checkReflexivity(const ResourceID resourceID);

public:

    AbstractDeletionTaskWorker(AbstractDeletionTask& deletionTask, DatalogEngineWorker& datalogEngineWorker);

    size_t getWorkerIndex();

    bool isSingleton(const ResourceID resourceID);

    void deleteTuple(ThreadContext& threadContext,  const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes);

    void applyPreviousLevelDelRulesPositive(ThreadContext& threadContext);

    void applyPreviousLevelDelRulesNegative(ThreadContext& threadContext);

    void applyRecursiveDelRules(ThreadContext& threadContext);

    virtual void run(ThreadContext& threadContext);

    virtual void stop();

};

#endif /* ABSTRACTDELETIONTASK_H_ */
