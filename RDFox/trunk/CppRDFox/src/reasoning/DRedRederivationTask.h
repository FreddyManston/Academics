// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef DREDREDERIVATIONTASK_H_
#define DREDREDERIVATIONTASK_H_

#include "../Common.h"
#include "ReasoningTask.h"

class DatalogEngine;
class DatalogEngineWorker;
class ThreadContext;
class TupleIterator;
class IncrementalMonitor;
class EqualityManager;
class RuleIndex;
class IncrementalReasoningState;

// DRedRederivationTask

class DRedRederivationTask : public ReasoningTask {

protected:

    template<bool callMonitor, bool optimizeEquality, bool hasRules, bool multithreaded>
    friend class DRedRederivationTaskWorker;

    IncrementalMonitor* const m_incrementalMonitor;
    IncrementalReasoningState& m_incrementalReasoningState;
    const size_t m_componentLevel;

    template<bool callMonitor>
    std::unique_ptr<ReasoningTaskWorker> doCreateWorker1(DatalogEngineWorker& datalogEngineWorker);

    template<bool callMonitor, bool optimizeEquality>
    std::unique_ptr<ReasoningTaskWorker> doCreateWorker2(DatalogEngineWorker& datalogEngineWorker);

    template<bool callMonitor, bool optimizeEquality, bool hasRules>
    std::unique_ptr<ReasoningTaskWorker> doCreateWorker3(DatalogEngineWorker& datalogEngineWorker);

    virtual std::unique_ptr<ReasoningTaskWorker> doCreateWorker(DatalogEngineWorker& datalogEngineWorker);

    virtual void doInitialize();

public:

    DRedRederivationTask(DatalogEngine& datalogEngine, IncrementalMonitor* const incrementalMonitor, IncrementalReasoningState& incrementalReasoningState, const size_t componentLevel);

};

// DRedRederivationTaskWorker

template<bool callMonitor, bool optimizeEquality, bool hasRules, bool multithreaded>
class DRedRederivationTaskWorker : public ReasoningTaskWorker {

protected:

    typedef DRedRederivationTaskWorker<callMonitor, optimizeEquality, hasRules, multithreaded> DRedRederivationTaskWorkerType;

    DRedRederivationTask& m_dredRederivationTask;
    IncrementalMonitor* const m_incrementalMonitor;
    const EqualityManager& m_equalityManager;
    const size_t m_workerIndex;
    TupleTable& m_tripleTable;
    RuleIndex& m_ruleIndex;
    IncrementalReasoningState& m_incrementalReasoningState;
    int32_t m_taskRunning;
    std::vector<ResourceID> m_currentTupleBuffer1;
    std::vector<ResourceID> m_currentTupleBuffer2;
    std::vector<ResourceID> m_currentTupleBuffer3;
    std::vector<ArgumentIndex> m_currentTupleArgumentIndexes;
    std::unique_ptr<TupleIterator> m_queriesForSameAsDerivation[3];

public:

    DRedRederivationTaskWorker(DRedRederivationTask& dredRederivationTask, DatalogEngineWorker& datalogEngineWorker);

    size_t getWorkerIndex();

    static bool isSupportingAtomPositive(DRedRederivationTaskWorkerType& target, const TupleIndex tupleIndex, const TupleStatus tupleStatus);

    bool containsResource(ThreadContext& threadContext, const ResourceID resourceID, const ArgumentIndex argumentIndex);

    bool isRederived(ThreadContext& threadContext, const std::vector<ResourceID>& currentTupleBuffer);

    virtual void run(ThreadContext& threadContext);

    virtual void stop();

};

#endif /* DREDREDERIVATIONTASK_H_ */
