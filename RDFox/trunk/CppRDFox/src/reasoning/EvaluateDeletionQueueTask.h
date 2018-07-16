// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef EVALUATEDELETIONQUEUETASK_H_
#define EVALUATEDELETIONQUEUETASK_H_

#include "../Common.h"
#include "../util/LockFreeQueue.h"
#include "ReasoningTask.h"

class DatalogEngine;
class DatalogEngineWorker;
class RuleIndex;
class ThreadContext;
class IncrementalMonitor;
class HeadAtomInfo;
class RuleInfo;
class IncrementalReasoningState;

// EvaluateDeletionQueueTask

class EvaluateDeletionQueueTask : public ReasoningTask {

protected:

    template<bool callMonitor, bool checkComponentLevel, bool multithreaded>
    friend class EvaluateDeletionQueueTaskWorker;

    IncrementalMonitor* const m_incrementalMonitor;
    IncrementalReasoningState& m_incrementalReasoningState;
    const bool m_checkComponentLevel;
    LockFreeQueue<RuleInfo*> m_ruleQueue;

    template<bool callMonitor>
    std::unique_ptr<ReasoningTaskWorker> doCreateWorker1(DatalogEngineWorker& datalogEngineWorker);

    template<bool callMonitor, bool checkComponentLevel>
    std::unique_ptr<ReasoningTaskWorker> doCreateWorker2(DatalogEngineWorker& datalogEngineWorker);

    virtual std::unique_ptr<ReasoningTaskWorker> doCreateWorker(DatalogEngineWorker& datalogEngineWorker);

    virtual void doInitialize();

public:

    EvaluateDeletionQueueTask(DatalogEngine& datalogEngine, IncrementalMonitor* const incrementalMonitor, IncrementalReasoningState& incrementalReasoningState, const bool checkComponentLevel);

};

// EvaluateDeletionQueueTaskWorker

template<bool callMonitor, bool checkComponentLevel, bool multithreaded>
class EvaluateDeletionQueueTaskWorker : public ReasoningTaskWorker {

protected:

    typedef EvaluateDeletionQueueTaskWorker<callMonitor, checkComponentLevel, multithreaded> EvaluateDeletionQueueTaskWorkerType;
    
    EvaluateDeletionQueueTask& m_evaluateDeletionQueueTask;
    IncrementalMonitor* const m_incrementalMonitor;
    const size_t m_workerIndex;
    LockFreeQueue<RuleInfo*>& m_ruleQueue;
    TupleTable& m_tripleTable;
    RuleIndex& m_ruleIndex;
    IncrementalReasoningState& m_incrementalReasoningState;
    int32_t m_taskRunning;

public:

    EvaluateDeletionQueueTaskWorker(EvaluateDeletionQueueTask& evaluateDeletionQueueTask, DatalogEngineWorker& datalogEngineWorker);

    always_inline size_t getWorkerIndex() {
        return m_workerIndex;
    }

    void deriveTuple(ThreadContext& threadContext, const HeadAtomInfo& headAtomInfo, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes);

    virtual void run(ThreadContext& threadContext);

    virtual void stop();

};

#endif /* EVALUATEDELETIONQUEUETASK_H_ */
