// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef EVALUATEINSERTIONQUEUETASK_H_
#define EVALUATEINSERTIONQUEUETASK_H_

#include "../Common.h"
#include "ReasoningTask.h"

class DatalogEngine;
class DatalogEngineWorker;
class RuleIndex;
class ThreadContext;
class IncrementalMonitor;
class HeadAtomInfo;
class RuleInfo;
class IncrementalReasoningState;
template<typename E>
class LockFreeQueue;

// EvaluateInsertionQueueTask

class EvaluateInsertionQueueTask : public ReasoningTask {

protected:

    template<bool callMonitor, bool checkComponentLevel, bool optimizeEquality, bool multithreaded>
    friend class EvaluateInsertionQueueTaskWorker;

    IncrementalMonitor* const m_incrementalMonitor;
    IncrementalReasoningState& m_incrementalReasoningState;
    LockFreeQueue<RuleInfo*>& m_ruleQueue;
    const size_t m_componentLevel;

    template<bool callMonitor>
    std::unique_ptr<ReasoningTaskWorker> doCreateWorker1(DatalogEngineWorker& datalogEngineWorker);

    template<bool callMonitor, bool checkComponentLevel>
    std::unique_ptr<ReasoningTaskWorker> doCreateWorker2(DatalogEngineWorker& datalogEngineWorker);

    template<bool callMonitor, bool checkComponentLevel, bool optimizeEquality>
    std::unique_ptr<ReasoningTaskWorker> doCreateWorker3(DatalogEngineWorker& datalogEngineWorker);

    virtual std::unique_ptr<ReasoningTaskWorker> doCreateWorker(DatalogEngineWorker& datalogEngineWorker);

    virtual void doInitialize();

public:

    EvaluateInsertionQueueTask(DatalogEngine& datalogEngine, IncrementalMonitor* const incrementalMonitor, IncrementalReasoningState& incrementalReasoningState, LockFreeQueue<RuleInfo*>& ruleQueue, const size_t componentLevel);

};

// EvaluateInsertionQueueTaskWorker

template<bool callMonitor, bool checkComponentLevel, bool optimizeEquality, bool multithreaded>
class EvaluateInsertionQueueTaskWorker : public ReasoningTaskWorker {

protected:

    typedef EvaluateInsertionQueueTaskWorker<callMonitor, checkComponentLevel, optimizeEquality, multithreaded> EvaluateInsertionQueueTaskWorkerType;
    
    EvaluateInsertionQueueTask& m_evaluateInsertionQueueTask;
    IncrementalMonitor* const m_incrementalMonitor;
    const size_t m_workerIndex;
    TupleTable& m_tripleTable;
    RuleIndex& m_ruleIndex;
    IncrementalReasoningState& m_incrementalReasoningState;
    LockFreeQueue<RuleInfo*>& m_ruleQueue;
    int32_t m_taskRunning;

public:

    EvaluateInsertionQueueTaskWorker(EvaluateInsertionQueueTask& evaluateInsertionQueueTask, DatalogEngineWorker& datalogEngineWorker);

    always_inline size_t getWorkerIndex() {
        return m_workerIndex;
    }

    void deriveTuple(ThreadContext&, const std::vector<ResourceID>&, const std::vector<ArgumentIndex>&);

    virtual void run(ThreadContext& threadContext);

    virtual void stop();

};

#endif /* EVALUATEINSERTIONQUEUETASK_H_ */
