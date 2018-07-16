// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef PROPAGATECHANGESPROVEDTASK_H_
#define PROPAGATECHANGESPROVEDTASK_H_

#include "../Common.h"
#include "ReasoningTask.h"

template<class T>
class LockFreeQueue;
class EqualityManager;
class DatalogEngine;
class DatalogEngineWorker;
class ThreadContext;
class IncrementalMonitor;
class IncrementalReasoningState;

// PropagateChangesTask

class PropagateChangesTask : public ReasoningTask {

protected:

    template<bool callMonitor, bool optimizeEquality, bool multithreaded>
    friend class PropagateChangesTaskWorker;

    const bool m_optimizeEquality;
    const bool m_byLevels;
    IncrementalMonitor* const m_incrementalMonitor;
    IncrementalReasoningState& m_incrementalReasoningState;

    template<bool callMonitor>
    std::unique_ptr<ReasoningTaskWorker> doCreateWorker1(DatalogEngineWorker& datalogEngineWorker);

    template<bool callMonitor, bool optimizeEquality>
    std::unique_ptr<ReasoningTaskWorker> doCreateWorker2(DatalogEngineWorker& datalogEngineWorker);

    virtual std::unique_ptr<ReasoningTaskWorker> doCreateWorker(DatalogEngineWorker& datalogEngineWorker);

    virtual void doInitialize();

public:

    PropagateChangesTask(DatalogEngine& datalogEngine, IncrementalMonitor* const incrementalMonitor, IncrementalReasoningState& incrementalReasoningState, const bool byLevels);

};

// PropagateChangesTaskWorker

template<bool callMonitor, bool optimizeEquality, bool multithreaded>
class PropagateChangesTaskWorker : public ReasoningTaskWorker {

protected:

    PropagateChangesTask& m_propagateChangesTask;
    IncrementalMonitor* const m_incrementalMonitor;
    const size_t m_workerIndex;
    TupleTable& m_tripleTable;
    EqualityManager& m_equalityManager;
    EqualityManager& m_provingEqualityManager;
    IncrementalReasoningState& m_incrementalReasoningState;
    int32_t m_taskRunning;

public:

    PropagateChangesTaskWorker(PropagateChangesTask& propagateChangesTask, DatalogEngineWorker& datalogEngineWorker);

    virtual void run(ThreadContext& threadContext);

    virtual void stop();

};

#endif /* PROPAGATECHANGESPROVEDTASK_H_ */
