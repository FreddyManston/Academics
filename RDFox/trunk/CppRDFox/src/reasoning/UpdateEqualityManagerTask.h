// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef UPDATEEQUALITYMANAGERTASK_H_
#define UPDATEEQUALITYMANAGERTASK_H_

#include "../Common.h"
#include "ReasoningTask.h"

class EqualityManager;
class DatalogEngine;
class DatalogEngineWorker;
class ThreadContext;
class IncrementalMonitor;
class IncrementalReasoningState;

// UpdateEqualityManagerTask

class UpdateEqualityManagerTask : public ReasoningTask {

public:

    enum UpdateType { COPY_CLASSES, UNREPRESENT, BREAK_EQUALS };

protected:

    template<bool callMonitor, bool multithreaded, UpdateType updateType>
    friend class UpdateEqualityManagerTaskWorker;

    IncrementalMonitor* const m_incrementalMonitor;
    IncrementalReasoningState& m_incrementalReasoningState;
    UpdateType m_updateType;

    template<bool callMonitor>
    std::unique_ptr<ReasoningTaskWorker> doCreateWorker1(DatalogEngineWorker& datalogEngineWorker);

    template<bool callMonitor, bool multithreaded>
    std::unique_ptr<ReasoningTaskWorker> doCreateWorker2(DatalogEngineWorker& datalogEngineWorker);
    
    virtual std::unique_ptr<ReasoningTaskWorker> doCreateWorker(DatalogEngineWorker& datalogEngineWorker);

    virtual void doInitialize();

public:


    UpdateEqualityManagerTask(DatalogEngine& datalogEngine, IncrementalMonitor* const incrementalMonitor, IncrementalReasoningState& incrementalReasoningState, const UpdateType updateType);

};

// UpdateEqualityManagerTaskWorker

template<bool callMonitor, bool multithreaded, UpdateEqualityManagerTask::UpdateType updateType>
class UpdateEqualityManagerTaskWorker : public ReasoningTaskWorker {

protected:

    UpdateEqualityManagerTask& m_updateEqualityManagerTask;
    IncrementalMonitor* const m_incrementalMonitor;
    const size_t m_workerIndex;
    TupleTable& m_tripleTable;
    IncrementalReasoningState& m_incrementalReasoningState;
    EqualityManager& m_equalityManager;
    EqualityManager& m_provingEqualityManager;
    int32_t m_taskRunning;
    std::vector<ResourceID> m_currentTupleBuffer;

public:

    UpdateEqualityManagerTaskWorker(UpdateEqualityManagerTask& updateEqualityManagerTask, DatalogEngineWorker& datalogEngineWorker);

    virtual void run(ThreadContext& threadContext);

    virtual void stop();

};

#endif /* UPDATEEQUALITYMANAGERTASK_H_ */
