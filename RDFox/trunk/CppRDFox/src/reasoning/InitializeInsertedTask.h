// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef INITIALIZEINSERTEDTASK_H_
#define INITIALIZEINSERTEDTASK_H_

#include "../Common.h"
#include "../util/Mutex.h"
#include "../util/Condition.h"
#include "ReasoningTask.h"

template<class T>
class LockFreeQueue;
class RuleIndex;
class IncrementalReasoningState;

// InitializeInsertedTask

class InitializeInsertedTask : public ReasoningTask {

protected:

    template<bool checkComponentLevel, bool multithreaded>
    friend class InitializeInsertedTaskWorker;

    IncrementalReasoningState& m_incrementalReasoningState;
    LockFreeQueue<TupleIndex>& m_edbInsertList;
    const bool m_checkComponentLevel;

    template<bool checkComponentLevel>
    std::unique_ptr<ReasoningTaskWorker> doCreateWorker1(DatalogEngineWorker& datalogEngineWorker);

    virtual std::unique_ptr<ReasoningTaskWorker> doCreateWorker(DatalogEngineWorker& datalogEngineWorker);

    virtual void doInitialize();

public:

    InitializeInsertedTask(DatalogEngine& datalogEngine, IncrementalReasoningState& incrementalReasoningState, LockFreeQueue<TupleIndex>& edbInsertList, const bool checkComponentLevel);

};

// InitializeInsertedTaskWorker

template<bool checkComponentLevel, bool multithreaded>
class InitializeInsertedTaskWorker : public ReasoningTaskWorker {

protected:

    InitializeInsertedTask& m_initializeInsertedTask;
    TupleTable& m_tripleTable;
    RuleIndex& m_ruleIndex;
    IncrementalReasoningState& m_incrementalReasoningState;
    LockFreeQueue<TupleIndex>& m_edbInsertList;
    int32_t m_taskRunning;
    std::vector<ResourceID> m_currentTupleBuffer;
    std::vector<ArgumentIndex> m_currentTupleArgumentIndexes;

public:

    InitializeInsertedTaskWorker(InitializeInsertedTask& initializeInsertedTask, DatalogEngineWorker& datalogEngineWorker);

    virtual void run(ThreadContext& threadContext);

    virtual void stop();

};

#endif /* INITIALIZEINSERTEDTASK_H_ */
