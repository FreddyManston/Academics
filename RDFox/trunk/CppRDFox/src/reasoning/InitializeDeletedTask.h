// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef INITIALIZEDELETEDTASK_H_
#define INITIALIZEDELETEDTASK_H_

#include "../Common.h"
#include "ReasoningTask.h"

template<class T>
class LockFreeQueue;
class EqualityManager;
class RuleIndex;
class IncrementalReasoningState;

// InitializeDeletedTask

class InitializeDeletedTask : public ReasoningTask {

    template<bool optimizeEquality, bool checkComponentLevel, bool multithreaded>
    friend class InitializeDeletedTaskWorker;

protected:

    IncrementalReasoningState& m_incrementalReasoningState;
    LockFreeQueue<TupleIndex>& m_edbDeleteList;
    const bool m_checkComponentLevel;

    void notifyFactsDerived();

    template<bool optimizeEquality>
    std::unique_ptr<ReasoningTaskWorker> doCreateWorker1(DatalogEngineWorker& datalogEngineWorker);

    template<bool optimizeEquality, bool checkComponentLevel>
    std::unique_ptr<ReasoningTaskWorker> doCreateWorker2(DatalogEngineWorker& datalogEngineWorker);

    virtual std::unique_ptr<ReasoningTaskWorker> doCreateWorker(DatalogEngineWorker& datalogEngineWorker);

    virtual void doInitialize();

public:

    InitializeDeletedTask(DatalogEngine& datalogEngine, IncrementalReasoningState& incrementalReasoningState, LockFreeQueue<TupleIndex>& edbDeleteList, const bool checkComponentLevel);

};

// InitializeDeletedTaskWorker

template<bool optimizeEquality, bool checkComponentLevel, bool multithreaded>
class InitializeDeletedTaskWorker : public ReasoningTaskWorker {

protected:

    InitializeDeletedTask& m_initializeDeletedTask;
    const EqualityManager& m_equalityManager;
    TupleTable& m_tripleTable;
    RuleIndex& m_ruleIndex;
    IncrementalReasoningState& m_incrementalReasoningState;
    LockFreeQueue<TupleIndex>& m_edbDeleteList;
    int32_t m_taskRunning;
    std::vector<ResourceID> m_currentTupleBuffer;
    std::vector<ArgumentIndex> m_currentTupleArgumentIndexes;

public:

    InitializeDeletedTaskWorker(InitializeDeletedTask& initializeDeletedTask, DatalogEngineWorker& datalogEngineWorker);

    virtual void run(ThreadContext& threadContext);

    virtual void stop();

};

#endif /* INITIALIZEDELETEDTASK_H_ */
