// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef DREDDELETIONTASK_H_
#define DREDDELETIONTASK_H_

#include "AbstractDeletionTask.h"

// DRedDeletionTask

class DRedDeletionTask : public AbstractDeletionTask {

protected:

    template<bool callMonitor, ReasoningModeType reasoningMode, bool hasRecursiveRules, bool multithreaded>
    friend class DRedDeletionTaskWorker;

    template<bool callMonitor>
    std::unique_ptr<ReasoningTaskWorker> doCreateWorker1(DatalogEngineWorker& datalogEngineWorker);
    
    template<bool callMonitor, ReasoningModeType reasoningMode>
    std::unique_ptr<ReasoningTaskWorker> doCreateWorker2(DatalogEngineWorker& datalogEngineWorker);

    template<bool callMonitor, ReasoningModeType reasoningMode, bool hasRecursiveRules>
    std::unique_ptr<ReasoningTaskWorker> doCreateWorker3(DatalogEngineWorker& datalogEngineWorker);

    virtual std::unique_ptr<ReasoningTaskWorker> doCreateWorker(DatalogEngineWorker& datalogEngineWorker);

public:

    DRedDeletionTask(DatalogEngine& datalogEngine, IncrementalMonitor* const incrementalMonitor, IncrementalReasoningState& incrementalReasoningState, const size_t componentLevel);

};

// DRedDeletionTaskWorker

template<bool callMonitor, ReasoningModeType reasoningMode, bool hasRecursiveRules, bool multithreaded>
class DRedDeletionTaskWorker : public AbstractDeletionTaskWorker<DRedDeletionTaskWorker<callMonitor, reasoningMode, hasRecursiveRules, multithreaded> > {

public:

    static const bool s_callMonitor = callMonitor;
    static const ReasoningModeType s_reasoningMode = reasoningMode;
    static const bool s_hasRecursiveRules = hasRecursiveRules;
    static const bool s_multithreaded = multithreaded;
    static const uint8_t s_numberOfReflexivityBits = 1;

    DRedDeletionTaskWorker(DRedDeletionTask& dredDeletionTask, DatalogEngineWorker& datalogEngineWorker);

    bool check(ThreadContext& threadContext, TupleIndex checkTupleIndex, const std::vector<ResourceID>* checkArgumentsBuffer, const std::vector<ArgumentIndex>* checkArgumentIndexes);

};

#endif /* DREDDELETIONTASK_H_ */
