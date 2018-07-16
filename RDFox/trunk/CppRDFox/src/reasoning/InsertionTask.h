// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef INSERTIONTASK_H_
#define INSERTIONTASK_H_

#include "AbstractMaterializationTask.h"

class TupleReceiver;
class IncrementalReasoningState;
class IncrementalMonitor;
class BodyLiteralInfo;

// InsertionTask

class InsertionTask : public AbstractMaterializationTask {

protected:

    template<bool callMonitor, ReasoningModeType reasoningMode, bool hasRecursiveRules, bool mutithreaded>
    friend class InsertionTaskWorker;

    IncrementalReasoningState& m_incrementalReasoningState;

    template<bool callMonitor>
    std::unique_ptr<ReasoningTaskWorker> doCreateWorker1(DatalogEngineWorker& datalogEngineWorker);
    
    template<bool callMonitor, ReasoningModeType reasoningMode>
    std::unique_ptr<ReasoningTaskWorker> doCreateWorker2(DatalogEngineWorker& datalogEngineWorker);

    template<bool callMonitor, ReasoningModeType reasoningMode, bool hasRecursiveRules>
    std::unique_ptr<ReasoningTaskWorker> doCreateWorker3(DatalogEngineWorker& datalogEngineWorker);
    
    virtual std::unique_ptr<ReasoningTaskWorker> doCreateWorker(DatalogEngineWorker& datalogEngineWorker);

    void doInitialize();

public:

    InsertionTask(DatalogEngine& datalogEngine, IncrementalMonitor* const incrementalMonitor, IncrementalReasoningState& incrementalReasoningState, const size_t componentLevel);

    __ALIGNED(InsertionTask)

};

// InsertionTaskWorker

template<bool callMonitor, ReasoningModeType reasoningMode, bool hasRecursiveRules, bool multithreaded>
class InsertionTaskWorker : public AbstractMaterializationTaskWorker<InsertionTaskWorker<callMonitor, reasoningMode, hasRecursiveRules, multithreaded> > {

protected:

    InsertionTask& m_insertionTask;
    IncrementalReasoningState& m_incrementalReasoningState;

    typedef InsertionTaskWorker<callMonitor, reasoningMode, hasRecursiveRules, multithreaded> InsertionTaskWorkerType;
    
public:

    static const bool s_callMonitor = callMonitor;
    static const ReasoningModeType s_reasoningMode = reasoningMode;
    static const bool s_multithreaded = multithreaded;

    InsertionTaskWorker(InsertionTask& insertionTask, DatalogEngineWorker& datalogEngineWorker);

    static bool isInIDA(InsertionTaskWorkerType& target, const TupleIndex tupleIndex, const TupleStatus tupleStatus);
    
    static bool isActiveAtomRewrite(InsertionTaskWorkerType& target, const TupleIndex tupleIndex, const TupleStatus tupleStatus);

    void startRuleReevaluation();
    
    bool markMerged(const TupleIndex tupleIndex);

    bool addTuple(ThreadContext& threadContext, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes);

    void applyPreviousLevelRulesPositive(ThreadContext& threadContext);

    void applyPreviousLevelRulesNegative(ThreadContext& threadContext);

    bool tryApplyRecursiveRules(ThreadContext& threadContext);

    bool hasAllocatedWork();

    bool canAllocateMoreWork();
    
    virtual void run(ThreadContext& threadContext);

};

#endif /* INSERTIONTASK_H_ */
