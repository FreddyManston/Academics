// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef MATERIALIZATIONTASK_H_
#define MATERIALIZATIONTASK_H_

#include "AbstractMaterializationTask.h"

class TupleReceiver;
class MaterializationMonitor;
class BodyLiteralInfo;

// MaterializationTask

class MaterializationTask : public AbstractMaterializationTask {

protected:

    template<bool callMonitor, ReasoningModeType reasoningMode, bool mutithreaded>
    friend class MaterializationTaskWorker;

    ALIGN_TO_CACHE_LINE TupleIndex m_firstUnreservedTupleIndex;

    template<bool callMonitor>
    std::unique_ptr<ReasoningTaskWorker> doCreateWorker1(DatalogEngineWorker& datalogEngineWorker);

    template<bool callMonitor, ReasoningModeType reasoningMode>
    std::unique_ptr<ReasoningTaskWorker> doCreateWorker2(DatalogEngineWorker& datalogEngineWorker);

    virtual std::unique_ptr<ReasoningTaskWorker> doCreateWorker(DatalogEngineWorker& datalogEngineWorker);

    virtual void doInitialize();

public:

    MaterializationTask(DatalogEngine& datalogEngine, MaterializationMonitor* const materializationMonitor, const size_t componentLevel);

    __ALIGNED(MaterializationTask)

};

// MaterializationTaskWorker

template<bool callMonitor, ReasoningModeType reasoningMode, bool multithreaded>
class MaterializationTaskWorker : public AbstractMaterializationTaskWorker<MaterializationTaskWorker<callMonitor, reasoningMode, multithreaded> > {

protected:

    friend class MaterializationTask;

    typedef MaterializationTaskWorker<callMonitor, reasoningMode, multithreaded> MaterializationTaskWorkerType;
    
    TupleIndex& m_firstUnreservedTupleIndex;
    TupleIndex m_afterCurrentReadWindowTupleIndex;
    TupleIndex m_ruleReevaluationLimitTupleIndex;

public:

    static const bool s_callMonitor = callMonitor;
    static const ReasoningModeType s_reasoningMode = reasoningMode;
    static const bool s_multithreaded = multithreaded;

    MaterializationTaskWorker(MaterializationTask& materializationTask, DatalogEngineWorker& datalogEngineWorker);

    static bool isActiveAtomRewrite(MaterializationTaskWorkerType& target, const TupleIndex tupleIndex, const TupleStatus tupleStatus);

    void startRuleReevaluation();

    bool markMerged(const TupleIndex tupleIndex);
    
    bool addTuple(ThreadContext& threadContext, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes);

    bool tryApplyRecursiveRules(ThreadContext& threadContext);
    
    bool hasAllocatedWork();

    bool canAllocateMoreWork();

    virtual void run(ThreadContext& threadContext);

};

#endif /* MATERIALIZATIONTASK_H_ */
