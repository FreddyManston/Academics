// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef FBFDELETIONTASK_H_
#define FBFDELETIONTASK_H_

#include "../util/LockFreeQueue.h"
#include "AbstractDeletionTask.h"

class Dictionary;

// FBFDeletionTask

class FBFDeletionTask : public AbstractDeletionTask {

protected:

    LockFreeQueue<RuleInfo*> m_ruleQueue;

    template<bool callMonitor, ReasoningModeType reasoningMode, bool hasRecursiveRules>
    friend class FBFDeletionTaskWorker;

    template<bool callMonitor>
    std::unique_ptr<ReasoningTaskWorker> doCreateWorker1(DatalogEngineWorker& datalogEngineWorker);
    
    template<bool callMonitor, ReasoningModeType reasoningMode>
    std::unique_ptr<ReasoningTaskWorker> doCreateWorker2(DatalogEngineWorker& datalogEngineWorker);

    virtual std::unique_ptr<ReasoningTaskWorker> doCreateWorker(DatalogEngineWorker& datalogEngineWorker);

    virtual void doInitialize();

public:

    FBFDeletionTask(DatalogEngine& datalogEngine, IncrementalMonitor* const incrementalMonitor, IncrementalReasoningState& incrementalReasoningState, const size_t componentLevel);

};

// FBFDeletionTaskWorker

template<bool callMonitor, ReasoningModeType reasoningMode, bool hasRecursiveRules>
class FBFDeletionTaskWorker : public AbstractDeletionTaskWorker<FBFDeletionTaskWorker<callMonitor, reasoningMode, hasRecursiveRules> > {

protected:

    typedef FBFDeletionTaskWorker<callMonitor, reasoningMode, hasRecursiveRules> FBFDeletionTaskWorkerType;
    
    struct ReflexivityCheckHelper : private ::Unmovable {
        std::unique_ptr<ReflexivityCheckHelper> m_previous;
        std::vector<ResourceID> m_currentTupleBuffer;
        std::unique_ptr<TupleIterator> m_tupleIterator;

        size_t ensureOnTuple(const IncrementalReasoningState& incrementalReasoningState, size_t multiplicity);
        
        ReflexivityCheckHelper(TupleTable& tripleTable, const size_t positionIndex);

        size_t open(ThreadContext& threadContext, const IncrementalReasoningState& incrementalReasoningState);
        
        size_t advance(const IncrementalReasoningState& incrementalReasoningState);
        
    };

    struct StackFrame : private ::Unmovable {
        std::unique_ptr<StackFrame> m_previous;
        size_t m_returnAddress;
        TupleIndex m_tupleIndex;
        std::vector<ResourceID> m_currentTupleBuffer;
        bool m_allProved;
        size_t m_multiplicity;
        size_t m_currentPositionIndex;
        std::unique_ptr<ReflexivityCheckHelper> m_currentReflexivityCheckHelper;
        size_t m_currentIndexingPatternNumber;
        HeadAtomInfo* m_currentHeadAtomInfo;
        std::unique_ptr<SupportingFactsEvaluator> m_currentBodyMatches;
        size_t m_currentBodyIndex;

        StackFrame();

    };

    Dictionary& m_dictionary;
    EqualityManager& m_provingEqualityManager;
    LockFreeQueue<RuleInfo*>& m_ruleQueue;
    std::unique_ptr<TupleIterator> m_queriesForNormalizationSPO[3];
    std::unique_ptr<TupleIterator> m_queriesForEBDContainmentCheckSPO[3];
    TupleIndex m_currentTupleIndex2;
    std::vector<ResourceID> m_currentTupleBuffer4;
    std::vector<ResourceID> m_currentTupleBuffer5;
    std::vector<ResourceID> m_currentTupleBuffer6;
    std::vector<ResourceID> m_currentTupleBuffer7;
    std::vector<ResourceID> m_currentTupleBuffer8;
    std::vector<ResourceID> m_currentTupleBuffer9;
    std::unique_ptr<ReflexivityCheckHelper> m_reflexivityCheckHelperHeads[3];
    std::unique_ptr<StackFrame> m_usedStackFrames;
    std::vector<TupleIndex> m_checkedTuples;

    std::unique_ptr<ReflexivityCheckHelper> getReflexivityCheckHelper(const size_t positionIndex);

    void leaveReflexivityCheckHelper(std::unique_ptr<ReflexivityCheckHelper> reflexivityCheckHelper, const size_t positionIndex);

    std::unique_ptr<StackFrame> getStackFrame(const TupleIndex tupleIndex, const std::vector<ResourceID> argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, std::unique_ptr<StackFrame> previous);

    void leaveStackFrame(std::unique_ptr<StackFrame> stackFrame);

public:

    static const bool s_callMonitor = callMonitor;
    static const ReasoningModeType s_reasoningMode = reasoningMode;
    static const bool s_hasRecursiveRules = hasRecursiveRules;
    static const bool s_multithreaded = false;
    static const uint8_t s_numberOfReflexivityBits = 3;

    enum AddResult { ALREADY_EXISTS, ADDED, ALREADY_DELAYED, DELAYED };

    FBFDeletionTaskWorker(FBFDeletionTask& deletionTask, DatalogEngineWorker& datalogEngineWorker);

    ~FBFDeletionTaskWorker();

    size_t ensureOnRewriteTuple(TupleIterator& tupleIterator, size_t multiplicity);
    
    void rewrite(ThreadContext& threadContext, const ResourceID resourceID1, const ResourceID resourceID2);

    void proveNormalTuple(ThreadContext& threadContext, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes);

    void proveTuple(ThreadContext& threadContext, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes);

    AddResult addProvedTuple(ThreadContext& threadContext, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes);

    bool occursInEDB(ThreadContext& threadContext, const ResourceID resourceID);

    bool headAtomInfoEquals(HeadAtomInfo& headAtomInfo, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes);

    bool provedByNonrecursiveRule(ThreadContext& threadContext, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const std::vector<ResourceID>& normalizedArgumentsBuffer, const std::vector<ArgumentIndex>& normalizedArgumentIndexes);

    bool saturate(ThreadContext& threadContext, const TupleIndex tupleIndex, const std::vector<ResourceID>& currentTupleBuffer);

    bool allProved(ThreadContext& threadContext, const TupleIndex normalizedTupleIndex, const std::vector<ResourceID>& normalizedArgumentsBuffer, const std::vector<ArgumentIndex>& normalizedArgumentIndexes);

    bool allDisproved(ThreadContext& threadContext, const TupleIndex normalizedTupleIndex);
    
    bool allReflexiveSameAsProved(ThreadContext& threadContext, const ResourceID resourceID);
    
    bool check(ThreadContext& threadContext, TupleIndex checkTupleIndex, const std::vector<ResourceID>* checkArgumentsBuffer, const std::vector<ArgumentIndex>* checkArgumentIndexes);

    virtual void run(ThreadContext& threadContext);

};

#endif /* FBFDELETIONTASK_H_ */
