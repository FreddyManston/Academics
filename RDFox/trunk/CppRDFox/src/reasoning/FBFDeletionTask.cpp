// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../dictionary/Dictionary.h"
#include "AbstractDeletionTaskImpl.h"
#include "FBFDeletionTask.h"

// FBFDeletionTask

template<bool callMonitor>
always_inline std::unique_ptr<ReasoningTaskWorker> FBFDeletionTask::doCreateWorker1(DatalogEngineWorker& datalogEngineWorker) {
    switch (m_datalogEngine.getDataStore().getEqualityAxiomatizationType()) {
    case EQUALITY_AXIOMATIZATION_NO_UNA:
        return doCreateWorker2<callMonitor, REASONING_MODE_EQUALITY_NO_UNA>(datalogEngineWorker);
    case EQUALITY_AXIOMATIZATION_UNA:
        return doCreateWorker2<callMonitor, REASONING_MODE_EQUALITY_UNA>(datalogEngineWorker);
    case EQUALITY_AXIOMATIZATION_OFF:
        if (m_componentLevel == static_cast<size_t>(-1))
            return doCreateWorker2<callMonitor, REASONING_MODE_NO_EQUALITY_NO_LEVELS>(datalogEngineWorker);
        else
            return doCreateWorker2<callMonitor, REASONING_MODE_NO_EQUALITY_BY_LEVELS>(datalogEngineWorker);
    default:
        UNREACHABLE;
    }
}

template<bool callMonitor, ReasoningModeType reasoningMode>
always_inline std::unique_ptr<ReasoningTaskWorker> FBFDeletionTask::doCreateWorker2(DatalogEngineWorker& datalogEngineWorker) {
    if (m_datalogEngine.getRuleIndex().hasRecursiveRules(m_componentLevel))
        return std::unique_ptr<ReasoningTaskWorker>(new FBFDeletionTaskWorker<callMonitor, reasoningMode, true>(*this, datalogEngineWorker));
    else
        return std::unique_ptr<ReasoningTaskWorker>(new FBFDeletionTaskWorker<callMonitor, reasoningMode, false>(*this, datalogEngineWorker));
}

std::unique_ptr<ReasoningTaskWorker> FBFDeletionTask::doCreateWorker(DatalogEngineWorker& datalogEngineWorker) {
    if (m_incrementalMonitor == nullptr)
        return doCreateWorker1<false>(datalogEngineWorker);
    else
        return doCreateWorker1<true>(datalogEngineWorker);
}

void FBFDeletionTask::doInitialize() {
    AbstractDeletionTask::doInitialize();
    m_datalogEngine.getRuleIndex().resetProving();
    m_datalogEngine.getRuleIndex().forgetTemporaryConstants();
}

FBFDeletionTask::FBFDeletionTask(DatalogEngine& datalogEngine, IncrementalMonitor* const incrementalMonitor, IncrementalReasoningState& incrementalReasoningState, const size_t componentLevel) :
    AbstractDeletionTask(datalogEngine, incrementalMonitor, incrementalReasoningState, componentLevel),
    m_ruleQueue(m_datalogEngine.getDataStore().getMemoryManager())
{
}

// FBFDeletionTaskWorker::ReflexivityCheckHelper

static const std::vector<ArgumentIndex> s_reflexivityCheckHelperArgumentIndexes{0, 1, 2};
static const ArgumentIndexSet s_reflexivityCheckHelperBoundArguments[3]{
    { 0 },
    { 1 },
    { 2 }
};

template<bool callMonitor, ReasoningModeType reasoningMode, bool hasRecursiveRules>
always_inline size_t FBFDeletionTaskWorker<callMonitor, reasoningMode, hasRecursiveRules>::ReflexivityCheckHelper::ensureOnTuple(const IncrementalReasoningState& incrementalReasoningState, size_t multiplicity) {
    while (multiplicity != 0 && ((incrementalReasoningState.getGlobalFlags(m_tupleIterator->getCurrentTupleIndex()) & GF_DISPROVED) == GF_DISPROVED))
        multiplicity = m_tupleIterator->advance();
    return multiplicity;
}

template<bool callMonitor, ReasoningModeType reasoningMode, bool hasRecursiveRules>
always_inline FBFDeletionTaskWorker<callMonitor, reasoningMode, hasRecursiveRules>::ReflexivityCheckHelper::ReflexivityCheckHelper(TupleTable& tripleTable, const size_t positionIndex) :
    m_previous(),
    m_currentTupleBuffer(3, INVALID_RESOURCE_ID),
    m_tupleIterator(tripleTable.createTupleIterator(m_currentTupleBuffer, s_reflexivityCheckHelperArgumentIndexes, s_reflexivityCheckHelperBoundArguments[positionIndex], s_reflexivityCheckHelperBoundArguments[positionIndex], TUPLE_STATUS_COMPLETE | TUPLE_STATUS_IDB | TUPLE_STATUS_IDB_MERGED, TUPLE_STATUS_COMPLETE | TUPLE_STATUS_IDB))
{
}

template<bool callMonitor, ReasoningModeType reasoningMode, bool hasRecursiveRules>
always_inline size_t FBFDeletionTaskWorker<callMonitor, reasoningMode, hasRecursiveRules>::ReflexivityCheckHelper::open(ThreadContext& threadContext, const IncrementalReasoningState &incrementalReasoningState) {
    return ensureOnTuple(incrementalReasoningState, m_tupleIterator->open(threadContext));
}

template<bool callMonitor, ReasoningModeType reasoningMode, bool hasRecursiveRules>
always_inline size_t FBFDeletionTaskWorker<callMonitor, reasoningMode, hasRecursiveRules>::ReflexivityCheckHelper::advance(const IncrementalReasoningState &incrementalReasoningState) {
    return ensureOnTuple(incrementalReasoningState, m_tupleIterator->advance());
}

// FBFDeletionTaskWorker::StackFrame

template<bool callMonitor, ReasoningModeType reasoningMode, bool hasRecursiveRules>
always_inline FBFDeletionTaskWorker<callMonitor, reasoningMode, hasRecursiveRules>::StackFrame::StackFrame() :
    m_previous(),
    m_returnAddress(0),
    m_tupleIndex(INVALID_TUPLE_INDEX),
    m_currentTupleBuffer(3, INVALID_RESOURCE_ID),
    m_allProved(false),
    m_multiplicity(0),
    m_currentPositionIndex(0),
    m_currentReflexivityCheckHelper(),
    m_currentIndexingPatternNumber(static_cast<size_t>(-1)),
    m_currentHeadAtomInfo(nullptr),
    m_currentBodyMatches(),
    m_currentBodyIndex(0)
{
}

// FBFDeletionTaskWorker

template<bool callMonitor, ReasoningModeType reasoningMode, bool hasRecursiveRules>
always_inline std::unique_ptr<typename FBFDeletionTaskWorker<callMonitor, reasoningMode, hasRecursiveRules>::ReflexivityCheckHelper> FBFDeletionTaskWorker<callMonitor, reasoningMode, hasRecursiveRules>::getReflexivityCheckHelper(const size_t positionIndex) {
    std::unique_ptr<ReflexivityCheckHelper>& head = m_reflexivityCheckHelperHeads[positionIndex];
    if (head.get() == 0)
        return std::unique_ptr<ReflexivityCheckHelper>(new ReflexivityCheckHelper(this->m_tripleTable, positionIndex));
    else {
        std::unique_ptr<ReflexivityCheckHelper> result = std::move(head);
        head = std::move(result->m_previous);
        return result;
    }
}

template<bool callMonitor, ReasoningModeType reasoningMode, bool hasRecursiveRules>
always_inline void FBFDeletionTaskWorker<callMonitor, reasoningMode, hasRecursiveRules>::leaveReflexivityCheckHelper(std::unique_ptr<ReflexivityCheckHelper> reflexivityCheckHelper, const size_t positionIndex) {
    std::unique_ptr<ReflexivityCheckHelper>& head = m_reflexivityCheckHelperHeads[positionIndex];
    reflexivityCheckHelper->m_previous = std::move(head);
    head = std::move(reflexivityCheckHelper);
}

template<bool callMonitor, ReasoningModeType reasoningMode, bool hasRecursiveRules>
always_inline std::unique_ptr<typename FBFDeletionTaskWorker<callMonitor, reasoningMode, hasRecursiveRules>::StackFrame> FBFDeletionTaskWorker<callMonitor, reasoningMode, hasRecursiveRules>::getStackFrame(const TupleIndex tupleIndex, const std::vector<ResourceID> argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, std::unique_ptr<StackFrame> previous) {
    assert(tupleIndex != INVALID_TUPLE_INDEX);
    std::unique_ptr<StackFrame> stackFrame;
    if (m_usedStackFrames.get() == 0)
        stackFrame.reset(new StackFrame());
    else {
        stackFrame = std::move(m_usedStackFrames);
        m_usedStackFrames = std::move(stackFrame->m_previous);
    }
    stackFrame->m_tupleIndex = tupleIndex;
    stackFrame->m_previous = std::move(previous);
    for (size_t positionIndex = 0; positionIndex < 3; ++positionIndex)
        stackFrame->m_currentTupleBuffer[positionIndex] = argumentsBuffer[argumentIndexes[positionIndex]];
    return std::move(stackFrame);
}

template<bool callMonitor, ReasoningModeType reasoningMode, bool hasRecursiveRules>
always_inline void FBFDeletionTaskWorker<callMonitor, reasoningMode, hasRecursiveRules>::leaveStackFrame(std::unique_ptr<StackFrame> stackFrame) {
    stackFrame->m_previous = std::move(m_usedStackFrames);
    stackFrame->m_returnAddress = 0;
    stackFrame->m_tupleIndex = INVALID_TUPLE_INDEX;
    stackFrame->m_allProved = false;
    stackFrame->m_multiplicity = 0;
    stackFrame->m_currentPositionIndex = 0;
    assert(stackFrame->m_currentReflexivityCheckHelper.get() == nullptr);
    stackFrame->m_currentIndexingPatternNumber = 0;
    stackFrame->m_currentHeadAtomInfo = nullptr;
    assert(stackFrame->m_currentBodyMatches.get() == nullptr);
    stackFrame->m_currentBodyIndex = 0;
    m_usedStackFrames = std::move(stackFrame);
}

template<bool callMonitor, ReasoningModeType reasoningMode, bool hasRecursiveRules>
FBFDeletionTaskWorker<callMonitor, reasoningMode, hasRecursiveRules>::FBFDeletionTaskWorker(FBFDeletionTask& deletionTask, DatalogEngineWorker& datalogEngineWorker) :
    AbstractDeletionTaskWorker<FBFDeletionTaskWorkerType>(deletionTask, datalogEngineWorker),
    m_dictionary(this->m_dataStore.getDictionary()),
    m_provingEqualityManager(this->m_ruleIndex.getProvingEqualityManager()),
    m_ruleQueue(deletionTask.m_ruleQueue),
    m_queriesForNormalizationSPO(),
    m_queriesForEBDContainmentCheckSPO(),
    m_currentTupleIndex2(INVALID_TUPLE_INDEX),
    m_currentTupleBuffer4(3, INVALID_RESOURCE_ID),
    m_currentTupleBuffer5(3, INVALID_RESOURCE_ID),
    m_currentTupleBuffer6(3, INVALID_RESOURCE_ID),
    m_currentTupleBuffer7(3, INVALID_RESOURCE_ID),
    m_currentTupleBuffer8(3, INVALID_RESOURCE_ID),
    m_currentTupleBuffer9(3, INVALID_RESOURCE_ID),
    m_reflexivityCheckHelperHeads(),
    m_usedStackFrames(),
    m_checkedTuples()
{
    ArgumentIndexSet allInputArguments;
    for (ArgumentIndex argumentIndex = 0; argumentIndex < 3; ++argumentIndex) {
        allInputArguments.clear();
        allInputArguments.add(argumentIndex);
        m_queriesForNormalizationSPO[argumentIndex] = this->m_tripleTable.createTupleIterator(m_currentTupleBuffer4, this->m_currentTupleArgumentIndexes, allInputArguments, allInputArguments, TUPLE_STATUS_COMPLETE, TUPLE_STATUS_COMPLETE);
        m_queriesForEBDContainmentCheckSPO[argumentIndex] = this->m_tripleTable.createTupleIterator(m_currentTupleBuffer6, this->m_currentTupleArgumentIndexes, allInputArguments, allInputArguments, TUPLE_STATUS_COMPLETE | TUPLE_STATUS_EDB, TUPLE_STATUS_COMPLETE | TUPLE_STATUS_EDB);
    }
}

template<bool callMonitor, ReasoningModeType reasoningMode, bool hasRecursiveRules>
FBFDeletionTaskWorker<callMonitor, reasoningMode, hasRecursiveRules>::~FBFDeletionTaskWorker() {
    // Just letting the object die would recursively kill the lists of stack frames etc., which could blow the stack.
    // Therefore, we perform the cleanup in a non-recursive way.
    while (m_usedStackFrames.get() != nullptr) {
        std::unique_ptr<StackFrame> topStackFrame = std::move(m_usedStackFrames);
        m_usedStackFrames = std::move(topStackFrame->m_previous);
    }
    for (size_t argumentIndex = 0; argumentIndex < 3; ++argumentIndex) {
        std::unique_ptr<ReflexivityCheckHelper> head = std::move(m_reflexivityCheckHelperHeads[argumentIndex]);
        while (head.get() != nullptr) {
            std::unique_ptr<ReflexivityCheckHelper> topHelper = std::move(head);
            head = std::move(topHelper->m_previous);
        }
    }
}

template<bool callMonitor, ReasoningModeType reasoningMode, bool hasRecursiveRules>
always_inline size_t FBFDeletionTaskWorker<callMonitor, reasoningMode, hasRecursiveRules>::ensureOnRewriteTuple(TupleIterator& tupleIterator, size_t multiplicity) {
    while (multiplicity != 0 && ((this->m_incrementalReasoningState.getCurrentLevelFlags(tupleIterator.getCurrentTupleIndex()) & (LF_PROVED | LF_PROVED_MERGED)) != LF_PROVED))
        multiplicity = tupleIterator.advance();
    return multiplicity;
}

template<bool callMonitor, ReasoningModeType reasoningMode, bool hasRecursiveRules>
always_inline void FBFDeletionTaskWorker<callMonitor, reasoningMode, hasRecursiveRules>::rewrite(ThreadContext& threadContext, const ResourceID resourceID1, const ResourceID resourceID2) {
    const DatatypeID resource1DatatypeID = m_dictionary.getDatatypeID(resourceID1);
    const DatatypeID resource2DatatypeID = m_dictionary.getDatatypeID(resourceID2);
    ResourceID sourceID;
    ResourceID targetID;
    bool clash;
    if (resource1DatatypeID == D_BLANK_NODE && resource2DatatypeID != D_BLANK_NODE) {
        sourceID = resourceID1;
        targetID = resourceID2;
        clash = false;
    }
    else if (resource1DatatypeID != D_BLANK_NODE && resource2DatatypeID == D_BLANK_NODE) {
        sourceID = resourceID2;
        targetID = resourceID1;
        clash = false;
    }
    else {
        // At this point, either both or none of the resources are blank nodes.
        // *DatatypeID > D_BLANK_NODE means that the resource is a literal; hence, if either resource is a literal,
        // then the other resource is not a blank node and so we've got a clash. In addition, we've got a clash if
        // we're using UNA and both resources are IRI references.
        clash = (resource1DatatypeID > D_BLANK_NODE) || (resource2DatatypeID > D_BLANK_NODE) || (reasoningMode == REASONING_MODE_EQUALITY_UNA && resource1DatatypeID == D_IRI_REFERENCE && resource2DatatypeID == D_IRI_REFERENCE);
        if (resourceID1 < resourceID2) {
            sourceID = resourceID2;
            targetID = resourceID1;
        }
        else {
            sourceID = resourceID1;
            targetID = resourceID2;
        }
    }
    if (m_provingEqualityManager.merge<false>(sourceID, targetID)) {
        if (callMonitor) {
            this->m_incrementalMonitor->constantMerged(this->m_workerIndex, sourceID, targetID, true);
            this->m_incrementalMonitor->normalizeConstantStarted(this->m_workerIndex, sourceID);
        }
        // Fix the facts
        for (size_t positionIndex = 0; positionIndex < 3; ++positionIndex) {
            TupleIterator& tupleIterator = *m_queriesForNormalizationSPO[positionIndex];
            m_currentTupleBuffer4[positionIndex] = sourceID;
            size_t multiplicity = ensureOnRewriteTuple(tupleIterator, tupleIterator.open(threadContext));
            while (multiplicity != 0) {
                // We must copy the tuple; otherwise, the following call to normalize() could override the value of mergedID, which breaks the iterator.
                AddResult addResult = ALREADY_EXISTS;
                if (m_provingEqualityManager.normalize(m_currentTupleBuffer4, m_currentTupleBuffer5, this->m_currentTupleArgumentIndexes) && this->m_incrementalReasoningState.template addCurrentLevelFlags<false>(tupleIterator.getCurrentTupleIndex(), LF_PROVED_MERGED))
                    addResult = addProvedTuple(threadContext, m_currentTupleBuffer5, this->m_currentTupleArgumentIndexes);
                if (callMonitor) {
                    switch (addResult) {
                    case ALREADY_EXISTS:
                        this->m_incrementalMonitor->tupleNormalized(this->m_workerIndex, m_currentTupleBuffer4, this->m_currentTupleArgumentIndexes, m_currentTupleBuffer5, this->m_currentTupleArgumentIndexes, false);
                        break;
                    case ADDED:
                        this->m_incrementalMonitor->tupleNormalized(this->m_workerIndex, m_currentTupleBuffer4, this->m_currentTupleArgumentIndexes, m_currentTupleBuffer5, this->m_currentTupleArgumentIndexes, true);
                        break;
                    case ALREADY_DELAYED:
                        this->m_incrementalMonitor->tupleNormalizedDelayed(this->m_workerIndex, m_currentTupleBuffer4, this->m_currentTupleArgumentIndexes, m_currentTupleBuffer5, this->m_currentTupleArgumentIndexes, false);
                        break;
                    case DELAYED:
                        this->m_incrementalMonitor->tupleNormalizedDelayed(this->m_workerIndex, m_currentTupleBuffer4, this->m_currentTupleArgumentIndexes, m_currentTupleBuffer5, this->m_currentTupleArgumentIndexes, true);
                        break;
                    }
                }
                multiplicity = ensureOnRewriteTuple(tupleIterator, tupleIterator.advance());
            }
        }
        if (callMonitor)
            this->m_incrementalMonitor->normalizeConstantFinished(this->m_workerIndex);
        // Fix the rules
        if (!m_ruleQueue.initializeLarge())
            throw RDF_STORE_EXCEPTION("Cannot initialize rule queue.");
        this->m_ruleIndex.template enqueueRulesToReevaluateIncremental<reasoningMode == REASONING_MODE_NO_EQUALITY_BY_LEVELS>(sourceID, this->m_componentLevel, m_ruleQueue);
        // Fix the facts. We call proveNormalTuple() to prevent recursion, which might prevent inlining (and some compilers complain then).
        RuleInfo* ruleInfo;
        while ((ruleInfo = m_ruleQueue.dequeue<false>()) != nullptr)
            ruleInfo->template evaluateRuleIncremental<reasoningMode == REASONING_MODE_NO_EQUALITY_BY_LEVELS, callMonitor>(threadContext, this->m_workerIndex, this->m_componentLevel, this->m_incrementalMonitor,
                [this](ThreadContext& threadContext, const HeadAtomInfo& headAtomInfo, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) {
                    this->proveNormalTuple(threadContext, argumentsBuffer, argumentIndexes);
                }
            );
    }
    else {
        if (callMonitor)
            this->m_incrementalMonitor->constantMerged(this->m_workerIndex, sourceID, targetID, false);
    }
    if (clash) {
        m_currentTupleBuffer5[0] = targetID;
        m_currentTupleBuffer5[1] = m_provingEqualityManager.normalize(this->m_dictionary.resolveResource(RDF_TYPE, D_IRI_REFERENCE));
        m_currentTupleBuffer5[2] = m_provingEqualityManager.normalize(this->m_dictionary.resolveResource(OWL_NOTHING, D_IRI_REFERENCE));
        // We call proveNormalTuple() to prevent recursion, which might prevent inlining (and some compilers complain then).
        proveNormalTuple(threadContext, m_currentTupleBuffer5, this->m_currentTupleArgumentIndexes);
    }
}

template<bool callMonitor, ReasoningModeType reasoningMode, bool hasRecursiveRules>
always_inline void FBFDeletionTaskWorker<callMonitor, reasoningMode, hasRecursiveRules>::proveNormalTuple(ThreadContext& threadContext, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) {
    switch (addProvedTuple(threadContext, argumentsBuffer, argumentIndexes)) {
    case ALREADY_EXISTS:
        if (callMonitor)
            this->m_incrementalMonitor->tupleDerived(this->m_workerIndex, argumentsBuffer, argumentIndexes, true, false);
        break;
    case ADDED:
        if (callMonitor)
            this->m_incrementalMonitor->tupleDerived(this->m_workerIndex, argumentsBuffer, argumentIndexes, true, true);
        break;
    case ALREADY_DELAYED:
        if (callMonitor)
            this->m_incrementalMonitor->tupleProvedDelayed(this->m_workerIndex, argumentsBuffer, argumentIndexes, false);
        break;
    case DELAYED:
        if (callMonitor)
            this->m_incrementalMonitor->tupleProvedDelayed(this->m_workerIndex, argumentsBuffer, argumentIndexes, true);
        break;
    }
}

template<bool callMonitor, ReasoningModeType reasoningMode, bool hasRecursiveRules>
always_inline void FBFDeletionTaskWorker<callMonitor, reasoningMode, hasRecursiveRules>::proveTuple(ThreadContext& threadContext, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) {
    if (!::isEqualityReasoningMode(reasoningMode) || m_provingEqualityManager.isNormal(argumentsBuffer, argumentIndexes)) {
        switch (addProvedTuple(threadContext, argumentsBuffer, argumentIndexes)) {
        case ALREADY_EXISTS:
            if (callMonitor)
                this->m_incrementalMonitor->tupleDerived(this->m_workerIndex, argumentsBuffer, argumentIndexes, true, false);
            break;
        case ADDED:
            if (callMonitor)
                this->m_incrementalMonitor->tupleDerived(this->m_workerIndex, argumentsBuffer, argumentIndexes, true, true);
            if (::isEqualityReasoningMode(reasoningMode) && argumentsBuffer[argumentIndexes[1]] == OWL_SAME_AS_ID && argumentsBuffer[argumentIndexes[0]] != argumentsBuffer[argumentIndexes[2]])
                rewrite(threadContext, argumentsBuffer[argumentIndexes[0]], argumentsBuffer[argumentIndexes[2]]);
            break;
        case ALREADY_DELAYED:
            if (callMonitor)
                this->m_incrementalMonitor->tupleProvedDelayed(this->m_workerIndex, argumentsBuffer, argumentIndexes, false);
            break;
        case DELAYED:
            if (callMonitor)
                this->m_incrementalMonitor->tupleProvedDelayed(this->m_workerIndex, argumentsBuffer, argumentIndexes, true);
            break;
        }
    }
    else {
        if (callMonitor)
            this->m_incrementalMonitor->tupleDerived(this->m_workerIndex, argumentsBuffer, argumentIndexes, false, false);
    }
}

template<bool callMonitor, ReasoningModeType reasoningMode, bool hasRecursiveRules>
always_inline typename FBFDeletionTaskWorker<callMonitor, reasoningMode, hasRecursiveRules>::AddResult FBFDeletionTaskWorker<callMonitor, reasoningMode, hasRecursiveRules>::addProvedTuple(ThreadContext& threadContext, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) {
    const TupleIndex tupleIndex = this->m_tripleTable.addTuple(threadContext, argumentsBuffer, argumentIndexes, 0, 0).second;
    bool inCheckedSet;
    if (::isEqualityReasoningMode(reasoningMode) && this->m_equalityManager.normalize(argumentsBuffer, argumentIndexes, m_currentTupleBuffer5, this->m_currentTupleArgumentIndexes)) {
        const TupleIndex normalizedTupleIndex = this->m_tripleTable.getTupleIndex(threadContext, m_currentTupleBuffer5, this->m_currentTupleArgumentIndexes);
        inCheckedSet = normalizedTupleIndex != INVALID_TUPLE_INDEX && ((this->m_incrementalReasoningState.getCurrentLevelFlags(normalizedTupleIndex) & LF_CHECKED) == LF_CHECKED);
    }
    else
        inCheckedSet = (this->m_incrementalReasoningState.getCurrentLevelFlags(tupleIndex) & LF_CHECKED) == LF_CHECKED;
    if (inCheckedSet) {
        if (this->m_incrementalReasoningState.template addCurrentLevelFlags<false>(tupleIndex, LF_PROVED_NEW)) {
            this->m_incrementalReasoningState.getProvedList().template enqueue<false>(tupleIndex);
            return ADDED;
        }
        else
            return ALREADY_EXISTS;
    }
    else {
        if (this->m_incrementalReasoningState.template addCurrentLevelFlags<false>(tupleIndex, LF_DELAYED))
            return DELAYED;
        else
            return ALREADY_DELAYED;
    }
}

template<bool callMonitor, ReasoningModeType reasoningMode, bool hasRecursiveRules>
always_inline bool FBFDeletionTaskWorker<callMonitor, reasoningMode, hasRecursiveRules>::occursInEDB(ThreadContext& threadContext, const ResourceID resourceID) {
    for (size_t positionIndex = 0; positionIndex < 3; ++positionIndex) {
        m_currentTupleBuffer6[positionIndex] = resourceID;
        if (m_queriesForEBDContainmentCheckSPO[positionIndex]->open(threadContext) != 0)
            return true;
    }
    return false;
}

template<bool callMonitor, ReasoningModeType reasoningMode, bool hasRecursiveRules>
always_inline bool FBFDeletionTaskWorker<callMonitor, reasoningMode, hasRecursiveRules>::headAtomInfoEquals(HeadAtomInfo& headAtomInfo, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) {
    const std::vector<ArgumentIndex>& headAtomInfoArgumentIndexes = headAtomInfo.getHeadArgumentIndexes();
    if (headAtomInfoArgumentIndexes.size() != argumentIndexes.size())
        return false;
    const std::vector<ResourceID>& headAtomInfoDefaultArgumentsBuffer = headAtomInfo.getRuleInfo().getDefaultArgumentsBuffer();
    for (auto iterator1 = headAtomInfoArgumentIndexes.begin(), iterator2 = argumentIndexes.begin(); iterator1 != headAtomInfoArgumentIndexes.end(); ++iterator1, ++iterator2)
        if (headAtomInfoDefaultArgumentsBuffer[*iterator1] != argumentsBuffer[*iterator2])
            return false;
    return true;
}

template<bool callMonitor, ReasoningModeType reasoningMode, bool hasRecursiveRules>
always_inline bool FBFDeletionTaskWorker<callMonitor, reasoningMode, hasRecursiveRules>::provedByNonrecursiveRule(ThreadContext& threadContext, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const std::vector<ResourceID>& normalizedArgumentsBuffer, const std::vector<ArgumentIndex>& normalizedArgumentIndexes) {
    if (::isEqualityReasoningMode(reasoningMode)) {
        if (this->m_ruleIndex.hasPivotlessRules(this->m_componentLevel)) {
            // With axiomatized equality, the only nonrecursive rules are pivotless, and they are in fact facts.
            // Now RuleIndex does not contain an index of facts, so we retrieve such facts in a somewhat longwinded way:
            // we query the index for all facts matching the normalized rule (since this index is being maintained),
            // and for each rule we check whether the given fact matches the rule head.
            for (HeadAtomInfo* headAtomInfo = this->m_ruleIndex.getMatchingHeadAtomInfos(threadContext, normalizedArgumentsBuffer, normalizedArgumentIndexes, 7); headAtomInfo != nullptr; headAtomInfo = headAtomInfo->getNextMatchingHeadAtomInfo(normalizedArgumentsBuffer, normalizedArgumentIndexes)) {
                // The recursive flag is not maintained in the case of euqality, so we check the pivotless flag.
                if (!headAtomInfo->isRecursive<false>() && headAtomInfoEquals(*headAtomInfo, argumentsBuffer, argumentIndexes)) {
                    if (callMonitor) {
                        this->m_incrementalMonitor->backwardNonrecursiveRuleStarted(this->m_workerIndex, *headAtomInfo);
                        this->m_incrementalMonitor->backwardNonrecursiveInstanceMatched(this->m_workerIndex, *headAtomInfo, headAtomInfo->getSupportingFactsEvaluatorPrototype(argumentsBuffer, argumentIndexes));
                        this->m_incrementalMonitor->backwardNonrecursiveRuleFinished(this->m_workerIndex);
                    }
                    return true;
                }
            }
        }
    }
    else {
        // The following call makes sure that, if we're not processing components, then only pivotless rules are counted as nonrecursive. 
        if (this->m_ruleIndex.hasNonrecursiveRules(this->m_componentLevel)) {
            for (size_t indexingPatternNumber = 0; indexingPatternNumber < 8; ++indexingPatternNumber) {
                for (HeadAtomInfo* headAtomInfo = this->m_ruleIndex.getMatchingHeadAtomInfos(threadContext, argumentsBuffer, argumentIndexes, indexingPatternNumber); headAtomInfo != nullptr; headAtomInfo = headAtomInfo->getNextMatchingHeadAtomInfo(argumentsBuffer, argumentIndexes)) {
                    // Process only nonrecursive rules here!
                    if (!headAtomInfo->template isRecursive<reasoningMode == REASONING_MODE_NO_EQUALITY_BY_LEVELS>()) {
                        if (callMonitor)
                            this->m_incrementalMonitor->backwardNonrecursiveRuleStarted(this->m_workerIndex, *headAtomInfo);
                        SupportingFactsEvaluator& supportingFactsEvaluator = headAtomInfo->getSupportingFactsEvaluatorPrototype(argumentsBuffer, argumentIndexes);
                        const bool result = (supportingFactsEvaluator.open(threadContext) != 0);
                        if (callMonitor) {
                            if (result)
                                this->m_incrementalMonitor->backwardNonrecursiveInstanceMatched(this->m_workerIndex, *headAtomInfo, supportingFactsEvaluator);
                            this->m_incrementalMonitor->backwardNonrecursiveRuleFinished(this->m_workerIndex);
                        }
                        if (result)
                            return true;
                    }
                }
            }
        }
    }
    return false;
}

template<bool callMonitor, ReasoningModeType reasoningMode, bool hasRecursiveRules>
bool FBFDeletionTaskWorker<callMonitor, reasoningMode, hasRecursiveRules>::saturate(ThreadContext& threadContext, const TupleIndex tupleIndex, const std::vector<ResourceID>& currentTupleBuffer) {
    // Check whether the propagated fact is proved
    if (::isEqualityReasoningMode(reasoningMode)){
        // Short-circuit the derivations of many reflexivity tuples by looking them up in the EDB.
        if (currentTupleBuffer[1] == OWL_SAME_AS_ID) {
            assert(currentTupleBuffer[0] == currentTupleBuffer[2]);
            const ResourceID originalResourceID = currentTupleBuffer[0];
            for (ResourceID denormalizedResourceID = originalResourceID; denormalizedResourceID != INVALID_RESOURCE_ID; denormalizedResourceID = this->m_equalityManager.getNextEqual(denormalizedResourceID)) {
                if (occursInEDB(threadContext, denormalizedResourceID)) {
                    const ResourceID normalizedProvedResourceID = m_provingEqualityManager.normalize(denormalizedResourceID);
                    m_currentTupleBuffer6[1] = OWL_SAME_AS_ID;
                    m_currentTupleBuffer6[0] = m_currentTupleBuffer6[2] = m_provingEqualityManager.normalize(normalizedProvedResourceID);
                    const TupleIndex tupleIndex = this->m_tripleTable.addTuple(threadContext, m_currentTupleBuffer6, this->m_currentTupleArgumentIndexes, 0, 0).second;
                    if (this->m_incrementalReasoningState.template addCurrentLevelFlags<false>(tupleIndex, LF_PROVED_NEW)) {
                        this->m_incrementalReasoningState.getProvedList().template enqueue<false>(tupleIndex);
                        if (callMonitor)
                            this->m_incrementalMonitor->checkedReflexiveSameAsTupleProvedFromEDB(this->m_workerIndex, originalResourceID, denormalizedResourceID, normalizedProvedResourceID, true);
                    }
                    else {
                        if (callMonitor)
                            this->m_incrementalMonitor->checkedReflexiveSameAsTupleProvedFromEDB(this->m_workerIndex, originalResourceID, denormalizedResourceID, normalizedProvedResourceID, false);
                    }
                }
            }
        }
        // Prove the tuple from EDB or delayed
        bool firstTuple = true;
        for (m_currentTupleBuffer6[0] = currentTupleBuffer[0]; m_currentTupleBuffer6[0] != INVALID_RESOURCE_ID; m_currentTupleBuffer6[0] = this->m_equalityManager.getNextEqual(m_currentTupleBuffer6[0]))
            for (m_currentTupleBuffer6[1] = currentTupleBuffer[1]; m_currentTupleBuffer6[1] != INVALID_RESOURCE_ID; m_currentTupleBuffer6[1] = this->m_equalityManager.getNextEqual(m_currentTupleBuffer6[1]))
                for (m_currentTupleBuffer6[2] = currentTupleBuffer[2]; m_currentTupleBuffer6[2] != INVALID_RESOURCE_ID; m_currentTupleBuffer6[2] = this->m_equalityManager.getNextEqual(m_currentTupleBuffer6[2])) {
                    // Check whether the denormalized tuple is in the EDB or whether the normalized version is proved; we use getTupleIndex() so that the triple is not written into the triple table unnecessarily.
                    // Since getTupleIndex() is quite an expensive operation, we omit the call for the first tuple (which, due to the way the above loops are implemented, is tupleIndex).
                    TupleIndex denormalizedTupleIndex;
                    if (firstTuple) {
                        denormalizedTupleIndex = tupleIndex;
                        firstTuple = false;
                    }
                    else
                        denormalizedTupleIndex = this->m_tripleTable.getTupleIndex(threadContext, m_currentTupleBuffer6, this->m_currentTupleArgumentIndexes);
                    if (callMonitor)
                        this->m_incrementalMonitor->checkedTupleChecked(this->m_workerIndex, currentTupleBuffer, m_currentTupleBuffer6, this->m_currentTupleArgumentIndexes);
                    if (denormalizedTupleIndex != INVALID_TUPLE_INDEX) {
                        const TupleStatus tupleStatus = this->m_tripleTable.getTupleStatus(denormalizedTupleIndex);
                        const bool fromEDB = ((tupleStatus & TUPLE_STATUS_EDB) == TUPLE_STATUS_EDB);
                        const bool fromDelayed = !fromEDB && (this->m_incrementalReasoningState.getCurrentLevelFlags(denormalizedTupleIndex) & LF_DELAYED) == LF_DELAYED;
                        const bool fromNonrecursiveRule = !fromEDB && !fromDelayed && provedByNonrecursiveRule(threadContext, m_currentTupleBuffer6, this->m_currentTupleArgumentIndexes, currentTupleBuffer, this->m_currentTupleArgumentIndexes);
                        if (fromEDB || fromDelayed || fromNonrecursiveRule) {
                            // Normalize by the new equality manager before adding to the triple table
                            if (m_provingEqualityManager.normalize(m_currentTupleBuffer6, m_currentTupleBuffer7, this->m_currentTupleArgumentIndexes))
                                denormalizedTupleIndex = this->m_tripleTable.addTuple(threadContext, m_currentTupleBuffer7, this->m_currentTupleArgumentIndexes, 0, 0).second;
                            if (this->m_incrementalReasoningState.template addCurrentLevelFlags<false>(denormalizedTupleIndex, LF_PROVED_NEW)) {
                                this->m_incrementalReasoningState.getProvedList().template enqueue<false>(denormalizedTupleIndex);
                                if (callMonitor)
                                    this->m_incrementalMonitor->checkedTupleProved(this->m_workerIndex, currentTupleBuffer, m_currentTupleBuffer6, m_currentTupleBuffer7, this->m_currentTupleArgumentIndexes, fromEDB, fromDelayed, fromNonrecursiveRule, true);
                            }
                            else {
                                if (callMonitor)
                                    this->m_incrementalMonitor->checkedTupleProved(this->m_workerIndex, currentTupleBuffer, m_currentTupleBuffer6, m_currentTupleBuffer7, this->m_currentTupleArgumentIndexes, fromEDB, fromDelayed, fromNonrecursiveRule, false);
                            }
                        }
                    }
                }
    }
    else {
        if (callMonitor)
            this->m_incrementalMonitor->checkedTupleChecked(this->m_workerIndex, currentTupleBuffer, currentTupleBuffer, this->m_currentTupleArgumentIndexes);
        const TupleStatus tupleStatus = this->m_tripleTable.getTupleStatus(tupleIndex);
        const bool fromEDB = ((tupleStatus & TUPLE_STATUS_EDB) == TUPLE_STATUS_EDB);
        const bool fromDelayed = !fromEDB && (this->m_incrementalReasoningState.getCurrentLevelFlags(tupleIndex) & LF_DELAYED) == LF_DELAYED;
        const bool fromNonrecursiveRule = !fromEDB && !fromDelayed && provedByNonrecursiveRule(threadContext, currentTupleBuffer, this->m_currentTupleArgumentIndexes, currentTupleBuffer, this->m_currentTupleArgumentIndexes);
        if (fromEDB || fromDelayed || fromNonrecursiveRule) {
            if (this->m_incrementalReasoningState.template addCurrentLevelFlags<false>(tupleIndex, LF_PROVED_NEW)) {
                this->m_incrementalReasoningState.getProvedList().template enqueue<false>(tupleIndex);
                if (callMonitor)
                    this->m_incrementalMonitor->checkedTupleProved(this->m_workerIndex, currentTupleBuffer, currentTupleBuffer, currentTupleBuffer, this->m_currentTupleArgumentIndexes, fromEDB, fromDelayed, fromNonrecursiveRule, true);
            }
            else {
                if (callMonitor)
                    this->m_incrementalMonitor->checkedTupleProved(this->m_workerIndex, currentTupleBuffer, currentTupleBuffer, currentTupleBuffer, this->m_currentTupleArgumentIndexes, fromEDB, fromDelayed, fromNonrecursiveRule, false);
            }
        }
    }
    if (this->m_incrementalReasoningState.getProvedList().canDequeue()) {
        // Now saturate the proved facts
        if (callMonitor)
            this->m_incrementalMonitor->processProvedStarted(this->m_workerIndex);
        while (::atomicRead(this->m_taskRunning)) {
            m_currentTupleIndex2 = this->m_incrementalReasoningState.getProvedList().template dequeue<false>();
            if (m_currentTupleIndex2 == INVALID_TUPLE_INDEX)
                break;
            const TupleFlags tupleFlags = this->m_incrementalReasoningState.getCurrentLevelFlags(m_currentTupleIndex2);
            if ((tupleFlags & (LF_PROVED | LF_PROVED_MERGED | LF_PROVED_NEW)) == LF_PROVED_NEW && this->m_incrementalReasoningState.template addCurrentLevelFlags<false>(m_currentTupleIndex2, LF_PROVED)) {
                this->m_tripleTable.getStatusAndTuple(m_currentTupleIndex2, m_currentTupleBuffer6);
                if (callMonitor)
                    this->m_incrementalMonitor->currentTupleExtracted(this->m_workerIndex, m_currentTupleBuffer6, this->m_currentTupleArgumentIndexes);
                if (::isEqualityReasoningMode(reasoningMode) && m_provingEqualityManager.normalize(m_currentTupleBuffer6, this->m_currentTupleArgumentIndexes)) {
                    this->m_incrementalReasoningState.template addCurrentLevelFlags<false>(m_currentTupleIndex2, LF_PROVED_MERGED);
                    const AddResult addResult = addProvedTuple(threadContext, m_currentTupleBuffer6, this->m_currentTupleArgumentIndexes);
                    if (callMonitor) {
                        switch (addResult) {
                        case ALREADY_EXISTS:
                            this->m_incrementalMonitor->currentTupleNormalized(this->m_workerIndex, m_currentTupleBuffer6, this->m_currentTupleArgumentIndexes, false);
                            break;
                        case ADDED:
                            this->m_incrementalMonitor->currentTupleNormalized(this->m_workerIndex, m_currentTupleBuffer6, this->m_currentTupleArgumentIndexes, true);
                            break;
                        case ALREADY_DELAYED:
                        case DELAYED:
                            throw RDF_STORE_EXCEPTION("Internal error: if a current tuple is normalized, it is in checked so it cannot be delayed.");
                        }
                    }
                }
                else if (::isEqualityReasoningMode(reasoningMode) && m_currentTupleBuffer6[1] == OWL_SAME_AS_ID && m_currentTupleBuffer6[0] != m_currentTupleBuffer6[2])
                    rewrite(threadContext, m_currentTupleBuffer6[0], m_currentTupleBuffer6[2]);
                else {
                    if (::isEqualityReasoningMode(reasoningMode)) {
                        m_currentTupleBuffer7[1] = OWL_SAME_AS_ID;
                        for (size_t positionIndex = 0; positionIndex < 3; ++positionIndex) {
                            const ResourceID resourceID = m_currentTupleBuffer6[positionIndex];
                            if (this->template checkReflexivity<1>(resourceID)) {
                                m_currentTupleBuffer7[0] = m_currentTupleBuffer7[2] = resourceID;
                                const AddResult addResult = addProvedTuple(threadContext, m_currentTupleBuffer7, this->m_currentTupleArgumentIndexes);
                                if (callMonitor) {
                                    switch (addResult) {
                                    case ALREADY_EXISTS:
                                        this->m_incrementalMonitor->reflexiveSameAsTupleDerived(this->m_workerIndex, resourceID, false);
                                        break;
                                    case ADDED:
                                        this->m_incrementalMonitor->reflexiveSameAsTupleDerived(this->m_workerIndex, resourceID, true);
                                        break;
                                    case ALREADY_DELAYED:
                                        this->m_incrementalMonitor->reflexiveSameAsTupleDerivedDelayed(this->m_workerIndex, resourceID, false);
                                        break;
                                    case DELAYED:
                                        this->m_incrementalMonitor->reflexiveSameAsTupleDerivedDelayed(this->m_workerIndex, resourceID, true);
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    this->m_ruleIndex.template applyRulesToPositiveLiteralIncremental<reasoningMode == REASONING_MODE_NO_EQUALITY_BY_LEVELS ? ALL_IN_COMPONENT : ALL_COMPONENTS, callMonitor>(threadContext, this->m_workerIndex, m_currentTupleBuffer6, this->m_currentTupleArgumentIndexes, this->m_componentLevel, this->m_incrementalMonitor,
                        [this](ThreadContext& threadContext, const HeadAtomInfo& headAtomInfo, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) {
                            this->proveTuple(threadContext, argumentsBuffer, argumentIndexes);
                        }
                    );
                }
                if (callMonitor)
                    this->m_incrementalMonitor->currentTupleProcessed(this->m_workerIndex);
            }
        }
        if (callMonitor)
            this->m_incrementalMonitor->processProvedFinished(this->m_workerIndex);
        return (::isEqualityReasoningMode(reasoningMode) ? allProved(threadContext, tupleIndex, currentTupleBuffer, this->m_currentTupleArgumentIndexes) : true);
    }
    else
        return false;
}

template<bool callMonitor, ReasoningModeType reasoningMode, bool hasRecursiveRules>
always_inline bool FBFDeletionTaskWorker<callMonitor, reasoningMode, hasRecursiveRules>::allProved(ThreadContext& threadContext, const TupleIndex normalizedTupleIndex, const std::vector<ResourceID>& normalizedArgumentsBuffer, const std::vector<ArgumentIndex>& normalizedArgumentIndexes) {
    if (::isEqualityReasoningMode(reasoningMode)) {
        const TupleFlags tupleFlags = this->m_incrementalReasoningState.getGlobalFlags(normalizedTupleIndex);
        if ((tupleFlags & GF_DISPROVED) == GF_DISPROVED)
            return false;
        else if ((tupleFlags & GF_NORM_PROVED) != GF_NORM_PROVED) {
            const ResourceID subjectID = normalizedArgumentsBuffer[normalizedArgumentIndexes[0]];
            const ResourceID predicateID = normalizedArgumentsBuffer[normalizedArgumentIndexes[1]];
            const ResourceID objectID = normalizedArgumentsBuffer[normalizedArgumentIndexes[2]];
            for (m_currentTupleBuffer8[0] = subjectID; m_currentTupleBuffer8[0] != INVALID_RESOURCE_ID; m_currentTupleBuffer8[0] = this->m_equalityManager.getNextEqual(m_currentTupleBuffer8[0]))
                for (m_currentTupleBuffer8[1] = predicateID; m_currentTupleBuffer8[1] != INVALID_RESOURCE_ID; m_currentTupleBuffer8[1] = this->m_equalityManager.getNextEqual(m_currentTupleBuffer8[1]))
                    for (m_currentTupleBuffer8[2] = objectID; m_currentTupleBuffer8[2] != INVALID_RESOURCE_ID; m_currentTupleBuffer8[2] = this->m_equalityManager.getNextEqual(m_currentTupleBuffer8[2])) {
                        // getTupleIndex() is an expensive operation, so we try to avoid it if we can.
                        m_provingEqualityManager.normalize(m_currentTupleBuffer8, this->m_currentTupleArgumentIndexes, m_currentTupleBuffer9, this->m_currentTupleArgumentIndexes);
                        TupleIndex tupleIndex;
                        if (m_currentTupleBuffer9[0] == subjectID && m_currentTupleBuffer9[1] == predicateID && m_currentTupleBuffer9[2] == objectID)
                            tupleIndex = normalizedTupleIndex;
                        else
                            tupleIndex = this->m_tripleTable.getTupleIndex(threadContext, m_currentTupleBuffer9, this->m_currentTupleArgumentIndexes);
                        if (tupleIndex == INVALID_TUPLE_INDEX || (this->m_incrementalReasoningState.getCurrentLevelFlags(tupleIndex) & LF_PROVED) == 0)
                            return false;
                    }
            this->m_incrementalReasoningState.template addGlobalFlags<false>(normalizedTupleIndex, GF_NORM_PROVED);
        }
        return true;
    }
    else
        return (this->m_incrementalReasoningState.getCurrentLevelFlags(normalizedTupleIndex) & LF_PROVED) == LF_PROVED;
}

template<bool callMonitor, ReasoningModeType reasoningMode, bool hasRecursiveRules>
always_inline bool FBFDeletionTaskWorker<callMonitor, reasoningMode, hasRecursiveRules>::allDisproved(ThreadContext& threadContext, const TupleIndex normalizedTupleIndex) {
    if (::isEqualityReasoningMode(reasoningMode)) {
        if ((this->m_incrementalReasoningState.getGlobalFlags(normalizedTupleIndex) & GF_NORM_PROVED) == GF_NORM_PROVED)
            return false;
        this->m_tripleTable.getStatusAndTuple(normalizedTupleIndex, m_currentTupleBuffer8);
        const ResourceID subjectID = m_currentTupleBuffer8[0];
        const ResourceID predicateID = m_currentTupleBuffer8[1];
        const ResourceID objectID = m_currentTupleBuffer8[2];
        for (m_currentTupleBuffer8[0] = subjectID; m_currentTupleBuffer8[0] != INVALID_RESOURCE_ID; m_currentTupleBuffer8[0] = this->m_equalityManager.getNextEqual(m_currentTupleBuffer8[0]))
            for (m_currentTupleBuffer8[1] = predicateID; m_currentTupleBuffer8[1] != INVALID_RESOURCE_ID; m_currentTupleBuffer8[1] = this->m_equalityManager.getNextEqual(m_currentTupleBuffer8[1]))
                for (m_currentTupleBuffer8[2] = objectID; m_currentTupleBuffer8[2] != INVALID_RESOURCE_ID; m_currentTupleBuffer8[2] = this->m_equalityManager.getNextEqual(m_currentTupleBuffer8[2])) {
                    // getTupleIndex() is an expensive operation, so we try to avoid it if we can.
                    m_provingEqualityManager.normalize(m_currentTupleBuffer8, this->m_currentTupleArgumentIndexes, m_currentTupleBuffer9, this->m_currentTupleArgumentIndexes);
                    TupleIndex tupleIndex;
                    if (m_currentTupleBuffer9[0] == subjectID && m_currentTupleBuffer9[1] == predicateID && m_currentTupleBuffer9[2] == objectID)
                        tupleIndex = normalizedTupleIndex;
                    else
                        tupleIndex = this->m_tripleTable.getTupleIndex(threadContext, m_currentTupleBuffer9, this->m_currentTupleArgumentIndexes);
                    if (tupleIndex != INVALID_TUPLE_INDEX && (this->m_incrementalReasoningState.getCurrentLevelFlags(tupleIndex) & LF_PROVED) == LF_PROVED)
                        return false;
                }
        return true;
    }
    else
        return (this->m_incrementalReasoningState.getCurrentLevelFlags(normalizedTupleIndex) & LF_PROVED) != LF_PROVED;
}

template<bool callMonitor, ReasoningModeType reasoningMode, bool hasRecursiveRules>
always_inline bool FBFDeletionTaskWorker<callMonitor, reasoningMode, hasRecursiveRules>::allReflexiveSameAsProved(ThreadContext& threadContext, const ResourceID resourceID) {
    m_currentTupleBuffer8[1] = OWL_SAME_AS_ID;
    for (ResourceID denormalizedResourceID = resourceID; denormalizedResourceID != INVALID_RESOURCE_ID; denormalizedResourceID = this->m_equalityManager.getNextEqual(denormalizedResourceID)) {
        m_currentTupleBuffer8[0] = m_currentTupleBuffer8[2] = denormalizedResourceID;
        m_provingEqualityManager.normalize(m_currentTupleBuffer8, this->m_currentTupleArgumentIndexes);
        const TupleIndex tupleIndex = this->m_tripleTable.getTupleIndex(threadContext, m_currentTupleBuffer8, this->m_currentTupleArgumentIndexes);
        if (tupleIndex != INVALID_TUPLE_INDEX && (this->m_incrementalReasoningState.getCurrentLevelFlags(tupleIndex) & LF_PROVED) == 0)
            return false;
    }
    return true;
}

template<bool callMonitor, ReasoningModeType reasoningMode, bool hasRecursiveRules>
always_inline bool FBFDeletionTaskWorker<callMonitor, reasoningMode, hasRecursiveRules>::check(ThreadContext& threadContext, TupleIndex checkTupleIndex, const std::vector<ResourceID>* checkArgumentsBuffer, const std::vector<ArgumentIndex>* checkArgumentIndexes) {
    m_checkedTuples.clear();
    // This implements the pseudo-code; however, due to possibly large recursion depth, we simulate recursion using a stack.
    std::unique_ptr<StackFrame> stack;
    std::unique_ptr<StackFrame> temporaryStackFrame;
    bool returnValue = false;
    // This is the beginning of the function as in the pseudo-code; arguments are stored in checkTupleIndex, checkArgumentsBuffer, and checkArgumentIndexes.
    check_start:
    if ((this->m_incrementalReasoningState.getGlobalFlags(checkTupleIndex) & (GF_DISPROVED | GF_ADDED)) != GF_DISPROVED && this->m_incrementalReasoningState.template addCurrentLevelFlags<false>(checkTupleIndex, LF_CHECKED)) {
        if (callMonitor)
            this->m_incrementalMonitor->checkingProvabilityStarted(this->m_workerIndex, *checkArgumentsBuffer, *checkArgumentIndexes, true);
        temporaryStackFrame = getStackFrame(checkTupleIndex, *checkArgumentsBuffer, *checkArgumentIndexes, std::move(stack));
        stack = std::move(temporaryStackFrame);
        // We record all checked tuples so that we can identify disproved facts after recursion finishes.
        m_checkedTuples.push_back(stack->m_tupleIndex);
        // Saturate the consequences of the current element.
        if (saturate(threadContext, stack->m_tupleIndex, stack->m_currentTupleBuffer)) {
            stack->m_allProved = true;
            if (callMonitor)
                this->m_incrementalMonitor->tupleOptimized(this->m_workerIndex, stack->m_currentTupleBuffer, this->m_currentTupleArgumentIndexes);
            goto check_return;
        }
        // If we're optimizing equality, we check reflexivity and replacement rules
        if (::isEqualityReasoningMode(reasoningMode)) {
            // If this is an equality, we check the reflexivity rules.
            if (stack->m_currentTupleBuffer[1] == OWL_SAME_AS_ID) {
                for (stack->m_currentPositionIndex = 0; stack->m_currentPositionIndex < 3; ++stack->m_currentPositionIndex) {
                    stack->m_currentReflexivityCheckHelper = getReflexivityCheckHelper(stack->m_currentPositionIndex);
                    stack->m_currentReflexivityCheckHelper->m_currentTupleBuffer[stack->m_currentPositionIndex] = stack->m_currentTupleBuffer[0];
                    for (stack->m_multiplicity = stack->m_currentReflexivityCheckHelper->open(threadContext, this->m_incrementalReasoningState); stack->m_currentPositionIndex < 3 && stack->m_multiplicity != 0; stack->m_multiplicity = stack->m_currentReflexivityCheckHelper->advance(this->m_incrementalReasoningState)) {
                        // Exit the loop if all reflexivity tuples have been proved.
                        if (allReflexiveSameAsProved(threadContext, stack->m_currentTupleBuffer[0])) {
                            leaveReflexivityCheckHelper(std::move(stack->m_currentReflexivityCheckHelper), stack->m_currentPositionIndex);
                            if (callMonitor)
                                this->m_incrementalMonitor->reflexiveSameAsRuleInstancesOptimized(this->m_workerIndex, stack->m_currentTupleBuffer, this->m_currentTupleArgumentIndexes);
                            // In Java, we'd use "break <label>" to break of the outer loop. Since C++ doesn't support this, we use a goto.
                            // One could solve this problem with a flag, but we'd need to add it to BackingAtom due to recursion, and
                            // all this seems too complex for a goto convert!
                            goto check_replacement;
                        }
                        // Check the rule instance.
                        if (callMonitor)
                            this->m_incrementalMonitor->backwardReflexiveSameAsRuleInstanceStarted(this->m_workerIndex, stack->m_currentTupleBuffer, this->m_currentTupleArgumentIndexes, stack->m_currentReflexivityCheckHelper->m_currentTupleBuffer, this->m_currentTupleArgumentIndexes);
                        // Make the recursive call
                        checkTupleIndex = stack->m_currentReflexivityCheckHelper->m_tupleIterator->getCurrentTupleIndex();
                        checkArgumentsBuffer = &stack->m_currentReflexivityCheckHelper->m_currentTupleBuffer;
                        checkArgumentIndexes = &s_reflexivityCheckHelperArgumentIndexes;
                        stack->m_returnAddress = 1;
                        goto check_start;
                        // Return address 1
                        check_return_address_1:
                        if (callMonitor)
                            this->m_incrementalMonitor->backwardReflexiveSameAsRuleInstanceFinished(this->m_workerIndex);
                        // Exit the function if the tuple has been proved.
                        if (allProved(threadContext, stack->m_tupleIndex, stack->m_currentTupleBuffer, this->m_currentTupleArgumentIndexes)) {
                            leaveReflexivityCheckHelper(std::move(stack->m_currentReflexivityCheckHelper), stack->m_currentPositionIndex);
                            stack->m_allProved = true;
                            if (callMonitor)
                                this->m_incrementalMonitor->tupleOptimized(this->m_workerIndex, stack->m_currentTupleBuffer, this->m_currentTupleArgumentIndexes);
                            goto check_return;
                        }
                    }
                    leaveReflexivityCheckHelper(std::move(stack->m_currentReflexivityCheckHelper), stack->m_currentPositionIndex);
                }
            }
            // Check replacement rules
            check_replacement:
            for (stack->m_currentPositionIndex = 0; stack->m_currentPositionIndex < 3; ++stack->m_currentPositionIndex) {
                if (!this->isSingleton(stack->m_currentTupleBuffer[stack->m_currentPositionIndex]) && this->template checkReflexivity<2>(stack->m_currentTupleBuffer[stack->m_currentPositionIndex])) {
                    m_currentTupleBuffer8[1] = OWL_SAME_AS_ID;
                    m_currentTupleBuffer8[0] = m_currentTupleBuffer8[2] = stack->m_currentTupleBuffer[stack->m_currentPositionIndex];
                    // Check if the tuple is disproved.
                    checkTupleIndex = this->m_tripleTable.addTuple(threadContext, m_currentTupleBuffer8, this->m_currentTupleArgumentIndexes, 0, 0).second;
                    if ((this->m_incrementalReasoningState.getGlobalFlags(checkTupleIndex) & GF_DISPROVED) != GF_DISPROVED) {
                        if (callMonitor)
                            this->m_incrementalMonitor->backwardReplacementRuleInstanceStarted(this->m_workerIndex, stack->m_currentPositionIndex, m_currentTupleBuffer8, this->m_currentTupleArgumentIndexes);
                        // Make the recursive call; we already loaded checkTupleIndex earlier.
                        checkArgumentsBuffer = &m_currentTupleBuffer8;
                        checkArgumentIndexes = &s_reflexivityCheckHelperArgumentIndexes;
                        stack->m_returnAddress = 2;
                        goto check_start;
                        // Return address 2
                        check_return_address_2:
                        if (callMonitor)
                            this->m_incrementalMonitor->backwardReplacementRuleInstanceFinished(this->m_workerIndex);
                        // Exit the function if the tuple has been proved.
                        if (allProved(threadContext, stack->m_tupleIndex, stack->m_currentTupleBuffer, this->m_currentTupleArgumentIndexes)) {
                            stack->m_allProved = true;
                            if (callMonitor)
                                this->m_incrementalMonitor->tupleOptimized(this->m_workerIndex, stack->m_currentTupleBuffer, this->m_currentTupleArgumentIndexes);
                            goto check_return;
                        }
                    }
                }
            }
        }
        if (hasRecursiveRules) {
            // Now check the actual recursive rules. At the outer level, iterate through all indenxing patterns
            for (stack->m_currentIndexingPatternNumber = 0; stack->m_currentIndexingPatternNumber < 8; ++stack->m_currentIndexingPatternNumber) {
                for (stack->m_currentHeadAtomInfo = this->m_ruleIndex.getMatchingHeadAtomInfos(threadContext, stack->m_currentTupleBuffer, this->m_currentTupleArgumentIndexes, stack->m_currentIndexingPatternNumber); stack->m_currentHeadAtomInfo != nullptr; stack->m_currentHeadAtomInfo = stack->m_currentHeadAtomInfo->getNextMatchingHeadAtomInfo(stack->m_currentTupleBuffer, this->m_currentTupleArgumentIndexes)) {
                    // If rules are processed by component level, we process only recursive rules here; otherwise, we process all rules.
                    if (stack->m_currentHeadAtomInfo->template isRecursive<reasoningMode == REASONING_MODE_NO_EQUALITY_BY_LEVELS>()) {
                        if (callMonitor)
                            this->m_incrementalMonitor->backwardRecursiveRuleStarted(this->m_workerIndex, *stack->m_currentHeadAtomInfo);
                        stack->m_currentBodyMatches = stack->m_currentHeadAtomInfo->getSupportingFactsEvaluator(stack->m_currentTupleBuffer, this->m_currentTupleArgumentIndexes);
                        for (stack->m_multiplicity = stack->m_currentBodyMatches->open(threadContext); stack->m_multiplicity != 0; stack->m_multiplicity = stack->m_currentBodyMatches->advance()) {
                            if (callMonitor)
                                this->m_incrementalMonitor->backwardRecursiveRuleInstanceStarted(this->m_workerIndex, *stack->m_currentHeadAtomInfo, *stack->m_currentBodyMatches);
                            for (stack->m_currentBodyIndex = 0; stack->m_currentBodyIndex < stack->m_currentBodyMatches->getNumberOfBodyLiterals(); ++stack->m_currentBodyIndex) {
                                if (stack->m_currentHeadAtomInfo->template isSupportingBodyAtom<reasoningMode == REASONING_MODE_NO_EQUALITY_BY_LEVELS>(stack->m_currentBodyIndex)) {
                                    if (callMonitor)
                                        this->m_incrementalMonitor->backwardRecursiveRuleInstanceAtomStarted(this->m_workerIndex, *stack->m_currentHeadAtomInfo, *stack->m_currentBodyMatches, stack->m_currentBodyIndex);
                                    // Make the recursive call
                                    checkTupleIndex = stack->m_currentBodyMatches->getBodyLiteral(stack->m_currentBodyIndex).getCurrentTupleIndex();
                                    checkArgumentsBuffer = &stack->m_currentBodyMatches->getBodyLiteral(stack->m_currentBodyIndex).getArgumentsBuffer();
                                    checkArgumentIndexes = &stack->m_currentBodyMatches->getBodyLiteral(stack->m_currentBodyIndex).getArgumentIndexes();
                                    stack->m_returnAddress = 3;
                                    goto check_start;
                                    // Return address 3
                                    check_return_address_3:
                                    if (callMonitor)
                                        this->m_incrementalMonitor->backwardRecursiveRuleInstanceAtomFinished(this->m_workerIndex);
                                    // Exit the function if the tuple has been proved.
                                    if (allProved(threadContext, stack->m_tupleIndex, stack->m_currentTupleBuffer, this->m_currentTupleArgumentIndexes)) {
                                        stack->m_currentHeadAtomInfo->leaveSupportFactsEvaluator(std::move(stack->m_currentBodyMatches));
                                        stack->m_allProved = true;
                                        if (callMonitor) {
                                            this->m_incrementalMonitor->tupleOptimized(this->m_workerIndex, stack->m_currentTupleBuffer, this->m_currentTupleArgumentIndexes);
                                            this->m_incrementalMonitor->backwardRecursiveRuleInstanceFinished(this->m_workerIndex);
                                            this->m_incrementalMonitor->backwardRecursiveRuleFinished(this->m_workerIndex);
                                        }
                                        goto check_return;
                                    }
                                }
                            }
                            if (callMonitor)
                                this->m_incrementalMonitor->backwardRecursiveRuleInstanceFinished(this->m_workerIndex);
                        }
                        stack->m_currentHeadAtomInfo->leaveSupportFactsEvaluator(std::move(stack->m_currentBodyMatches));
                        if (callMonitor)
                            this->m_incrementalMonitor->backwardRecursiveRuleFinished(this->m_workerIndex);
                    }
                }
            }
        }
        // Return here
        check_return:
        returnValue = stack->m_allProved;
        temporaryStackFrame = std::move(stack);
        stack = std::move(temporaryStackFrame->m_previous);
        leaveStackFrame(std::move(temporaryStackFrame));
    }
    else {
        if (callMonitor)
            this->m_incrementalMonitor->checkingProvabilityStarted(this->m_workerIndex, *checkArgumentsBuffer, *checkArgumentIndexes, false);
        // In our pseudo-code, we compute return value here. However, this is useful only at the top of recursion
        // (the return value is ignored outherwise) so the following 'if' saves us unnecessary calls to allProved().
        if (stack.get() == nullptr)
            returnValue = allProved(threadContext, checkTupleIndex, *checkArgumentsBuffer, *checkArgumentIndexes);
    }
    if (callMonitor)
        this->m_incrementalMonitor->checkingProvabilityFinished(this->m_workerIndex);
    // We're done if there is nothing on the stack; otherwise, we continue recursion from the place as shown on the stack.
    if (stack.get() != nullptr) {
        switch (stack->m_returnAddress) {
        case 1:
            goto check_return_address_1;
        case 2:
            goto check_return_address_2;
        case 3:
            if (hasRecursiveRules)
                goto check_return_address_3;
            else
                UNREACHABLE;
        }
    }
    // This is executed after recursion finishes, and it determines which of the checked tuples are disproved.
    for (auto iterator = m_checkedTuples.begin(); iterator != m_checkedTuples.end(); ++iterator) {
        if (allDisproved(threadContext, *iterator)) {
            if (this->m_incrementalReasoningState.template addGlobalFlags<false>(*iterator, GF_DISPROVED)) {
                if (callMonitor) {
                    this->m_tripleTable.getStatusAndTuple(*iterator, m_currentTupleBuffer8);
                    this->m_incrementalMonitor->checkedTupleDisproved(this->m_workerIndex, m_currentTupleBuffer8, this->m_currentTupleArgumentIndexes, true);
                }
            }
            else {
                if (callMonitor) {
                    this->m_tripleTable.getStatusAndTuple(*iterator, m_currentTupleBuffer8);
                    this->m_incrementalMonitor->checkedTupleDisproved(this->m_workerIndex, m_currentTupleBuffer8, this->m_currentTupleArgumentIndexes, false);
                }
            }
        }
    }
    return returnValue;
}

template<bool callMonitor, ReasoningModeType reasoningMode, bool hasRecursiveRules>
void FBFDeletionTaskWorker<callMonitor, reasoningMode, hasRecursiveRules>::run(ThreadContext& threadContext) {
    // Set up the incremental filters.
    auto filter1 = this->m_ruleIndex.template setBodyLiteralInfoFilter<INCREMENTAL_POSITIVE_BEFORE_PIVOT>(*this, [](FBFDeletionTaskWorkerType& target, const BodyLiteralInfo& bodyLiteralInfo, const TupleIndex tupleIndex, const TupleStatus tupleStatus) {
        // Atoms from the previous component cannot be euqal to the current tuple and they are matched in I\(D\A); and atoms in the current component are matched in P.
        if (reasoningMode == REASONING_MODE_NO_EQUALITY_BY_LEVELS && bodyLiteralInfo.getComponentLevel() != target.m_componentLevel)
            return
                ((tupleStatus & (TUPLE_STATUS_IDB | TUPLE_STATUS_IDB_MERGED)) == TUPLE_STATUS_IDB) &&
                ((target.m_incrementalReasoningState.getGlobalFlags(tupleIndex) & (GF_DISPROVED | GF_ADDED)) != GF_DISPROVED);
        else
            return
                ((target.m_incrementalReasoningState.getCurrentLevelFlags(tupleIndex) & (LF_PROVED | LF_PROVED_MERGED)) == LF_PROVED) &&
                (target.m_currentTupleIndex2 != tupleIndex);
    });
    auto filter2 = this->m_ruleIndex.template setBodyLiteralInfoFilter<INCREMENTAL_POSITIVE_AFTER_PIVOT>(*this, [](FBFDeletionTaskWorkerType& target, const BodyLiteralInfo& bodyLiteralInfo, const TupleIndex tupleIndex, const TupleStatus tupleStatus) {
        // Atoms from the previous component cannot be euqal to the current tuple and they are matched in I\(D\A); and atoms in the current component are matched in P.
        if (reasoningMode == REASONING_MODE_NO_EQUALITY_BY_LEVELS && bodyLiteralInfo.getComponentLevel() != target.m_componentLevel)
            return
                ((tupleStatus & (TUPLE_STATUS_IDB | TUPLE_STATUS_IDB_MERGED)) == TUPLE_STATUS_IDB) &&
                ((target.m_incrementalReasoningState.getGlobalFlags(tupleIndex) & (GF_DISPROVED | GF_ADDED)) != GF_DISPROVED);
        else
            return
                ((target.m_incrementalReasoningState.getCurrentLevelFlags(tupleIndex) & (LF_PROVED | LF_PROVED_MERGED)) == LF_PROVED);
    });
    auto filter3 = this->m_ruleIndex.template setBodyLiteralInfoFilter<INCREMENTAL_NEGATIVE>(*this, [](FBFDeletionTaskWorkerType& target, const BodyLiteralInfo& bodyLiteralInfo, const TupleIndex tupleIndex, const TupleStatus tupleStatus) {
        // Condition is I+A.
        return
            ((tupleStatus & (TUPLE_STATUS_IDB | TUPLE_STATUS_IDB_MERGED)) == TUPLE_STATUS_IDB) ||
            ((target.m_incrementalReasoningState.getGlobalFlags(tupleIndex) & (GF_ADDED | GF_ADDED_MERGED)) == GF_ADDED);
    });
    auto filters4 = this->m_ruleIndex.template setTupleIteratorFilters<INCREMENTAL_TUPLE_ITERATOR_FILTER>(*this,
        [](FBFDeletionTaskWorkerType& target, const TupleIndex tupleIndex, const TupleStatus tupleStatus) {
            // Reevaluation is used only with equality so there are components, and atoms are matched in P.
            return
                ((target.m_incrementalReasoningState.getCurrentLevelFlags(tupleIndex) & (LF_PROVED | LF_PROVED_MERGED)) == LF_PROVED);
        },
        [](FBFDeletionTaskWorkerType& target, const TupleIndex tupleIndex, const TupleStatus tupleStatus) {
            // This should never be used since reevaluation is used only with equality, in which case there are no negative atoms.
            return false;
        }
    );
    auto filters5 = this->m_ruleIndex.template setTupleIteratorFilters<SUPPORTING_FACTS_TUPLE_ITERATOR_FILTER>(*this,
        [](FBFDeletionTaskWorkerType& target, const TupleIndex tupleIndex, const TupleStatus tupleStatus) {
            // Condition is I\(D\A).
            return
                ((tupleStatus & (TUPLE_STATUS_IDB | TUPLE_STATUS_IDB_MERGED)) == TUPLE_STATUS_IDB) &&
                ((target.m_incrementalReasoningState.getGlobalFlags(tupleIndex) & (GF_DISPROVED | GF_ADDED)) != GF_DISPROVED);
        },
        [](FBFDeletionTaskWorkerType& target, const TupleIndex tupleIndex, const TupleStatus tupleStatus) {
            // Condition is I+A.
            return
                ((tupleStatus & (TUPLE_STATUS_IDB | TUPLE_STATUS_IDB_MERGED)) == TUPLE_STATUS_IDB) ||
                ((target.m_incrementalReasoningState.getGlobalFlags(tupleIndex) & (GF_ADDED | GF_ADDED_MERGED)) == GF_ADDED);
        }
    );
    // Now do the usual thing.
    AbstractDeletionTaskWorker<FBFDeletionTaskWorkerType>::run(threadContext);
}
