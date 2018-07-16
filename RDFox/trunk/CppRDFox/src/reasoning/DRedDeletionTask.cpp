// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "AbstractDeletionTaskImpl.h"
#include "DRedDeletionTask.h"

// DRedDeletionTask

template<bool callMonitor>
always_inline std::unique_ptr<ReasoningTaskWorker> DRedDeletionTask::doCreateWorker1(DatalogEngineWorker& datalogEngineWorker) {
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
always_inline std::unique_ptr<ReasoningTaskWorker> DRedDeletionTask::doCreateWorker2(DatalogEngineWorker& datalogEngineWorker) {
    if (m_datalogEngine.getRuleIndex().hasRecursiveRules(m_componentLevel))
        return doCreateWorker3<callMonitor, reasoningMode, true>(datalogEngineWorker);
    else
        return doCreateWorker3<callMonitor, reasoningMode, false>(datalogEngineWorker);
}

template<bool callMonitor, ReasoningModeType reasoningMode, bool hasRecursiveRules>
always_inline std::unique_ptr<ReasoningTaskWorker> DRedDeletionTask::doCreateWorker3(DatalogEngineWorker& datalogEngineWorker) {
    if (isMultithreaded())
        return std::unique_ptr<ReasoningTaskWorker>(new DRedDeletionTaskWorker<callMonitor, reasoningMode, hasRecursiveRules, true>(*this, datalogEngineWorker));
    else
        return std::unique_ptr<ReasoningTaskWorker>(new DRedDeletionTaskWorker<callMonitor, reasoningMode, hasRecursiveRules, false>(*this, datalogEngineWorker));
}

std::unique_ptr<ReasoningTaskWorker> DRedDeletionTask::doCreateWorker(DatalogEngineWorker& datalogEngineWorker) {
    if (m_incrementalMonitor == nullptr)
        return doCreateWorker1<false>(datalogEngineWorker);
    else
        return doCreateWorker1<true>(datalogEngineWorker);
}

DRedDeletionTask::DRedDeletionTask(DatalogEngine& datalogEngine, IncrementalMonitor* const incrementalMonitor, IncrementalReasoningState& incrementalReasoningState, const size_t componentLevel) : AbstractDeletionTask(datalogEngine, incrementalMonitor, incrementalReasoningState, componentLevel) {
}

// DReadDeletionTaskWorker

template<bool callMonitor, ReasoningModeType reasoningMode, bool hasRecursiveRules, bool multithreaded>
DRedDeletionTaskWorker<callMonitor, reasoningMode, hasRecursiveRules, multithreaded>::DRedDeletionTaskWorker(DRedDeletionTask& dredDeletionTask, DatalogEngineWorker& datalogEngineWorker) :
    AbstractDeletionTaskWorker<DRedDeletionTaskWorker<callMonitor, reasoningMode, hasRecursiveRules, multithreaded> >(dredDeletionTask, datalogEngineWorker)
{
}

template<bool callMonitor, ReasoningModeType reasoningMode, bool hasRecursiveRules, bool multithreaded>
always_inline bool DRedDeletionTaskWorker<callMonitor, reasoningMode, hasRecursiveRules, multithreaded>::check(ThreadContext& threadContext, TupleIndex checkTupleIndex, const std::vector<ResourceID>* checkArgumentsBuffer, const std::vector<ArgumentIndex>* checkArgumentIndexes) {
    return false;
}
