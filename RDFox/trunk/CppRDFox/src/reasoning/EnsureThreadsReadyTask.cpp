// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "DatalogEngine.h"
#include "DatalogEngineWorker.h"
#include "EnsureThreadsReadyTask.h"

// EnsureThreadsReadyTask

std::unique_ptr<ReasoningTaskWorker> EnsureThreadsReadyTask::doCreateWorker(DatalogEngineWorker& datalogEngineWorker) {
    return std::unique_ptr<ReasoningTaskWorker>(new EnsureThreadsReadyTaskWorker(*this, datalogEngineWorker));
}

void EnsureThreadsReadyTask::doInitialize() {
}

EnsureThreadsReadyTask::EnsureThreadsReadyTask(DatalogEngine& datalogEngine) : ReasoningTask(datalogEngine) {
}

// EnsureThreadsReadyTaskWorker

EnsureThreadsReadyTaskWorker::EnsureThreadsReadyTaskWorker(EnsureThreadsReadyTask& ensureThreadsReadyTask, DatalogEngineWorker& datalogEngineWorker) :
    m_ensureThreadsReadyTask(ensureThreadsReadyTask),
    m_workerIndex(datalogEngineWorker.getWorkerIndex())
{
}

void EnsureThreadsReadyTaskWorker::run(ThreadContext& threadContext) {
    m_ensureThreadsReadyTask.getDatalogEngine().getRuleIndex().ensureThreadReady(m_workerIndex);
}

void EnsureThreadsReadyTaskWorker::stop() {
}
