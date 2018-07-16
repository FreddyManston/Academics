#include "ReasoningTask.h"

// ReasoningTask

ReasoningTask::ReasoningTask(DatalogEngine& datalogEngine) :
    m_datalogEngine(datalogEngine),
    m_totalNumberOfWorkers(0),
    m_workers()
{
}

ReasoningTask::~ReasoningTask() {
}

ReasoningTaskWorker* ReasoningTask::createWorker(DatalogEngineWorker& datalogEngineWorker) {
    m_workers.push_back(doCreateWorker(datalogEngineWorker));
    return m_workers.back().get();
}

// ReasoningTaskWorker

ReasoningTaskWorker::~ReasoningTaskWorker() {
}
