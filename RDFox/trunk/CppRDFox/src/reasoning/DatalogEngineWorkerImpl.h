// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef DATALOGENGINEWORKERIMPL_H_
#define DATALOGENGINEWORKERIMPL_H_

#include "../RDFStoreException.h"
#include "../util/ThreadContext.h"
#include "DatalogEngine.h"
#include "DatalogEngineWorker.h"
#include "ReasoningTask.h"

DatalogEngineWorker::DatalogEngineWorker(DatalogEngine& datalogEngine, const size_t workerIndex) :
    m_datalogEngine(datalogEngine),
    m_workerIndex(workerIndex),
    m_mutex(),
    m_condition(),
    m_shouldTerminate(false),
    m_workerState(IDLE),
    m_reasoningTaskWorker(0),
    m_workerError()
{
}

always_inline std::exception_ptr DatalogEngineWorker::extractWorkerError() {
    std::exception_ptr result = m_workerError;
    m_workerError = nullptr;
    return result;
}

void DatalogEngineWorker::run() {
    ThreadContext& threadContext = ThreadContext::getCurrentThreadContext();
    while (true) {
        ReasoningTaskWorker* reasoningTaskWorker = nullptr;
        m_mutex.lock();
        while (!m_shouldTerminate && m_workerState != REASONING_STARTING)
            m_condition.wait(m_mutex);
        bool shouldTerminate = m_shouldTerminate;
        if (m_workerState == REASONING_STARTING) {
            m_workerState = REASONING;
            reasoningTaskWorker = m_reasoningTaskWorker;
        }
        m_workerError = nullptr;
        m_mutex.unlock();
        if (shouldTerminate)
            break;
        else if (reasoningTaskWorker) {
            try {
                reasoningTaskWorker->run(threadContext);
            }
            catch (...) {
                m_mutex.lock();
                m_workerError = std::current_exception();
                m_mutex.unlock();
                m_datalogEngine.stopReasoningTask();
            }
            m_mutex.lock();
            assert(m_workerState == REASONING);
            m_workerState = IDLE;
            m_reasoningTaskWorker = 0;
            m_mutex.unlock();
            m_datalogEngine.notifyWorkerIdle();
        }
    }
}

always_inline void DatalogEngineWorker::initializeWorker(ReasoningTaskWorker* reasoningTaskWorker) {
    MutexHolder mutexHolder(m_mutex);
    assert(m_workerState == IDLE);
    m_workerError = nullptr;
    m_reasoningTaskWorker = reasoningTaskWorker;
    m_workerState = REASONING_TASK_LOADED;
}

always_inline void DatalogEngineWorker::startReasoning() {
    MutexHolder mutexHolder(m_mutex);
    assert(m_workerState == REASONING_TASK_LOADED);
    m_workerState = REASONING_STARTING;
    m_condition.signalAll();
}

always_inline void DatalogEngineWorker::stopReasoning() {
    MutexHolder mutexHolder(m_mutex);
    if (m_reasoningTaskWorker != 0)
        m_reasoningTaskWorker->stop();
}

always_inline void DatalogEngineWorker::terminate() {
    MutexHolder mutexHolder(m_mutex);
    m_shouldTerminate = true;
    m_condition.signalAll();
}

#endif /* DATALOGENGINEWORKERIMPL_H_ */
