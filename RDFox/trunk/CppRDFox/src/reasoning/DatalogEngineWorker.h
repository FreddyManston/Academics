// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef DATALOGENGINEWORKER_H_
#define DATALOGENGINEWORKER_H_

#include "../Common.h"
#include "../util/Thread.h"
#include "../util/Mutex.h"
#include "../util/Condition.h"

class DatalogEngine;
class ReasoningTaskWorker;

class DatalogEngineWorker : public Thread {

protected:

    friend class DatalogEngine;

    enum WorkerState { IDLE, REASONING_TASK_LOADED, REASONING_STARTING, REASONING };

    DatalogEngine& m_datalogEngine;
    const size_t m_workerIndex;
    Mutex m_mutex;
    Condition m_condition;
    bool m_shouldTerminate;
    WorkerState m_workerState;
    ReasoningTaskWorker* m_reasoningTaskWorker;
    std::exception_ptr m_workerError;

public:

    DatalogEngineWorker(DatalogEngine& datalogEngine, const size_t workerIndex);

    always_inline size_t getWorkerIndex() const {
        return m_workerIndex;
    }

    std::exception_ptr extractWorkerError();

    virtual void run();

    void initializeWorker(ReasoningTaskWorker* reasoningTaskWorker);

    void startReasoning();

    void stopReasoning();

    void terminate();

};

#endif /* DATALOGENGINEWORKER_H_ */
