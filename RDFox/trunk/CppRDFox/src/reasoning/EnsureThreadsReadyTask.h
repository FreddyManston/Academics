// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef ENSURETHREADSREADYTASK_H_
#define ENSURETHREADSREADYTASK_H_

#include "ReasoningTask.h"

// EnsureThreadsReadyTask

class EnsureThreadsReadyTask : public ReasoningTask {

protected:

    virtual std::unique_ptr<ReasoningTaskWorker> doCreateWorker(DatalogEngineWorker& datalogEngineWorker);

    virtual void doInitialize();

public:

    EnsureThreadsReadyTask(DatalogEngine& datalogEngine);

};

// EnsureThreadsReadyTaskWorker

class EnsureThreadsReadyTaskWorker : public ReasoningTaskWorker {

protected:

    EnsureThreadsReadyTask& m_ensureThreadsReadyTask;
    const size_t m_workerIndex;

public:

    EnsureThreadsReadyTaskWorker(EnsureThreadsReadyTask& ensureThreadsReadyTask, DatalogEngineWorker& datalogEngineWorker);

    virtual void run(ThreadContext& threadContext);

    virtual void stop();

};

#endif /* ENSURETHREADSREADYTASK_H_ */
