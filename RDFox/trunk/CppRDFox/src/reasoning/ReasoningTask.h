// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef REASONINGTASK_H_
#define REASONINGTASK_H_

#include "../Common.h"
#include "DatalogEngine.h"

class ThreadContext;
class ReasoningTaskWorker;
class DatalogEngineWorker;

// ReasoningTask

class ReasoningTask : private Unmovable {

    friend class DatalogEngine;

protected:

    DatalogEngine& m_datalogEngine;
    size_t m_totalNumberOfWorkers;
    unique_ptr_vector<ReasoningTaskWorker> m_workers;

    virtual std::unique_ptr<ReasoningTaskWorker> doCreateWorker(DatalogEngineWorker& datalogEngineWorker) = 0;

    virtual void doInitialize() = 0;

public:

    ReasoningTask(DatalogEngine& datalogEngine);

    virtual ~ReasoningTask();

    always_inline size_t getTotalNumberOfWorkers() const {
        return m_totalNumberOfWorkers;
    }

    always_inline bool isMultithreaded() const {
        return getTotalNumberOfWorkers() > 1;
    }

    always_inline DatalogEngine& getDatalogEngine() {
        return m_datalogEngine;
    }

    always_inline void initialize(const size_t totalNumberOfWorkers) {
        m_totalNumberOfWorkers = totalNumberOfWorkers;
        doInitialize();
    }

    virtual ReasoningTaskWorker* createWorker(DatalogEngineWorker& datalogEngineWorker);

};

// ReasoningTaskWorker

class ReasoningTaskWorker : private Unmovable {

public:

    virtual ~ReasoningTaskWorker();

    virtual void run(ThreadContext& threadContext) = 0;

    virtual void stop() = 0;

};

#endif /* REASONINGTASK_H_ */
