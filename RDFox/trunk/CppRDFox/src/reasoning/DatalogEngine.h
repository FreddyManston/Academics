// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef DATALOGENGINE_H_
#define DATALOGENGINE_H_

#include "../Common.h"
#include "../logic/Logic.h"
#include "../util/Mutex.h"
#include "../util/Condition.h"
#include "../util/LockFreeQueue.h"
#include "../util/LockFreeBitSet.h"
#include "RuleIndex.h"

class InputStream;
class OutputStream;
class DataStore;
class DatalogEngineWorker;
class ReasoningTask;
class MaterializationMonitor;
class IncrementalMonitor;
class IncrementalReasoningState;

class DatalogEngine : private Unmovable {

protected:

    friend class DatalogEngineWorker;

    DataStore& m_dataStore;
    unique_ptr_vector<DatalogEngineWorker> m_workers;
    RuleIndex m_ruleIndex;
    Mutex m_mutex;
    Condition m_condition;
    aligned_size_t m_numberOfRunningWorkers;

    void notifyWorkerIdle();

public:

    DatalogEngine(DataStore& dataStore);

    ~DatalogEngine();

    always_inline DataStore& getDataStore() {
        return m_dataStore;
    }

    always_inline RuleIndex& getRuleIndex() {
        return m_ruleIndex;
    }

    always_inline const RuleIndex& getRuleIndex() const {
        return m_ruleIndex;
    }

    void setNumberOfThreads(const size_t numberOfThreads);

    size_t getNumberOfThreads() const;

    bool executeReasoningTask(ReasoningTask& reasoningTask);

    bool executeReasoningTaskSerially(ReasoningTask& reasoningTask);

    void stopReasoningTask();

    void clearRules();

    bool addRules(const DatalogProgram& datalogProgram);

    bool removeRules(const DatalogProgram& datalogProgram);

    void recompileRules();

    bool applyRules(const bool processComponentsByLevels, MaterializationMonitor* const materializationMonitor);

    bool applyRulesByLevels(MaterializationMonitor* const materializationMonitor);

    bool applyRulesNoLevels(MaterializationMonitor* const materializationMonitor);

    bool applyRulesIncrementally(const bool processComponentsByLevels, const bool useDRedAlgorithm, IncrementalMonitor* const incrementalMonitor);

    bool applyRulesIncrementallyToLevel(const bool useDRedAlgorithm, IncrementalMonitor* const incrementalMonitor, IncrementalReasoningState& incrementalReasoningState, const size_t componentLevel, LockFreeQueue<RuleInfo*>* const insertedRulesQueue);

    bool applyRulesIncrementallyByLevels(const bool useDRedAlgorithm, IncrementalMonitor* const incrementalMonitor);

    bool applyRulesIncrementallyNoLevels(const bool useDRedAlgorithm, IncrementalMonitor* const incrementalMonitor);

    void save(OutputStream& outputStream) const;

    void load(InputStream& inputStream);

};

#endif /* DATALOGENGINE_H_ */
