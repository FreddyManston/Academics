// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../storage/TupleTable.h"
#include "../storage/DataStore.h"
#include "../equality/EqualityManager.h"
#include "../util/LockFreeQueue.h"
#include "../util/Vocabulary.h"
#include "IncrementalMonitor.h"
#include "DatalogEngine.h"
#include "DatalogEngineWorkerImpl.h"
#include "RuleIndexImpl.h"
#include "ReasoningTask.h"
#include "EnsureThreadsReadyTask.h"
#include "MaterializationTask.h"
#include "IncrementalReasoningState.h"
#include "InitializeDeletedTask.h"
#include "EvaluateDeletionQueueTask.h"
#include "FBFDeletionTask.h"
#include "DRedDeletionTask.h"
#include "DRedRederivationTask.h"
#include "InitializeInsertedTask.h"
#include "EvaluateInsertionQueueTask.h"
#include "InsertionTask.h"
#include "UpdateEqualityManagerTask.h"
#include "PropagateChangesTask.h"

always_inline void DatalogEngine::notifyWorkerIdle() {
    MutexHolder mutexHolder(m_mutex);
    if (--m_numberOfRunningWorkers == 0)
        m_condition.signalAll();
}

DatalogEngine::DatalogEngine(DataStore& dataStore) :
    m_dataStore(dataStore),
    m_workers(),
    m_ruleIndex(m_dataStore),
    m_mutex(),
    m_condition(),
    m_numberOfRunningWorkers(0)
{
}

DatalogEngine::~DatalogEngine() {
     for (size_t workerIndex = 0; workerIndex < m_workers.size(); workerIndex++) {
         m_workers[workerIndex]->terminate();
         m_workers[workerIndex]->join();
     }
}

void DatalogEngine::setNumberOfThreads(const size_t numberOfThreads) {
    if (numberOfThreads != 0) {
        MutexHolder mutexHolder(m_mutex);
        m_ruleIndex.setThreadCapacity(numberOfThreads);
        while (m_workers.size() > numberOfThreads) {
            m_workers.back()->terminate();
            m_workers.back()->join();
            m_workers.pop_back();
        }
        while (m_workers.size() < numberOfThreads) {
            const size_t threadIndex = m_workers.size();
            m_workers.push_back(std::unique_ptr<DatalogEngineWorker>(new DatalogEngineWorker(*this, threadIndex)));
            m_workers.back()->start();
        }
    }
    EnsureThreadsReadyTask ensureThreadsReadyTask(*this);
    executeReasoningTask(ensureThreadsReadyTask);
}

size_t  DatalogEngine::getNumberOfThreads() const {
    return m_workers.size();
}

bool DatalogEngine::executeReasoningTask(ReasoningTask& reasoningTask) {
    MutexHolder mutexHolder(m_mutex);
    if (m_numberOfRunningWorkers == 0) {
        reasoningTask.initialize(m_workers.size());
        for (unique_ptr_vector<DatalogEngineWorker>::iterator iterator = m_workers.begin(); iterator != m_workers.end(); ++iterator)
            (*iterator)->initializeWorker(reasoningTask.createWorker(*(*iterator)));
        for (unique_ptr_vector<DatalogEngineWorker>::iterator iterator = m_workers.begin(); iterator != m_workers.end(); ++iterator) {
            ++m_numberOfRunningWorkers;
            (*iterator)->startReasoning();
        }
        while (m_numberOfRunningWorkers != 0)
            m_condition.wait(m_mutex);
        std::vector<std::exception_ptr> workerErrors;
        for (size_t workerIndex = 0; workerIndex < m_workers.size(); workerIndex++) {
            std::exception_ptr workerError = m_workers[workerIndex]->extractWorkerError();
            if (workerError != nullptr)
                workerErrors.push_back(workerError);
        }
        if (!workerErrors.empty())
            throw RDF_STORE_EXCEPTION_WITH_CAUSES("Error(s) were encounted on worker thread(s).", workerErrors);
        return true;
    }
    else
        return false;
}

bool DatalogEngine::executeReasoningTaskSerially(ReasoningTask& reasoningTask) {
    MutexHolder mutexHolder(m_mutex);
    if (m_numberOfRunningWorkers == 0) {
        reasoningTask.initialize(1);
        DatalogEngineWorker& datalogEngineWorker = *m_workers[0];
        datalogEngineWorker.initializeWorker(reasoningTask.createWorker(datalogEngineWorker));
        ++m_numberOfRunningWorkers;
        datalogEngineWorker.startReasoning();
        while (m_numberOfRunningWorkers != 0)
            m_condition.wait(m_mutex);
        std::exception_ptr workerError = datalogEngineWorker.extractWorkerError();
        if (workerError != nullptr) {
            std::vector<std::exception_ptr> workerErrors;
            workerErrors.push_back(workerError);
            throw RDF_STORE_EXCEPTION_WITH_CAUSES("An error was encounted during worker execution.", workerErrors);
        }
        return true;
    }
    else
        return false;
}

void DatalogEngine::stopReasoningTask() {
    MutexHolder mutexHolder(m_mutex);
    for (unique_ptr_vector<DatalogEngineWorker>::iterator iterator = m_workers.begin(); iterator != m_workers.end(); ++iterator)
        (*iterator)->stopReasoning();
}

void DatalogEngine::clearRules() {
    m_ruleIndex.initialize();
    if (m_dataStore.getEqualityAxiomatizationType() != EQUALITY_AXIOMATIZATION_OFF) {
        LogicFactory logicFactory(::newLogicFactory());
        Variable X = logicFactory->getVariable("X");
        Atom bodyAtom = logicFactory->getRDFAtom(X, logicFactory->getIRIReference(OWL_DIFFERENT_FROM), X);
        Atom headAtom = logicFactory->getRDFAtom(X, logicFactory->getIRIReference(RDF_TYPE), logicFactory->getIRIReference(OWL_NOTHING));
        std::vector<Literal> bodyAtoms;
        bodyAtoms.push_back(bodyAtom);
        Rule differentFromRule = logicFactory->getRule(headAtom, bodyAtoms);
        m_ruleIndex.addRule(m_workers.size(), differentFromRule, true);
    }
}

bool DatalogEngine::addRules(const DatalogProgram& datalogProgram) {
    bool hasChange = false;
    for (DatalogProgram::const_iterator iterator = datalogProgram.begin(); iterator != datalogProgram.end(); ++iterator)
        if (m_ruleIndex.addRule(m_workers.size(), *iterator, false))
            hasChange = true;
    if (hasChange && m_workers.size() > 1)
        setNumberOfThreads(m_workers.size());
    return hasChange;
}

bool DatalogEngine::removeRules(const DatalogProgram& datalogProgram) {
    bool hasChange = false;
    for (DatalogProgram::const_iterator iterator = datalogProgram.begin(); iterator != datalogProgram.end(); ++iterator)
        if (m_ruleIndex.removeRule(*iterator))
            hasChange = true;
    return hasChange;
}

void DatalogEngine::recompileRules() {
    m_ruleIndex.recompileRules();
    setNumberOfThreads(m_workers.size());
}

bool DatalogEngine::applyRules(const bool processComponentsByLevels, MaterializationMonitor* const materializationMonitor) {
    if (processComponentsByLevels && m_dataStore.getEqualityAxiomatizationType() == EQUALITY_AXIOMATIZATION_OFF)
        return applyRulesByLevels(materializationMonitor);
    else
        return applyRulesNoLevels(materializationMonitor);
}

bool DatalogEngine::applyRulesByLevels(MaterializationMonitor* const materializationMonitor) {
    assert(m_dataStore.getEqualityAxiomatizationType() == EQUALITY_AXIOMATIZATION_OFF);
    m_ruleIndex.forgetTemporaryConstants();
    m_ruleIndex.propagateDeletions();
    m_ruleIndex.propagateInsertions();
    if (!m_ruleIndex.isStratified()) {
        std::ostringstream message;
        message << "The program is not stratified because these components of the dependency graph contain cycles through negation and/or aggregation:" << std::endl;
        const std::vector<std::vector<const DependencyGraphNode*> >& unstratifiedComponents = m_ruleIndex.getUnstratifiedComponents();
        size_t componentNumber = 1;
        for (auto componentIterator = unstratifiedComponents.begin(); componentIterator != unstratifiedComponents.end(); ++componentIterator) {
            message << "-- Component " << (componentNumber++) << " --" << std::endl;
            const std::vector<const DependencyGraphNode*>& componentNodes = *componentIterator;
            for (auto nodeIterator = componentNodes.begin(); nodeIterator != componentNodes.end(); ++nodeIterator) {
                const std::vector<HeadAtomInfo*>& headAtomInfos = (*nodeIterator)->getHeadAtomInfos();
                for (auto headAtomInfoIterator = headAtomInfos.begin(); headAtomInfoIterator != headAtomInfos.end(); ++headAtomInfoIterator)
                    message << "    " << (*headAtomInfoIterator)->getRuleInfo().getRule()->toString(Prefixes::s_defaultPrefixes) << std::endl;
            }
        }
        message << "------------------------------------------------------------" << std::endl;
        throw RDF_STORE_EXCEPTION(message.str());
    }
    if (materializationMonitor != nullptr)
        materializationMonitor->taskStarted(m_dataStore, m_ruleIndex.getMaxComponentLevel());
    if (m_ruleIndex.getFirstRuleComponentLevel() <= m_ruleIndex.getMaxComponentLevel()) {
        for (size_t componentLevel = m_ruleIndex.getFirstRuleComponentLevel(); componentLevel <= m_ruleIndex.getMaxComponentLevel(); ++componentLevel) {
            if (materializationMonitor != nullptr)
                materializationMonitor->componentLevelStarted(componentLevel);
            MaterializationTask materializationTask(*this, materializationMonitor, componentLevel);
            if (!executeReasoningTask(materializationTask))
                return false;
            if (materializationMonitor != nullptr)
                materializationMonitor->componentLevelFinished(componentLevel);
        }
    }
    if (materializationMonitor != nullptr)
        materializationMonitor->taskFinished(m_dataStore);
    return true;
}

bool DatalogEngine::applyRulesNoLevels(MaterializationMonitor* const materializationMonitor) {
    if (m_ruleIndex.hasRulesWithNegation(static_cast<size_t>(-1)) || m_ruleIndex.hasRulesWithAggregation(static_cast<size_t>(-1)))
        throw RDF_STORE_EXCEPTION("The rules cannot the applied in a single stratum because they contain negation or aggregation.");
    m_ruleIndex.forgetTemporaryConstants();
    m_ruleIndex.propagateDeletions();
    m_ruleIndex.propagateInsertions();
    if (materializationMonitor != nullptr)
        materializationMonitor->taskStarted(m_dataStore, 0);
    if (materializationMonitor != nullptr)
        materializationMonitor->componentLevelStarted(static_cast<size_t>(-1));
    MaterializationTask materializationTask(*this, materializationMonitor, static_cast<size_t>(-1));
    if (!executeReasoningTask(materializationTask))
        return false;
    if (materializationMonitor != nullptr)
        materializationMonitor->componentLevelFinished(static_cast<size_t>(-1));
    if (materializationMonitor != nullptr)
        materializationMonitor->taskFinished(m_dataStore);
    return true;
}

bool DatalogEngine::applyRulesIncrementally(const bool processComponentsByLevels, const bool useDRedAlgorithm, IncrementalMonitor* const incrementalMonitor) {
    if (processComponentsByLevels && m_dataStore.getEqualityAxiomatizationType() == EQUALITY_AXIOMATIZATION_OFF)
        return applyRulesIncrementallyByLevels(useDRedAlgorithm, incrementalMonitor);
    else
        return applyRulesIncrementallyNoLevels(useDRedAlgorithm, incrementalMonitor);
}

always_inline bool DatalogEngine::applyRulesIncrementallyToLevel(const bool useDRedAlgorithm, IncrementalMonitor* const incrementalMonitor, IncrementalReasoningState& incrementalReasoningState, const size_t componentLevel, LockFreeQueue<RuleInfo*>* const insertedRulesQueue) {
    // Start processing level.
    if (incrementalMonitor != nullptr)
        incrementalMonitor->componentLevelStarted(componentLevel);
    incrementalReasoningState.initializeCurrentLevel();
    // Before calling the usual reasoning, evaluate the rules that were inserted into this component.
    if (insertedRulesQueue != nullptr) {
        EvaluateInsertionQueueTask evaluateInsertionQueueTask(*this, incrementalMonitor, incrementalReasoningState, *insertedRulesQueue, componentLevel);
        if (!executeReasoningTask(evaluateInsertionQueueTask))
            return false;
    }
    // Apply deletions.
    if (useDRedAlgorithm) {
        // Overdelete...
        DRedDeletionTask dredDeletionTask(*this, incrementalMonitor, incrementalReasoningState, componentLevel);
        if (!executeReasoningTask(dredDeletionTask))
            return false;
        // ... and then rederive.
        DRedRederivationTask dredRederivationTask(*this, incrementalMonitor, incrementalReasoningState, componentLevel);
        if (!executeReasoningTask(dredRederivationTask))
            return false;
    }
    else {
        // FBF has exact deletion, so we just do that.
        FBFDeletionTask fbfDeletionTask(*this, incrementalMonitor, incrementalReasoningState, componentLevel);
        if (!executeReasoningTaskSerially(fbfDeletionTask))
            return false;
    }
    // Apply insertions.
    InsertionTask insertionTask(*this, incrementalMonitor, incrementalReasoningState, componentLevel);
    if (!executeReasoningTask(insertionTask))
        return false;
    // Record the positions of the deleted and the added lists at the end of the level.
    incrementalReasoningState.getDeleteListEnd(componentLevel) = incrementalReasoningState.getDeleteList().getFirstFreePosition();
    incrementalReasoningState.getAddedListEnd(componentLevel) = incrementalReasoningState.getAddedList().getFirstFreePosition();
    // Level processing finished.
    if (incrementalMonitor != nullptr)
        incrementalMonitor->componentLevelFinished(componentLevel);
    return true;
}

bool DatalogEngine::applyRulesIncrementallyByLevels(const bool useDRedAlgorithm, IncrementalMonitor* const incrementalMonitor) {
    if (m_ruleIndex.hasRulesWithAggregation(static_cast<size_t>(-1)))
        throw RDF_STORE_EXCEPTION("Incremental reasoning is currently not supported when rules contain aggregation.");
    assert(m_dataStore.getEqualityAxiomatizationType() == EQUALITY_AXIOMATIZATION_OFF);
    if (incrementalMonitor != nullptr)
        incrementalMonitor->taskStarted(m_dataStore, m_ruleIndex.getMaxComponentLevel());
    TupleTable& tripleTable = m_dataStore.getTupleTable("internal$rdf");
    LockFreeQueue<TupleIndex>& edbDeleteList = const_cast<LockFreeQueue<TupleIndex>&>(tripleTable.getTupleIndexesScheduledForDeletion());
    LockFreeQueue<TupleIndex>& edbInsertList = const_cast<LockFreeQueue<TupleIndex>&>(tripleTable.getTupleIndexesScheduledForAddition());
    // Create the object that keeps the incremental reasoning state.
    IncrementalReasoningState incrementalReasoningState(m_dataStore.getMemoryManager());
    incrementalReasoningState.initializeGlobal(m_ruleIndex.getMaxComponentLevel());
    // Evaluate rules that will be deleted. The following will construct all consequences of these rules,
    // so we can delete the rules after their evaluation.
    if (m_ruleIndex.hasJustDeletedRules()) {
        EvaluateDeletionQueueTask evaluateDeletionQueueTask(*this, incrementalMonitor, incrementalReasoningState, true);
        if (!executeReasoningTask(evaluateDeletionQueueTask))
            return false;
        m_ruleIndex.propagateDeletions();
    }
    // Process EDB deletions.
    InitializeDeletedTask initializeDeletedTask(*this, incrementalReasoningState, edbDeleteList, true);
    if (!executeReasoningTask(initializeDeletedTask))
        return false;
    // Process EDB insertions.
    InitializeInsertedTask initializeInsertedTask(*this, incrementalReasoningState, edbInsertList, true);
    if (!executeReasoningTask(initializeInsertedTask))
        return false;
    // Process components.
    for (size_t componentLevel = 0; componentLevel <= m_ruleIndex.getMaxComponentLevel(); ++componentLevel)
        if (!applyRulesIncrementallyToLevel(useDRedAlgorithm, incrementalMonitor, incrementalReasoningState, componentLevel, nullptr))
            return false;
    // Propagate changes to the database.
    PropagateChangesTask propagateChangesTask(*this, incrementalMonitor, incrementalReasoningState, true);
    if (!executeReasoningTask(propagateChangesTask))
        return false;
    // Evaluate rules that will be inserted. This is done without any components.
    if (m_ruleIndex.hasJustAddedRules()) {
        // Load the queue with all the rules and propagate all the changes so that the rules are active and included into the dependency graph.
        LockFreeQueue<RuleInfo*> insertedRulesQueue(m_dataStore.getMemoryManager());
        insertedRulesQueue.initializeLarge();
        m_ruleIndex.enqueueInsertedRules(insertedRulesQueue);
        m_ruleIndex.propagateInsertions();
        // Reset the reasoning state. We do this after propagating insertions so that the maximum component level gets updated.
        incrementalReasoningState.initializeGlobal(m_ruleIndex.getMaxComponentLevel());
        incrementalReasoningState.initializeCurrentLevel();
        // Now go through all components and repeat the process from before.
        for (size_t componentLevel = 0; componentLevel <= m_ruleIndex.getMaxComponentLevel(); ++componentLevel)
            if (!applyRulesIncrementallyToLevel(useDRedAlgorithm, incrementalMonitor, incrementalReasoningState, componentLevel, &insertedRulesQueue))
                return false;
        // Propagate changes again. There is no need to create a new instance of the task.
        if (!executeReasoningTask(propagateChangesTask))
            return false;
    }
    else {
        // This updates the dependency graph in case there were rule deletions.
        m_ruleIndex.propagateInsertions();
    }
    // Wipe the incremental changes buffers.
    if (!edbDeleteList.initializeLarge())
        throw RDF_STORE_EXCEPTION("Cannot reinitialize the EDB deletion list.");
    if (!edbInsertList.initializeLarge())
        throw RDF_STORE_EXCEPTION("Cannot reinitialize the EDB insertion list.");
    if (incrementalMonitor != nullptr)
        incrementalMonitor->taskFinished(m_dataStore);
    return true;
}

bool DatalogEngine::applyRulesIncrementallyNoLevels(const bool useDRedAlgorithm, IncrementalMonitor* const incrementalMonitor) {
    if (m_ruleIndex.hasRulesWithNegation(static_cast<size_t>(-1)) || m_ruleIndex.hasRulesWithAggregation(static_cast<size_t>(-1)))
        throw RDF_STORE_EXCEPTION("The rules cannot the applied in a single stratum because they contain negation or aggregation.");
    if (incrementalMonitor != nullptr)
        incrementalMonitor->taskStarted(m_dataStore, 0);
    TupleTable& tripleTable = m_dataStore.getTupleTable("internal$rdf");
    LockFreeQueue<TupleIndex>& edbDeleteList = const_cast<LockFreeQueue<TupleIndex>&>(tripleTable.getTupleIndexesScheduledForDeletion());
    LockFreeQueue<TupleIndex>& edbInsertList = const_cast<LockFreeQueue<TupleIndex>&>(tripleTable.getTupleIndexesScheduledForAddition());
    // Create the object that keeps the incremental reasoning state.
    IncrementalReasoningState incrementalReasoningState(m_dataStore.getMemoryManager());
    incrementalReasoningState.initializeGlobal(0);
    // Evaluate rules that will be deleted. The following will construct all consequences of these rules,
    // so we can delete the rules after their evaluation.
    if (m_ruleIndex.hasJustDeletedRules()) {
        EvaluateDeletionQueueTask evaluateDeletionQueueTask(*this, incrementalMonitor, incrementalReasoningState, false);
        if (!executeReasoningTask(evaluateDeletionQueueTask))
            return false;
        m_ruleIndex.propagateDeletions();
    }
    // Process EDB deletions.
    InitializeDeletedTask initializeDeletedTask(*this, incrementalReasoningState, edbDeleteList, false);
    if (!executeReasoningTask(initializeDeletedTask))
        return false;
    // Process EDB insertions.
    InitializeInsertedTask initializeInsertedTask(*this, incrementalReasoningState, edbInsertList, false);
    if (!executeReasoningTask(initializeInsertedTask))
        return false;
    // Start the only component level
    if (incrementalMonitor != nullptr)
        incrementalMonitor->componentLevelStarted(static_cast<size_t>(-1));
    incrementalReasoningState.initializeCurrentLevel();
    // Apply deletions.
    if (useDRedAlgorithm) {
        // Overdelete...
        DRedDeletionTask dredDeletionTask(*this, incrementalMonitor, incrementalReasoningState, static_cast<size_t>(-1));
        if (!executeReasoningTask(dredDeletionTask))
            return false;
        // ...reset representatives...
        if (m_dataStore.getEqualityAxiomatizationType() != EQUALITY_AXIOMATIZATION_OFF) {
            UpdateEqualityManagerTask updateEqualityManagerTask(*this, incrementalMonitor, incrementalReasoningState, UpdateEqualityManagerTask::UNREPRESENT);
            if (!executeReasoningTask(updateEqualityManagerTask))
                return false;
            // Ensure that the rules are properly indexed.
            m_ruleIndex.ensureConstantsAreNormalizedMain();
        }
        // ...rederive...
        DRedRederivationTask dredRederivationTask(*this, incrementalMonitor, incrementalReasoningState, static_cast<size_t>(-1));
        if (!executeReasoningTask(dredRederivationTask))
            return false;
        // and break equivalence classes.
        if (m_dataStore.getEqualityAxiomatizationType() != EQUALITY_AXIOMATIZATION_OFF) {
            UpdateEqualityManagerTask updateEqualityManagerTask(*this, incrementalMonitor, incrementalReasoningState, UpdateEqualityManagerTask::BREAK_EQUALS);
            if (!executeReasoningTask(updateEqualityManagerTask))
                return false;
        }
    }
    else {
        // Apply deletions.
        FBFDeletionTask fbfDeletionTask(*this, incrementalMonitor, incrementalReasoningState, static_cast<size_t>(-1));
        if (!executeReasoningTaskSerially(fbfDeletionTask))
            return false;
        // If we're optimizing equality, we must adjust the equality manager before proceeding with insertions.
        if (m_dataStore.getEqualityAxiomatizationType() != EQUALITY_AXIOMATIZATION_OFF) {
            UpdateEqualityManagerTask updateEqualityManagerTask(*this, incrementalMonitor, incrementalReasoningState, UpdateEqualityManagerTask::COPY_CLASSES);
            if (!executeReasoningTask(updateEqualityManagerTask))
                return false;
            // Ensure that the rules are properly indexed.
            m_ruleIndex.ensureConstantsAreNormalizedMain();
        }
    }
    // Evalaute rules that will be inserted.
    if (m_ruleIndex.hasJustAddedRules()) {
        LockFreeQueue<RuleInfo*> ruleQueue(m_dataStore.getMemoryManager());
        ruleQueue.initializeLarge();
        m_ruleIndex.enqueueInsertedRules(ruleQueue);
        m_ruleIndex.propagateInsertions();
        EvaluateInsertionQueueTask evaluateInsertionQueueTask(*this, incrementalMonitor, incrementalReasoningState, ruleQueue, static_cast<size_t>(-1));
        if (!executeReasoningTask(evaluateInsertionQueueTask))
            return false;
    }
    // Apply insertions.
    InsertionTask insertionTask(*this, incrementalMonitor, incrementalReasoningState, static_cast<size_t>(-1));
    if (!executeReasoningTask(insertionTask))
        return false;
    // Reasoning finished.
    if (incrementalMonitor != nullptr)
        incrementalMonitor->componentLevelFinished(static_cast<size_t>(-1));
    // Record the positions of the deleted and the added lists at the end of the level.
    incrementalReasoningState.getDeleteListEnd(0) = incrementalReasoningState.getDeleteList().getFirstFreePosition();
    incrementalReasoningState.getAddedListEnd(0) = incrementalReasoningState.getAddedList().getFirstFreePosition();
    // Propagate changes during both deletion and insertion.
    PropagateChangesTask propagateChangesTask(*this, incrementalMonitor, incrementalReasoningState, false);
    if (!executeReasoningTask(propagateChangesTask))
        return false;
    // Wipe the incremental changes buffers.
    if (!edbDeleteList.initializeLarge())
        throw RDF_STORE_EXCEPTION("Cannot reinitialize the EDB deletion list.");
    if (!edbInsertList.initializeLarge())
        throw RDF_STORE_EXCEPTION("Cannot reinitialize the EDB insertion list.");
    if (incrementalMonitor != nullptr)
        incrementalMonitor->taskFinished(m_dataStore);
    return true;
}

void DatalogEngine::save(OutputStream& outputStream) const {
    m_ruleIndex.save(outputStream);
}

void DatalogEngine::load(InputStream& inputStream) {
    m_ruleIndex.load(inputStream);
    setNumberOfThreads(m_workers.size());
}
