// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef ABSTRACTDELETIONTASKIMPL_H_
#define ABSTRACTDELETIONTASKIMPL_H_

#include "../equality/EqualityManager.h"
#include "../storage/DataStore.h"
#include "../storage/TupleTable.h"
#include "../util/Vocabulary.h"
#include "RuleIndexImpl.h"
#include "DatalogEngine.h"
#include "DatalogEngineWorker.h"
#include "AbstractDeletionTask.h"
#include "IncrementalMonitor.h"
#include "IncrementalReasoningState.h"

// AbstractDeletionTaskWorker

template<class Derived>
template<uint8_t reflexivityType>
always_inline bool AbstractDeletionTaskWorker<Derived>::checkReflexivity(const ResourceID resourceID) {
    return m_reflexiveSameAsStatus.add<Derived::s_multithreaded>(static_cast<size_t>(resourceID) * Derived::s_numberOfReflexivityBits + static_cast<size_t>(reflexivityType));
}

template<class Derived>
AbstractDeletionTaskWorker<Derived>::AbstractDeletionTaskWorker(AbstractDeletionTask& deletionTask, DatalogEngineWorker& datalogEngineWorker) :
    m_deletionTask(deletionTask),
    m_incrementalMonitor(m_deletionTask.m_incrementalMonitor),
    m_workerIndex(datalogEngineWorker.getWorkerIndex()),
    m_dataStore(m_deletionTask.getDatalogEngine().getDataStore()),
    m_equalityManager(m_dataStore.getEqualityManager()),
    m_tripleTable(m_dataStore.getTupleTable("internal$rdf")),
    m_ruleIndex(m_deletionTask.getDatalogEngine().getRuleIndex()),
    m_incrementalReasoningState(m_deletionTask.m_incrementalReasoningState),
    m_reflexiveSameAsStatus(m_deletionTask.m_reflexiveSameAsStatus),
    m_taskRunning(1),
    m_currentTupleIndex(INVALID_TUPLE_INDEX),
    m_currentTupleBuffer1(3, INVALID_RESOURCE_ID),
    m_currentTupleBuffer2(3, INVALID_RESOURCE_ID),
    m_currentTupleBuffer3(3, INVALID_RESOURCE_ID),
    m_currentTupleArgumentIndexes(),
    m_componentLevel(m_deletionTask.m_componentLevel)
{
    m_currentTupleArgumentIndexes.push_back(0);
    m_currentTupleArgumentIndexes.push_back(1);
    m_currentTupleArgumentIndexes.push_back(2);
    ArgumentIndexSet allInputArguments;
    for (ArgumentIndex argumentIndex = 0; argumentIndex < 3; ++argumentIndex) {
        allInputArguments.clear();
        allInputArguments.add(argumentIndex);
        m_queriesForDeletionPropagationViaReplacementSPO[argumentIndex] = m_tripleTable.createTupleIterator(m_currentTupleBuffer2, m_currentTupleArgumentIndexes, allInputArguments, allInputArguments, TUPLE_STATUS_COMPLETE | TUPLE_STATUS_IDB | TUPLE_STATUS_IDB_MERGED, TUPLE_STATUS_COMPLETE | TUPLE_STATUS_IDB);
    }
}

template<class Derived>
always_inline size_t AbstractDeletionTaskWorker<Derived>::getWorkerIndex() {
    return m_workerIndex;
}

template<class Derived>
always_inline bool AbstractDeletionTaskWorker<Derived>::isSingleton(const ResourceID resourceID) {
    return m_equalityManager.getNextEqual(resourceID) == INVALID_RESOURCE_ID;
}

template<class Derived>
always_inline void AbstractDeletionTaskWorker<Derived>::deleteTuple(ThreadContext& threadContext, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) {
    const TupleIndex tupleIndex = m_tripleTable.getTupleIndex(threadContext, argumentsBuffer, argumentIndexes);
    if (tupleIndex == INVALID_TUPLE_INDEX)
        throw RDF_STORE_EXCEPTION("Error in incremental reasoning: the rules do not seem to match the current data.");
    if (m_incrementalReasoningState.addGlobalFlags<Derived::s_multithreaded>(tupleIndex, GF_DELETED_NEW)) {
        m_incrementalReasoningState.getDeleteList().template enqueue<Derived::s_multithreaded>(tupleIndex);
        if (Derived::s_callMonitor)
            m_incrementalMonitor->tupleDerived(m_workerIndex, argumentsBuffer, argumentIndexes, true, true);
    }
    else {
        if (Derived::s_callMonitor)
            m_incrementalMonitor->tupleDerived(m_workerIndex, argumentsBuffer, argumentIndexes, true, false);
    }
}

template<class Derived>
always_inline void AbstractDeletionTaskWorker<Derived>::applyPreviousLevelDelRulesPositive(ThreadContext& threadContext) {
    auto filter1 = m_ruleIndex.template setBodyLiteralInfoFilter<MAIN_POSITIVE_BEFORE_PIVOT>(*static_cast<Derived*>(this), [](Derived& target, const BodyLiteralInfo& bodyLiteralInfo, const TupleIndex tupleIndex, const TupleStatus tupleStatus) {
        // Condition is I\(D\A).
        return
            ((tupleStatus & (TUPLE_STATUS_IDB | TUPLE_STATUS_IDB_MERGED)) == TUPLE_STATUS_IDB) &&
            ((target.m_incrementalReasoningState.getGlobalFlags(tupleIndex) & (GF_ADDED | GF_DELETED)) != GF_DELETED);
    });
    auto filter2 = m_ruleIndex.template setBodyLiteralInfoFilter<MAIN_POSITIVE_AFTER_PIVOT>(*static_cast<Derived*>(this), [](Derived& target, const BodyLiteralInfo& bodyLiteralInfo, const TupleIndex tupleIndex, const TupleStatus tupleStatus) {
        // Condition is I.
        return
            ((tupleStatus & (TUPLE_STATUS_IDB | TUPLE_STATUS_IDB_MERGED)) == TUPLE_STATUS_IDB);
    });
    // Pivot atoms are positive, and all negative atoms are "after" all positive atoms; therefore, we just need the "after pivot" filter for negative atoms here.
    auto filter3 = m_ruleIndex.template setBodyLiteralInfoFilter<MAIN_NEGATIVE_AT_PIVOT_OR_AFTER_PIVOT>(*static_cast<Derived*>(this), [](Derived& target, const BodyLiteralInfo& bodyLiteralInfo, const TupleIndex tupleIndex, const TupleStatus tupleStatus) {
        // Condition is I.
        return
            ((tupleStatus & (TUPLE_STATUS_IDB | TUPLE_STATUS_IDB_MERGED)) == TUPLE_STATUS_IDB);
    });

    while (::atomicRead(m_taskRunning) && (m_currentTupleIndex = m_incrementalReasoningState.getDeleteList().template dequeue<Derived::s_multithreaded>(m_deletionTask.m_afterLastDeletedInPreviousLevels)) != INVALID_TUPLE_INDEX) {
        if ((m_incrementalReasoningState.getGlobalFlags(m_currentTupleIndex) & (GF_ADDED | GF_DELETED)) == GF_DELETED) {
            m_tripleTable.getStatusAndTuple(m_currentTupleIndex, m_currentTupleBuffer1);
            if (Derived::s_callMonitor)
                m_incrementalMonitor->deletionPropagationStarted(m_workerIndex, m_currentTupleBuffer1, m_currentTupleArgumentIndexes, true);
            m_ruleIndex.applyRulesToPositiveLiteralMain<Derived::s_reasoningMode == REASONING_MODE_NO_EQUALITY_BY_LEVELS ? ALL_IN_COMPONENT : ALL_COMPONENTS, Derived::s_callMonitor>(threadContext, m_workerIndex, m_currentTupleBuffer1, m_currentTupleArgumentIndexes, m_componentLevel, m_incrementalMonitor,
                [this](ThreadContext& threadContext, const HeadAtomInfo& headAtomInfo, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) {
                    this->deleteTuple(threadContext, argumentsBuffer, argumentIndexes);
                }
            );
            if (Derived::s_callMonitor)
                m_incrementalMonitor->deletionPropagationFinished(m_workerIndex);
        }
    }
}

template<class Derived>
always_inline void AbstractDeletionTaskWorker<Derived>::applyPreviousLevelDelRulesNegative(ThreadContext& threadContext) {
    // Pivot atoms are negative, and all negative atoms are "after" all positive atoms; therefore, we just need the "before pivot" filter for positive atoms here.
    auto filter1 = m_ruleIndex.template setBodyLiteralInfoFilter<MAIN_POSITIVE_BEFORE_PIVOT>(*static_cast<Derived*>(this), [](Derived& target, const BodyLiteralInfo& bodyLiteralInfo, const TupleIndex tupleIndex, const TupleStatus tupleStatus) {
        // Condition is I\(D\A).
        return
            ((tupleStatus & (TUPLE_STATUS_IDB | TUPLE_STATUS_IDB_MERGED)) == TUPLE_STATUS_IDB) &&
            ((target.m_incrementalReasoningState.getGlobalFlags(tupleIndex) & (GF_ADDED | GF_DELETED)) != GF_DELETED);
    });
    auto filter2 = m_ruleIndex.template setBodyLiteralInfoFilter<MAIN_NEGATIVE_SINGLE_ATOM_BEFORE_PIVOT>(*static_cast<Derived*>(this), [](Derived& target, const BodyLiteralInfo& bodyLiteralInfo, const TupleIndex tupleIndex, const TupleStatus tupleStatus) {
        // The condition is I + A\D = I+A.
        return
            ((tupleStatus & (TUPLE_STATUS_IDB | TUPLE_STATUS_IDB_MERGED)) == TUPLE_STATUS_IDB) ||
            ((target.m_incrementalReasoningState.getGlobalFlags(tupleIndex) & (GF_ADDED | GF_ADDED_MERGED)) == GF_ADDED);
    });
    auto filter3 = m_ruleIndex.template setBodyLiteralInfoFilter<MAIN_NEGATIVE_MULTIPLE_ATOMS_BEFORE_PIVOT>(*static_cast<Derived*>(this), [](Derived& target, const BodyLiteralInfo& bodyLiteralInfo, const TupleIndex tupleIndex, const TupleStatus tupleStatus) {
        // This check does not ensure the nonrepetition property! Nonrepetition would be ensured by checking on the last atom in the conjunction whether
        // - all conjuncts are in I, or
        // - all conjuncts are in (I\D)+A and at least one conjunct is in A\D.
        // Implementing this test is a pain because it is nonlocal, so we don't bother. Instead, we just check whether the atom is in I, which ensures correctness but not the nonrepetition property.
        return
            ((tupleStatus & (TUPLE_STATUS_IDB | TUPLE_STATUS_IDB_MERGED)) == TUPLE_STATUS_IDB);
    });
    auto filter4 = m_ruleIndex.template setBodyLiteralInfoFilter<MAIN_NEGATIVE_AT_PIVOT_OR_AFTER_PIVOT>(*static_cast<Derived*>(this), [](Derived& target, const BodyLiteralInfo& bodyLiteralInfo, const TupleIndex tupleIndex, const TupleStatus tupleStatus) {
        // Condition is I.
        return
            ((tupleStatus & (TUPLE_STATUS_IDB | TUPLE_STATUS_IDB_MERGED)) == TUPLE_STATUS_IDB);
    });
    auto filter5 = m_ruleIndex.template setUnderlyingLiteralInfoFilter<UNDERLYING_BEFORE_PIVOT>(*static_cast<Derived*>(this), [](Derived& target, const TupleIndex tupleIndex, const TupleStatus tupleStatus) {
        // The pivot is in A\D, so atoms before pivot are in [(I\D)+A] \ [A\D] = I\(D\A).
        return
            ((tupleStatus & (TUPLE_STATUS_IDB | TUPLE_STATUS_IDB_MERGED)) == TUPLE_STATUS_IDB) &&
            ((target.m_incrementalReasoningState.getGlobalFlags(tupleIndex) & (GF_ADDED | GF_DELETED)) != GF_DELETED);
    });
    auto filter6 = m_ruleIndex.template setUnderlyingLiteralInfoFilter<UNDERLYING_AFTER_PIVOT>(*static_cast<Derived*>(this), [](Derived& target, const TupleIndex tupleIndex, const TupleStatus tupleStatus) {
        // The pivot is in A\D, so atoms after pivot are in (I\D)+A.
        const TupleFlags tupleFlags = target.m_incrementalReasoningState.getGlobalFlags(tupleIndex);
        return
            (((tupleStatus & (TUPLE_STATUS_IDB | TUPLE_STATUS_IDB_MERGED)) == TUPLE_STATUS_IDB) && ((tupleFlags & (GF_DELETED | GF_ADDED_MERGED)) == 0)) ||
            ((tupleFlags & (GF_ADDED | GF_ADDED_MERGED)) == GF_ADDED);
    });

    while (::atomicRead(m_taskRunning) && (m_currentTupleIndex = m_incrementalReasoningState.getAddedList().template dequeue<Derived::s_multithreaded>()) != INVALID_TUPLE_INDEX) {
        if ((m_incrementalReasoningState.getGlobalFlags(m_currentTupleIndex) & (GF_ADDED | GF_DELETED)) == GF_ADDED) {
            m_tripleTable.getStatusAndTuple(m_currentTupleIndex, m_currentTupleBuffer1);
            if (Derived::s_callMonitor)
                m_incrementalMonitor->deletionPropagationStarted(m_workerIndex, m_currentTupleBuffer1, m_currentTupleArgumentIndexes, true);
            m_ruleIndex.applyRulesToUnderlyingNegationLiteralMain<Derived::s_reasoningMode == REASONING_MODE_NO_EQUALITY_BY_LEVELS ? ALL_IN_COMPONENT : ALL_COMPONENTS, Derived::s_callMonitor>(threadContext, m_workerIndex, m_currentTupleBuffer1, m_currentTupleArgumentIndexes, m_componentLevel, m_incrementalMonitor,
                [this](ThreadContext& threadContext, const HeadAtomInfo& headAtomInfo, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) {
                    this->deleteTuple(threadContext, argumentsBuffer, argumentIndexes);
                }
            );
            if (Derived::s_callMonitor)
                m_incrementalMonitor->deletionPropagationFinished(m_workerIndex);
        }
    }
}

template<class Derived>
always_inline void AbstractDeletionTaskWorker<Derived>::applyRecursiveDelRules(ThreadContext& threadContext) {
    auto filter1 = m_ruleIndex.setBodyLiteralInfoFilter<MAIN_POSITIVE_BEFORE_PIVOT>(*static_cast<Derived*>(this), [](Derived& target, const BodyLiteralInfo& bodyLiteralInfo, const TupleIndex tupleIndex, const TupleStatus tupleStatus) {
        // Condition is I\(D\A), but without the current tuple, which plays the role of \Delta_D.
        return
            (target.m_currentTupleIndex != tupleIndex) &&
            ((tupleStatus & (TUPLE_STATUS_IDB | TUPLE_STATUS_IDB_MERGED)) == TUPLE_STATUS_IDB) &&
            ((target.m_incrementalReasoningState.getGlobalFlags(tupleIndex) & (GF_DELETED | GF_ADDED)) != GF_DELETED);
    });
    auto filter2 = m_ruleIndex.setBodyLiteralInfoFilter<MAIN_POSITIVE_AFTER_PIVOT>(*static_cast<Derived*>(this), [](Derived& target, const BodyLiteralInfo& bodyLiteralInfo, const TupleIndex tupleIndex, const TupleStatus tupleStatus) {
        // Condition is I\(D\A).
        return
            ((tupleStatus & (TUPLE_STATUS_IDB | TUPLE_STATUS_IDB_MERGED)) == TUPLE_STATUS_IDB) &&
            ((target.m_incrementalReasoningState.getGlobalFlags(tupleIndex) & (GF_DELETED | GF_ADDED)) != GF_DELETED);
    });
    // Pivot atoms are positive, and all negative atoms are "after" all positive atoms; therefore, we just need the "after pivot" filter for negative atoms here.
    auto filter3 = m_ruleIndex.setBodyLiteralInfoFilter<MAIN_NEGATIVE_AT_PIVOT_OR_AFTER_PIVOT>(*static_cast<Derived*>(this), [](Derived& target, const BodyLiteralInfo& bodyLiteralInfo, const TupleIndex tupleIndex, const TupleStatus tupleStatus) {
        // Condition is I+A.
        return
            ((tupleStatus & (TUPLE_STATUS_IDB | TUPLE_STATUS_IDB_MERGED)) == TUPLE_STATUS_IDB) ||
            (target.m_incrementalReasoningState.getGlobalFlags(tupleIndex) & (GF_ADDED | GF_ADDED_MERGED)) == GF_ADDED;
    });

    while (::atomicRead(m_taskRunning) && (m_currentTupleIndex = m_incrementalReasoningState.getDeleteList().template dequeue<Derived::s_multithreaded>()) != INVALID_TUPLE_INDEX) {
        assert((m_incrementalReasoningState.getGlobalFlags(m_currentTupleIndex) & (GF_DELETED | GF_DELETED_NEW)) == GF_DELETED_NEW);
        m_tripleTable.getStatusAndTuple(m_currentTupleIndex, m_currentTupleBuffer1);
        if (Derived::s_callMonitor)
            m_incrementalMonitor->possiblyDeletedTupleExtracted(m_workerIndex, m_currentTupleBuffer1, m_currentTupleArgumentIndexes);
        if (!static_cast<Derived*>(this)->check(threadContext, m_currentTupleIndex, &m_currentTupleBuffer1, &m_currentTupleArgumentIndexes)) {
            if (Derived::s_callMonitor)
                m_incrementalMonitor->deletionPropagationStarted(m_workerIndex, m_currentTupleBuffer1, m_currentTupleArgumentIndexes, false);
            if (::isEqualityReasoningMode(Derived::s_reasoningMode)  && m_currentTupleBuffer1[1] == OWL_SAME_AS_ID) {
                const ResourceID resourceID = m_currentTupleBuffer1[0];
                assert(resourceID == m_currentTupleBuffer1[2]);
                if (!isSingleton(resourceID)) {
                    for (size_t positionIndex = 0; positionIndex < 3; ++positionIndex) {
                        TupleIterator& tupleIterator = *m_queriesForDeletionPropagationViaReplacementSPO[positionIndex];
                        m_currentTupleBuffer2[positionIndex] = resourceID;
                        size_t multiplicity = tupleIterator.open();
                        while (multiplicity != 0) {
                            const TupleIndex deletedTupleIndex = tupleIterator.getCurrentTupleIndex();
                            if (m_incrementalReasoningState.addGlobalFlags<Derived::s_multithreaded>(deletedTupleIndex, GF_DELETED_NEW)) {
                                m_incrementalReasoningState.getDeleteList().template enqueue<Derived::s_multithreaded>(deletedTupleIndex);
                                if (Derived::s_callMonitor)
                                    m_incrementalMonitor->propagatedDeletionViaReplacement(m_workerIndex, resourceID, tupleIterator.getArgumentsBuffer(), tupleIterator.getArgumentIndexes(), true);
                            }
                            else {
                                if (Derived::s_callMonitor)
                                    m_incrementalMonitor->propagatedDeletionViaReplacement(m_workerIndex, resourceID, tupleIterator.getArgumentsBuffer(), tupleIterator.getArgumentIndexes(), false);
                            }
                            multiplicity = tupleIterator.advance();
                        }
                    }
                }
            }
            if (::isEqualityReasoningMode(Derived::s_reasoningMode)) {
                for (size_t positionIndex = 0; positionIndex < 3; ++positionIndex) {
                    const ResourceID resourceID = m_currentTupleBuffer1[positionIndex];
                    if (checkReflexivity<0>(resourceID)) {
                        m_currentTupleBuffer3[1] = OWL_SAME_AS_ID;
                        m_currentTupleBuffer3[0] = m_currentTupleBuffer3[2] = resourceID;
                        const TupleIndex deletedTupleIndex = m_tripleTable.getTupleIndex(threadContext, m_currentTupleBuffer3, m_currentTupleArgumentIndexes);
                        if (deletedTupleIndex == INVALID_TUPLE_INDEX)
                            throw RDF_STORE_EXCEPTION("Error in incremental reasoning: the rules do not seem to match the current data.");
                        if (m_incrementalReasoningState.addGlobalFlags<Derived::s_multithreaded>(deletedTupleIndex, GF_DELETED_NEW)) {
                            m_incrementalReasoningState.getDeleteList().template enqueue<Derived::s_multithreaded>(deletedTupleIndex);
                            if (Derived::s_callMonitor)
                                m_incrementalMonitor->reflexiveSameAsTupleDerived(m_workerIndex, resourceID, true);
                        }
                        else {
                            if (Derived::s_callMonitor)
                                m_incrementalMonitor->reflexiveSameAsTupleDerived(m_workerIndex, resourceID, false);
                        }
                    }

                }
            }
            if (Derived::s_hasRecursiveRules)
                m_ruleIndex.applyRulesToPositiveLiteralMain<Derived::s_reasoningMode == REASONING_MODE_NO_EQUALITY_BY_LEVELS ? ALL_IN_COMPONENT : ALL_COMPONENTS, Derived::s_callMonitor>(threadContext, m_workerIndex, m_currentTupleBuffer1, m_currentTupleArgumentIndexes, m_componentLevel, m_incrementalMonitor,
                    [this](ThreadContext& threadContext, const HeadAtomInfo& headAtomInfo, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) {
                        this->deleteTuple(threadContext, argumentsBuffer, argumentIndexes);
                    }
                );
            // Mark triple deleted
            m_incrementalReasoningState.addGlobalFlags<Derived::s_multithreaded>(m_currentTupleIndex, GF_DELETED);
            if (Derived::s_callMonitor) {
                m_incrementalMonitor->deletionPropagationFinished(m_workerIndex);
                m_incrementalMonitor->possiblyDeletedTupleProcessed(m_workerIndex, false);
            }
        }
        else {
            if (Derived::s_callMonitor)
                m_incrementalMonitor->possiblyDeletedTupleProcessed(m_workerIndex, true);
        }
    }
}

template<class Derived>
void AbstractDeletionTaskWorker<Derived>::run(ThreadContext& threadContext) {
    if (Derived::s_callMonitor)
        m_incrementalMonitor->tupleDeletionPreviousLevelsStarted(m_workerIndex);
    if (this->m_componentLevel != static_cast<size_t>(-1) && this->m_componentLevel > 0 && m_ruleIndex.hasRules(m_componentLevel)) {
        applyPreviousLevelDelRulesPositive(threadContext);
        if (m_ruleIndex.hasRulesWithNegation(m_componentLevel))
            applyPreviousLevelDelRulesNegative(threadContext);
    }
    if (Derived::s_callMonitor)
        m_incrementalMonitor->tupleDeletionRecursiveStarted(m_workerIndex);
    applyRecursiveDelRules(threadContext);
    if (Derived::s_callMonitor)
        m_incrementalMonitor->tupleDeletionFinished(m_workerIndex);
}

template<class Derived>
void AbstractDeletionTaskWorker<Derived>::stop() {
    ::atomicWrite(m_taskRunning, 0);
}

#endif // ABSTRACTDELETIONTASKIMPL_H_
