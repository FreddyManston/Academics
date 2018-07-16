// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../storage/TupleTable.h"
#include "AbstractStatisticsImpl.h"
#include "IncrementalStatistics.h"

// Redirection arrays

#define DECLARE_INCREMENTAL_REDIRECTION(name) \
    DECLARE_REDIRECTION_TABLE(name, IncrementalStatistics::name##_TITLE + 1)

DECLARE_INCREMENTAL_REDIRECTION(EVALUATION_OF_DELETED_RULES);

DECLARE_INCREMENTAL_REDIRECTION(SATURATE);

DECLARE_INCREMENTAL_REDIRECTION(DELETION_PROPAGATION);

DECLARE_INCREMENTAL_REDIRECTION(EVALUATION_OF_INSERTED_RULES);

DECLARE_INCREMENTAL_REDIRECTION(TUPLE_INSERTION);

// IncrementalStatistics::ThreadStateEx

always_inline IncrementalStatistics::ThreadStateEx::ThreadStateEx(const size_t numberOfCountersPerLevel, const char* const* counterDescriptions, const size_t initialNumberOfLevels) :
    ThreadState(numberOfCountersPerLevel, counterDescriptions, initialNumberOfLevels),
    m_recursiveRuleInstanceSeenInBackwardChaining()
{
}

// IncrementalStatistics

const char* const* IncrementalStatistics::describeStatistics(size_t& numberOfCountersPerLevel) {
    static const char* const s_counterDescriptions[NUMBER_OF_INCREMENTAL_COUNTERS] = {
        "+Number of EDB tuples before update",
        "+Number of IDB tuples before update",
        "+Number of tuples to delete",
        "+Number of tuples to insert",
        "+Number of EDB tuples after update",
        "+Number of IDB tuples after update",

        "-EVALUATION OF DELETED RULES",
        "$Non-merged IDB tuples extracted from the store during materialization",
        "$    Tuples that were not normal after extraction",
        "$    Normal extracted tuples matching at least one body atom",
        "$        Producing no derivation",
        "$Total number of matched body atoms",
        "$    The number of matches of the first atom in a rule",
        "$        Producing no derivation",
        "Matched rule instances",
        "$    Matched rule instances during tuple extraction",
        "$    Matched rule instances during rule reevaluation",
        "$    Matched rule instances during pivotless rule evaluation",
        "$Number of attempts to reevaluate a rule",
        "$    Producing no derivation",
        "$Derived equalities (via rules or via data)",
        "$    Successful",
        "$Derivations",
        "$    Via replacement rules (at extraction or due to merging)",
        "$        Successful",
        "$    Via reflexivity rules",
        "$        Successful",
        "Derivations via deleted rules",
        "$        Not normal at derivation time",
        "    Successful",

        "-DELETION PROPAGATION",
        "Tuples to which deletion propagation rules were applied",
        "$    Tuples that were not normal after extraction",
        "    Matching at least one body atom",
        "        Producing no derivation",
        "Total number of matched body atoms",
        "    The number of matches of the first atom in a rule",
        "        Producing no derivation",
        "Matched rule instances",
        "$    Matched rule instances during tuple extraction",
        "$    Matched rule instances during rule reevaluation",
        "$    Matched rule instances during pivotless rule evaluation",
        "$Number of attempts to reevaluate a rule",
        "$    Producing no derivation",
        "$Derived equalities (via rules or via data)",
        "$    Successful",
        "Derivations",
        "    Via replacement rules",
        "        Successful",
        "    Via reflexivity rules",
        "        Successful",
        "    Via user-defined rules",
        "$        Not normal at derivation time",
        "        Successful",

        "-BACKWARD CHAINING",
        "Top-level calls to the provability check",
        "    Proved after checking provability",
        "Tuples added to the checked list",
        "    Tuples whose proof trees were not explored fully",
        "The number nonrecursive rules whose head matched a fact",
        "    Matching a rule instance",
        "The number recursive rules whose head matched a fact",
        "    Matching a rule instance",
        "Recursive rule instances checked",
        "    Via replacement rules",
        "    Via reflexivity rules",
        "    Via used-defined rules",

        "-SATURATION",
        "Non-merged IDB tuples extracted from proved list",
        "    Tuples that were not normal after extraction",
        "    Normal extracted tuples matching at least one body atom",
        "        Producing no derivation",
        "Total number of matched body atoms",
        "    The number of matches of the first atom in a rule",
        "        Producing no derivation",
        "Matched rule instances",
        "    Matched rule instances during tuple extraction",
        "    Matched rule instances during rule reevaluation",
        "$    Matched rule instances during pivotless rule evaluation",
        "Number of attempts to reevaluate a rule",
        "    Producing no derivation",
        "Derived equalities (via rules or via data)",
        "    Successful",
        "Non-delayed derivations",
        "    Via replacement rules (at extraction or due to merging)",
        "        Successful",
        "    Via reflexive owl:sameAs tuples",
        "        Successful",
        "    Via rules",
        "        Not normal at derivation time",
        "        Successful",
        "Delayed derivations",
        "    Via replacement rules (at extraction or due to merging)",
        "        Successful",
        "    Via reflexivity rules",
        "        Successful",
        "    Via used-defined rules",
        "        Successful",
        "Short-circuit proofs of reflexive owl:sameAs tuples from EDB",
        "Denormalized checked tuples",
        "Delayed, non-EDB, non-previous-level tuples used to prove a checked tuple",
        "EDB, non-previous level tuples added to the proved list",
        "Tuples from the previous level added to the proved list",
        "Disproved checked tuples",

        "-EVALUATION OF INSERTED RULES",
        "$Non-merged IDB tuples extracted from the store during materialization",
        "$    Tuples that were not normal after extraction",
        "$    Normal extracted tuples matching at least one body atom",
        "$        Producing no derivation",
        "$Total number of matched body atoms",
        "$    The number of matches of the first atom in a rule",
        "$        Producing no derivation",
        "Matched rule instances",
        "$    Matched rule instances during tuple extraction",
        "$    Matched rule instances during rule reevaluation",
        "$    Matched rule instances during pivotless rule evaluation",
        "$Number of attempts to reevaluate a rule",
        "$    Producing no derivation",
        "$Derived equalities (via rules or via data)",
        "$    Successful",
        "$Derivations",
        "$    Via replacement rules (at extraction or due to merging)",
        "$        Successful",
        "$    Via reflexivity rules",
        "$        Successful",
        "Derivations via inserted rules",
        "$        Not normal at derivation time",
        "    Successful",

        "-TUPLE INSERTION",
        "Non-merged IDB tuples extracted from the insertion list",
        "    Tuples that were not normal after extraction",
        "    Normal extracted tuples matching at least one body atom",
        "        Producing no derivation",
        "Total number of matched body atoms",
        "    The number of matches of the first atom in a rule",
        "        Producing no derivation",
        "Matched rule instances",
        "    Matched rule instances during tuple extraction",
        "    Matched rule instances during rule reevaluation",
        "$    Matched rule instances during pivotless rule evaluation",
        "Number of attempts to reevaluate a rule",
        "Normalized constants",
        "Derived equalities (via rules or via data)",
        "    Successful",
        "Derivations",
        "    Via replacement rules (at extraction or due to merging)",
        "        Successful",
        "    Via reflexivity rules",
        "        Successful",
        "    Via user-defined rules",
        "        Not normal at derivation time",
        "        Successful",
        "    From the INS list",
        "        Successful",

        "-UPDATING THE IDB",
        "Attempts to delete a tuple",
        "    Successful attempts",
        "Attempts to add a tuple",
        "    Successful attempts",
        "Copied equivalence classes",
    };
    numberOfCountersPerLevel = NUMBER_OF_INCREMENTAL_COUNTERS;
    return s_counterDescriptions;
}

std::unique_ptr<IncrementalStatistics::ThreadState> IncrementalStatistics::newThreadState(const size_t numberOfCountersPerLevel, const char* const* counterDescriptions, const size_t initialNumberOfLevels) {
    return std::unique_ptr<ThreadState>(new ThreadStateEx(numberOfCountersPerLevel, counterDescriptions, initialNumberOfLevels));
}

IncrementalStatistics::IncrementalStatistics() : AbstractStatisticsImpl<IncrementalMonitor>() {
}

void IncrementalStatistics::taskStarted(const DataStore& dataStore, const size_t maxComponentLevel) {
    AbstractStatisticsImpl<IncrementalMonitor>::taskStarted(dataStore, maxComponentLevel);
    TupleTable& tupleTable = dataStore.getTupleTable("internal$rdf");
    ThreadState& threadState = *m_statesByThread[0];
    threadState.set(SUMMARY_EDB_BEFORE, tupleTable.getTupleCount(TUPLE_STATUS_COMPLETE | TUPLE_STATUS_EDB, TUPLE_STATUS_COMPLETE | TUPLE_STATUS_EDB));
    threadState.set(SUMMARY_IDB_BEFORE, tupleTable.getTupleCount(TUPLE_STATUS_COMPLETE | TUPLE_STATUS_IDB_MERGED | TUPLE_STATUS_IDB, TUPLE_STATUS_COMPLETE | TUPLE_STATUS_IDB));
    threadState.set(SUMMARY_TO_DELETE, tupleTable.getTupleCount(TUPLE_STATUS_COMPLETE | TUPLE_STATUS_EDB_DEL, TUPLE_STATUS_COMPLETE | TUPLE_STATUS_EDB_DEL));
    threadState.set(SUMMARY_TO_INSERT, tupleTable.getTupleCount(TUPLE_STATUS_COMPLETE | TUPLE_STATUS_EDB_INS, TUPLE_STATUS_COMPLETE | TUPLE_STATUS_EDB_INS));
}

void IncrementalStatistics::taskFinished(const DataStore& dataStore) {
    AbstractStatisticsImpl<IncrementalMonitor>::taskFinished(dataStore);
    TupleTable& tupleTable = dataStore.getTupleTable("internal$rdf");
    ThreadState& threadState = *m_statesByThread[0];
    threadState.set(SUMMARY_EDB_AFTER, tupleTable.getTupleCount(TUPLE_STATUS_COMPLETE | TUPLE_STATUS_EDB, TUPLE_STATUS_COMPLETE | TUPLE_STATUS_EDB));
    threadState.set(SUMMARY_IDB_AFTER, tupleTable.getTupleCount(TUPLE_STATUS_COMPLETE | TUPLE_STATUS_IDB_MERGED | TUPLE_STATUS_IDB, TUPLE_STATUS_COMPLETE | TUPLE_STATUS_IDB));
}

void IncrementalStatistics::deletedRuleEvaluationStarted(const size_t workerIndex, const RuleInfo& ruleInfo) {
    setRedirectionTable(workerIndex, EVALUATION_OF_DELETED_RULES_REDIRECTION);
    ThreadState& threadState = *m_statesByThread[workerIndex];
    threadState.m_matchedInstancesForRule = 0;
    threadState.increment(ATTEMPTS_TO_REEVALUATE_RULE);
}

void IncrementalStatistics::deletedRuleEvaluationFinished(const size_t workerIndex) {
    ThreadState& threadState = *m_statesByThread[workerIndex];
    if (threadState.m_matchedInstancesForRule == 0)
        threadState.increment(ATTEMPTS_TO_REEVALUATE_RULE_NO_DERIVATION);
    setRedirectionTable(workerIndex, nullptr);
}

void IncrementalStatistics::addedRuleEvaluationStarted(const size_t workerIndex, const RuleInfo& ruleInfo) {
    setRedirectionTable(workerIndex, EVALUATION_OF_INSERTED_RULES_REDIRECTION);
    ThreadState& threadState = *m_statesByThread[workerIndex];
    threadState.m_matchedInstancesForRule = 0;
    threadState.increment(ATTEMPTS_TO_REEVALUATE_RULE);
}

void IncrementalStatistics::addedRuleEvaluationFinished(const size_t workerIndex) {
    ThreadState& threadState = *m_statesByThread[workerIndex];
    if (threadState.m_matchedInstancesForRule == 0)
        threadState.increment(ATTEMPTS_TO_REEVALUATE_RULE_NO_DERIVATION);
    setRedirectionTable(workerIndex, nullptr);
}

void IncrementalStatistics::tupleDeletionPreviousLevelsStarted(const size_t workerIndex) {
}

void IncrementalStatistics::tupleDeletionRecursiveStarted(const size_t workerIndex) {
}

void IncrementalStatistics::tupleDeletionFinished(const size_t workerIndex) {
}

void IncrementalStatistics::possiblyDeletedTupleExtracted(const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) {
    m_statesByThread[workerIndex]->incrementNoRedirect(BACKWARD_CHAINING_TOP_LEVEL_CALLS);
}

void IncrementalStatistics::possiblyDeletedTupleProcessed(const size_t workerIndex, const bool proved) {
    if (proved)
        m_statesByThread[workerIndex]->incrementNoRedirect(BACKWARD_CHAINING_TOP_LEVEL_CALLS_PROVED);
}

void IncrementalStatistics::deletionPropagationStarted(const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const bool fromPreviousLevel) {
    setRedirectionTable(workerIndex, DELETION_PROPAGATION_REDIRECTION);
    ThreadState& threadState = *m_statesByThread[workerIndex];
    threadState.increment(EXTRACTED_TUPLES);
    threadState.m_matchedFirstBodyLiterals = 0;
    threadState.m_matchedFirstBodyLiteralsWithNoDerivation = 0;
}

void IncrementalStatistics::propagatedDeletionViaReplacement(const size_t workerIndex, const ResourceID resourceID, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const bool wasAdded) {
    ThreadState& threadState = *m_statesByThread[workerIndex];
    threadState.increment(DERIVATIONS);
    threadState.increment(DERIVATIONS_REPLACEMENT);
    if (wasAdded)
        threadState.increment(DERIVATIONS_REPLACEMENT_SUCCESFUL);
}

void IncrementalStatistics::deletionPropagationFinished(const size_t workerIndex) {
    ThreadState& threadState = *m_statesByThread[workerIndex];
    if (threadState.m_matchedFirstBodyLiterals > 0) {
        threadState.increment(EXTRACTED_TUPLES_MATCHING_A_BODY_LITERAL);
        if (threadState.m_matchedFirstBodyLiteralsWithNoDerivation > 0)
            threadState.increment(EXTRACTED_TUPLES_MATCHING_NO_DERIVATION);
    }
    setRedirectionTable(workerIndex, nullptr);
}

void IncrementalStatistics::checkingProvabilityStarted(const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const bool addedToChecked) {
    if (addedToChecked)
        m_statesByThread[workerIndex]->incrementNoRedirect(BACKWARD_CHAINING_ADDED_TO_CHECKED);
}

void IncrementalStatistics::tupleOptimized(const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) {
    m_statesByThread[workerIndex]->incrementNoRedirect(BACKWARD_CHAINING_ADDED_TO_CHECKED_OPTIMIZED);
}

void IncrementalStatistics::reflexiveSameAsRuleInstancesOptimized(const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) {
}

void IncrementalStatistics::backwardReflexiveSameAsRuleInstanceStarted(const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const std::vector<ResourceID>& supportingArgumentsBuffer, const std::vector<ArgumentIndex>& supportingArgumentIndexes) {
    m_statesByThread[workerIndex]->incrementNoRedirect(BACKWARD_CHAINING_RECURSIVE_RULE_INSTANCES);
    m_statesByThread[workerIndex]->incrementNoRedirect(BACKWARD_CHAINING_RECURSIVE_RULE_INSTANCES_REFLEXIVITY);
}

void IncrementalStatistics::backwardReflexiveSameAsRuleInstanceFinished(const size_t workerIndex) {
}

void IncrementalStatistics::backwardReplacementRuleInstanceStarted(const size_t workerIndex, const size_t positionIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) {
    m_statesByThread[workerIndex]->incrementNoRedirect(BACKWARD_CHAINING_RECURSIVE_RULE_INSTANCES);
    m_statesByThread[workerIndex]->incrementNoRedirect(BACKWARD_CHAINING_RECURSIVE_RULE_INSTANCES_REPLACEMENT);
}

void IncrementalStatistics::backwardReplacementRuleInstanceFinished(const size_t workerIndex) {
}

void IncrementalStatistics::backwardRecursiveRuleStarted(const size_t workerIndex, const HeadAtomInfo& headAtomInfo) {
    ThreadStateEx& threadState = static_cast<ThreadStateEx&>(*m_statesByThread[workerIndex]);
    threadState.incrementNoRedirect(BACKWARD_CHAINING_RECURSIVE_RULES);
    threadState.m_recursiveRuleInstanceSeenInBackwardChaining.push_back(false);
}

void IncrementalStatistics::backwardRecursiveRuleInstanceStarted(const size_t workerIndex, const HeadAtomInfo& headAtomInfo, const SupportingFactsEvaluator& currentBodyMatches) {
    ThreadStateEx& threadState = static_cast<ThreadStateEx&>(*m_statesByThread[workerIndex]);
    threadState.incrementNoRedirect(BACKWARD_CHAINING_RECURSIVE_RULE_INSTANCES);
    threadState.incrementNoRedirect(BACKWARD_CHAINING_RECURSIVE_RULE_INSTANCES_RULES);
    threadState.m_recursiveRuleInstanceSeenInBackwardChaining.back() = true;
}

void IncrementalStatistics::backwardRecursiveRuleInstanceAtomStarted(const size_t workerIndex, const HeadAtomInfo& headAtomInfo, const SupportingFactsEvaluator& currentBodyMatches, const size_t currentBodyIndex) {
}

void IncrementalStatistics::backwardRecursiveRuleInstanceAtomFinished(const size_t workerIndex) {
}

void IncrementalStatistics::backwardRecursiveRuleInstanceFinished(const size_t workerIndex) {
}

void IncrementalStatistics::backwardRecursiveRuleFinished(const size_t workerIndex) {
    ThreadStateEx& threadState = static_cast<ThreadStateEx&>(*m_statesByThread[workerIndex]);
    if (threadState.m_recursiveRuleInstanceSeenInBackwardChaining.back())
        threadState.incrementNoRedirect(BACKWARD_CHAINING_RECURSIVE_RULES_MATCHED);
    threadState.m_recursiveRuleInstanceSeenInBackwardChaining.pop_back();
}

void IncrementalStatistics::checkingProvabilityFinished(const size_t workerIndex) {
}

void IncrementalStatistics::backwardNonrecursiveRuleStarted(const size_t workerIndex, const HeadAtomInfo& headAtomInfo) {
    m_statesByThread[workerIndex]->incrementNoRedirect(BACKWARD_CHAINING_NONRECURSIVE_RULES);
}

void IncrementalStatistics::backwardNonrecursiveInstanceMatched(const size_t workerIndex, const HeadAtomInfo& headAtomInfo, const SupportingFactsEvaluator& currentBodyMatches) {
    m_statesByThread[workerIndex]->incrementNoRedirect(BACKWARD_CHAINING_NONRECURSIVE_RULES_MATCHED);
}

void IncrementalStatistics::backwardNonrecursiveRuleFinished(const size_t workerIndex) {
}

void IncrementalStatistics::checkedReflexiveSameAsTupleProvedFromEDB(const size_t workerIndex, const ResourceID originalResourceID, const ResourceID denormalizedResourceID, const ResourceID normalizedProvedResourceID, const bool wasAdded) {
    if (wasAdded)
        m_statesByThread[workerIndex]->incrementNoRedirect(SATURATE_SHORT_CIRCUIT_REFLEXIVITY_EDB);
}

void IncrementalStatistics::checkedTupleChecked(const size_t workerIndex, const std::vector<ResourceID>& originalArgumentsBuffer, const std::vector<ResourceID>& denormalizedArgumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) {
    if (originalArgumentsBuffer[argumentIndexes[0]] != denormalizedArgumentsBuffer[argumentIndexes[0]] || originalArgumentsBuffer[argumentIndexes[1]] != denormalizedArgumentsBuffer[argumentIndexes[1]] || originalArgumentsBuffer[argumentIndexes[2]] != denormalizedArgumentsBuffer[argumentIndexes[2]])
        m_statesByThread[workerIndex]->incrementNoRedirect(SATURATE_DENORMALIZED_CHECKED);
}

void IncrementalStatistics::checkedTupleProved(const size_t workerIndex, const std::vector<ResourceID>& originalArgumentsBuffer, const std::vector<ResourceID>& denormalizedArgumentsBuffer, const std::vector<ResourceID>& normalizedProvedArgumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const bool fromEDB, const bool fromDelayed, const bool fromPrviousLevel, const bool wasAdded) {
    if (wasAdded) {
        ThreadState& threadState = *m_statesByThread[workerIndex];
        if (fromDelayed && !fromEDB && !fromPrviousLevel)
            threadState.incrementNoRedirect(SATURATE_USED_DELAYED);
        if (fromEDB && !fromPrviousLevel)
            threadState.incrementNoRedirect(SATURATE_USED_EDB);
        if (fromPrviousLevel)
            threadState.incrementNoRedirect(SATURATE_USED_PREVIOUS_LEVEL);
    }
}

void IncrementalStatistics::processProvedStarted(const size_t workerIndex) {
    setRedirectionTable(workerIndex, SATURATE_REDIRECTION);
}

void IncrementalStatistics::tupleProvedDelayed(const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const bool wasAdded) {
    ThreadState& threadState = *m_statesByThread[workerIndex];
    threadState.incrementNoRedirect(SATURATE_DELAYED);
    threadState.incrementNoRedirect(SATURATE_DELAYED_RULES);
    if (wasAdded)
        threadState.incrementNoRedirect(SATURATE_DELAYED_RULES_SUCCESFUL);
}

void IncrementalStatistics::reflexiveSameAsTupleDerivedDelayed(const size_t workerIndex, const ResourceID resourceID, const bool wasAdded) {
    ThreadState& threadState = *m_statesByThread[workerIndex];
    threadState.incrementNoRedirect(SATURATE_DELAYED);
    threadState.incrementNoRedirect(SATURATE_DELAYED_REFLEXIVITY);
    if (wasAdded)
        threadState.incrementNoRedirect(SATURATE_DELAYED_REFLEXIVITY_SUCCESFUL);
}

void IncrementalStatistics::tupleNormalizedDelayed(const size_t workerIndex, std::vector<ResourceID>& originalArgumentsBuffer, const std::vector<ArgumentIndex>& originalArgumentIndexes, std::vector<ResourceID>& normalizedArgumentsBuffer, const std::vector<ArgumentIndex>& normalizedArgumentIndexes, const bool wasAdded) {
    ThreadState& threadState = *m_statesByThread[workerIndex];
    threadState.incrementNoRedirect(SATURATE_DELAYED);
    threadState.incrementNoRedirect(SATURATE_DELAYED_REPLACEMENT);
    if (wasAdded)
        threadState.incrementNoRedirect(SATURATE_DELAYED_REPLACEMENT_SUCCESFUL);
}

void IncrementalStatistics::checkedTupleDisproved(const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const bool wasAdded) {
    if (wasAdded)
        m_statesByThread[workerIndex]->incrementNoRedirect(SATURATE_DISPROVED);
}

void IncrementalStatistics::processProvedFinished(const size_t workerIndex) {
    setRedirectionTable(workerIndex, nullptr);
}

void IncrementalStatistics::updateEqualityManagerStarted(const size_t workerIndex) {
}

void IncrementalStatistics::equivalenceClassCopied(const size_t workerIndex, const ResourceID resourceID, const EqualityManager& sourceEqualityManager, const EqualityManager& targetEqualityManager) {
    m_statesByThread[workerIndex]->incrementNoRedirect(UPDATE_IDB_COPIED_CLASSES);
}

void IncrementalStatistics::updateEqualityManagerFinished(const size_t workerIndex) {
}

void IncrementalStatistics::insertionPreviousLevelsStarted(const size_t workerIndex) {
    setRedirectionTable(workerIndex, TUPLE_INSERTION_REDIRECTION);
}

void IncrementalStatistics::insertionRecursiveStarted(const size_t workerIndex) {
}

void IncrementalStatistics::insertedTupleAddedToIDB(const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const bool wasAdded) {
    ThreadState& threadState = *m_statesByThread[workerIndex];
    threadState.increment(DERIVATIONS);
    threadState.incrementNoRedirect(TUPLE_INSERTION_DERIVATIONS_FROM_INS);
    if (wasAdded)
        threadState.incrementNoRedirect(TUPLE_INSERTION_DERIVATIONS_FROM_INS_SUCCESSFUL);
}

void IncrementalStatistics::insertionFinished(const size_t workerIndex) {
    setRedirectionTable(workerIndex, nullptr);
}

void IncrementalStatistics::propagateDeletedProvedStarted(const size_t workerIndex, const size_t componentLevel) {
    if (componentLevel == static_cast<size_t>(-1))
        m_statesByThread[workerIndex]->setCurrentComponentLevel(0);
    else
        m_statesByThread[workerIndex]->setCurrentComponentLevel(componentLevel);
}

void IncrementalStatistics::tupleDeleted(const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const bool wasDeleted) {
    m_statesByThread[workerIndex]->incrementNoRedirect(UPDATE_IDB_DELETED);
    if (wasDeleted)
        m_statesByThread[workerIndex]->incrementNoRedirect(UPDATE_IDB_DELETED_CHANGED);
}

void IncrementalStatistics::tupleAdded(const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const bool wasAdded) {
    m_statesByThread[workerIndex]->incrementNoRedirect(UPDATE_IDB_ADDED);
    if (wasAdded)
        m_statesByThread[workerIndex]->incrementNoRedirect(UPDATE_IDB_ADDED_CHANGED);
}

void IncrementalStatistics::propagateDeletedProvedFinished(const size_t workerIndex) {
}
