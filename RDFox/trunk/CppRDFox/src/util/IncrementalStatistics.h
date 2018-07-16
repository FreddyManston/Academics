// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef INCREMENTALSTATISTICS_H_
#define INCREMENTALSTATISTICS_H_

#include "../reasoning/IncrementalMonitor.h"
#include "AbstractStatistics.h"

class IncrementalStatistics : public AbstractStatisticsImpl<IncrementalMonitor> {

public:

    static const size_t SUMMARY_EDB_BEFORE                                      = 0;
    static const size_t SUMMARY_IDB_BEFORE                                      = SUMMARY_EDB_BEFORE + 1;
    static const size_t SUMMARY_TO_DELETE                                       = SUMMARY_IDB_BEFORE + 1;
    static const size_t SUMMARY_TO_INSERT                                       = SUMMARY_TO_DELETE + 1;
    static const size_t SUMMARY_EDB_AFTER                                       = SUMMARY_TO_INSERT + 1;
    static const size_t SUMMARY_IDB_AFTER                                       = SUMMARY_EDB_AFTER + 1;

    static const size_t EVALUATION_OF_DELETED_RULES_TITLE                       = SUMMARY_IDB_AFTER + 1;

    static const size_t DELETION_PROPAGATION_TITLE                              = EVALUATION_OF_DELETED_RULES_TITLE + 1 + NUMBER_OF_DERIVATION_COUNTERS;

    static const size_t BACKWARD_CHAINING_TITLE                                 = DELETION_PROPAGATION_TITLE + 1 + NUMBER_OF_DERIVATION_COUNTERS;
    static const size_t BACKWARD_CHAINING_TOP_LEVEL_CALLS                       = BACKWARD_CHAINING_TITLE + 1;
    static const size_t BACKWARD_CHAINING_TOP_LEVEL_CALLS_PROVED                = BACKWARD_CHAINING_TOP_LEVEL_CALLS + 1;
    static const size_t BACKWARD_CHAINING_ADDED_TO_CHECKED                      = BACKWARD_CHAINING_TOP_LEVEL_CALLS_PROVED + 1;
    static const size_t BACKWARD_CHAINING_ADDED_TO_CHECKED_OPTIMIZED            = BACKWARD_CHAINING_ADDED_TO_CHECKED + 1;
    static const size_t BACKWARD_CHAINING_NONRECURSIVE_RULES                    = BACKWARD_CHAINING_ADDED_TO_CHECKED_OPTIMIZED + 1;
    static const size_t BACKWARD_CHAINING_NONRECURSIVE_RULES_MATCHED            = BACKWARD_CHAINING_NONRECURSIVE_RULES + 1;
    static const size_t BACKWARD_CHAINING_RECURSIVE_RULES                       = BACKWARD_CHAINING_NONRECURSIVE_RULES_MATCHED + 1;
    static const size_t BACKWARD_CHAINING_RECURSIVE_RULES_MATCHED               = BACKWARD_CHAINING_RECURSIVE_RULES + 1;
    static const size_t BACKWARD_CHAINING_RECURSIVE_RULE_INSTANCES              = BACKWARD_CHAINING_RECURSIVE_RULES_MATCHED + 1;
    static const size_t BACKWARD_CHAINING_RECURSIVE_RULE_INSTANCES_REPLACEMENT  = BACKWARD_CHAINING_RECURSIVE_RULE_INSTANCES + 1;
    static const size_t BACKWARD_CHAINING_RECURSIVE_RULE_INSTANCES_REFLEXIVITY  = BACKWARD_CHAINING_RECURSIVE_RULE_INSTANCES_REPLACEMENT + 1;
    static const size_t BACKWARD_CHAINING_RECURSIVE_RULE_INSTANCES_RULES        = BACKWARD_CHAINING_RECURSIVE_RULE_INSTANCES_REFLEXIVITY + 1;

    static const size_t SATURATE_TITLE                                          = BACKWARD_CHAINING_RECURSIVE_RULE_INSTANCES_RULES + 1;
    static const size_t SATURATE_DELAYED                                        = SATURATE_TITLE + 1 + NUMBER_OF_DERIVATION_COUNTERS;
    static const size_t SATURATE_DELAYED_REPLACEMENT                            = SATURATE_DELAYED + 1;
    static const size_t SATURATE_DELAYED_REPLACEMENT_SUCCESFUL                  = SATURATE_DELAYED_REPLACEMENT + 1;
    static const size_t SATURATE_DELAYED_REFLEXIVITY                            = SATURATE_DELAYED_REPLACEMENT_SUCCESFUL + 1;
    static const size_t SATURATE_DELAYED_REFLEXIVITY_SUCCESFUL                  = SATURATE_DELAYED_REFLEXIVITY + 1;
    static const size_t SATURATE_DELAYED_RULES                                  = SATURATE_DELAYED_REFLEXIVITY_SUCCESFUL + 1;
    static const size_t SATURATE_DELAYED_RULES_SUCCESFUL                        = SATURATE_DELAYED_RULES + 1;
    static const size_t SATURATE_SHORT_CIRCUIT_REFLEXIVITY_EDB                  = SATURATE_DELAYED_RULES_SUCCESFUL + 1;
    static const size_t SATURATE_DENORMALIZED_CHECKED                           = SATURATE_SHORT_CIRCUIT_REFLEXIVITY_EDB + 1;
    static const size_t SATURATE_USED_DELAYED                                   = SATURATE_DENORMALIZED_CHECKED + 1;
    static const size_t SATURATE_USED_EDB                                       = SATURATE_USED_DELAYED + 1;
    static const size_t SATURATE_USED_PREVIOUS_LEVEL                            = SATURATE_USED_EDB + 1;
    static const size_t SATURATE_DISPROVED                                      = SATURATE_USED_PREVIOUS_LEVEL + 1;

    static const size_t EVALUATION_OF_INSERTED_RULES_TITLE                      = SATURATE_DISPROVED + 1;

    static const size_t TUPLE_INSERTION_TITLE                                   = EVALUATION_OF_INSERTED_RULES_TITLE + 1 + NUMBER_OF_DERIVATION_COUNTERS;
    static const size_t TUPLE_INSERTION_DERIVATIONS_FROM_INS                    = TUPLE_INSERTION_TITLE + 1 + NUMBER_OF_DERIVATION_COUNTERS;
    static const size_t TUPLE_INSERTION_DERIVATIONS_FROM_INS_SUCCESSFUL         = TUPLE_INSERTION_DERIVATIONS_FROM_INS + 1;

    static const size_t UPDATE_IDB_TITLE                                        = TUPLE_INSERTION_DERIVATIONS_FROM_INS_SUCCESSFUL + 1;
    static const size_t UPDATE_IDB_DELETED                                      = UPDATE_IDB_TITLE + 1;
    static const size_t UPDATE_IDB_DELETED_CHANGED                              = UPDATE_IDB_DELETED + 1;
    static const size_t UPDATE_IDB_ADDED                                        = UPDATE_IDB_DELETED_CHANGED + 1;
    static const size_t UPDATE_IDB_ADDED_CHANGED                                = UPDATE_IDB_ADDED + 1;
    static const size_t UPDATE_IDB_COPIED_CLASSES                               = UPDATE_IDB_ADDED_CHANGED + 1;

    static const size_t NUMBER_OF_INCREMENTAL_COUNTERS                          = UPDATE_IDB_COPIED_CLASSES + 1;

protected:

    struct ThreadStateEx : public ThreadState {
        std::vector<bool> m_recursiveRuleInstanceSeenInBackwardChaining;

        ThreadStateEx(const size_t numberOfCountersPerLevel, const char* const* counterDescriptions, const size_t initialNumberOfLevels);

    };

    virtual const char* const* describeStatistics(size_t& numberOfCounters);

    virtual std::unique_ptr<ThreadState> newThreadState(const size_t numberOfCountersPerLevel, const char* const* counterDescriptions, const size_t initialNumberOfLevels);

public:

    IncrementalStatistics();

    virtual void taskStarted(const DataStore& dataStore, const size_t maxComponentLevel);

    virtual void taskFinished(const DataStore& dataStore);

    virtual void deletedRuleEvaluationStarted(const size_t workerIndex, const RuleInfo& ruleInfo);
    
    virtual void deletedRuleEvaluationFinished(const size_t workerIndex);
    
    virtual void addedRuleEvaluationStarted(const size_t workerIndex, const RuleInfo& ruleInfo);
    
    virtual void addedRuleEvaluationFinished(const size_t workerIndex);
    
    virtual void tupleDeletionPreviousLevelsStarted(const size_t workerIndex);

    virtual void tupleDeletionRecursiveStarted(const size_t workerIndex);

    virtual void tupleDeletionFinished(const size_t workerIndex);

    virtual void possiblyDeletedTupleExtracted(const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes);

    virtual void possiblyDeletedTupleProcessed(const size_t workerIndex, const bool proved);

    virtual void deletionPropagationStarted(const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const bool fromPreviousLevel);

    virtual void propagatedDeletionViaReplacement(const size_t workerIndex, const ResourceID resourceID, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const bool wasAdded);

    virtual void deletionPropagationFinished(const size_t workerIndex);

    virtual void checkingProvabilityStarted(const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const bool addedToChecked);

    virtual void tupleOptimized(const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes);

    virtual void reflexiveSameAsRuleInstancesOptimized(const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes);

    virtual void backwardReflexiveSameAsRuleInstanceStarted(const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const std::vector<ResourceID>& supportingArgumentsBuffer, const std::vector<ArgumentIndex>& supportingArgumentIndexes);

    virtual void backwardReflexiveSameAsRuleInstanceFinished(const size_t workerIndex);

    virtual void backwardReplacementRuleInstanceStarted(const size_t workerIndex, const size_t positionIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes);

    virtual void backwardReplacementRuleInstanceFinished(const size_t workerIndex);

    virtual void backwardRecursiveRuleStarted(const size_t workerIndex, const HeadAtomInfo& headAtomInfo);

    virtual void backwardRecursiveRuleInstanceStarted(const size_t workerIndex, const HeadAtomInfo& headAtomInfo, const SupportingFactsEvaluator& currentBodyMatches);

    virtual void backwardRecursiveRuleInstanceAtomStarted(const size_t workerIndex, const HeadAtomInfo& headAtomInfo, const SupportingFactsEvaluator& currentBodyMatches, const size_t currentBodyIndex);

    virtual void backwardRecursiveRuleInstanceAtomFinished(const size_t workerIndex);

    virtual void backwardRecursiveRuleInstanceFinished(const size_t workerIndex);

    virtual void backwardRecursiveRuleFinished(const size_t workerIndex);

    virtual void checkingProvabilityFinished(const size_t workerIndex);

    virtual void backwardNonrecursiveRuleStarted(const size_t workerIndex, const HeadAtomInfo& headAtomInfo);

    virtual void backwardNonrecursiveInstanceMatched(const size_t workerIndex, const HeadAtomInfo& headAtomInfo, const SupportingFactsEvaluator& currentBodyMatches);

    virtual void backwardNonrecursiveRuleFinished(const size_t workerIndex);

    virtual void checkedReflexiveSameAsTupleProvedFromEDB(const size_t workerIndex, const ResourceID originalResourceID, const ResourceID denormalizedResourceID, const ResourceID normalizedProvedResourceID, const bool wasAdded);

    virtual void checkedTupleChecked(const size_t workerIndex, const std::vector<ResourceID>& originalArgumentsBuffer, const std::vector<ResourceID>& denormalizedArgumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes);

    virtual void checkedTupleProved(const size_t workerIndex, const std::vector<ResourceID>& originalArgumentsBuffer, const std::vector<ResourceID>& denormalizedArgumentsBuffer, const std::vector<ResourceID>& normalizedProvedArgumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const bool fromEDB, const bool fromDelayed, const bool fromPrviousLevel, const bool wasAdded);

    virtual void processProvedStarted(const size_t workerIndex);

    virtual void tupleProvedDelayed(const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const bool wasAdded);

    virtual void reflexiveSameAsTupleDerivedDelayed(const size_t workerIndex, const ResourceID resourceID, const bool wasAdded);

    virtual void tupleNormalizedDelayed(const size_t workerIndex, std::vector<ResourceID>& originalArgumentsBuffer, const std::vector<ArgumentIndex>& originalArgumentIndexes, std::vector<ResourceID>& normalizedArgumentsBuffer, const std::vector<ArgumentIndex>& normalizedArgumentIndexes, const bool wasAdded);

    virtual void checkedTupleDisproved(const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const bool wasAdded);

    virtual void processProvedFinished(const size_t workerIndex);

    virtual void updateEqualityManagerStarted(const size_t workerIndex);

    virtual void equivalenceClassCopied(const size_t workerIndex, const ResourceID resourceID, const EqualityManager& sourceEqualityManager, const EqualityManager& targetEqualityManager);

    virtual void updateEqualityManagerFinished(const size_t workerIndex);

    virtual void insertionPreviousLevelsStarted(const size_t workerIndex);
    
    virtual void insertionRecursiveStarted(const size_t workerIndex);

    virtual void insertedTupleAddedToIDB(const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const bool wasAdded);

    virtual void insertionFinished(const size_t workerIndex);

    virtual void propagateDeletedProvedStarted(const size_t workerIndex, const size_t componentLevel);

    virtual void tupleDeleted(const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const bool wasDeleted);

    virtual void tupleAdded(const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const bool wasAdded);

    virtual void propagateDeletedProvedFinished(const size_t workerIndex);
    
};

#endif /* INCREMENTALSTATISTICS_H_ */
