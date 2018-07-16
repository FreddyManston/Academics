// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef INCREMENTALMONITOR_H_
#define INCREMENTALMONITOR_H_

#include "MaterializationMonitor.h"

class EqualityManager;
class SupportingFactsEvaluator;
class HeadAtomInfo;
class RuleInfo;

class IncrementalMonitor : public MaterializationMonitor {

public:

    virtual ~IncrementalMonitor() {
    }

    virtual void deletedRuleEvaluationStarted(const size_t workerIndex, const RuleInfo& ruleInfo) = 0;

    virtual void deletedRuleEvaluationFinished(const size_t workerIndex) = 0;

    virtual void addedRuleEvaluationStarted(const size_t workerIndex, const RuleInfo& ruleInfo) = 0;
    
    virtual void addedRuleEvaluationFinished(const size_t workerIndex) = 0;
    
    virtual void tupleDeletionPreviousLevelsStarted(const size_t workerIndex) = 0;

    virtual void tupleDeletionRecursiveStarted(const size_t workerIndex) = 0;

    virtual void tupleDeletionFinished(const size_t workerIndex) = 0;

    virtual void possiblyDeletedTupleExtracted(const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) = 0;

    virtual void possiblyDeletedTupleProcessed(const size_t workerIndex, const bool proved) = 0;

    virtual void deletionPropagationStarted(const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const bool fromPreviousLevel) = 0;

    virtual void propagatedDeletionViaReplacement(const size_t workerIndex, const ResourceID resourceID, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const bool wasAdded) = 0;
    
    virtual void deletionPropagationFinished(const size_t workerIndex) = 0;

    virtual void checkingProvabilityStarted(const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const bool addedToChecked) = 0;

    virtual void tupleOptimized(const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) = 0;

    virtual void reflexiveSameAsRuleInstancesOptimized(const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) = 0;

    virtual void backwardReflexiveSameAsRuleInstanceStarted(const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const std::vector<ResourceID>& supportingArgumentsBuffer, const std::vector<ArgumentIndex>& supportingArgumentIndexes) = 0;

    virtual void backwardReflexiveSameAsRuleInstanceFinished(const size_t workerIndex) = 0;

    virtual void backwardReplacementRuleInstanceStarted(const size_t workerIndex, const size_t positionIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) = 0;

    virtual void backwardReplacementRuleInstanceFinished(const size_t workerIndex) = 0;

    virtual void backwardRecursiveRuleStarted(const size_t workerIndex, const HeadAtomInfo& headAtomInfo) = 0;

    virtual void backwardRecursiveRuleInstanceStarted(const size_t workerIndex, const HeadAtomInfo& headAtomInfo, const SupportingFactsEvaluator& currentBodyMatches) = 0;

    virtual void backwardRecursiveRuleInstanceAtomStarted(const size_t workerIndex, const HeadAtomInfo& headAtomInfo, const SupportingFactsEvaluator& currentBodyMatches, const size_t currentBodyIndex) = 0;

    virtual void backwardRecursiveRuleInstanceAtomFinished(const size_t workerIndex) = 0;

    virtual void backwardRecursiveRuleInstanceFinished(const size_t workerIndex) = 0;

    virtual void backwardRecursiveRuleFinished(const size_t workerIndex) = 0;

    virtual void checkingProvabilityFinished(const size_t workerIndex) = 0;

    virtual void backwardNonrecursiveRuleStarted(const size_t workerIndex, const HeadAtomInfo& headAtomInfo) = 0;

    virtual void backwardNonrecursiveInstanceMatched(const size_t workerIndex, const HeadAtomInfo& headAtomInfo, const SupportingFactsEvaluator& currentBodyMatches) = 0;

    virtual void backwardNonrecursiveRuleFinished(const size_t workerIndex) = 0;

    virtual void checkedReflexiveSameAsTupleProvedFromEDB(const size_t workerIndex, const ResourceID originalResourceID, const ResourceID denormalizedResourceID, const ResourceID normalizedProvedResourceID, const bool wasAdded) = 0;

    virtual void checkedTupleChecked(const size_t workerIndex, const std::vector<ResourceID>& originalArgumentsBuffer, const std::vector<ResourceID>& denormalizedArgumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) = 0;

    virtual void checkedTupleProved(const size_t workerIndex, const std::vector<ResourceID>& originalArgumentsBuffer, const std::vector<ResourceID>& denormalizedArgumentsBuffer, const std::vector<ResourceID>& normalizedProvedArgumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const bool fromEDB, const bool fromDelayed, const bool fromPrviousLevel, const bool wasAdded) = 0;

    virtual void processProvedStarted(const size_t workerIndex) = 0;

    virtual void tupleProvedDelayed(const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const bool wasAdded) = 0;

    virtual void reflexiveSameAsTupleDerivedDelayed(const size_t workerIndex, const ResourceID resourceID, const bool wasAdded) = 0;

    virtual void tupleNormalizedDelayed(const size_t workerIndex, std::vector<ResourceID>& originalArgumentsBuffer, const std::vector<ArgumentIndex>& originalArgumentIndexes, std::vector<ResourceID>& normalizedArgumentsBuffer, const std::vector<ArgumentIndex>& normalizedArgumentIndexes, const bool wasAdded) = 0;

    virtual void checkedTupleDisproved(const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const bool wasAdded) = 0;

    virtual void processProvedFinished(const size_t workerIndex) = 0;

    virtual void updateEqualityManagerStarted(const size_t workerIndex) = 0;

    virtual void equivalenceClassCopied(const size_t workerIndex, const ResourceID resourceID, const EqualityManager& sourceEqualityManager, const EqualityManager& targetEqualityManager) = 0;

    virtual void updateEqualityManagerFinished(const size_t workerIndex) = 0;

    virtual void insertionPreviousLevelsStarted(const size_t workerIndex) = 0;

    virtual void insertionRecursiveStarted(const size_t workerIndex) = 0;

    virtual void insertedTupleAddedToIDB(const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const bool wasAdded) = 0;

    virtual void insertionFinished(const size_t workerIndex) = 0;

    virtual void propagateDeletedProvedStarted(const size_t workerIndex, const size_t componentLevel) = 0;

    virtual void tupleDeleted(const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const bool wasDeleted) = 0;

    virtual void tupleAdded(const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const bool wasAdded) = 0;

    virtual void propagateDeletedProvedFinished(const size_t workerIndex) = 0;
    
};

#endif /* INCREMENTALMONITOR_H_ */
