// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef INCREMENTALTRACER_H_
#define INCREMENTALTRACER_H_

#include "../reasoning/IncrementalMonitor.h"
#include "AbstractTracer.h"

class IncrementalTracer : public AbstractTracer<IncrementalMonitor> {

protected:

    void printRuleInstance(const HeadAtomInfo& headAtomInfo, const SupportingFactsEvaluator& currentBodyMatches);

public:

    IncrementalTracer(Prefixes& prefixes, Dictionary& dictionary, std::ostream& output);

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

    void reflexiveSameAsRuleInstancesOptimized(const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes);

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

    virtual void checkedTupleProved(const size_t workerIndex, const std::vector<ResourceID>& originalArgumentsBuffer, const std::vector<ResourceID>& denormalizedArgumentsBuffer, const std::vector<ResourceID>& normalizedProvedArgumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const bool fromEDB, const bool fromDelayed, const bool fromPreviousLevel, const bool wasAdded);

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

#endif /* INCREMENTALTRACER_H_ */
