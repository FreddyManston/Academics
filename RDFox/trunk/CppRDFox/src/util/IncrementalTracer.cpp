// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../reasoning/RuleIndexImpl.h"
#include "AbstractTracerImpl.h"
#include "IncrementalTracer.h"

void IncrementalTracer::printRuleInstance(const HeadAtomInfo& headAtomInfo, const SupportingFactsEvaluator& currentBodyMatches) {
    print(currentBodyMatches.getArgumentsBuffer(), headAtomInfo.getSupportingFactsHeadArgumentIndexes());
    m_output << " :- ";
    const size_t numberOfBodyLiterals = currentBodyMatches.getNumberOfBodyLiterals();
    for (size_t bodyLiteralIndex = 0; bodyLiteralIndex < numberOfBodyLiterals; ++bodyLiteralIndex) {
        if (bodyLiteralIndex != 0)
            m_output << ", ";
        const TupleIterator& tupleIterator = currentBodyMatches.getBodyLiteral(bodyLiteralIndex);
        print(tupleIterator.getArgumentsBuffer(), tupleIterator.getArgumentIndexes());
        if (headAtomInfo.isSupportingBodyAtom<true>(bodyLiteralIndex))
            m_output << '*';
    }
    m_output << " .";
}

IncrementalTracer::IncrementalTracer(Prefixes& prefixes, Dictionary& dictionary, std::ostream& output) : AbstractTracer<IncrementalMonitor>(prefixes, dictionary, output) {
}

void IncrementalTracer::deletedRuleEvaluationStarted(const size_t workerIndex, const RuleInfo& ruleInfo) {
    MutexHolder mutexHolder(m_mutex);
    printIndent(workerIndex);
    m_output << "Evaluating deleted rule " << ruleInfo.getRule()->toString(m_prefixes) << std::endl;
    increaseIndent(workerIndex);
}

void IncrementalTracer::deletedRuleEvaluationFinished(const size_t workerIndex) {
    MutexHolder mutexHolder(m_mutex);
    decreaseIndent(workerIndex);
}

void IncrementalTracer::addedRuleEvaluationStarted(const size_t workerIndex, const RuleInfo& ruleInfo) {
    MutexHolder mutexHolder(m_mutex);
    printIndent(workerIndex);
    m_output << "Evaluating inserted rule " << ruleInfo.getRule()->toString(m_prefixes) << std::endl;
    increaseIndent(workerIndex);
}

void IncrementalTracer::addedRuleEvaluationFinished(const size_t workerIndex) {
    MutexHolder mutexHolder(m_mutex);
    decreaseIndent(workerIndex);
}

void IncrementalTracer::tupleDeletionPreviousLevelsStarted(const size_t workerIndex) {
    MutexHolder mutexHolder(m_mutex);
    printIndent(workerIndex);
    m_output << "Applying deletion rules to tuples from previous levels" << std::endl;
    increaseIndent(workerIndex);
}

void IncrementalTracer::tupleDeletionRecursiveStarted(const size_t workerIndex) {
    MutexHolder mutexHolder(m_mutex);
    decreaseIndent(workerIndex);
    printIndent(workerIndex);
    m_output << "Applying recursive deletion rules" << std::endl;
    increaseIndent(workerIndex);
}

void IncrementalTracer::tupleDeletionFinished(const size_t workerIndex) {
    MutexHolder mutexHolder(m_mutex);
    decreaseIndent(workerIndex);
}

void IncrementalTracer::possiblyDeletedTupleExtracted(const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) {
    MutexHolder mutexHolder(m_mutex);
    printIndent(workerIndex);
    m_output << "Extracted possibly deleted tuple ";
    print(argumentsBuffer, argumentIndexes);
    m_output << std::endl;
    increaseIndent(workerIndex);
}

void IncrementalTracer::possiblyDeletedTupleProcessed(const size_t workerIndex, const bool proved) {
    MutexHolder mutexHolder(m_mutex);
    if (proved) {
        printIndent(workerIndex);
        m_output << "Possibly deleted tuple proved" << std::endl;
    }
    decreaseIndent(workerIndex);
}

void IncrementalTracer::deletionPropagationStarted(const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const bool fromPreviousLevel) {
    MutexHolder mutexHolder(m_mutex);
    printIndent(workerIndex);
    m_output << "Applying deletion rules to ";
    print(argumentsBuffer, argumentIndexes);
    m_output << std::endl;
    increaseIndent(workerIndex);
}

void IncrementalTracer::propagatedDeletionViaReplacement(const size_t workerIndex, const ResourceID resourceID, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const bool wasAdded) {
    MutexHolder mutexHolder(m_mutex);
    printIndent(workerIndex);
    m_output << "Propagating replacament of ";
    print(resourceID);
    m_output << " to ";
    print(argumentsBuffer, argumentIndexes);
    m_output << "    { ";
    if (!wasAdded)
        m_output << "not ";
    m_output << "added }" << std::endl;
}

void IncrementalTracer::deletionPropagationFinished(const size_t workerIndex) {
    MutexHolder mutexHolder(m_mutex);
    decreaseIndent(workerIndex);
}

void IncrementalTracer::checkingProvabilityStarted(const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const bool addedToChecked) {
    MutexHolder mutexHolder(m_mutex);
    printIndent(workerIndex);
    m_output << "Checking provability of ";
    print(argumentsBuffer, argumentIndexes);
    m_output << "    {";
    if (!addedToChecked)
        m_output << " not";
    m_output << " added to checked }" << std::endl;
    increaseIndent(workerIndex);
}

void IncrementalTracer::tupleOptimized(const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) {
    MutexHolder mutexHolder(m_mutex);
    printIndent(workerIndex);
    m_output << "Backward chaining stopped because the tuple was proved" << std::endl;
}

void IncrementalTracer::reflexiveSameAsRuleInstancesOptimized(const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) {
    MutexHolder mutexHolder(m_mutex);
    printIndent(workerIndex);
    m_output << "All consequences of reflexivity rules proved for ";
    print(argumentsBuffer, argumentIndexes);
    m_output << std::endl;
}

void IncrementalTracer::backwardReflexiveSameAsRuleInstanceStarted(const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const std::vector<ResourceID>& supportingArgumentsBuffer, const std::vector<ArgumentIndex>& supportingArgumentIndexes) {
    MutexHolder mutexHolder(m_mutex);
    printIndent(workerIndex);
    m_output << "Matched reflexive owl:sameAs rule instance ";
    print(argumentsBuffer, argumentIndexes);
    m_output << " :- ";
    print(supportingArgumentsBuffer, supportingArgumentIndexes);
    m_output << " ." << std::endl;
    increaseIndent(workerIndex);
}

void IncrementalTracer::backwardReflexiveSameAsRuleInstanceFinished(const size_t workerIndex) {
    MutexHolder mutexHolder(m_mutex);
    decreaseIndent(workerIndex);
}

void IncrementalTracer::backwardReplacementRuleInstanceStarted(const size_t workerIndex, const size_t positionIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) {
    MutexHolder mutexHolder(m_mutex);
    printIndent(workerIndex);
    m_output << "Checking replacement owl:sameAs rule for ";
    print(argumentsBuffer, argumentIndexes);
    m_output << " at position " << positionIndex << std::endl;
    increaseIndent(workerIndex);
}

void IncrementalTracer::backwardReplacementRuleInstanceFinished(const size_t workerIndex) {
    MutexHolder mutexHolder(m_mutex);
    decreaseIndent(workerIndex);
}

void IncrementalTracer::backwardRecursiveRuleStarted(const size_t workerIndex, const HeadAtomInfo& headAtomInfo) {
    const Rule& rule = headAtomInfo.getRuleInfo().getRule();
    MutexHolder mutexHolder(m_mutex);
    printIndent(workerIndex);
    m_output << "Checking recursive rule " << rule->toString(m_prefixes);
    if (rule->getNumberOfHeadAtoms() != 1)
        m_output << " for head atom " << rule->getHead(headAtomInfo.getHeadAtomIndex());
    m_output << std::endl;
    increaseIndent(workerIndex);
}

void IncrementalTracer::backwardRecursiveRuleInstanceStarted(const size_t workerIndex, const HeadAtomInfo& headAtomInfo, const SupportingFactsEvaluator& currentBodyMatches) {
    MutexHolder mutexHolder(m_mutex);
    printIndent(workerIndex);
    m_output << "Matched recursive rule instance ";
    printRuleInstance(headAtomInfo, currentBodyMatches);
    m_output << std::endl;
    increaseIndent(workerIndex);
}

void IncrementalTracer::backwardRecursiveRuleInstanceAtomStarted(const size_t workerIndex, const HeadAtomInfo& headAtomInfo, const SupportingFactsEvaluator& currentBodyMatches, const size_t currentBodyIndex) {
    MutexHolder mutexHolder(m_mutex);
    printIndent(workerIndex);
    m_output << "Checking body atom ";
    const TupleIterator& tupleIterator = currentBodyMatches.getBodyLiteral(currentBodyIndex);
    print(tupleIterator.getArgumentsBuffer(), tupleIterator.getArgumentIndexes());
    m_output << std::endl;
    increaseIndent(workerIndex);
}

void IncrementalTracer::backwardRecursiveRuleInstanceAtomFinished(const size_t workerIndex) {
    MutexHolder mutexHolder(m_mutex);
    decreaseIndent(workerIndex);
}

void IncrementalTracer::backwardRecursiveRuleInstanceFinished(const size_t workerIndex) {
    MutexHolder mutexHolder(m_mutex);
    decreaseIndent(workerIndex);
}

void IncrementalTracer::backwardRecursiveRuleFinished(const size_t workerIndex) {
    MutexHolder mutexHolder(m_mutex);
    decreaseIndent(workerIndex);
}

void IncrementalTracer::checkingProvabilityFinished(const size_t workerIndex) {
    MutexHolder mutexHolder(m_mutex);
    decreaseIndent(workerIndex);
}

void IncrementalTracer::backwardNonrecursiveRuleStarted(const size_t workerIndex, const HeadAtomInfo& headAtomInfo) {
    const Rule& rule = headAtomInfo.getRuleInfo().getRule();
    MutexHolder mutexHolder(m_mutex);
    printIndent(workerIndex);
    m_output << "Checking nonrecursive rule " << rule->toString(m_prefixes);
    if (rule->getNumberOfHeadAtoms() != 1)
        m_output << " for head atom " << rule->getHead(headAtomInfo.getHeadAtomIndex());
    m_output << std::endl;
}

void IncrementalTracer::backwardNonrecursiveInstanceMatched(const size_t workerIndex, const HeadAtomInfo& headAtomInfo, const SupportingFactsEvaluator& currentBodyMatches) {
    MutexHolder mutexHolder(m_mutex);
    increaseIndent(workerIndex);
    printIndent(workerIndex);
    m_output << "Matched nonrecursive rule instance ";
    printRuleInstance(headAtomInfo, currentBodyMatches);
    m_output << std::endl;
    decreaseIndent(workerIndex);
}

void IncrementalTracer::backwardNonrecursiveRuleFinished(const size_t workerIndex) {
}

void IncrementalTracer::checkedReflexiveSameAsTupleProvedFromEDB(const size_t workerIndex, const ResourceID originalResourceID, const ResourceID denormalizedResourceID, const ResourceID normalizedProvedResourceID, const bool wasAdded) {
    MutexHolder mutexHolder(m_mutex);
    printIndent(workerIndex);
    m_output << "Derived reflexive tuple [ ";
    print(normalizedProvedResourceID);
    m_output << ", owl:sameAs, ";
    print(normalizedProvedResourceID);
    m_output << " ] directly from EDB    { " << (wasAdded ? "" : "not ") << "added }" << std::endl;
}

void IncrementalTracer::checkedTupleChecked(const size_t workerIndex, const std::vector<ResourceID>& originalArgumentsBuffer, const std::vector<ResourceID>& denormalizedArgumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) {
}

void IncrementalTracer::checkedTupleProved(const size_t workerIndex, const std::vector<ResourceID>& originalArgumentsBuffer, const std::vector<ResourceID>& denormalizedArgumentsBuffer, const std::vector<ResourceID>& normalizedProvedArgumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const bool fromEDB, const bool fromDelayed, const bool fromPreviousLevel, const bool wasAdded) {
    MutexHolder mutexHolder(m_mutex);
    printIndent(workerIndex);
    m_output << "Derived tuple ";
    print(normalizedProvedArgumentsBuffer, argumentIndexes);
    m_output << "    { " << (fromEDB ? "" : "not ") << "from EDB, " << (fromDelayed ? "" : "not ") << "from delayed, " << (fromPreviousLevel ? "" : "not ") << "from previous level, " << (wasAdded ? "" : "not ") << "added }" << std::endl;
}

void IncrementalTracer::processProvedStarted(const size_t workerIndex) {
    MutexHolder mutexHolder(m_mutex);
    printIndent(workerIndex);
    m_output << "Processing the proved list" << std::endl;
    increaseIndent(workerIndex);
}

void IncrementalTracer::tupleProvedDelayed(const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const bool wasAdded) {
    MutexHolder mutexHolder(m_mutex);
    printIndent(workerIndex);
    m_output << "Delaying tuple ";
    print(argumentsBuffer, argumentIndexes);
    m_output << "    { " << (wasAdded ? "" : "not ") << "added }" << std::endl;
}

void IncrementalTracer::reflexiveSameAsTupleDerivedDelayed(const size_t workerIndex, const ResourceID resourceID, const bool wasAdded) {
    MutexHolder mutexHolder(m_mutex);
    printIndent(workerIndex);
    m_output << "Delaying reflexive tuple [ ";
    print(resourceID);
    m_output << ", owl:sameAs, ";
    print(resourceID);
    m_output << " ]    { " << (wasAdded ? "" : "not ") << "added }" << std::endl;
}

void IncrementalTracer::tupleNormalizedDelayed(const size_t workerIndex, std::vector<ResourceID>& originalArgumentsBuffer, const std::vector<ArgumentIndex>& originalArgumentIndexes, std::vector<ResourceID>& normalizedArgumentsBuffer, const std::vector<ArgumentIndex>& normalizedArgumentIndexes, const bool wasAdded) {
    MutexHolder mutexHolder(m_mutex);
    printIndent(workerIndex);
    m_output << "Delaying normalized tuple ";
    print(normalizedArgumentsBuffer, normalizedArgumentIndexes);
    m_output << "    { " << (wasAdded ? "" : "not ") << "added }" << std::endl;
}

void IncrementalTracer::checkedTupleDisproved(const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const bool wasAdded) {
    MutexHolder mutexHolder(m_mutex);
    printIndent(workerIndex);
    m_output << "Tuple disproved ";
    print(argumentsBuffer, argumentIndexes);
    m_output << "    { " << (wasAdded ? "" : "not ") << "added }" << std::endl;
}

void IncrementalTracer::processProvedFinished(const size_t workerIndex) {
    MutexHolder mutexHolder(m_mutex);
    decreaseIndent(workerIndex);
}

void IncrementalTracer::updateEqualityManagerStarted(const size_t workerIndex) {
    MutexHolder mutexHolder(m_mutex);
    printIndent(workerIndex);
    m_output << "Updating equality manager" << std::endl;
    increaseIndent(workerIndex);
}

void IncrementalTracer::equivalenceClassCopied(const size_t workerIndex, const ResourceID resourceID, const EqualityManager& sourceEqualityManager, const EqualityManager& targetEqualityManager) {
    MutexHolder mutexHolder(m_mutex);
    printIndent(workerIndex);
    m_output << "Copying the equivalence class for ";
    print(resourceID);
    m_output << std::endl;
}

void IncrementalTracer::updateEqualityManagerFinished(const size_t workerIndex) {
    MutexHolder mutexHolder(m_mutex);
    decreaseIndent(workerIndex);
}

void IncrementalTracer::insertionPreviousLevelsStarted(const size_t workerIndex) {
    MutexHolder mutexHolder(m_mutex);
    printIndent(workerIndex);
    m_output << "Applying insertion rules to tuples from previous levels" << std::endl;
    increaseIndent(workerIndex);
}

void IncrementalTracer::insertionRecursiveStarted(const size_t workerIndex) {
    MutexHolder mutexHolder(m_mutex);
    decreaseIndent(workerIndex);
    printIndent(workerIndex);
    m_output << "Applying recursive insertion rules" << std::endl;
    increaseIndent(workerIndex);
}

void IncrementalTracer::insertedTupleAddedToIDB(const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const bool wasAdded) {
    MutexHolder mutexHolder(m_mutex);
    printIndent(workerIndex);
    m_output << "Tuple added ";
    print(argumentsBuffer, argumentIndexes);
    m_output << "    { " << (wasAdded ? "" : "not ") << "added }" << std::endl;
}

void IncrementalTracer::insertionFinished(const size_t workerIndex) {
    MutexHolder mutexHolder(m_mutex);
    decreaseIndent(workerIndex);
}

void IncrementalTracer::propagateDeletedProvedStarted(const size_t workerIndex, const size_t componentLevel) {
    MutexHolder mutexHolder(m_mutex);
    printIndent(workerIndex);
    m_output << "Propagating deleted and proved tuples into the store";
    if (componentLevel != static_cast<size_t>(-1))
        m_output << " for level " << componentLevel;
    m_output << std::endl;
    increaseIndent(workerIndex);
}

void IncrementalTracer::tupleDeleted(const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const bool wasDeleted) {
    MutexHolder mutexHolder(m_mutex);
    printIndent(workerIndex);
    m_output << "Tuple deleted ";
    print(argumentsBuffer, argumentIndexes);
    m_output << "    { " << (wasDeleted ? "" : "not ") << "deleted }" << std::endl;
}

void IncrementalTracer::tupleAdded(const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const bool wasAdded) {
    MutexHolder mutexHolder(m_mutex);
    printIndent(workerIndex);
    m_output << "Tuple added ";
    print(argumentsBuffer, argumentIndexes);
    m_output << "    { " << (wasAdded ? "" : "not ") << "added }" << std::endl;
}

void IncrementalTracer::propagateDeletedProvedFinished(const size_t workerIndex) {
    MutexHolder mutexHolder(m_mutex);
    decreaseIndent(workerIndex);
}
