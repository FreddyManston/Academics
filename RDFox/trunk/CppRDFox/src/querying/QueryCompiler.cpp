// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../RDFStoreException.h"
#include "../dictionary/Dictionary.h"
#include "../dictionary/ResourceValueCache.h"
#include "../storage/DataStore.h"
#include "../storage/TupleTable.h"
#include "../storage/Parameters.h"
#include "../util/Vocabulary.h"
#include "../util/ThreadContext.h"
#include "../util/LogicObjectWalker.h"
#include "QueryCompiler.h"
#include "TermArray.h"
#include "QueryDecomposition.h"
#include "NestedIndexLoopJoinIterator.h"
#include "LeftJoinIterator.h"
#include "UnionIterator.h"
#include "DifferenceIterator.h"
#include "ValuesIterator.h"
#include "LimitOneIterator.h"
#include "DistinctIterator.h"
#include "EmptyTupleIterator.h"
#include "EqualityExpansionIterator.h"
#include "QueryIterator.h"

// NeedsRenamingDetector

class NeedsRenamingDetector : public LogicObjectWalker {

protected:

    bool m_needsRenaming;

public:

    NeedsRenamingDetector() : m_needsRenaming(false) {
    }

    bool getNeedsRenaming() const {
        return m_needsRenaming;
    }

    virtual void visit(const Minus& object) {
        m_needsRenaming = true;
    }

    virtual void visit(const Query& object) {
        m_needsRenaming = true;
    }

};

// QueryCompiler

CardinalityType QueryCompiler::getCardinalityType() const {
    if (m_exactCardinality)
        return m_queryDomain == QUERY_DOMAIN_IDB && m_dataStore.getEqualityAxiomatizationType() != EQUALITY_AXIOMATIZATION_OFF ? CARDINALITY_EXACT_WITH_EQUALITY : CARDINALITY_EXACT_NO_EQUALITY;
    else
        return CARDINALITY_NOT_EXACT;
}

void QueryCompiler::getTupleStatusFlags(TupleStatus& tupleStatusMask, TupleStatus& tupleStatusExpectedValue) const {
    switch (m_queryDomain) {
    case QUERY_DOMAIN_EDB:
        tupleStatusMask = TUPLE_STATUS_COMPLETE | TUPLE_STATUS_EDB;
        tupleStatusExpectedValue = TUPLE_STATUS_COMPLETE | TUPLE_STATUS_EDB;
        break;
    case QUERY_DOMAIN_IDBrepNoEDB:
        tupleStatusMask = TUPLE_STATUS_COMPLETE | TUPLE_STATUS_EDB | TUPLE_STATUS_IDB | TUPLE_STATUS_IDB_MERGED;
        tupleStatusExpectedValue = TUPLE_STATUS_COMPLETE | TUPLE_STATUS_IDB;
        break;
    default:
        tupleStatusMask = TUPLE_STATUS_COMPLETE | TUPLE_STATUS_IDB | TUPLE_STATUS_IDB_MERGED;
        tupleStatusExpectedValue = TUPLE_STATUS_COMPLETE | TUPLE_STATUS_IDB;
        break;
    }
}

template<>
const QueryDecompositionNode& QueryCompiler::chooseRootNode<true>(const QueryDecomposition& queryDecomposition, const ArgumentIndexSet& possiblyBoundVariables, const ArgumentIndexSet& surelyBoundVariables, std::vector<ResourceID>& argumentsBuffer) const {
    const QueryDecompositionNode* selectedRoot = 0;
    size_t selectedRootCountEstimate = 0xffffffffffffffffULL;
    bool selectedRootHasAllAnswerVariables = false;
    const size_t numberOfNodes = queryDecomposition.getNumberOfNodes();
    for (size_t nodeIndex = 0; nodeIndex < numberOfNodes; ++nodeIndex) {
        const QueryDecompositionNode& node = queryDecomposition.getNode(nodeIndex);
        bool isCrossProduct;
        size_t countEstimate;
        evaluateNode(node, possiblyBoundVariables, surelyBoundVariables, argumentsBuffer, isCrossProduct, countEstimate);
        const bool hasAllAnswerVariables = node.getNodeVariables().contains(queryDecomposition.getAnswerVariables());
        if ((hasAllAnswerVariables && !selectedRootHasAllAnswerVariables) || (countEstimate <= selectedRootCountEstimate && (hasAllAnswerVariables || !selectedRootHasAllAnswerVariables))) {
            selectedRoot = &node;
            selectedRootCountEstimate = countEstimate;
            selectedRootHasAllAnswerVariables = hasAllAnswerVariables;
        }
    }
    if (selectedRoot == 0)
        throw RDF_STORE_EXCEPTION("This query decomposition is empty.");
    return *selectedRoot;
}

template<>
const QueryDecompositionNode& QueryCompiler::chooseRootNode<false>(const QueryDecomposition& queryDecomposition, const ArgumentIndexSet& possiblyBoundVariables, const ArgumentIndexSet& surelyBoundVariables, std::vector<ResourceID>& argumentsBuffer) const {
    const QueryDecompositionNode* selectedRoot = 0;
    size_t selectedRootCountEstimate = 0xffffffffffffffffULL;
    const size_t numberOfNodes = queryDecomposition.getNumberOfNodes();
    for (size_t nodeIndex = 0; nodeIndex < numberOfNodes; ++nodeIndex) {
        const QueryDecompositionNode& node = queryDecomposition.getNode(nodeIndex);
        bool isCrossProduct;
        size_t countEstimate;
        evaluateNode(node, possiblyBoundVariables, surelyBoundVariables, argumentsBuffer, isCrossProduct, countEstimate);
        if (countEstimate <= selectedRootCountEstimate) {
            selectedRoot = &node;
            selectedRootCountEstimate = countEstimate;
        }
    }
    if (selectedRoot == 0)
        throw RDF_STORE_EXCEPTION("This query decomposition is empty.");
    return *selectedRoot;
}

void QueryCompiler::evaluateNode(const QueryDecompositionNode& node, const ArgumentIndexSet& possiblyBoundVariablesSoFar, const ArgumentIndexSet& surelyBoundVariablesSoFar, std::vector<ResourceID>& argumentsBuffer, bool& isCrossProduct, size_t& countEstimate) const {
    const size_t possiblyBoundVariablesSoFarSize = possiblyBoundVariablesSoFar.size();
    isCrossProduct = possiblyBoundVariablesSoFarSize != 0 && possiblyBoundVariablesSoFar.hasEmptyIntersectionWith(node.getNodeVariables());
    countEstimate = 0xffffffffffffffffULL;
    const size_t numberOfFormulas = node.getNumberOfFormulas();
    for (size_t formulaIndex = 0; formulaIndex < numberOfFormulas; ++formulaIndex) {
        const QueryDecompositionFormula& formula = node.getFormula(formulaIndex);
        if (possiblyBoundVariablesSoFarSize == 0 || formula.getVariables().hasNonemptyIntersectionWith(possiblyBoundVariablesSoFar)) {
            ArgumentIndexSet atomPossiblyBoundArguments(possiblyBoundVariablesSoFar);
            atomPossiblyBoundArguments.unionWith(formula.getConstants());
            ArgumentIndexSet atomSurelyBoundArguments(surelyBoundVariablesSoFar);
            atomSurelyBoundArguments.unionWith(formula.getConstants());
            const size_t atomCountEstimate = formula.getCountEstimate(m_dataStore, possiblyBoundVariablesSoFar, surelyBoundVariablesSoFar, argumentsBuffer);
            if (atomCountEstimate < countEstimate)
                countEstimate = atomCountEstimate;
        }
    }
}

void QueryCompiler::chooseNextConjunct(const QueryDecompositionNode& node, const QueryDecompositionNode* const parentNode, const ArgumentIndexSet& possiblyBoundVariablesSoFar, const ArgumentIndexSet& surelyBoundVariablesSoFar, std::vector<ResourceID>& argumentsBuffer, std::unordered_set<Formula>& processedFormulas, std::unordered_set<const QueryDecompositionNode*>& processedChildren, const QueryDecompositionFormula*& nextFormula, const QueryDecompositionNode*& nextChildNode, bool& allProcessed) const {
    nextFormula = nullptr;
    nextChildNode = nullptr;
    allProcessed = true;
    bool bestSoFarIsCrossProduct = true;
    size_t bestSoFarCountEstimate = 0xffffffffffffffffULL;
    const size_t numberOfFormulas = node.getNumberOfFormulas();
    for (size_t formulaIndex = 0; formulaIndex < numberOfFormulas; ++formulaIndex) {
        const QueryDecompositionFormula& formula = node.getFormula(formulaIndex);
        if (processedFormulas.find(formula.getFormula()) == processedFormulas.end()) {
            allProcessed = false;
            const bool isCrossProduct = possiblyBoundVariablesSoFar.size() != 0 && formula.getVariables().hasEmptyIntersectionWith(possiblyBoundVariablesSoFar);
            ArgumentIndexSet possiblyBoundArguments(possiblyBoundVariablesSoFar);
            possiblyBoundArguments.unionWith(formula.getConstants());
            ArgumentIndexSet surelyBoundArguments(surelyBoundVariablesSoFar);
            surelyBoundArguments.unionWith(formula.getConstants());
            const size_t countEstimate = formula.getCountEstimate(m_dataStore, possiblyBoundArguments, surelyBoundArguments, argumentsBuffer);
            if (countEstimate != static_cast<size_t>(-1) && (nextFormula == 0 || (bestSoFarIsCrossProduct && !isCrossProduct) || (!bestSoFarIsCrossProduct && !isCrossProduct && countEstimate < bestSoFarCountEstimate))) {
                nextFormula = &formula;
                bestSoFarIsCrossProduct = isCrossProduct;
                bestSoFarCountEstimate = countEstimate;
            }
        }
    }
    const size_t numberOfAdjacentNodes = node.getNumberOfAdjacentNodes();
    for (size_t adjacentNodeIndex = 0; adjacentNodeIndex < numberOfAdjacentNodes; ++adjacentNodeIndex) {
        const QueryDecompositionNode& childNode = node.getAdjacentNode(adjacentNodeIndex);
        if (&childNode != parentNode && processedChildren.find(&childNode) == processedChildren.end()) {
            allProcessed = false;
            bool childIsCrossProduct;
            size_t childCountEstimate;
            evaluateNode(childNode, possiblyBoundVariablesSoFar, surelyBoundVariablesSoFar, argumentsBuffer, childIsCrossProduct, childCountEstimate);
            if ((nextFormula == 0 && nextChildNode == 0) || (bestSoFarIsCrossProduct && !childIsCrossProduct) || (!bestSoFarIsCrossProduct && !childIsCrossProduct && childCountEstimate < bestSoFarCountEstimate)) {
                nextFormula = 0;
                nextChildNode = &childNode;
                bestSoFarIsCrossProduct = childIsCrossProduct;
                bestSoFarCountEstimate = childCountEstimate;
            }
        }
    }
}

std::unique_ptr<TupleIterator> QueryCompiler::processNode(ResourceValueCache& resourceValueCache, const QueryDecompositionNode& node, const QueryDecompositionNode* const parentNode, const ArgumentIndexSet& possiblyInitiallyBoundVariables, const ArgumentIndexSet& surelyInitiallyBoundVariables, const ArgumentIndexSet& answerVariables, std::vector<ResourceID>& argumentsBuffer, std::unordered_set<Formula>& processedFormulas) const {
    TupleStatus tupleStatusMask;
    TupleStatus tupleStatusExpectedValue;
    getTupleStatusFlags(tupleStatusMask, tupleStatusExpectedValue);
    std::unordered_set<const QueryDecompositionNode*> processedChildren;
    unique_ptr_vector<TupleIterator> childIterators;
    ArgumentIndexSet possiblyBoundVariablesSoFar(possiblyInitiallyBoundVariables);
    ArgumentIndexSet surelyBoundVariablesSoFar(surelyInitiallyBoundVariables);
    bool allProcessed;
    do {
        const QueryDecompositionFormula* nextFormula;
        const QueryDecompositionNode* nextChildNode;
        if (m_reorderConjunctions)
            chooseNextConjunct(node, parentNode, possiblyBoundVariablesSoFar, surelyBoundVariablesSoFar, argumentsBuffer, processedFormulas, processedChildren, nextFormula, nextChildNode, allProcessed);
        else {
            nextFormula = nullptr;
            nextChildNode = nullptr;
            allProcessed = false;
            const size_t numberOfFormulas = node.getNumberOfFormulas();
            for (size_t formulaIndex = 0; nextFormula == nullptr && formulaIndex < numberOfFormulas; ++formulaIndex) {
                const QueryDecompositionFormula& formula = node.getFormula(formulaIndex);
                if (processedFormulas.find(formula.getFormula()) == processedFormulas.end())
                    nextFormula = &formula;
            }
            if (nextFormula == nullptr) {
                const size_t numberOfAdjacentNodes = node.getNumberOfAdjacentNodes();
                for (size_t adjacentNodeIndex = 0; nextChildNode == nullptr && adjacentNodeIndex < numberOfAdjacentNodes; ++adjacentNodeIndex) {
                    const QueryDecompositionNode& childNode = node.getAdjacentNode(adjacentNodeIndex);
                    if (&childNode != parentNode && processedChildren.find(&childNode) == processedChildren.end())
                        nextChildNode = &childNode;
                }
                if (nextChildNode == nullptr)
                    allProcessed = true;
            }
        }
        std::unique_ptr<TupleIterator> childIterator;
        if (nextFormula != nullptr) {
            ArgumentIndexSet nextFormulaAllInputArguments(possiblyBoundVariablesSoFar);
            nextFormulaAllInputArguments.intersectWith(nextFormula->getVariables());
            nextFormulaAllInputArguments.unionWith(nextFormula->getConstants());
            ArgumentIndexSet nextFormulaSurelyBoundInputArguments(surelyBoundVariablesSoFar);
            nextFormulaSurelyBoundInputArguments.intersectWith(nextFormula->getVariables());
            nextFormulaSurelyBoundInputArguments.unionWith(nextFormula->getConstants());
            const QueryDecompositionLiteral* nextLiteral = dynamic_cast<const QueryDecompositionLiteral*>(nextFormula);
            if (nextLiteral != nullptr)
                childIterator = nextLiteral->createTupleIterator(m_dataStore, resourceValueCache, argumentsBuffer, nextFormulaAllInputArguments, nextFormulaSurelyBoundInputArguments, tupleStatusMask, tupleStatusExpectedValue, m_tupleIteratorMonitor);
            else
                childIterator = processFormula(resourceValueCache, nextFormula->getFormula(), nextFormula->getQueryDecomposition().getTermArray(), nextFormulaAllInputArguments, nextFormulaSurelyBoundInputArguments, nextFormula->getVariables(), nextFormula->getVariables(), argumentsBuffer);
            processedFormulas.insert(nextFormula->getFormula());
            possiblyBoundVariablesSoFar.unionWithIntersection(childIterator->getAllArguments(), nextFormula->getVariables());
            surelyBoundVariablesSoFar.unionWithIntersection(childIterator->getSurelyBoundArguments(), nextFormula->getVariables());
        }
        else if (nextChildNode != nullptr) {
            childIterator = processNode(resourceValueCache, *nextChildNode, &node, possiblyBoundVariablesSoFar, surelyBoundVariablesSoFar, answerVariables, argumentsBuffer, processedFormulas);
            processedChildren.insert(nextChildNode);
            possiblyBoundVariablesSoFar.unionWith(childIterator->getAllArguments());
            surelyBoundVariablesSoFar.unionWith(childIterator->getSurelyBoundArguments());
        }
        else if (!allProcessed)
            throw RDF_STORE_EXCEPTION("Cannot reorder atoms to satisfy the binding constraints.");
        if (childIterator.get() != nullptr)
            childIterators.push_back(std::move(childIterator));
    } while (!allProcessed);
    // Sanity check: all variables of the node must have been bound.
    if (!possiblyBoundVariablesSoFar.contains(node.getNodeVariables()))
        throw RDF_STORE_EXCEPTION("Compilation error: some of the variables of the node have not been bound.");
    // The variables visible to the parent are those bound variables in the node that are either in the parent (if one exists) or are propagated to root.
    ArgumentIndexSet allVariablesVisibleToParent(answerVariables);
    if (parentNode != nullptr)
        allVariablesVisibleToParent.unionWith(parentNode->getNodeVariables());
    allVariablesVisibleToParent.intersectWith(possiblyBoundVariablesSoFar);
    allVariablesVisibleToParent.intersectWith(node.getNodeVariables());
    ArgumentIndexSet surelyBoundVariablesVisibleToParent(allVariablesVisibleToParent);
    surelyBoundVariablesVisibleToParent.intersectWith(surelyBoundVariablesSoFar);
    // Create the iterator for this node.
    std::unique_ptr<TupleIterator> nodeIterator;
    if (childIterators.size() == 0)
        nodeIterator = newEmptyTupleIterator(m_tupleIteratorMonitor, argumentsBuffer, allVariablesVisibleToParent, surelyBoundVariablesVisibleToParent);
    else {
        if (childIterators.size() == 1 && childIterators[0]->getAllArguments() == allVariablesVisibleToParent && childIterators[0]->getSurelyBoundArguments() == surelyBoundVariablesVisibleToParent)
            nodeIterator = std::move(childIterators[0]);
        else {
            ArgumentIndexSet allInputArguments(possiblyInitiallyBoundVariables);
            allInputArguments.intersectWith(allVariablesVisibleToParent);
            ArgumentIndexSet surelyBoundInputArguments(surelyInitiallyBoundVariables);
            surelyBoundInputArguments.intersectWith(allVariablesVisibleToParent);
            nodeIterator = newNestedIndexLoopJoinIterator(m_tupleIteratorMonitor, getCardinalityType(), m_dataStore, argumentsBuffer, allInputArguments, surelyBoundInputArguments, allVariablesVisibleToParent, surelyBoundVariablesVisibleToParent, node.getNodeVariables(), std::move(childIterators));
        }
        // Limit the output to one if desired and possible
        if (!m_exactCardinality && allVariablesVisibleToParent.empty())
            nodeIterator = newLimitOneIterator(m_tupleIteratorMonitor, std::move(nodeIterator));
        // Now wrap the iterator in a distinct one if needed
        if (m_distinctThroughout)
            nodeIterator = newDistinctIterator(m_tupleIteratorMonitor, m_dataStore.getMemoryManager(), std::move(nodeIterator));
    }
    return nodeIterator;
}

std::unique_ptr<TupleIterator> QueryCompiler::processConjunctiveFormula(ResourceValueCache& resourceValueCache, const Formula& formula, const TermArray& termArray, const ArgumentIndexSet& possiblyInitiallyBoundVariables, const ArgumentIndexSet& surelyInitiallyBoundVariables, const ArgumentIndexSet& answerVariables, const ArgumentIndexSet& freeVariables, std::vector<ResourceID>& argumentsBuffer) const {
    QueryDecomposition queryDecomposition(termArray, answerVariables, formula);
    if (m_useBushyJoins)
        initializeUsingPrimalGraph(queryDecomposition, m_chooseRootWithAllAnswerVariables);
    else
        initializeWithoutDecomposition(queryDecomposition);
    const QueryDecompositionNode& rootNode = (m_chooseRootWithAllAnswerVariables ? chooseRootNode<true>(queryDecomposition, possiblyInitiallyBoundVariables, surelyInitiallyBoundVariables, argumentsBuffer) : chooseRootNode<false>(queryDecomposition, possiblyInitiallyBoundVariables, surelyInitiallyBoundVariables, argumentsBuffer));
    std::unordered_set<Formula> processedFormulas;
    std::unique_ptr<TupleIterator> rootIterator = processNode(resourceValueCache, rootNode, 0, possiblyInitiallyBoundVariables, surelyInitiallyBoundVariables, answerVariables, argumentsBuffer, processedFormulas);
    ArgumentIndexSet allInstantiatedVariables(possiblyInitiallyBoundVariables);
    allInstantiatedVariables.unionWith(rootIterator->getAllArguments());
    if (!allInstantiatedVariables.contains(answerVariables))
        throw RDF_STORE_EXCEPTION("The query does not instantiate all variables of the chosen root node.");
    return rootIterator;
}

std::unique_ptr<TupleIterator> QueryCompiler::processOptional(ResourceValueCache& resourceValueCache, const Optional& optional, const TermArray& termArray, const ArgumentIndexSet& possiblyInitiallyBoundVariables, const ArgumentIndexSet& surelyInitiallyBoundVariables, const ArgumentIndexSet& answerVariables, const ArgumentIndexSet& freeVariables, std::vector<ResourceID>& argumentsBuffer) const {
    // Invariant: surelyInitiallyBoundVariables \subseteq possiblyInitiallyBoundVariables \subseteq answerVariables \subseteq freeVariables
    unique_ptr_vector<TupleIterator> childIterators;
    ArgumentIndexSet subformulaAllVariables;
    ArgumentIndexSet subformulaAllInputVariables;
    ArgumentIndexSet subformulaSurelyBoundInputVariables;
    getFreeVariables(optional->getMain(), termArray, subformulaAllVariables);
    subformulaAllInputVariables.setToIntersection(possiblyInitiallyBoundVariables, subformulaAllVariables);
    subformulaSurelyBoundInputVariables.setToIntersection(surelyInitiallyBoundVariables, subformulaAllVariables);
    childIterators.push_back(processFormula(resourceValueCache, optional->getMain(), termArray, subformulaAllInputVariables, subformulaSurelyBoundInputVariables, subformulaAllVariables, subformulaAllVariables, argumentsBuffer));
    ArgumentIndexSet possiblyBoundVariablesSoFar(possiblyInitiallyBoundVariables);
    possiblyBoundVariablesSoFar.unionWith(childIterators.back()->getAllArguments());
    ArgumentIndexSet surelyBoundVariables(surelyInitiallyBoundVariables);
    surelyBoundVariables.unionWith(childIterators.back()->getSurelyBoundArguments());
    for (size_t index = 0; index < optional->getNumberOfOptionals(); ++index) {
        getFreeVariables(optional->getOptional(index), termArray, subformulaAllVariables);
        subformulaAllInputVariables.setToIntersection(possiblyBoundVariablesSoFar, subformulaAllVariables);
        subformulaSurelyBoundInputVariables.setToIntersection(surelyBoundVariables, subformulaAllVariables);
        childIterators.push_back(processFormula(resourceValueCache, optional->getOptional(index), termArray, subformulaAllInputVariables, subformulaSurelyBoundInputVariables, subformulaAllVariables, subformulaAllVariables, argumentsBuffer));
        possiblyBoundVariablesSoFar.unionWith(childIterators.back()->getAllArguments());
    }
    assert(possiblyBoundVariablesSoFar.contains(answerVariables));
    return newLeftJoinIterator(m_tupleIteratorMonitor, getCardinalityType(), m_dataStore, argumentsBuffer, possiblyInitiallyBoundVariables, surelyInitiallyBoundVariables, answerVariables, surelyBoundVariables, freeVariables, std::move(childIterators));
}

std::unique_ptr<TupleIterator> QueryCompiler::processDisjunction(ResourceValueCache& resourceValueCache, const Disjunction& disjunction, const TermArray& termArray, const ArgumentIndexSet& possiblyInitiallyBoundVariables, const ArgumentIndexSet& surelyInitiallyBoundVariables, const ArgumentIndexSet& answerVariables, const ArgumentIndexSet& freeVariables, std::vector<ResourceID>& argumentsBuffer) const {
    // Invariant: surelyInitiallyBoundVariables \subseteq possiblyInitiallyBoundVariables \subseteq answerVariables \subseteq freeVariables
    unique_ptr_vector<TupleIterator> childIterators;
    ArgumentIndexSet subformulaAllVariables;
    ArgumentIndexSet subformulaAllInputVariables;
    ArgumentIndexSet subformulaSurelyBoundInputVariables;
    ArgumentIndexSet surelyBoundVariablesSoFar(answerVariables);
    for (size_t index = 0; index < disjunction->getNumberOfDisjuncts(); ++index) {
        getFreeVariables(disjunction->getDisjunct(index), termArray, subformulaAllVariables);
        subformulaAllInputVariables.setToIntersection(possiblyInitiallyBoundVariables, subformulaAllVariables);
        subformulaSurelyBoundInputVariables.setToIntersection(surelyInitiallyBoundVariables, subformulaAllVariables);
        childIterators.push_back(processFormula(resourceValueCache, disjunction->getDisjunct(index), termArray, subformulaAllInputVariables, subformulaSurelyBoundInputVariables, subformulaAllVariables, subformulaAllVariables, argumentsBuffer));
        surelyBoundVariablesSoFar.intersectWith(childIterators.back()->getSurelyBoundArguments());
    }
    return newUnionIterator(m_tupleIteratorMonitor, getCardinalityType(), m_dataStore, argumentsBuffer, possiblyInitiallyBoundVariables, surelyInitiallyBoundVariables, answerVariables, surelyBoundVariablesSoFar, freeVariables, std::move(childIterators));
}

std::unique_ptr<TupleIterator> QueryCompiler::processMinus(ResourceValueCache& resourceValueCache, const Minus& minus, const TermArray& termArray, const ArgumentIndexSet& possiblyInitiallyBoundVariables, const ArgumentIndexSet& surelyInitiallyBoundVariables, const ArgumentIndexSet& answerVariables, const ArgumentIndexSet& freeVariables, std::vector<ResourceID>& argumentsBuffer) const {
    // Invariant: surelyInitiallyBoundVariables \subseteq possiblyInitiallyBoundVariables \subseteq answerVariables \subseteq freeVariables
    // Invariant of MINUS: freeVariables == minus.getMain().getFreeVariables()
    unique_ptr_vector<TupleIterator> childIterators;
    childIterators.push_back(processFormula(resourceValueCache, minus->getMain(), termArray, possiblyInitiallyBoundVariables, surelyInitiallyBoundVariables, answerVariables, freeVariables, argumentsBuffer));
    const ArgumentIndexSet& mainAllVariables = childIterators.back()->getAllArguments();
    const ArgumentIndexSet& mainSurelyBoundVariables = childIterators.back()->getSurelyBoundArguments();
    ArgumentIndexSet subformulaAllVariables;
    ArgumentIndexSet subformulaAllInputVariables;
    ArgumentIndexSet subformulaSurelyBoundInputVariables;
    for (size_t index = 0; index < minus->getNumberOfSubtrahends(); ++index) {
        getFreeVariables(minus->getSubtrahend(index), termArray, subformulaAllVariables);
        subformulaAllInputVariables.setToIntersection(mainAllVariables, subformulaAllVariables);
        subformulaSurelyBoundInputVariables.setToIntersection(mainSurelyBoundVariables, subformulaAllVariables);
        childIterators.push_back(processFormula(resourceValueCache, minus->getSubtrahend(index), termArray, subformulaAllInputVariables, subformulaSurelyBoundInputVariables, subformulaAllInputVariables, subformulaAllVariables, argumentsBuffer));
    }
    return newDifferenceIterator(m_tupleIteratorMonitor, std::move(childIterators));
}

std::unique_ptr<TupleIterator> QueryCompiler::processValues(ResourceValueCache& resourceValueCache, const Values& values, const TermArray& termArray, const ArgumentIndexSet& possiblyInitiallyBoundVariables, const ArgumentIndexSet& surelyInitiallyBoundVariables, const ArgumentIndexSet& answerVariables, const ArgumentIndexSet& freeVariables, std::vector<ResourceID>& argumentsBuffer) const {
    // Invariant: surelyInitiallyBoundVariables \subseteq possiblyInitiallyBoundVariables \subseteq answerVariables \subseteq freeVariables
    const std::vector<Variable>& variables = values->getVariables();
    const std::vector<std::vector<GroundTerm> >& data = values->getData();
    std::vector<ArgumentIndex> iteratorData;
    std::vector<ArgumentIndex> argumentIndexes;
    std::vector<size_t> rowIndexes;
    for (ArgumentIndexSet::const_iterator iterator = answerVariables.begin(); iterator != answerVariables.end(); ++iterator) {
        const ArgumentIndex argumentIndex = *iterator;
        argumentIndexes.push_back(argumentIndex);
        Variable variable = static_pointer_cast<Variable>(termArray.getTerm(argumentIndex));
        rowIndexes.push_back(std::find(variables.begin(), variables.end(), variable) - variables.begin());
    }
    for (std::vector<std::vector<GroundTerm> >::const_iterator rowIterator = data.begin(); rowIterator != data.end(); ++rowIterator) {
        const std::vector<GroundTerm> row = *rowIterator;
        for (std::vector<size_t>::iterator columnIterator = rowIndexes.begin(); columnIterator != rowIndexes.end(); ++columnIterator) {
            const GroundTerm& dataItem = row[*columnIterator];
            iteratorData.push_back(termArray.getPosition(dataItem));
        }
    }
    return newValuesIterator(m_tupleIteratorMonitor, argumentsBuffer, possiblyInitiallyBoundVariables, surelyInitiallyBoundVariables, argumentIndexes, data.size(), iteratorData);
}

std::unique_ptr<TupleIterator> QueryCompiler::processQuery(ResourceValueCache& resourceValueCache, const Query& query, const TermArray& termArray, const ArgumentIndexSet& possiblyInitiallyBoundVariables, const ArgumentIndexSet& surelyInitiallyBoundVariables, const ArgumentIndexSet& answerVariables, const ArgumentIndexSet& freeVariables, std::vector<ResourceID>& argumentsBuffer) const {
    // Invariant: surelyInitiallyBoundVariables \subseteq possiblyInitiallyBoundVariables \subseteq answerVariables \subseteq freeVariables
    // In this case, freeVariables are the answer variables of the query (not the answer terms: freeVariables excludes answer constants).
    // We next check whether freeVariables is a subset of the free variables of the internal formula.
    ArgumentIndexSet queryFormulaFreeVariables;
    getFreeVariables(query->getQueryFormula(), termArray, queryFormulaFreeVariables);
    if (!queryFormulaFreeVariables.contains(freeVariables)) {
        std::ostringstream message;
        message << "The query formula does not instantiate the following answer variables: ";
        bool first = true;
        for (ArgumentIndexSet::const_iterator iterator = freeVariables.begin(); iterator != freeVariables.end(); ++iterator) {
            if (!queryFormulaFreeVariables.contains(*iterator)) {
                if (first)
                    first = false;
                else
                    message << ", ";
                message << termArray.getTerm(*iterator)->toString(Prefixes::s_defaultPrefixes);
            }
        }
        throw RDF_STORE_EXCEPTION(message.str());
    }
    // New invariant: surelyInitiallyBoundVariables \subseteq possiblyInitiallyBoundVariables \subseteq answerVariables \subseteq freeVariables \subseteq queryFormulaFreeVariables
    // Compile the query formula
    std::unique_ptr<TupleIterator> queryFormulaIterator = processFormula(resourceValueCache, query->getQueryFormula(), termArray, possiblyInitiallyBoundVariables, surelyInitiallyBoundVariables, answerVariables, queryFormulaFreeVariables, argumentsBuffer);
    // Add distinct if necessary
    if (query->isDistinct() && ::strcmp(queryFormulaIterator->getName(), "DistinctIterator") != 0)
        queryFormulaIterator = newDistinctIterator(m_tupleIteratorMonitor, m_dataStore.getMemoryManager(), std::move(queryFormulaIterator));
    return queryFormulaIterator;
}

std::unique_ptr<TupleIterator> QueryCompiler::processFormula(ResourceValueCache& resourceValueCache, const Formula& formula, const TermArray& termArray, const ArgumentIndexSet& possiblyInitiallyBoundVariables, const ArgumentIndexSet& surelyInitiallyBoundVariables, const ArgumentIndexSet& answerVariables, const ArgumentIndexSet& freeVariables, std::vector<ResourceID>& argumentsBuffer) const {
    // Check invariant: surelyInitiallyBoundVariables \subseteq possiblyInitiallyBoundVariables \subseteq answerVariables \subseteq freeVariables
    assert(possiblyInitiallyBoundVariables.contains(surelyInitiallyBoundVariables));
    assert(answerVariables.contains(possiblyInitiallyBoundVariables));
    assert(freeVariables.contains(answerVariables));
    // Compile formula depending on its type
    switch (formula->getType()) {
    case OPTIONAL_FORMULA:
        return processOptional(resourceValueCache, static_pointer_cast<Optional>(formula), termArray, possiblyInitiallyBoundVariables, surelyInitiallyBoundVariables, answerVariables, freeVariables, argumentsBuffer);
    case CONJUNCTION_FORMULA:
    case ATOM_FORMULA:
    case BIND_FORMULA:
    case FILTER_FORMULA:
        return processConjunctiveFormula(resourceValueCache, formula, termArray, possiblyInitiallyBoundVariables, surelyInitiallyBoundVariables, answerVariables, freeVariables, argumentsBuffer);
    case DISJUNCTION_FORMULA:
        return processDisjunction(resourceValueCache, static_pointer_cast<Disjunction>(formula), termArray, possiblyInitiallyBoundVariables, surelyInitiallyBoundVariables, answerVariables, freeVariables, argumentsBuffer);
    case MINUS_FORMULA:
        return processMinus(resourceValueCache, static_pointer_cast<Minus>(formula), termArray, possiblyInitiallyBoundVariables, surelyInitiallyBoundVariables, answerVariables, freeVariables, argumentsBuffer);
    case VALUES_FORMULA:
        return processValues(resourceValueCache, static_pointer_cast<Values>(formula), termArray, possiblyInitiallyBoundVariables, surelyInitiallyBoundVariables, answerVariables, freeVariables, argumentsBuffer);
    case QUERY_FORMULA:
        return processQuery(resourceValueCache, static_pointer_cast<Query>(formula), termArray, possiblyInitiallyBoundVariables, surelyInitiallyBoundVariables, answerVariables, freeVariables, argumentsBuffer);
    default:
        UNREACHABLE;
    }
}

always_inline static QueryDomain getQueryDomain(const char* const queryDomain) {
    if (queryDomain == nullptr || ::strcmp("IDB", queryDomain) == 0)
        return QUERY_DOMAIN_IDB;
    else if (::strcmp("EDB", queryDomain) == 0)
        return QUERY_DOMAIN_EDB;
    else if (::strcmp("IDBrep", queryDomain) == 0)
        return QUERY_DOMAIN_IDBrep;
    else if (::strcmp("IDBrepNoEDB", queryDomain) == 0)
        return QUERY_DOMAIN_IDBrepNoEDB;
    else {
        std::ostringstream message;
        message << "Invalid query domain '" << queryDomain << "'.";
        throw RDF_STORE_EXCEPTION(message.str());
    }
}

QueryCompiler::QueryCompiler(const DataStore& dataStore, const Parameters& parameters, TupleIteratorMonitor* const tupleIteratorMonitor) :
    m_dataStore(dataStore),
    m_queryDomain(getQueryDomain(parameters.getString("domain", nullptr))),
    m_reorderConjunctions(parameters.getBoolean("reorder-conjunctions", true)),
    m_useBushyJoins(parameters.getBoolean("bushy", false)),
    m_chooseRootWithAllAnswerVariables(parameters.getBoolean("root-has-answers", true)),
    m_distinctThroughout(parameters.getBoolean("distinct", false)),
    m_exactCardinality(parameters.getBoolean("cardinality", true)),
    m_normalizeConstants(parameters.getBoolean("normalizeConstants", true)),
    m_normalizeConstantsInQueryHead(parameters.getBoolean("normalizeConstantsInQueryHead", false)),
    m_tupleIteratorMonitor(tupleIteratorMonitor)
{
}

void QueryCompiler::getFreeVariables(const Formula& formula, const TermArray& termArray, ArgumentIndexSet& freeVariables) const {
    freeVariables.clear();
    std::unordered_set<Variable> freeVariablesSet = formula->getFreeVariables();
    for (std::unordered_set<Variable>::iterator iterator = freeVariablesSet.begin(); iterator != freeVariablesSet.end(); ++iterator)
        freeVariables.add(termArray.getPosition(*iterator));
}

std::unique_ptr<QueryIterator> QueryCompiler::compileQuery(ResourceValueCache& resourceValueCache, std::unique_ptr<ResourceValueCache> resourceValueCacheOwner, const Query& query, const TermArray& termArray, const std::vector<Variable>& boundVariables, std::vector<ResourceID>& argumentsBuffer, std::unique_ptr<std::vector<ResourceID> > argumentsBufferOwner) const {
    // WARNING: This method expects all subqueries to have their variables renamed. There is no check (that would be quite complicated to implement),
    // so use this method at your own peril!

    // Compute the set of variables passed from the outside
    ArgumentIndexSet variablesBoundFromOutside;
    for (std::vector<Variable>::const_iterator iterator = boundVariables.begin(); iterator != boundVariables.end(); ++iterator)
        variablesBoundFromOutside.add(termArray.getPosition(*iterator));
    // When users are asking queries, they do not expect constants in the head of the query to be subject
    // to equality handling, so in that case we extend the buffer with a fresh copy of these constants.
    // In contrast, when queries are used to evaluate rules, we *do* want to normalize constants in query
    // head, so in that case we just point the index to the appropriate place in argumentsBuffer.
    // We also compute the set of answer variables
    ArgumentIndexSet answerVariables;
    std::vector<ArgumentIndex> argumentIndexes;
    const size_t numberOfConstantsToNormalize = (m_normalizeConstants ? argumentsBuffer.size() : 0);
    const std::vector<Term>& answerTerms = query->getAnswerTerms();
    for (std::vector<Term>::const_iterator iterator = answerTerms.begin(); iterator != answerTerms.end(); ++iterator) {
        const ArgumentIndex termIndex = termArray.getPosition(*iterator);
        if ((*iterator)->getType() == VARIABLE) {
            answerVariables.add(termIndex);
            argumentIndexes.push_back(termIndex);
        }
        else if (!m_normalizeConstants || m_normalizeConstantsInQueryHead)
            argumentIndexes.push_back(termIndex);
        else {
            argumentIndexes.push_back(static_cast<ArgumentIndex>(argumentsBuffer.size()));
            argumentsBuffer.push_back(argumentsBuffer[termIndex]);
        }
    }
    // Compile the query
    std::unique_ptr<TupleIterator> queryFormulaIterator = processQuery(resourceValueCache, query, termArray, variablesBoundFromOutside, variablesBoundFromOutside, answerVariables, answerVariables, argumentsBuffer);
    // Expand equality if necessary
    if (m_queryDomain == QUERY_DOMAIN_IDB && m_dataStore.getEqualityAxiomatizationType() != EQUALITY_AXIOMATIZATION_OFF)
        queryFormulaIterator = newEqualityExpansionIterator(m_tupleIteratorMonitor, m_dataStore, std::move(queryFormulaIterator));
    // Create the final iterator
    return newQueryIterator(m_tupleIteratorMonitor, m_dataStore, resourceValueCache, std::move(resourceValueCacheOwner), argumentsBuffer, std::move(argumentsBufferOwner), numberOfConstantsToNormalize, argumentIndexes, std::move(queryFormulaIterator));
}

std::unique_ptr<QueryIterator> QueryCompiler::compileQuery(const Query& query, const TermArray& termArray, const std::vector<Variable>& boundVariables) const {
    // WARNING: This method expects all subqueries to have their variables renamed. There is no check (that would be quite complicated to implement),
    // so use this method at your own peril!

    std::unique_ptr<ResourceValueCache> resourceValueCacheOwner(new ResourceValueCache(m_dataStore.getDictionary(), m_dataStore.getMemoryManager()));
    // Populate argumentsBuffer with all terms in the query
    std::unique_ptr<std::vector<ResourceID> > argumentsBufferOwner(new std::vector<ResourceID>(termArray.getNumberOfTerms(), INVALID_RESOURCE_ID));
    std::vector<ResourceID>& argumentsBuffer = *argumentsBufferOwner;
    ThreadContext& threadContext = ThreadContext::getCurrentThreadContext();
    for (ArgumentIndex argumentIndex = 0; argumentIndex < termArray.getNumberOfTerms(); ++argumentIndex) {
        const Term& term = termArray.getTerm(argumentIndex);
        switch (term->getType()) {
        case RESOURCE_BY_ID:
            argumentsBuffer[argumentIndex] = to_reference_cast<ResourceByID>(term).getResourceID();
            break;
        case RESOURCE_BY_NAME:
            {
                ResourceValue resourceValue;
                Dictionary::parseResourceValue(resourceValue, to_reference_cast<ResourceByName>(term).getResourceText());
                argumentsBuffer[argumentIndex] = resourceValueCacheOwner->resolveResource(threadContext, resourceValue);
            }
            break;
        case VARIABLE:
        default:
            break;
        }
    }
    // Now compile the query. Exlicitly storing the cache is important because the order of evaluation of aguments
    // varies from platform to platform.
    ResourceValueCache& resourceValueCache = *resourceValueCacheOwner;
    return compileQuery(resourceValueCache, std::move(resourceValueCacheOwner), query, termArray, boundVariables, argumentsBuffer, std::move(argumentsBufferOwner));
}

std::unique_ptr<QueryIterator> QueryCompiler::compileQuery(const Query& query, TermArray& termArray) const {
    // If the query contains a subquery or a MINUS clause, ensure that the variables in the relevant
    // query subparts are renamed apart, so that we can use just one argumentsBuffer.
    Query renamedQuery;
    NeedsRenamingDetector needsRenamingDetector;
    query->accept(needsRenamingDetector);
    if (needsRenamingDetector.getNeedsRenaming()) {
        size_t formulaWithImplicitExistentialVariablesCounter = 0;
        Formula renamedQueryFormula = query->getQueryFormula()->applyEx(Substitution(), true, formulaWithImplicitExistentialVariablesCounter);
        renamedQuery = query->getFactory()->getQuery(query->isDistinct(), query->getAnswerTerms(), renamedQueryFormula);
    }
    else
        renamedQuery = query;
    // Now compile the query
    termArray.visitFormula(renamedQuery);
    return compileQuery(renamedQuery, termArray, std::vector<Variable>());
}

std::unique_ptr<QueryIterator> QueryCompiler::compileQuery(const Query& query) const {
    TermArray termArray;
    return compileQuery(query, termArray);
}
