// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef QUERYCOMPILER_H_
#define QUERYCOMPILER_H_

#include "../logic/Logic.h"
#include "../storage/TupleTable.h"
#include "../storage/TupleIterator.h"
#include "QueryIterator.h"

class DataStore;
class ResourceValueCache;
class QueryDecompositionFormula;
class QueryDecompositionNode;
class QueryDecomposition;
class TermArray;
class Parameters;

enum QueryDomain {
	QUERY_DOMAIN_EDB,
	QUERY_DOMAIN_IDB,
	QUERY_DOMAIN_IDBrep,
	QUERY_DOMAIN_IDBrepNoEDB
};

class QueryCompiler : private Unmovable {

protected:

    const DataStore& m_dataStore;
    const QueryDomain m_queryDomain;
    const bool m_reorderConjunctions;
    const bool m_useBushyJoins;
    const bool m_chooseRootWithAllAnswerVariables;
    const bool m_distinctThroughout;
    const bool m_exactCardinality;
    const bool m_normalizeConstants;
    const bool m_normalizeConstantsInQueryHead;
    TupleIteratorMonitor* const m_tupleIteratorMonitor;

    CardinalityType getCardinalityType() const;

    void getTupleStatusFlags(TupleStatus& tupleStatusMask, TupleStatus& tupleStatusExpectedValue) const;

    template<bool chooseRootWithAllAnswerVariables>
    const QueryDecompositionNode& chooseRootNode(const QueryDecomposition& queryDecomposition, const ArgumentIndexSet& possiblyBoundVariables, const ArgumentIndexSet& surelyBoundVariables, std::vector<ResourceID>& argumentsBuffer) const;

    void evaluateNode(const QueryDecompositionNode& node, const ArgumentIndexSet& possiblyBoundVariablesSoFar, const ArgumentIndexSet& surelyBoundVariablesSoFar, std::vector<ResourceID>& argumentsBuffer, bool& isCrossProduct, size_t& countEstimate) const;

    void chooseNextConjunct(const QueryDecompositionNode& node, const QueryDecompositionNode* const parentNode, const ArgumentIndexSet& possiblyBoundVariablesSoFar, const ArgumentIndexSet& surelyBoundVariablesSoFar, std::vector<ResourceID>& argumentsBuffer, std::unordered_set<Formula>& processedFormulas, std::unordered_set<const QueryDecompositionNode*>& processedChildren, const QueryDecompositionFormula*& nextFormula, const QueryDecompositionNode*& nextChildNode, bool& allProcessed) const;

    std::unique_ptr<TupleIterator> processNode(ResourceValueCache& resourceValueCache, const QueryDecompositionNode& node, const QueryDecompositionNode* const parentNode, const ArgumentIndexSet& possiblyInitiallyBoundVariables, const ArgumentIndexSet& surelyInitiallyBoundVariables, const ArgumentIndexSet& answerVariables, std::vector<ResourceID>& argumentsBuffer, std::unordered_set<Formula>& processedFormulas) const;

    std::unique_ptr<TupleIterator> processConjunctiveFormula(ResourceValueCache& resourceValueCache, const Formula& formula, const TermArray& termArray, const ArgumentIndexSet& possiblyInitiallyBoundVariables, const ArgumentIndexSet& surelyInitiallyBoundVariables, const ArgumentIndexSet& answerVariables, const ArgumentIndexSet& freeVariables, std::vector<ResourceID>& argumentsBuffer) const;

    std::unique_ptr<TupleIterator> processOptional(ResourceValueCache& resourceValueCache, const Optional& optional, const TermArray& termArray, const ArgumentIndexSet& possiblyInitiallyBoundVariables, const ArgumentIndexSet& surelyInitiallyBoundVariables, const ArgumentIndexSet& answerVariables, const ArgumentIndexSet& freeVariables, std::vector<ResourceID>& argumentsBuffer) const;

    std::unique_ptr<TupleIterator> processDisjunction(ResourceValueCache& resourceValueCache, const Disjunction& disjunction, const TermArray& termArray, const ArgumentIndexSet& possiblyInitiallyBoundVariables, const ArgumentIndexSet& surelyInitiallyBoundVariables, const ArgumentIndexSet& answerVariables, const ArgumentIndexSet& freeVariables, std::vector<ResourceID>& argumentsBuffer) const;

    std::unique_ptr<TupleIterator> processMinus(ResourceValueCache& resourceValueCache, const Minus& minus, const TermArray& termArray, const ArgumentIndexSet& possiblyInitiallyBoundVariables, const ArgumentIndexSet& surelyInitiallyBoundVariables, const ArgumentIndexSet& answerVariables, const ArgumentIndexSet& freeVariables, std::vector<ResourceID>& argumentsBuffer) const;

    std::unique_ptr<TupleIterator> processValues(ResourceValueCache& resourceValueCache, const Values& values, const TermArray& termArray, const ArgumentIndexSet& possiblyInitiallyBoundVariables, const ArgumentIndexSet& surelyInitiallyBoundVariables, const ArgumentIndexSet& answerVariables, const ArgumentIndexSet& freeVariables, std::vector<ResourceID>& argumentsBuffer) const;

    std::unique_ptr<TupleIterator> processQuery(ResourceValueCache& resourceValueCache, const Query& query, const TermArray& termArray, const ArgumentIndexSet& possiblyInitiallyBoundVariables, const ArgumentIndexSet& surelyInitiallyBoundVariables, const ArgumentIndexSet& answerVariables, const ArgumentIndexSet& freeVariables, std::vector<ResourceID>& argumentsBuffer) const;

public:

    QueryCompiler(const DataStore& dataStore, const Parameters& parameters, TupleIteratorMonitor* const tupleIteratorMonitor);

    always_inline const DataStore& getDataStore() const {
        return m_dataStore;
    }

    always_inline TupleIteratorMonitor* getTupleIteratorMonitor() const {
        return m_tupleIteratorMonitor;
    }

    void getFreeVariables(const Formula& formula, const TermArray& termArray, ArgumentIndexSet& freeVariables) const;

    std::unique_ptr<TupleIterator> processFormula(ResourceValueCache& resourceValueCache, const Formula& formula, const TermArray& termArray, const ArgumentIndexSet& possiblyInitiallyBoundVariables, const ArgumentIndexSet& surelyInitiallyBoundVariables, const ArgumentIndexSet& answerVariables, const ArgumentIndexSet& freeVariables, std::vector<ResourceID>& argumentsBuffer) const;

    std::unique_ptr<QueryIterator> compileQuery(ResourceValueCache& resourceValueCache, std::unique_ptr<ResourceValueCache> resourceValueCacheOwner, const Query& query, const TermArray& termArray, const std::vector<Variable>& boundVariables, std::vector<ResourceID>& argumentsBuffer, std::unique_ptr<std::vector<ResourceID> > argumentsBufferOwner) const;

    std::unique_ptr<QueryIterator> compileQuery(const Query& query, const TermArray& termArray, const std::vector<Variable>& boundVariables) const;

    std::unique_ptr<QueryIterator> compileQuery(const Query& query, TermArray& termArray) const;

    std::unique_ptr<QueryIterator> compileQuery(const Query& query) const;

};

#endif /* QUERYCOMPILER_H_ */
