// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef QUERYDECOMPOSITION_H_
#define QUERYDECOMPOSITION_H_

#include "../Common.h"
#include "../logic/Logic.h"
#include "../storage/ArgumentIndexSet.h"
#include "../storage/TupleIterator.h"
#include "../storage/TupleTable.h"
#include "TermArray.h"

class DataStore;
class ResourceValueCache;
class QueryDecomposition;
class TupleTable;

// QueryDecompositionFormula

class QueryDecompositionFormula : private Unmovable {

protected:

    const QueryDecomposition& m_queryDecomposition;
    const Formula m_formula;
    const ArgumentIndexSet m_variables;
    const ArgumentIndexSet m_constants;

public:

    QueryDecompositionFormula(const QueryDecomposition& queryDecomposition, const Formula& formula);

    virtual ~QueryDecompositionFormula();

    always_inline const QueryDecomposition& getQueryDecomposition() const {
        return m_queryDecomposition;
    }

    always_inline const Formula& getFormula() const {
        return m_formula;
    }

    always_inline const ArgumentIndexSet& getVariables() const {
        return m_variables;
    }

    always_inline const ArgumentIndexSet& getConstants() const {
        return m_constants;
    }

    virtual size_t getCountEstimate(const DataStore& dataStore, const ArgumentIndexSet& allInputArguments, const ArgumentIndexSet& surelyBoundInputArguments, const std::vector<ResourceID>& defaultArgumentsBuffer) const = 0;

};

// QueryDecompositionNonLiteral

class QueryDecompositionNonLiteral : public QueryDecompositionFormula {

public:

    QueryDecompositionNonLiteral(const QueryDecomposition& queryDecomposition, const Formula& formula);

    virtual size_t getCountEstimate(const DataStore& dataStore, const ArgumentIndexSet& allInputArguments, const ArgumentIndexSet& surelyBoundInputArguments, const std::vector<ResourceID>& defaultArgumentsBuffer) const;

};

// QueryDecompositionLiteral

class QueryDecompositionLiteral : public QueryDecompositionFormula {

protected:

    std::vector<ArgumentIndex> m_atomArguments;

public:

    QueryDecompositionLiteral(const QueryDecomposition& queryDecomposition, const Literal& literal);

    virtual std::unique_ptr<TupleIterator> createTupleIterator(const DataStore& dataStore, ResourceValueCache& resourceValueCache, std::vector<ResourceID>& argumentsBuffer, const ArgumentIndexSet& allInputArguments, const ArgumentIndexSet& surelyBoundInputArguments, const TupleStatus tupleStatusMask, const TupleStatus tupleStatusExpectedValue, TupleIteratorMonitor* const tupleIteratorMonitor) const = 0;

};

// QueryDecompositionTupleTableAtom

class QueryDecompositionTupleTableAtom : public QueryDecompositionLiteral {

public:

    QueryDecompositionTupleTableAtom(const QueryDecomposition& queryDecomposition, const Atom& atom);

    virtual size_t getCountEstimate(const DataStore& dataStore, const ArgumentIndexSet& allInputArguments, const ArgumentIndexSet& surelyBoundInputArguments, const std::vector<ResourceID>& defaultArgumentsBuffer) const;

    virtual std::unique_ptr<TupleIterator> createTupleIterator(const DataStore& dataStore, ResourceValueCache& resourceValueCache, std::vector<ResourceID>& argumentsBuffer, const ArgumentIndexSet& allInputArguments, const ArgumentIndexSet& surelyBoundInputArguments, const TupleStatus tupleStatusMask, const TupleStatus tupleStatusExpectedValue, TupleIteratorMonitor* const tupleIteratorMonitor) const;

};

// QueryDecompositionBindAtom

class QueryDecompositionBindAtom : public QueryDecompositionLiteral {

public:

    QueryDecompositionBindAtom(const QueryDecomposition& queryDecomposition, const Bind& bind);

    virtual size_t getCountEstimate(const DataStore& dataStore, const ArgumentIndexSet& allInputArguments, const ArgumentIndexSet& surelyBoundInputArguments, const std::vector<ResourceID>& defaultArgumentsBuffer) const;

    virtual std::unique_ptr<TupleIterator> createTupleIterator(const DataStore& dataStore, ResourceValueCache& resourceValueCache, std::vector<ResourceID>& argumentsBuffer, const ArgumentIndexSet& allInputArguments, const ArgumentIndexSet& surelyBoundInputArguments, const TupleStatus tupleStatusMask, const TupleStatus tupleStatusExpectedValue, TupleIteratorMonitor* const tupleIteratorMonitor) const;

};

// QueryDecompositionFilterAtom

class QueryDecompositionFilterAtom : public QueryDecompositionLiteral {

public:

    QueryDecompositionFilterAtom(const QueryDecomposition& queryDecomposition, const Filter& filter);

    virtual size_t getCountEstimate(const DataStore& dataStore, const ArgumentIndexSet& allInputArguments, const ArgumentIndexSet& surelyBoundInputArguments, const std::vector<ResourceID>& defaultArgumentsBuffer) const;

    virtual std::unique_ptr<TupleIterator> createTupleIterator(const DataStore& dataStore, ResourceValueCache& resourceValueCache, std::vector<ResourceID>& argumentsBuffer, const ArgumentIndexSet& allInputArguments, const ArgumentIndexSet& surelyBoundInputArguments, const TupleStatus tupleStatusMask, const TupleStatus tupleStatusExpectedValue, TupleIteratorMonitor* const tupleIteratorMonitor) const;

};

// QueryDecompositionNegation

class QueryDecompositionNegation : public QueryDecompositionLiteral {

public:

    QueryDecompositionNegation(const QueryDecomposition& queryDecomposition, const Negation& negation);

    virtual size_t getCountEstimate(const DataStore& dataStore, const ArgumentIndexSet& allInputArguments, const ArgumentIndexSet& surelyBoundInputArguments, const std::vector<ResourceID>& defaultArgumentsBuffer) const;

    virtual std::unique_ptr<TupleIterator> createTupleIterator(const DataStore& dataStore, ResourceValueCache& resourceValueCache, std::vector<ResourceID>& argumentsBuffer, const ArgumentIndexSet& allInputArguments, const ArgumentIndexSet& surelyBoundInputArguments, const TupleStatus tupleStatusMask, const TupleStatus tupleStatusExpectedValue, TupleIteratorMonitor* const tupleIteratorMonitor) const;
    
};

// QueryDecompositionNode

class QueryDecompositionNode : private Unmovable {

protected:

    const QueryDecomposition& m_queryDecomposition;
    const size_t m_nodeIndex;
    ArgumentIndexSet m_nodeVariables;
    std::vector<QueryDecompositionFormula*> m_formulas;
    std::vector<QueryDecompositionNode*> m_adjacentNodes;

public:

    QueryDecompositionNode(const QueryDecomposition& queryDecomposition, const size_t nodeIndex);

    always_inline const QueryDecomposition& getQueryDecomposition() const {
        return m_queryDecomposition;
    }

    always_inline size_t getNodeIndex() const {
        return m_nodeIndex;
    }

    always_inline ArgumentIndexSet& getNodeVariables() {
        return m_nodeVariables;
    }

    always_inline const ArgumentIndexSet& getNodeVariables() const {
        return m_nodeVariables;
    }

    always_inline size_t getNumberOfFormulas() const {
        return m_formulas.size();
    }

    always_inline QueryDecompositionFormula& getFormula(const size_t formulaIndex) {
        return *m_formulas[formulaIndex];
    }

    always_inline const QueryDecompositionFormula& getFormula(const size_t formulaIndex) const {
        return *m_formulas[formulaIndex];
    }

    always_inline void addFormula(QueryDecompositionFormula& formula) {
        m_formulas.push_back(&formula);
    }

    always_inline size_t getNumberOfAdjacentNodes() const {
        return m_adjacentNodes.size();
    }

    always_inline QueryDecompositionNode& getAdjacentNode(const size_t nodeIndex) {
        return *m_adjacentNodes[nodeIndex];
    }

    always_inline const QueryDecompositionNode& getAdjacentNode(const size_t nodeIndex) const {
        return *m_adjacentNodes[nodeIndex];
    }

    always_inline void addAdjacentNode(QueryDecompositionNode& adjacentNode) {
        m_adjacentNodes.push_back(&adjacentNode);
    }

};

// QueryDecomposition

class QueryDecomposition : private Unmovable {

protected:

    const TermArray& m_termArray;
    const ArgumentIndexSet m_answerVariables;
    unique_ptr_vector<QueryDecompositionFormula> m_formulas;
    unique_ptr_vector<QueryDecompositionNode> m_nodes;

public:

    QueryDecomposition(const TermArray& termArray, const ArgumentIndexSet& answerVariables);

    QueryDecomposition(const TermArray& termArray, const ArgumentIndexSet& answerVariables, const Formula& formula);

    always_inline const TermArray& getTermArray() const {
        return m_termArray;
    }

    always_inline const ArgumentIndexSet& getAnswerVariables() const {
        return m_answerVariables;
    }

    void addFormula(const Formula& formula);

    always_inline size_t getNumberOfFormulas() const {
        return m_formulas.size();
    }

    always_inline QueryDecompositionFormula& getFormula(const size_t formulaIndex) {
        return *m_formulas[formulaIndex];
    }

    always_inline const QueryDecompositionFormula& getFormula(const size_t formulaIndex) const {
        return *m_formulas[formulaIndex];
    }

    always_inline size_t getNumberOfNodes() const {
        return m_nodes.size();
    }

    always_inline QueryDecompositionNode& getNode(const size_t nodeIndex) {
        return *m_nodes[nodeIndex];
    }

    always_inline const QueryDecompositionNode& getNode(const size_t nodeIndex) const {
        return *m_nodes[nodeIndex];
    }

    QueryDecompositionNode& addNewNode();

};

extern void initializeUsingPrimalGraph(QueryDecomposition& decomposition, const bool addNodeWithAnswerVariables);

extern void initializeWithoutDecomposition(QueryDecomposition& decomposition);

#endif /* QUERYDECOMPOSITION_H_ */
