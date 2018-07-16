// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../Common.h"

#if !defined(WIN32) && !defined(__APPLE__)
#pragma GCC diagnostic ignored "-Wunused-local-typedefs"
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#pragma GCC diagnostic ignored "-Wmisleading-indentation"
#endif

#include <dlib/graph/graph_kernel_1.h>
#include <dlib/graph_utils/graph_utils.h>
#include <dlib/set.h>

#include "../RDFStoreException.h"
#include "../builtins/BuiltinExpressionEvaluator.h"
#include "../builtins/BindTupleIterator.h"
#include "../builtins/FilterTupleIterator.h"
#include "../querying/NegationIterator.h"
#include "../storage/DataStore.h"
#include "../storage/TupleTable.h"
#include "TermArray.h"
#include "QueryDecomposition.h"

// QueryDecompositionFormula

QueryDecompositionFormula::QueryDecompositionFormula(const QueryDecomposition& queryDecomposition, const Formula& formula) :
    m_queryDecomposition(queryDecomposition),
    m_formula(formula),
    m_variables(),
    m_constants()
{
}

QueryDecompositionFormula::~QueryDecompositionFormula() {
}

// QueryDecompositionNonLiteral

QueryDecompositionNonLiteral::QueryDecompositionNonLiteral(const QueryDecomposition& queryDecomposition, const Formula& formula) : QueryDecompositionFormula(queryDecomposition, formula) {
    const TermArray& termArray = m_queryDecomposition.getTermArray();
    std::unordered_set<Variable> freeVariables = m_formula->getFreeVariables();
    for (std::unordered_set<Variable>::iterator iterator = freeVariables.begin(); iterator != freeVariables.end(); ++iterator)
        const_cast<ArgumentIndexSet&>(static_cast<const ArgumentIndexSet&>(m_variables)).add(termArray.getPosition(*iterator));
}

size_t QueryDecompositionNonLiteral::getCountEstimate(const DataStore& dataStore, const ArgumentIndexSet& allInputArguments, const ArgumentIndexSet& surelyBoundInputArguments, const std::vector<ResourceID>& defaultArgumentsBuffer) const {
    return 0xffffffff;
}

// QueryDecompositionLiteral

QueryDecompositionLiteral::QueryDecompositionLiteral(const QueryDecomposition& queryDecomposition, const Literal& literal) : QueryDecompositionFormula(queryDecomposition, literal), m_atomArguments() {
    const TermArray& termArray = m_queryDecomposition.getTermArray();
    for (size_t index = 0; index < literal->getNumberOfArguments(); ++index) {
        const Term& argument = literal->getArgument(index);
        m_atomArguments.push_back(termArray.getPosition(argument));
        if (argument->getType() == VARIABLE)
            const_cast<ArgumentIndexSet&>(static_cast<const ArgumentIndexSet&>(m_variables)).add(m_atomArguments[index]);
        else
            const_cast<ArgumentIndexSet&>(static_cast<const ArgumentIndexSet&>(m_constants)).add(m_atomArguments[index]);
    }
}

// QueryDecompositionTupleTableAtom

QueryDecompositionTupleTableAtom::QueryDecompositionTupleTableAtom(const QueryDecomposition& queryDecomposition, const Atom& atom) : QueryDecompositionLiteral(queryDecomposition, atom) {
}

size_t QueryDecompositionTupleTableAtom::getCountEstimate(const DataStore& dataStore, const ArgumentIndexSet& allInputArguments, const ArgumentIndexSet& surelyBoundInputArguments, const std::vector<ResourceID>& defaultArgumentsBuffer) const {
    const TupleTable& tupleTable = dataStore.getTupleTable(to_pointer_cast<Atom>(m_formula)->getPredicate()->getName());
    // We cannot use allInputArguments here because argumentsBuffer does not contain the actual bindings.
    // Therefore, we use m_constants in the following call.
    return tupleTable.getCountEstimate(defaultArgumentsBuffer, m_atomArguments, m_constants);
}

std::unique_ptr<TupleIterator> QueryDecompositionTupleTableAtom::createTupleIterator(const DataStore& dataStore, ResourceValueCache& resourceValueCache, std::vector<ResourceID>& argumentsBuffer, const ArgumentIndexSet& allInputArguments, const ArgumentIndexSet& surelyBoundInputArguments, const TupleStatus tupleStatusMask, const TupleStatus tupleStatusExpectedValue, TupleIteratorMonitor* const tupleIteratorMonitor) const {
    const TupleTable& tupleTable = dataStore.getTupleTable(to_pointer_cast<Atom>(m_formula)->getPredicate()->getName());
    return tupleTable.createTupleIterator(argumentsBuffer, m_atomArguments, allInputArguments, surelyBoundInputArguments, tupleStatusMask, tupleStatusExpectedValue, tupleIteratorMonitor);
}

// QueryDecompositionBindAtom

QueryDecompositionBindAtom::QueryDecompositionBindAtom(const QueryDecomposition& queryDecomposition, const Bind& bind) : QueryDecompositionLiteral(queryDecomposition, bind) {
}

size_t QueryDecompositionBindAtom::getCountEstimate(const DataStore& dataStore, const ArgumentIndexSet& allInputArguments, const ArgumentIndexSet& surelyBoundInputArguments, const std::vector<ResourceID>& defaultArgumentsBuffer) const {
    for (std::vector<ArgumentIndex>::const_iterator iterator = m_atomArguments.begin() + 1; iterator != m_atomArguments.end(); ++iterator)
        if (!allInputArguments.contains(*iterator))
            return static_cast<size_t>(-1);
    return 1;
}

std::unique_ptr<TupleIterator> QueryDecompositionBindAtom::createTupleIterator(const DataStore& dataStore, ResourceValueCache& resourceValueCache, std::vector<ResourceID>& argumentsBuffer, const ArgumentIndexSet& allInputArguments, const ArgumentIndexSet& surelyBoundInputArguments, const TupleStatus tupleStatusMask, const TupleStatus tupleStatusExpectedValue, TupleIteratorMonitor* const tupleIteratorMonitor) const {
    std::unique_ptr<BuiltinExpressionEvaluator> builtinExpressionEvaluator(BuiltinExpressionEvaluator::compile(tupleIteratorMonitor, dataStore, resourceValueCache, m_queryDecomposition.getTermArray(), allInputArguments, surelyBoundInputArguments, argumentsBuffer, to_pointer_cast<Bind>(m_formula)->getBuiltinExpression()));
    return ::newBindTupleIterator<ResourceValueCache>(resourceValueCache, std::move(builtinExpressionEvaluator), tupleIteratorMonitor, argumentsBuffer, m_atomArguments, allInputArguments, surelyBoundInputArguments);
}

// QueryDecompositionFilterAtom

QueryDecompositionFilterAtom::QueryDecompositionFilterAtom(const QueryDecomposition& queryDecomposition, const Filter& filter) : QueryDecompositionLiteral(queryDecomposition, filter) {
}

size_t QueryDecompositionFilterAtom::getCountEstimate(const DataStore& dataStore, const ArgumentIndexSet& allInputArguments, const ArgumentIndexSet& surelyBoundInputArguments, const std::vector<ResourceID>& defaultArgumentsBuffer) const {
    for (std::vector<ArgumentIndex>::const_iterator iterator = m_atomArguments.begin(); iterator != m_atomArguments.end(); ++iterator)
        if (!allInputArguments.contains(*iterator))
            return static_cast<size_t>(-1);
    return 1;
}

std::unique_ptr<TupleIterator> QueryDecompositionFilterAtom::createTupleIterator(const DataStore& dataStore, ResourceValueCache& resourceValueCache, std::vector<ResourceID>& argumentsBuffer, const ArgumentIndexSet& allInputArguments, const ArgumentIndexSet& surelyBoundInputArguments, const TupleStatus tupleStatusMask, const TupleStatus tupleStatusExpectedValue, TupleIteratorMonitor* const tupleIteratorMonitor) const {
    std::unique_ptr<BuiltinExpressionEvaluator> builtinExpressionEvaluator(BuiltinExpressionEvaluator::compile(tupleIteratorMonitor, dataStore, resourceValueCache, m_queryDecomposition.getTermArray(), allInputArguments, surelyBoundInputArguments, argumentsBuffer, to_pointer_cast<Filter>(m_formula)->getBuiltinExpression()));
    return ::newFilterTupleIterator(std::move(builtinExpressionEvaluator), tupleIteratorMonitor, argumentsBuffer, m_atomArguments, allInputArguments, surelyBoundInputArguments);
}

// QueryDecompositionNegation

QueryDecompositionNegation::QueryDecompositionNegation(const QueryDecomposition& queryDecomposition, const Negation& negation) : QueryDecompositionLiteral(queryDecomposition, negation) {
}

size_t QueryDecompositionNegation::getCountEstimate(const DataStore& dataStore, const ArgumentIndexSet& allInputArguments, const ArgumentIndexSet& surelyBoundInputArguments, const std::vector<ResourceID>& defaultArgumentsBuffer) const {
    for (std::vector<ArgumentIndex>::const_iterator iterator = m_atomArguments.begin(); iterator != m_atomArguments.end(); ++iterator)
        if (!allInputArguments.contains(*iterator))
            return static_cast<size_t>(-1);
    return 1;
}

std::unique_ptr<TupleIterator> QueryDecompositionNegation::createTupleIterator(const DataStore& dataStore, ResourceValueCache& resourceValueCache, std::vector<ResourceID>& argumentsBuffer, const ArgumentIndexSet& allInputArguments, const ArgumentIndexSet& surelyBoundInputArguments, const TupleStatus tupleStatusMask, const TupleStatus tupleStatusExpectedValue, TupleIteratorMonitor* const tupleIteratorMonitor) const {
    Atom atom = static_pointer_cast<Atom>(to_pointer_cast<Negation>(m_formula)->getAtomicFormula(0));
    std::vector<ArgumentIndex> atomArgumentIndexes;
    ArgumentIndexSet atomAllInputArguments(allInputArguments);
    ArgumentIndexSet atomSurelyBoundInputArguments(surelyBoundInputArguments);
    const std::vector<Term>& arguments = atom->getArguments();
    for (auto iterator = arguments.begin(); iterator != arguments.end(); ++iterator) {
        const ArgumentIndex argumentIndex = m_queryDecomposition.getTermArray().getPosition(*iterator);
        atomArgumentIndexes.push_back(argumentIndex);
        if ((*iterator)->getType() != VARIABLE) {
            atomAllInputArguments.add(argumentIndex);
            atomSurelyBoundInputArguments.add(argumentIndex);
        }
    }
    std::unique_ptr<TupleIterator> atomTupleIterator = dataStore.getTupleTable(atom->getPredicate()->getName()).createTupleIterator(argumentsBuffer, atomArgumentIndexes, atomAllInputArguments, atomSurelyBoundInputArguments, tupleStatusMask, tupleStatusExpectedValue, tupleIteratorMonitor);
    return ::newNegationIterator(tupleIteratorMonitor, std::move(atomTupleIterator));
}

// QueryDecompositionNode

QueryDecompositionNode::QueryDecompositionNode(const QueryDecomposition& queryDecomposition, size_t nodeIndex) :
    m_queryDecomposition(queryDecomposition),
    m_nodeIndex(nodeIndex),
    m_nodeVariables(),
    m_formulas(),
    m_adjacentNodes()
{
}

// QueryDecomposition

class FormulaLoader : public LogicObjectVisitor {

protected:

    const QueryDecomposition& m_queryDecomposition;
    unique_ptr_vector<QueryDecompositionFormula>& m_formulas;

public:

    FormulaLoader(const QueryDecomposition& queryDecomposition, unique_ptr_vector<QueryDecompositionFormula>& formulas) : m_queryDecomposition(queryDecomposition), m_formulas(formulas) {
    }

    virtual void visit(const Predicate& object) {
    }

    virtual void visit(const BuiltinFunctionCall& object) {
    }

    virtual void visit(const ExistenceExpression& object) {
    }

    virtual void visit(const Variable& object) {
    }

    virtual void visit(const ResourceByID& object) {
    }

    virtual void visit(const ResourceByName& object) {
    }

    virtual void visit(const Atom& object) {
        m_formulas.push_back(std::unique_ptr<QueryDecompositionFormula>(new QueryDecompositionTupleTableAtom(m_queryDecomposition, object)));
    }

    virtual void visit(const Negation& object) {
        if (object->getNumberOfExistentialVariables() != 0 || object->getNumberOfAtomicFormulas() != 1 || object->getAtomicFormula(0)->getType() != ATOM_FORMULA)
            throw RDF_STORE_EXCEPTION("Only negations with no existential variables and just one atom are currently supported in queries.");
        m_formulas.push_back(std::unique_ptr<QueryDecompositionFormula>(new QueryDecompositionNegation(m_queryDecomposition, object)));
    }

    virtual void visit(const Bind& object) {
        m_formulas.push_back(std::unique_ptr<QueryDecompositionFormula>(new QueryDecompositionBindAtom(m_queryDecomposition, object)));
    }

    virtual void visit(const Filter& object) {
        m_formulas.push_back(std::unique_ptr<QueryDecompositionFormula>(new QueryDecompositionFilterAtom(m_queryDecomposition, object)));
    }

    virtual void visit(const AggregateBind& object) {
    }

    virtual void visit(const Aggregate& object) {
        throw RDF_STORE_EXCEPTION("Aggregate atoms are not supported yet.");
    }

    virtual void visit(const Conjunction& object) {
        const size_t numberOfConjuncts = object->getNumberOfConjuncts();
        for (size_t index = 0; index < numberOfConjuncts; ++index)
            object->getConjunct(index)->accept(*this);
    }

    virtual void visit(const Disjunction& object) {
        m_formulas.push_back(std::unique_ptr<QueryDecompositionFormula>(new QueryDecompositionNonLiteral(m_queryDecomposition, object)));
    }

    virtual void visit(const Optional& object) {
        m_formulas.push_back(std::unique_ptr<QueryDecompositionFormula>(new QueryDecompositionNonLiteral(m_queryDecomposition, object)));
    }

    virtual void visit(const Minus& object) {
        m_formulas.push_back(std::unique_ptr<QueryDecompositionFormula>(new QueryDecompositionNonLiteral(m_queryDecomposition, object)));
    }

    virtual void visit(const Values& object) {
        m_formulas.push_back(std::unique_ptr<QueryDecompositionFormula>(new QueryDecompositionNonLiteral(m_queryDecomposition, object)));
    }

    virtual void visit(const Query& object) {
        m_formulas.push_back(std::unique_ptr<QueryDecompositionFormula>(new QueryDecompositionNonLiteral(m_queryDecomposition, object)));
    }

    virtual void visit(const Rule& object) {
    }

};

QueryDecomposition::QueryDecomposition(const TermArray& termArray, const ArgumentIndexSet& answerVariables) :
    m_termArray(termArray), m_answerVariables(answerVariables), m_formulas(), m_nodes()
{
}

QueryDecomposition::QueryDecomposition(const TermArray& termArray, const ArgumentIndexSet& answerVariables, const Formula& formula) : QueryDecomposition(termArray, answerVariables) {
    addFormula(formula);
}

void QueryDecomposition::addFormula(const Formula& formula) {
    FormulaLoader formulaLoader(*this, m_formulas);
    formula->accept(formulaLoader);
}

QueryDecompositionNode& QueryDecomposition::addNewNode() {
    m_nodes.push_back(std::unique_ptr<QueryDecompositionNode>(new QueryDecompositionNode(*this, m_nodes.size())));
    return *m_nodes.back();
}

// Initialization functions

typedef dlib::graph_kernel_1<ArgumentIndex> PrimalGraph;
typedef dlib::graph_kernel_1<dlib::set<unsigned long>::compare_1a, dlib::set<unsigned long>::compare_1a> JoinTree;

void initializeUsingPrimalGraph(QueryDecomposition& decomposition, const bool addNodeWithAnswerVariables) {
    const TermArray& termArray = decomposition.getTermArray();
    PrimalGraph primalGraph;
    std::vector<unsigned long> graphNodeIndexes;
    for (ArgumentIndex index = 0; index < termArray.getNumberOfTerms(); ++index) {
        const Term& term = termArray.getTerm(index);
        unsigned long nodeIndex;
        if (term->getType() == VARIABLE) {
            nodeIndex = primalGraph.add_node();
            primalGraph.node(nodeIndex).data = index;
        }
        else
            nodeIndex = static_cast<unsigned long>(-1);
        graphNodeIndexes.push_back(nodeIndex);
    }
    size_t numberOfDecompositionFormulas = decomposition.getNumberOfFormulas();
    for (size_t index = 0; index < numberOfDecompositionFormulas; ++index) {
        const ArgumentIndexSet& variables = decomposition.getFormula(index).getVariables();
        for (ArgumentIndexSet::const_iterator iterator1 = variables.begin(); iterator1 != variables.end(); ++iterator1) {
            if (variables.contains(*iterator1)) {
                unsigned long node1Index = graphNodeIndexes[*iterator1];
                ArgumentIndexSet::const_iterator iterator2 = iterator1;
                ++iterator2;
                for (; iterator2 != variables.end(); ++iterator2) {
                    if (variables.contains(*iterator2)) {
                        unsigned long node2Index = graphNodeIndexes[*iterator2];
                        if (!primalGraph.has_edge(node1Index, node2Index))
                            primalGraph.add_edge(node1Index, node2Index);
                    }
                }
            }
        }
    }
    if (addNodeWithAnswerVariables) {
        const ArgumentIndexSet& answerVariables = decomposition.getAnswerVariables();
        for (ArgumentIndexSet::const_iterator iterator1 = answerVariables.begin(); iterator1 != answerVariables.end(); ++iterator1) {
            if (answerVariables.contains(*iterator1)) {
                unsigned long node1Index = graphNodeIndexes[*iterator1];
                ArgumentIndexSet::const_iterator iterator2 = iterator1;
                ++iterator2;
                for (; iterator2 != answerVariables.end(); ++iterator2) {
                    if (answerVariables.contains(*iterator2)) {
                        unsigned long node2Index = graphNodeIndexes[*iterator2];
                        if (!primalGraph.has_edge(node1Index, node2Index))
                            primalGraph.add_edge(node1Index, node2Index);
                    }
                }
            }
        }
    }
    JoinTree joinTree;
    dlib::create_join_tree(primalGraph, joinTree);
    unsigned long numberOfJoinTreeNodes = joinTree.number_of_nodes();
    for (unsigned long nodeIndex = 0; nodeIndex < numberOfJoinTreeNodes; nodeIndex++) {
        QueryDecompositionNode& decompositionNode = decomposition.addNewNode();
        ArgumentIndexSet& nodeVariables = decompositionNode.getNodeVariables();
        JoinTree::type& nodeVariableSet = joinTree.node(nodeIndex).data;
        nodeVariableSet.reset();
        while (nodeVariableSet.move_next()) {
            ArgumentIndex variableIndex = primalGraph.node(nodeVariableSet.element()).data;
            nodeVariables.add(variableIndex);
        }
        for (size_t index = 0; index < numberOfDecompositionFormulas; ++index) {
            QueryDecompositionFormula& formula = decomposition.getFormula(index);
            if (nodeVariables.contains(formula.getVariables()))
                decompositionNode.addFormula(formula);
        }
    }
    for (unsigned long nodeIndex = 0; nodeIndex < numberOfJoinTreeNodes; nodeIndex++) {
        const JoinTree::node_type& node = joinTree.node(nodeIndex);
        QueryDecompositionNode& decompositionNode = decomposition.getNode(nodeIndex);
        unsigned long numberOfAdjacentNodes = node.number_of_neighbors();
        for (unsigned long edgeIndex = 0; edgeIndex < numberOfAdjacentNodes; ++edgeIndex) {
            const JoinTree::node_type& adjacentNode = node.neighbor(edgeIndex);
            QueryDecompositionNode& adjacentDecompositionNode = decomposition.getNode(adjacentNode.index());
            decompositionNode.addAdjacentNode(adjacentDecompositionNode);
        }
    }
}

void initializeWithoutDecomposition(QueryDecomposition& decomposition) {
    QueryDecompositionNode& decompositionNode = decomposition.addNewNode();
    ArgumentIndexSet& nodeVariables = decompositionNode.getNodeVariables();
    size_t numberOfDecompositionFormulas = decomposition.getNumberOfFormulas();
    for (size_t index = 0; index < numberOfDecompositionFormulas; ++index) {
        QueryDecompositionFormula& formula = decomposition.getFormula(index);
        nodeVariables.unionWith(formula.getVariables());
        decompositionNode.addFormula(formula);
    }
}
