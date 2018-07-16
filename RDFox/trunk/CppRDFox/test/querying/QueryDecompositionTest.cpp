// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifdef WITH_TEST

#define AUTO_TEST    QueryDecompositionTest

#include <CppTest/AutoTest.h>

#include "../../src/logic/Logic.h"
#include "../../src/formats/turtle/SPARQLParser.h"
#include "../../src/querying/QueryDecomposition.h"

class QueryDecompositionTest {

protected:

    LogicFactory m_factory;

public:

    QueryDecompositionTest() : m_factory(::newLogicFactory()) {
    }

    Query getQuery(const char* const queryText) {
        Prefixes prefixes;
        SPARQLParser parser(prefixes);
        return parser.parse(m_factory, queryText, ::strlen(queryText));
    }

    void assertFormulas(const QueryDecomposition& queryDecomposition, const std::vector<Formula>& formulas) {
        const TermArray& termArray = queryDecomposition.getTermArray();
        ASSERT_EQUAL(formulas.size(), queryDecomposition.getNumberOfFormulas());
        for (ArgumentIndex formulaIndex = 0; formulaIndex < formulas.size(); ++formulaIndex) {
            const QueryDecompositionFormula& queryDecompositionFormula = queryDecomposition.getFormula(formulaIndex);
            const Formula& formula = queryDecompositionFormula.getFormula();
            ASSERT_EQUAL(formulas[formulaIndex], formula);
            const ArgumentIndexSet& variables = queryDecompositionFormula.getVariables();
            std::unordered_set<Variable> freeVariables = formula->getFreeVariables();
            for (std::unordered_set<Variable>::iterator iterator = freeVariables.begin(); iterator != freeVariables.end(); ++iterator) {
                ArgumentIndex termIndex = termArray.getPosition(*iterator);
                ASSERT_TRUE(variables.contains(termIndex));
            }
            ASSERT_EQUAL(freeVariables.size(), variables.size());
        }
    }

    std::vector<Formula> extractAtomsFrom(const Query& query) {
        return ::static_pointer_cast<Conjunction>(query->getQueryFormula())->getConjuncts();
    }

    void extractAnswerVariablesFrom(const Query& query, ArgumentIndexSet& answerVariables, TermArray& termArray) {
        for (size_t index = 0; index < query->getNumberOfAnswerTerms(); ++index) {
            const Term& answerTerm = query->getAnswerTerm(index);
            if (answerTerm->getType() == VARIABLE)
                answerVariables.add(termArray.getPosition(answerTerm));
        }
    }

    bool isCoveredBy(const Formula& formula, const std::unordered_set<std::string>& variableNames) {
        std::unordered_set<Variable> freeVariables = formula->getFreeVariables();
        for (std::unordered_set<Variable>::iterator iterator = freeVariables.begin(); iterator != freeVariables.end(); ++iterator)
            if (variableNames.find((*iterator)->getName()) == variableNames.end())
                return false;
        return true;
    }

    void assertConainsFormula(const QueryDecompositionNode& node, const QueryDecompositionFormula& queryDecompositionFormula) {
        for (size_t index = 0; index < node.getNumberOfFormulas(); ++index)
            if (&node.getFormula(index) == &queryDecompositionFormula)
                return;
        std::ostringstream message;
        message << "Atom '" << queryDecompositionFormula.getFormula()->toString(Prefixes::s_defaultPrefixes) << "' is not contained in decomposition node with index '" << node.getNodeIndex() << "'.";
        FAIL2(message.str());
    }

    void assertNode(const QueryDecomposition& queryDecomposition, const size_t nodeIndex, const std::unordered_set<std::string>& expectedVariableNames, const std::unordered_set<size_t>& expectedAdjacentNodeIndexes) {
        const QueryDecompositionNode& node = queryDecomposition.getNode(nodeIndex);
        ASSERT_EQUAL(nodeIndex, node.getNodeIndex());
        std::unordered_set<Variable> actualNodeVariables;
        for (ArgumentIndex argumentIndex = 0; argumentIndex < queryDecomposition.getTermArray().getNumberOfTerms(); ++argumentIndex) {
            const Term& term = queryDecomposition.getTermArray().getTerm(argumentIndex);
            if (term->getType() == VARIABLE && node.getNodeVariables().contains(argumentIndex))
                actualNodeVariables.insert(::static_pointer_cast<Variable>(term));
        }
        CppTest::CollectionTester<std::unordered_set<Variable> > variableTester(actualNodeVariables);
        for (std::unordered_set<std::string>::const_iterator iterator = expectedVariableNames.begin(); iterator != expectedVariableNames.end(); ++iterator)
            variableTester.m_expected.insert(m_factory->getVariable(*iterator));
        variableTester.doAssert(__FILE__, __LINE__);
        size_t numberOfFormulas = 0;
        for (size_t formulaIndex = 0; formulaIndex < queryDecomposition.getNumberOfFormulas(); ++formulaIndex) {
            const QueryDecompositionFormula& queryDecompositionFormula = queryDecomposition.getFormula(formulaIndex);
            if (isCoveredBy(queryDecompositionFormula.getFormula(), expectedVariableNames)) {
                ++numberOfFormulas;
                assertConainsFormula(node, queryDecompositionFormula);
            }
        }
        ASSERT_EQUAL(numberOfFormulas, node.getNumberOfFormulas());
        std::unordered_set<size_t> actualAdjacentNodeIndexes;
        for (size_t nodeIndex = 0; nodeIndex < node.getNumberOfAdjacentNodes(); ++nodeIndex)
            actualAdjacentNodeIndexes.insert(node.getAdjacentNode(nodeIndex).getNodeIndex());
        CppTest::CollectionTester<std::unordered_set<size_t> > nodeIndexTester(actualAdjacentNodeIndexes);
        nodeIndexTester.m_expected.insert(expectedAdjacentNodeIndexes.begin(), expectedAdjacentNodeIndexes.end());
        nodeIndexTester.doAssert(__FILE__, __LINE__);
    }

};

always_inline std::unordered_set<std::string> V() {
    return std::unordered_set<std::string>();
}

always_inline const std::unordered_set<std::string>& operator<<(const std::unordered_set<std::string>& set, const std::string& string) {
    const_cast<std::unordered_set<std::string>&>(set).insert(string);
    return set;
}

always_inline std::unordered_set<size_t> N() {
    return std::unordered_set<size_t>();
}

always_inline const std::unordered_set<size_t>& operator<<(const std::unordered_set<size_t>& set, const size_t nodeIndex) {
    const_cast<std::unordered_set<size_t>&>(set).insert(nodeIndex);
    return set;
}

TEST(testQuery1) {
    Query query = getQuery("SELECT ?X ?Y ?Z WHERE { ?X a <cls1> . ?X <prop1> ?Y . ?Y a <cls2> . ?Y <prop2> ?Z . ?Z a <cls3> }");
    std::vector<Formula> formulas = extractAtomsFrom(query);
    TermArray termArray(query);
    ArgumentIndexSet answerVariables;
    extractAnswerVariablesFrom(query, answerVariables, termArray);
    QueryDecomposition queryDecomposition(termArray, answerVariables, query->getQueryFormula());
    assertFormulas(queryDecomposition, formulas);
    ASSERT_EQUAL(0, queryDecomposition.getNumberOfNodes());
    ::initializeUsingPrimalGraph(queryDecomposition, false);
    ASSERT_EQUAL(2, queryDecomposition.getNumberOfNodes());
    assertNode(queryDecomposition, 0,
        V() << "X" << "Y",
        N() << 1
    );
    assertNode(queryDecomposition, 1,
        V() << "Y" << "Z",
        N() << 0
    );
}

TEST(testQuery2) {
    Query query = getQuery("SELECT ?X ?Y ?Z WHERE { ?X a <cls1> . ?X <prop1> ?Y . ?Y a <cls2> . ?Y <prop2> ?Z . ?Z a <cls3> . ?Z <prop3> ?X }");
    std::vector<Formula> formulas = extractAtomsFrom(query);
    TermArray termArray(query);
    ArgumentIndexSet answerVariables;
    extractAnswerVariablesFrom(query, answerVariables, termArray);
    QueryDecomposition queryDecomposition(termArray, answerVariables, query->getQueryFormula());
    assertFormulas(queryDecomposition, formulas);
    ASSERT_EQUAL(0, queryDecomposition.getNumberOfNodes());
    ::initializeUsingPrimalGraph(queryDecomposition, false);
    ASSERT_EQUAL(1, queryDecomposition.getNumberOfNodes());
    assertNode(queryDecomposition, 0,
        V() << "X" << "Y" << "Z",
        N()
    );
}

TEST(testQuery3) {
    Query query = getQuery("SELECT ?X0 WHERE { ?X0 a <cls1> . ?X0 <prop> ?X1 . ?X1 <prop> ?X2 . ?X2 <prop> ?X3 . ?X3 <prop> ?X4 . ?X4 <prop> ?X5 . ?X5 <prop> ?X0 }");
    std::vector<Formula> formulas = extractAtomsFrom(query);
    TermArray termArray(query);
    ArgumentIndexSet answerVariables;
    extractAnswerVariablesFrom(query, answerVariables, termArray);
    QueryDecomposition queryDecomposition(termArray, answerVariables, query->getQueryFormula());
    assertFormulas(queryDecomposition, formulas);
    ASSERT_EQUAL(0, queryDecomposition.getNumberOfNodes());
    ::initializeUsingPrimalGraph(queryDecomposition, false);
    ASSERT_EQUAL(4, queryDecomposition.getNumberOfNodes());
    assertNode(queryDecomposition, 0,
        V() << "X0" << "X1" << "X2",
        N() << 1
    );
    assertNode(queryDecomposition, 1,
        V() << "X0" << "X2" << "X3",
        N() << 0 << 2
    );
    assertNode(queryDecomposition, 2,
        V() << "X0" << "X3" << "X4",
        N() << 1 << 3
    );
    assertNode(queryDecomposition, 3,
        V() << "X0" << "X4" << "X5",
        N() << 2
    );
}

TEST(testAnswerVariables) {
    Query query = getQuery("SELECT ?X0 <ind> ?X3 WHERE { ?X0 a <cls1> . ?X0 <prop> ?X1 . ?X1 <prop> ?X2 . ?X2 <prop> ?X3 }");
    std::vector<Formula> formulas = extractAtomsFrom(query);
    TermArray termArray(query);
    ArgumentIndexSet answerVariables;
    extractAnswerVariablesFrom(query, answerVariables, termArray);
    QueryDecomposition queryDecomposition(termArray, answerVariables, query->getQueryFormula());
    assertFormulas(queryDecomposition, formulas);
    ASSERT_EQUAL(0, queryDecomposition.getNumberOfNodes());
    ::initializeUsingPrimalGraph(queryDecomposition, true);
    ASSERT_EQUAL(2, queryDecomposition.getNumberOfNodes());
    assertNode(queryDecomposition, 0,
        V() << "X0" << "X2" << "X3",
        N() << 1
    );
    assertNode(queryDecomposition, 1,
        V() << "X0" << "X1" << "X2",
        N() << 0
    );
}

#endif
