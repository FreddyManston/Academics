// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifdef WITH_TEST

#define AUTO_TEST ExplanationTest

#include <CppTest/AutoTest.h>

#include "../../src/formats/datalog/DatalogParser.h"
#include "../../src/formats/sources/MemorySource.h"
#include "../../src/storage/DataStore.h"
#include "../../src/storage/Explanation.h"
#include "../querying/DataStoreTest.h"

class ExplanationTest : public DataStoreTest {

protected:

    std::unique_ptr<ExplanationProvider> m_explanationProvider;

public:

    ExplanationTest() : DataStoreTest("par-complex-ww", EQUALITY_AXIOMATIZATION_NO_UNA), m_explanationProvider(m_dataStore->createExplanationProvider()) {
    }

    Atom A(const char* const text) {
        MemorySource memorySource(text, ::strlen(text));
        DatalogParser datalogParser(m_prefixes);
        datalogParser.bind(memorySource);
        Atom atom = datalogParser.parseAtom(m_factory);
        datalogParser.unbind();
        return atom;
    }

    void assertExplanation(const char* const fileName, const long lineNumber, const char* const atomText, const ExplanationAtomNode::AtomNodeType expectedType, const char* const expectedRuleInstancesText) {
        Atom atom = A(atomText);
        const ExplanationAtomNode& atomNode = m_explanationProvider->getNode(atom);
        CppTest::assertTrue(&atomNode == &m_explanationProvider->getNode(atom), fileName, lineNumber, "Uniquness of atom nodes violated.");
        CppTest::assertEqual(expectedType, atomNode.getType(), fileName, lineNumber);
        const std::vector<std::unique_ptr<ExplanationRuleInstanceNode> >& atomNodeChildren = atomNode.getChildren();
        std::unordered_set<Rule> actualRuleInstances;
        for (auto iterator = atomNodeChildren.begin(); iterator != atomNodeChildren.end(); ++iterator) {
            const ExplanationRuleInstanceNode& ruleInstanceNode = **iterator;
            Rule ruleInstance = ruleInstanceNode.getRuleInstance()->clone(m_factory);
            actualRuleInstances.insert(ruleInstance);
            const std::vector<Literal>& ruleInstanceBody = ruleInstance->getBody();
            const std::vector<const ExplanationAtomNode*>& ruleInstanceNodeChildren = ruleInstanceNode.getChildren();
            auto childrenIterator = ruleInstanceNodeChildren.begin();
            for (auto bodyIterator = ruleInstanceBody.begin(); bodyIterator != ruleInstanceBody.end(); ++bodyIterator) {
                Literal bodyLiteral = *bodyIterator;
                if (bodyLiteral->getType() == ATOM_FORMULA) {
                    const ExplanationAtomNode& bodyNode = m_explanationProvider->getNode(static_pointer_cast<Atom>(bodyLiteral));
                    CppTest::assertTrue(&bodyNode == *childrenIterator, fileName, lineNumber, "Invalid child atom.");
                    ++childrenIterator;
                }
            }
        }
        CppTest::CollectionTester<std::unordered_set<Rule> > collectionTester(actualRuleInstances);
        MemorySource memorySource(expectedRuleInstancesText, ::strlen(expectedRuleInstancesText));
        DatalogParser datalogParser(m_prefixes);
        datalogParser.bind(memorySource);
        while (!datalogParser.isEOF()) {
            Rule rule = datalogParser.parseRule(m_factory);
            collectionTester.m_expected.insert(rule);
        }
        datalogParser.unbind();
        collectionTester.doAssert(fileName, lineNumber);
    }

    void assertShortestExplanation(const char* const fileName, const long lineNumber, const char* const atomText, const ExplanationAtomNode::AtomNodeType expectedType, const size_t expectedHeight, const char* const expectedShortestRuleInstanceText) {
        Atom atom = A(atomText);
        const ExplanationAtomNode& atomNode = m_explanationProvider->getNode(atom);
        CppTest::assertTrue(&atomNode == &m_explanationProvider->getNode(atom), fileName, lineNumber, "Uniquness of atom nodes violated.");
        CppTest::assertEqual(expectedType, atomNode.getType(), fileName, lineNumber);
        CppTest::assertEqual(expectedHeight, atomNode.getHeight(), fileName, lineNumber);
        const ExplanationRuleInstanceNode* const shortestChild = atomNode.getShortestChild();
        if (expectedShortestRuleInstanceText == nullptr)
            CppTest::assertTrue(shortestChild == nullptr, fileName, lineNumber, "No shortest proof was expected.");
        else {
            CppTest::assertTrue(shortestChild != nullptr, fileName, lineNumber, "A shortest proof was expected.");
            Rule actualShortestRuleInstance = shortestChild->getRuleInstance()->clone(m_factory);
            MemorySource memorySource(expectedShortestRuleInstanceText, ::strlen(expectedShortestRuleInstanceText));
            DatalogParser datalogParser(m_prefixes);
            datalogParser.bind(memorySource);
            Rule expectedShortestRuleInstance = datalogParser.parseRule(m_factory);
            datalogParser.unbind();
            CppTest::assertEqual(expectedShortestRuleInstance, actualShortestRuleInstance, fileName, lineNumber);
        }
    }

};

#define ASSERT_EXPLANATION(atom, type, expected) \
    assertExplanation(__FILE__, __LINE__, atom, type, expected)

#define ASSERT_SHORTEST_EXPLANATION(atom, type, height, expected) \
    assertShortestExplanation(__FILE__, __LINE__, atom, type, height, expected)

TEST(testBasic) {
    addTriples(
        ":a rdf:type :A . "
        ":a :R :b . "
        ":a :S :c . "
        ":a :S :d . "
    );
    addRules(
        ":B(?X) :- :A(?X) . "
        ":C(?X) :- :A(?X) . "
        ":D(?X) :- :B(?X) . "
        ":D(?X) :- :C(?X) . "
        ":A(?X) :- :R(?X,?Y) . "
        ":D(?X) :- :S(?X,?Y) . "
    );
    applyRules();

    ASSERT_EXPLANATION(":D(:a)", ExplanationAtomNode::IDB_ATOM,
        ":D(:a) :- :B(:a) . "
        ":D(:a) :- :C(:a) . "
        ":D(:a) :- :S(:a,:c) . "
        ":D(:a) :- :S(:a,:d) . "
    );
    ASSERT_EXPLANATION(":C(:a)", ExplanationAtomNode::IDB_ATOM,
        ":C(:a) :- :A(:a) . "
    );
    ASSERT_EXPLANATION(":B(:a)", ExplanationAtomNode::IDB_ATOM,
        ":B(:a) :- :A(:a) . "
    );
    ASSERT_EXPLANATION(":A(:a)", ExplanationAtomNode::EDB_ATOM,
        ""
    );
}

TEST(testEquality) {
    addTriples(
        ":a :R :b . "
        ":a :R :c . "
        ":b rdf:type :B . "
        ":c rdf:type :C . "
    );
    addRules(
        ":D(?X) :- :B(?X), :C(?X) . "
        "owl:sameAs(?Y1,?Y2) :- :R(?X,?Y1), :R(?X,?Y2) . "
    );
    applyRules();

    ASSERT_EXPLANATION(":D(:b)", ExplanationAtomNode::IDB_ATOM,
        ":D(:b) :- [:b, owl:sameAs, :b], :D(:b) . "
        ":D(:b) :- :B(:b), :C(:b) . "
    );
    ASSERT_EXPLANATION(":D(:c)", ExplanationAtomNode::IDB_ATOM,
        ":D(:b) :- [:b, owl:sameAs, :b], :D(:b) . "
        ":D(:b) :- :B(:b), :C(:b) . "
    );
    ASSERT_EXPLANATION(":C(:b)", ExplanationAtomNode::EQUAL_TO_EDB_ATOM,
        ""
    );
}

TEST(testShortestExplanation) {
    addTriples(
        ":i rdf:type :A . "
        ":i rdf:type :B . "
        ":i rdf:type :D . "
    );
    addRules(
        "[?X, rdf:type, :C] :- [?X, rdf:type, :B] . "
        "[?X, rdf:type, :G] :- [?X, rdf:type, :A], [?X, rdf:type, :C] . "
        "[?X, rdf:type, :E] :- [?X, rdf:type, :D] . "
        "[?X, rdf:type, :F] :- [?X, rdf:type, :E] . "
        "[?X, rdf:type, :G] :- [?X, rdf:type, :F], [?X, rdf:type, :A] . "
        "[?X, rdf:type, :H] :- [?X, rdf:type, :G] . "
        "[?X, rdf:type, :I] :- [?X, rdf:type, :H] . "
        "[?X, rdf:type, :G] :- [?X, rdf:type, :I] . "
    );
    applyRules();

    ASSERT_SHORTEST_EXPLANATION("[:i, rdf:type, :G]", ExplanationAtomNode::IDB_ATOM, 2,
        "[:i, rdf:type, :G] :- [:i, rdf:type, :A], [:i, rdf:type, :C] . "
    );
    ASSERT_SHORTEST_EXPLANATION("[:i, rdf:type, :A]", ExplanationAtomNode::EDB_ATOM, 0, nullptr);
}

TEST(testExplanationWithPivotlessRules) {
    addTriples(
        ":i1 :R :i2 . "
        ":i2 :R :i3 . "
        ":i3 :R :i4 . "
        ":i4 :R :i5 . "
    );
    addRules(
        "[:i5, rdf:type, :A] :- . "
        "[?X, rdf:type, :A] :- [?X, :R, ?Y], [?Y, rdf:type, :A] . "
    );
    applyRules();

    ASSERT_SHORTEST_EXPLANATION("[:i1, rdf:type, :A]", ExplanationAtomNode::IDB_ATOM, 5,
        "[:i1, rdf:type, :A] :- [:i1, :R, :i2], [:i2, rdf:type, :A] . "
    );
    ASSERT_SHORTEST_EXPLANATION("[:i2, rdf:type, :A]", ExplanationAtomNode::IDB_ATOM, 4,
        "[:i2, rdf:type, :A] :- [:i2, :R, :i3], [:i3, rdf:type, :A] . "
    );
    ASSERT_SHORTEST_EXPLANATION("[:i3, rdf:type, :A]", ExplanationAtomNode::IDB_ATOM, 3,
        "[:i3, rdf:type, :A] :- [:i3, :R, :i4], [:i4, rdf:type, :A] . "
    );
    ASSERT_SHORTEST_EXPLANATION("[:i4, rdf:type, :A]", ExplanationAtomNode::IDB_ATOM, 2,
        "[:i4, rdf:type, :A] :- [:i4, :R, :i5], [:i5, rdf:type, :A] . "
    );
    ASSERT_SHORTEST_EXPLANATION("[:i5, rdf:type, :A]", ExplanationAtomNode::IDB_ATOM, 1,
        "[:i5, rdf:type, :A] :- . "
    );
}

#endif
