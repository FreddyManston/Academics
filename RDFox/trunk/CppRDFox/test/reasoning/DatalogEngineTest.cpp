// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifdef WITH_TEST

#define AUTO_TEST DatalogEngineTest

#include <CppTest/TestCase.h>

#include "../../src/Common.h"
#include "../../src/dictionary/Dictionary.h"
#include "../../src/util/Vocabulary.h"
#include "../../src/tasks/Tasks.h"
#include "../querying/DataStoreTest.h"

// Define all other tests quites by copying DatalogEngineTest.byLevels as the template.

SUITE(DatalogEngineTest);

__REGISTER(DatalogEngineTest, DatalogEngineTest__byLevels);
__MAKE_SUITE(DatalogEngineTest__byLevels, "byLevels");

__REGISTER(DatalogEngineTest, DatalogEngineTest__noLevels);
__MAKE_ADAPTER(DatalogEngineTest__noLevels, "noLevels", DatalogEngineTest__byLevels) {
    testParameters.setBoolean("reason.by-Levels", false);
}

// Now define the DatalogEngineTest.byLevels template suite; to avoid confusion, use a neutral fixture name.

#define TEST(testName)                                                                                  \
    __REGISTER(DatalogEngineTest__byLevels, DatalogEngineTest__byLevels##testName);                     \
    __MAKE_TEST(DatalogEngineTest__byLevels##testName, #testName, : public DatalogEngineTestFixture)

class DatalogEngineTestFixture : public DataStoreTest {

protected:

    virtual void initializeQueryParameters() {
        m_queryParameters.setBoolean("bushy", false);
        m_queryParameters.setBoolean("distinct", false);
        m_queryParameters.setBoolean("cardinality", false);
    }

public:

    DatalogEngineTestFixture() : DataStoreTest("par-complex-ww", EQUALITY_AXIOMATIZATION_OFF) {
    }

    void initializeEx(const CppTest::TestParameters& testParameters) {
        m_processComponentsByLevels = testParameters.getBoolean("reason.by-Levels", true);
    }

    void initializeTableWithAChain(size_t length, std::vector<ResourceID>& elementIDs) {
        std::vector<ResourceID> argumentsBuffer;
        std::vector<ArgumentIndex> argumentIndices;
        std::stringstream ss;
        for (ArgumentIndex i = 0; i < 3; i++) {
            argumentsBuffer.push_back(INVALID_RESOURCE_ID);
            argumentIndices.push_back(i);
        }
        elementIDs.clear();
        argumentsBuffer[1] = resolve("R");
        for (size_t i = 0; i < length; i++) {
            ss.str("");
            ss << "a" << i;
            argumentsBuffer[0] = resolve(ss.str());
            ss.str("");
            ss << "a" << i + 1;
            argumentsBuffer[2] = resolve(ss.str());
            m_tripleTable.addTuple(argumentsBuffer, argumentIndices, 0, TUPLE_STATUS_EDB | TUPLE_STATUS_IDB);
            elementIDs.push_back(argumentsBuffer[0]);
            if (i == length - 1)
                elementIDs.push_back(argumentsBuffer[2]);
        }
        argumentsBuffer[0] = resolve("a0");
        argumentsBuffer[1] = resolve(RDF_TYPE);
        argumentsBuffer[2] = resolve("A");
        m_tripleTable.addTuple(argumentsBuffer, argumentIndices, 0, TUPLE_STATUS_EDB | TUPLE_STATUS_IDB);
    }

    void initializeTableWithATree(size_t depth, std::vector<ResourceID>& elementIDs) {
        std::vector<ResourceID> argumentsBuffer;
        std::vector<ArgumentIndex> argumentIndices;
        std::stringstream ss;
        for (ArgumentIndex i = 0; i < 3; i++) {
            argumentsBuffer.push_back(INVALID_RESOURCE_ID);
            argumentIndices.push_back(i);
        }
        elementIDs.clear();
        argumentsBuffer[1] = resolve("R");
        size_t next = 1;
        addTwoSuccessors(depth, static_cast<size_t>(0), next, argumentsBuffer, argumentIndices, ss, elementIDs);
        argumentsBuffer[0] = resolve("a0");
        argumentsBuffer[1] = resolve(RDF_TYPE);
        argumentsBuffer[2] = resolve("A");
        m_tripleTable.addTuple(argumentsBuffer, argumentIndices, 0, TUPLE_STATUS_EDB | TUPLE_STATUS_IDB);
    }

    void addTwoSuccessors(size_t depth, size_t current, size_t& nextAvailable, std::vector<ResourceID>& argumentsBuffer, std::vector<ArgumentIndex>& argumentIndices, std::stringstream& ss, std::vector<ResourceID>& elementIDs) {
        ss.str("");
        ss << "a" << current;
        ResourceID currentID = resolve(ss.str());
        elementIDs.push_back(currentID);
        if (depth > 0) {
            for (size_t i = 0; i < 2; i++) {
                argumentsBuffer[0] = currentID;
                ss.str("");
                ss << "a" << nextAvailable;
                argumentsBuffer[2] = resolve(ss.str());
                m_tripleTable.addTuple(argumentsBuffer, argumentIndices, 0, TUPLE_STATUS_EDB | TUPLE_STATUS_IDB);
                nextAvailable++;
                addTwoSuccessors(depth - 1, nextAvailable - 1, nextAvailable, argumentsBuffer, argumentIndices, ss, elementIDs);
            }
        }
    }

public:

    void testTransitiveClosure(size_t numberOfThreads) {
        size_t chainLength = 100;
        std::vector<ResourceID> elementIDs;
        initializeTableWithAChain(chainLength, elementIDs);

        addRules(
            "<R>(?X,?Z) :- <R>(?X,?Y), <R>(?Y,?Z) ."
        );
        applyRules(numberOfThreads);

        std::vector<ResourceID> argumentsBuffer;
        std::vector<ArgumentIndex> argumentIndices;
        for (ArgumentIndex i = 0; i < 3; i++) {
            argumentsBuffer.push_back(INVALID_RESOURCE_ID);
            argumentIndices.push_back(i);
        }
        argumentsBuffer[1] = resolve("R");
        for (size_t indexOne = 0; indexOne <= chainLength; indexOne++) {
            for (size_t indexTwo = 0; indexTwo <= chainLength; indexTwo++) {
                argumentsBuffer[0] = elementIDs[indexOne];
                argumentsBuffer[2] = elementIDs[indexTwo];
                ASSERT_EQUAL(indexOne < indexTwo, m_tripleTable.containsTuple(argumentsBuffer, argumentIndices, TUPLE_STATUS_IDB, TUPLE_STATUS_IDB));
            }
        }
    }

    void testConceptPropagation(size_t numberOfThreads, bool isTree, size_t depthOrLength) {
        std::vector<ResourceID> elementIDs;
        if (isTree)
            initializeTableWithATree(depthOrLength, elementIDs);
        else
            initializeTableWithAChain(depthOrLength, elementIDs);

        addRules(
            "<A>(?Y) :- <A>(?X), <R>(?X,?Y) ."
        );
        applyRules(numberOfThreads);

        std::vector<ResourceID> argumentsBuffer;
        std::vector<ArgumentIndex> argumentIndices;
        for (ArgumentIndex i = 0; i < 3; i++) {
            argumentsBuffer.push_back(INVALID_RESOURCE_ID);
            argumentIndices.push_back(i);
        }
        argumentsBuffer[1] = resolve(RDF_TYPE);
        argumentsBuffer[2] = resolve("A");
        for (size_t indexOne = 0; indexOne <= depthOrLength; indexOne++) {
            argumentsBuffer[0] = elementIDs[indexOne];
            ASSERT_TRUE(m_tripleTable.containsTuple(argumentsBuffer, argumentIndices, TUPLE_STATUS_IDB, TUPLE_STATUS_IDB));
        }
    }

    void testConceptsPropagation(size_t numberOfThreads, bool isTree, size_t depthOrLength) {
        std::vector<ResourceID> elementIDs;
        if (isTree)
            initializeTableWithATree(depthOrLength, elementIDs);
        else
            initializeTableWithAChain(depthOrLength, elementIDs);

        addRules(
            "<B>(?X) :- <A>(?X) ."
            "<B>(?Y) :- <B>(?X), <R>(?X,?Y) ."
            "<C>(?X) :- <A>(?X) ."
            "<C>(?Y) :- <C>(?X), <R>(?X,?Y) ."
        );
        applyRules(numberOfThreads);

        std::vector<ResourceID> argumentsBuffer;
        std::vector<ArgumentIndex> argumentIndices;
        for (ArgumentIndex i = 0; i < 3; i++) {
            argumentsBuffer.push_back(INVALID_RESOURCE_ID);
            argumentIndices.push_back(i);
        }
        argumentsBuffer[1] = resolve(RDF_TYPE);
        ResourceID bRID = resolve("B");
        ResourceID cRID = resolve("C");
        for (size_t indexOne = 0; indexOne <= depthOrLength; indexOne++) {
            argumentsBuffer[0] = elementIDs[indexOne];
            argumentsBuffer[2] = bRID;
            ASSERT_TRUE(m_tripleTable.containsTuple(argumentsBuffer, argumentIndices, TUPLE_STATUS_IDB, TUPLE_STATUS_IDB));
            argumentsBuffer[2] = cRID;
            ASSERT_TRUE(m_tripleTable.containsTuple(argumentsBuffer, argumentIndices, TUPLE_STATUS_IDB, TUPLE_STATUS_IDB));
        }
    }

};

TEST(testSmallStratum) {
    size_t chainLength = 10000;
    std::vector<ResourceID> elementIDs;
    initializeTableWithAChain(chainLength, elementIDs);

    addRules(
        "<A>(?Y) :- <A>(?X), <R>(?X,?Y) ."
        "<S>(?X,?Y) :- <R>(?X,?Y) ."
        "<T>(?Y,?X) :- <R>(?X,?Y) ."
        "<B>(?X) :- <S>(?X,?Y) ."
        "<C>(?X) :- <S>(?Y,?X) ."
        "<D>(?X) :- <B>(?X) ."
        "<D>(?X) :- <C>(?X) ."
        "<E>(?X) :- <T>(?Y,?X) ."
    );
    applyRules();

    ResourceID typeID = resolve(RDF_TYPE);
    ResourceID aID = resolve("A");
    ResourceID bID = resolve("B");
    ResourceID cID = resolve("C");
    ResourceID dID = resolve("D");
    ResourceID eID = resolve("E");
    ResourceID sID = resolve("S");
    ResourceID tID = resolve("T");

    std::vector<ResourceID> argumentsBuffer;
    std::vector<ArgumentIndex> argumentIndices;
    for (ArgumentIndex i = 0; i < 3; i++) {
        argumentsBuffer.push_back(INVALID_RESOURCE_ID);
        argumentIndices.push_back(i);
    }
    std::stringstream ss;

    for (size_t i = 0; i <= chainLength; i++) {
        argumentsBuffer[1] = typeID;
        argumentsBuffer[0] = elementIDs[i];
        argumentsBuffer[2] = dID;
        ASSERT_TRUE(m_tripleTable.containsTuple(argumentsBuffer, argumentIndices, TUPLE_STATUS_IDB, TUPLE_STATUS_IDB));
        if (i > 0) {
            argumentsBuffer[2] = cID;
            ASSERT_TRUE(m_tripleTable.containsTuple(argumentsBuffer, argumentIndices, TUPLE_STATUS_IDB, TUPLE_STATUS_IDB));
        }
        if (i < chainLength) {
            argumentsBuffer[2] = aID;
            ASSERT_TRUE(m_tripleTable.containsTuple(argumentsBuffer, argumentIndices, TUPLE_STATUS_IDB, TUPLE_STATUS_IDB));
            argumentsBuffer[2] = bID;
            ASSERT_TRUE(m_tripleTable.containsTuple(argumentsBuffer, argumentIndices, TUPLE_STATUS_IDB, TUPLE_STATUS_IDB));
            argumentsBuffer[2] = eID;
            ASSERT_TRUE(m_tripleTable.containsTuple(argumentsBuffer, argumentIndices, TUPLE_STATUS_IDB, TUPLE_STATUS_IDB));
            argumentsBuffer[1] = sID;
            argumentsBuffer[2] = elementIDs[i + 1];
            ASSERT_TRUE(m_tripleTable.containsTuple(argumentsBuffer, argumentIndices, TUPLE_STATUS_IDB, TUPLE_STATUS_IDB));
            argumentsBuffer[0] = elementIDs[i + 1];
            argumentsBuffer[1] = tID;
            argumentsBuffer[2] = elementIDs[i];
        }
    }
}

TEST(transitiveClosureTestThreads1) {
    testTransitiveClosure(1);
}

TEST(transitiveClosureTestThreads2) {
    testTransitiveClosure(2);
}

const size_t chainLength = 100000;

TEST(conceptPropagationChain1) {
    testConceptPropagation(1, false, chainLength);
}

TEST(conceptPropagationChain2) {
    testConceptPropagation(2, false, chainLength);
}

const size_t treeDepth = 16;

TEST(conceptPropagationTree1) {
    testConceptPropagation(1, true, treeDepth);
}

TEST(conceptPropagationTree2) {
    testConceptPropagation(2, true, treeDepth);
}

TEST(conceptPropagationTree3) {
    testConceptPropagation(3, true, treeDepth);
}

TEST(conceptPropagationTree4) {
    testConceptPropagation(4, true, treeDepth);
}

TEST(conceptPropagationTree5) {
    testConceptPropagation(5, true, treeDepth);
}

TEST(conceptPropagationTree6) {
    testConceptPropagation(6, true, treeDepth);
}

TEST(conceptPropagationTree7) {
    testConceptPropagation(7, true, treeDepth);
}

TEST(conceptPropagationTree8) {
    testConceptPropagation(8, true, treeDepth);
}

TEST(conceptsPropagationTree1) {
    testConceptsPropagation(1, true, treeDepth);
}

TEST(conceptsPropagationTree2) {
    testConceptsPropagation(2, true, treeDepth);
}

TEST(conceptsPropagationTree3) {
    testConceptsPropagation(3, true, treeDepth);
}

TEST(conceptsPropagationTree4) {
    testConceptsPropagation(4, true, treeDepth);
}

TEST(conceptsPropagationTree5) {
    testConceptsPropagation(5, true, treeDepth);
}

TEST(conceptsPropagationTree6) {
    testConceptsPropagation(6, true, treeDepth);
}

TEST(conceptsPropagationTree7) {
    testConceptsPropagation(7, true, treeDepth);
}

TEST(conceptsPropagationTree8) {
    testConceptsPropagation(8, true, treeDepth);
}

TEST(testBuiltinsInRules) {
    addTriples(
        ":i1 :R \"a\" ."
        ":i2 :R \"abcd\" ."
        ":i3 :R \"abcdefg\" ."

        ":i1 :S 1 ."
        ":i2 :S 6 ."
        ":i3 :S 10 ."
    );

    addRules(
        ":L(?X,?W) :- FILTER ?W != ?Z, BIND (STRLEN(?Y) AS ?W), :R(?X,?Y), :S(?X,?Z) ."
    );
    applyRules();

    ASSERT_QUERY("SELECT ?X ?Y WHERE { ?X :L ?Y }",
        ":i2 4 ."
        ":i3 7 ."
    );
}

TEST(testBasic1) {
    addTriples(
        ":i1 a :A ."
        ":i2 a :B ."
    );

    addRules(
        ":B(?X) :- :A(?X) ."
        ":C(?X) :- :A(?X) ."
    );
    applyRules();

    ASSERT_QUERY("SELECT ?X WHERE { ?X a :A }",
        ":i1 ."
    );

    ASSERT_QUERY("SELECT ?X WHERE { ?X a :B }",
        ":i1 ."
        ":i2 ."
    );

    ASSERT_QUERY("SELECT ?X WHERE { ?X a :C }",
        ":i1 ."
    );
}

TEST(testBasic2) {
    addTriples(
        ":i1 a :A ."
        ":i1 :R :i2 ."
        ":i2 :R :i3 ."
        ":i3 :R :i4 ."
        ":i4 :R :i5 ."
    );

    addRules(
        ":A(?Y) :- :A(?X), :R(?X,?Y) ."
    );
    applyRules();

    ASSERT_QUERY("SELECT ?X WHERE { ?X a :A }",
        ":i1 ."
        ":i2 ."
        ":i3 ."
        ":i4 ."
        ":i5 ."
    );
}

TEST(testRepeatedVarsInRule) {
    addTriples(
        ":R :R :R ."
        ":i1 :R :i2 ."
        ":i3 :R :i3 ."
    );

    addRules(
        ":A(?X) :- [?X,?X,?X] ."
        ":B(?X) :- :R(?X,?X) ."
    );
    applyRules();

    ASSERT_QUERY("SELECT ?X WHERE { ?X a :A }",
        ":R ."
    );

    ASSERT_QUERY("SELECT ?X WHERE { ?X a :B }",
        ":R ."
        ":i3 ."
    );
}

TEST(testAddRemoveRules) {
    // The second rule is deliberately repeated: we need to test that the set of rules in the engine indeed behaves like a set.
    addRules(
        ":B(?X) :- :A(?X) ."
        ":C(?X) :- :A(?X) ."
        ":C(?X) :- :A(?X) ."
    );

    addTriples(
        ":a1 rdf:type :A ."
        ":a2 rdf:type :A ."
        ":a3 rdf:type :A ."
        ":a4 rdf:type :A ."
    );
    ASSERT_QUERY("SELECT ?X WHERE { ?X a :A }",
        ":a1 ."
        ":a2 ."
        ":a3 ."
        ":a4 ."
    );
    ASSERT_QUERY("SELECT ?X WHERE { ?X a :B }",
        ""
    );
    ASSERT_QUERY("SELECT ?X WHERE { ?X a :C }",
        ""
    );

    applyRules();
    ASSERT_QUERY("SELECT ?X WHERE { ?X a :A }",
        ":a1 ."
        ":a2 ."
        ":a3 ."
        ":a4 ."
    );
    ASSERT_QUERY("SELECT ?X WHERE { ?X a :B }",
        ":a1 ."
        ":a2 ."
        ":a3 ."
        ":a4 ."
    );
    ASSERT_QUERY("SELECT ?X WHERE { ?X a :C }",
        ":a1 ."
        ":a2 ."
        ":a3 ."
        ":a4 ."
    );

    removeTriples(
        ":a1 rdf:type :B ."
        ":a2 rdf:type :B ."
        ":a3 rdf:type :B ."
        ":a4 rdf:type :B ."
        ":a1 rdf:type :C ."
        ":a2 rdf:type :C ."
        ":a3 rdf:type :C ."
        ":a4 rdf:type :C ."
    );
    ASSERT_QUERY("SELECT ?X WHERE { ?X a :A }",
        ":a1 ."
        ":a2 ."
        ":a3 ."
        ":a4 ."
    );
    ASSERT_QUERY("SELECT ?X WHERE { ?X a :B }",
        ""
    );
    ASSERT_QUERY("SELECT ?X WHERE { ?X a :C }",
        ""
    );

    removeRules(
        ":C(?X) :- :A(?X) ."
    );
    applyRules();
    ASSERT_QUERY("SELECT ?X WHERE { ?X a :A }",
        ":a1 ."
        ":a2 ."
        ":a3 ."
        ":a4 ."
    );
    ASSERT_QUERY("SELECT ?X WHERE { ?X a :B }",
        ":a1 ."
        ":a2 ."
        ":a3 ."
        ":a4 ."
    );
    ASSERT_QUERY("SELECT ?X WHERE { ?X a :C }",
        ""
    );

    addRules(
        ":D(?X) :- :A(?X) ."
    );
    applyRules(2);
    ASSERT_QUERY("SELECT ?X WHERE { ?X a :A }",
        ":a1 ."
        ":a2 ."
        ":a3 ."
        ":a4 ."
    );
    ASSERT_QUERY("SELECT ?X WHERE { ?X a :B }",
        ":a1 ."
        ":a2 ."
        ":a3 ."
        ":a4 ."
    );
    ASSERT_QUERY("SELECT ?X WHERE { ?X a :C }",
        ""
    );
    ASSERT_QUERY("SELECT ?X WHERE { ?X a :D }",
        ":a1 ."
        ":a2 ."
        ":a3 ."
        ":a4 ."
    );

    removeTriples(
        ":a1 rdf:type :D ."
        ":a2 rdf:type :D ."
        ":a3 rdf:type :D ."
        ":a4 rdf:type :D ."
    );
    applyRules();
    ASSERT_QUERY("SELECT ?X WHERE { ?X a :A }",
        ":a1 ."
        ":a2 ."
        ":a3 ."
        ":a4 ."
    );
    ASSERT_QUERY("SELECT ?X WHERE { ?X a :B }",
        ":a1 ."
        ":a2 ."
        ":a3 ."
        ":a4 ."
    );
    ASSERT_QUERY("SELECT ?X WHERE { ?X a :C }",
        ""
    );
    ASSERT_QUERY("SELECT ?X WHERE { ?X a :D }",
        ":a1 ."
        ":a2 ."
        ":a3 ."
        ":a4 ."
    );
}

TEST(testBindInRules) {
    addTriples(
        ":a :R 1 ."
        ":b :R 2 ."
    );
    addRules(
        ":S(?X,?Z) :- :R(?X,?Y), BIND(?Y + 1 AS ?Z) ."
        ":T(?X,2) :- :R(?X,?Y), BIND(?Y + 1 AS 2) ."
    );
    applyRules();

    ASSERT_QUERY("SELECT ?X ?Y WHERE { ?X :S ?Y }",
        ":a 2 ."
        ":b 3 ."
    );

    ASSERT_QUERY("SELECT ?X ?Y WHERE { ?X :T ?Y }",
        ":a 2 ."
    );
}

TEST(testPivotlessRules) {
    addTriples(
        ":i rdf:type :D . "
    );
    addRules(
        ":A(:i) :- ."
        ":B(:i) :- FILTER(1 <= 1) ."
        ":C(?X) :- :A(?X), :B(?X) ."
        ":E(?X) :- :C(?X), :D(?X) ."
    );
    applyRules();

    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":i rdf:type :A ."
        ":i rdf:type :B ."
        ":i rdf:type :C ."
        ":i rdf:type :D ."
        ":i rdf:type :E ."
    );
}

TEST(testNegation1) {
    addTriples(
        ":a rdf:type :O . "

        ":b rdf:type :O . "
        ":b rdf:type :A . "
    );
    addRules(
        ":B(?X) :- :O(?X), NOT :A(?X) ."
        ":C(?X) :- :O(?X), NOT :B(?X) ."
        ":D(?X) :- :O(?X), NOT :C(?X) ."
        ":E(?X) :- :O(?X), NOT :D(?X) ."
        ":F(?X) :- :O(?X), NOT :E(?X) ."
    );
    if (m_processComponentsByLevels) {
        applyRules();

        ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
            ":a rdf:type :O ."
            ":a rdf:type :B ."
            ":a rdf:type :D ."
            ":a rdf:type :F ."

            ":b rdf:type :O ."
            ":b rdf:type :A ."
            ":b rdf:type :C ."
            ":b rdf:type :E ."
        );
    }
    else {
        try {
            applyRules();
            FAIL2("It should not be possible to run rules with negation in a single stratum.");
        }
        catch (const RDFStoreException&) {
        }
    }
}

TEST(testNegation2) {
    addTriples(
        ":a :R :b . "
        ":a :R :c . "
        ":a :R :d . "
        ":b :R :d . "
        ":c :R :d . "
        ":d :R :e . "
        ":e :R :f . "
        ":b :R :f . "
        ":f :R :g . "
    );
    addRules(
        "[?X,:R,?Z] :- [?X,:R,?Y], [?Y,:R,?Z] ."
        "[?X,:S,?Y] :- [?X,:R,?Y], NOT EXISTS ?Z IN([?X,:R,?Z], [?Z,:R,?Y], FILTER(?X != ?Z && ?Y != ?Z)) ."
    );
    if (m_processComponentsByLevels) {
        applyRules();

        ASSERT_QUERY("SELECT ?X ?Y WHERE { ?X :S ?Y }",
            ":a :b . "
            ":b :d . "
            ":a :c . "
            ":c :d . "
            ":d :e . "
            ":e :f . "
            ":f :g . "
        );
    }
    else {
        try {
            applyRules();
            FAIL2("It should not be possible to run rules with negation in a single stratum.");
        }
        catch (const RDFStoreException&) {
        }
    }
}

TEST(testNegationFlagsBug) {
    addTriples(
        ":a rdf:type :A . "
    );
    // The following fact isn't added to the IDB, but it exists in the main table.
    // This test checks whether the evaluation of NAF consults the correct flags.
    addTriple(":a", "rdf:type", ":B", 0, 0);
    addRules(
        ":C(?X) :- :A(?X), not :B(?X) . "
    );
    if (m_processComponentsByLevels) {
        applyRules();

        ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
            ":a rdf:type :A ."
            ":a rdf:type :C ."
        );
    }
    else {
        try {
            applyRules();
            FAIL2("It should not be possible to run rules with negation in a single stratum.");
        }
        catch (const RDFStoreException&) {
        }
    }
}

TEST(testNonrecursiveMatches1) {
    addTriples(
        ":a rdf:type :A . "
        ":b rdf:type :B . "
        ":c rdf:type :C . "

        ":a rdf:type :O . "
        ":b rdf:type :O . "
        ":c rdf:type :O . "
    );
    addRules(
        ":B(?X) :- :A(?X), :O(?X) . "
        ":C(?X) :- :B(?X), :O(?X) . "
        ":D(?X) :- :C(?X), :O(?X) . "
    );
    applyRules();

    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":a rdf:type :A ."
        ":a rdf:type :B ."
        ":a rdf:type :C ."
        ":a rdf:type :D ."
        ":a rdf:type :O ."

        ":b rdf:type :B ."
        ":b rdf:type :C ."
        ":b rdf:type :D ."
        ":b rdf:type :O ."

        ":c rdf:type :C ."
        ":c rdf:type :D ."
        ":c rdf:type :O ."
    );
}

TEST(testNonrecursiveMatches2) {
    addTriples(
        ":a rdf:type :A . "

        ":a :R :b . "
        ":a :R :c . "
        ":b :R :d . "
    );
    addRules(
        ":A(?Y) :- :A(?X), :R(?X,?Y) . "
    );
    if (m_processComponentsByLevels) {
        m_traceReasoning = true;
        ASSERT_APPLY_RULES(
            "== LEVEL   1 ===============================================\n"
            "  0:    Extracted current tuple [ :a, rdf:type, :A ]\n"
            "  0:        Matched atom [?X, rdf:type, :A] to tuple [ :a, rdf:type, :A ]\n"
            "  0:            Matching atom [?X, :R, ?Y]\n"
            "  0:                Matched atom [?X, :R, ?Y] to tuple [ :a, :R, :b ]\n"
            "  0:                    Matched rule [?Y, rdf:type, :A] :- [?X, rdf:type, :A], [?X, :R, ?Y] .\n"
            "  0:                        Derived tuple [ :b, rdf:type, :A ]    { normal, added }\n"
            "  0:                Matched atom [?X, :R, ?Y] to tuple [ :a, :R, :c ]\n"
            "  0:                    Matched rule [?Y, rdf:type, :A] :- [?X, rdf:type, :A], [?X, :R, ?Y] .\n"
            "  0:                        Derived tuple [ :c, rdf:type, :A ]    { normal, added }\n"
            "  0:    Extracted current tuple [ :a, :R, :b ]\n"
            "  0:    Extracted current tuple [ :a, :R, :c ]\n"
            "  0:    Extracted current tuple [ :b, :R, :d ]\n"
            "  0:    Extracted current tuple [ :b, rdf:type, :A ]\n"
            "  0:        Matched atom [?X, rdf:type, :A] to tuple [ :b, rdf:type, :A ]\n"
            "  0:            Matching atom [?X, :R, ?Y]\n"
            "  0:                Matched atom [?X, :R, ?Y] to tuple [ :b, :R, :d ]\n"
            "  0:                    Matched rule [?Y, rdf:type, :A] :- [?X, rdf:type, :A], [?X, :R, ?Y] .\n"
            "  0:                        Derived tuple [ :d, rdf:type, :A ]    { normal, added }\n"
            "  0:    Extracted current tuple [ :c, rdf:type, :A ]\n"
            "  0:        Matched atom [?X, rdf:type, :A] to tuple [ :c, rdf:type, :A ]\n"
            "  0:            Matching atom [?X, :R, ?Y]\n"
            "  0:    Extracted current tuple [ :d, rdf:type, :A ]\n"
            "  0:        Matched atom [?X, rdf:type, :A] to tuple [ :d, rdf:type, :A ]\n"
            "  0:            Matching atom [?X, :R, ?Y]\n"
            "============================================================\n"
        );
    }
    else {
        ASSERT_APPLY_RULES(
            "============================================================\n"
            "  0:    Extracted current tuple [ :a, rdf:type, :A ]\n"
            "  0:        Matched atom [?X, rdf:type, :A] to tuple [ :a, rdf:type, :A ]\n"
            "  0:            Matching atom [?X, :R, ?Y]\n"
            "  0:    Extracted current tuple [ :a, :R, :b ]\n"
            "  0:        Matched atom [?X, :R, ?Y] to tuple [ :a, :R, :b ]\n"
            "  0:            Matching atom [?X, rdf:type, :A]\n"
            "  0:                Matched atom [?X, rdf:type, :A] to tuple [ :a, rdf:type, :A ]\n"
            "  0:                    Matched rule [?Y, rdf:type, :A] :- [?X, rdf:type, :A], [?X, :R, ?Y] .\n"
            "  0:                        Derived tuple [ :b, rdf:type, :A ]    { normal, added }\n"
            "  0:    Extracted current tuple [ :a, :R, :c ]\n"
            "  0:        Matched atom [?X, :R, ?Y] to tuple [ :a, :R, :c ]\n"
            "  0:            Matching atom [?X, rdf:type, :A]\n"
            "  0:                Matched atom [?X, rdf:type, :A] to tuple [ :a, rdf:type, :A ]\n"
            "  0:                    Matched rule [?Y, rdf:type, :A] :- [?X, rdf:type, :A], [?X, :R, ?Y] .\n"
            "  0:                        Derived tuple [ :c, rdf:type, :A ]    { normal, added }\n"
            "  0:    Extracted current tuple [ :b, :R, :d ]\n"
            "  0:        Matched atom [?X, :R, ?Y] to tuple [ :b, :R, :d ]\n"
            "  0:            Matching atom [?X, rdf:type, :A]\n"
            "  0:    Extracted current tuple [ :b, rdf:type, :A ]\n"
            "  0:        Matched atom [?X, rdf:type, :A] to tuple [ :b, rdf:type, :A ]\n"
            "  0:            Matching atom [?X, :R, ?Y]\n"
            "  0:                Matched atom [?X, :R, ?Y] to tuple [ :b, :R, :d ]\n"
            "  0:                    Matched rule [?Y, rdf:type, :A] :- [?X, rdf:type, :A], [?X, :R, ?Y] .\n"
            "  0:                        Derived tuple [ :d, rdf:type, :A ]    { normal, added }\n"
            "  0:    Extracted current tuple [ :c, rdf:type, :A ]\n"
            "  0:        Matched atom [?X, rdf:type, :A] to tuple [ :c, rdf:type, :A ]\n"
            "  0:            Matching atom [?X, :R, ?Y]\n"
            "  0:    Extracted current tuple [ :d, rdf:type, :A ]\n"
            "  0:        Matched atom [?X, rdf:type, :A] to tuple [ :d, rdf:type, :A ]\n"
            "  0:            Matching atom [?X, :R, ?Y]\n"
            "============================================================\n"
        );
    }
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":a rdf:type :A ."
        ":b rdf:type :A ."
        ":c rdf:type :A ."
        ":d rdf:type :A ."

        ":a :R :b . "
        ":a :R :c . "
        ":b :R :d . "
    );
}

TEST(testAggregateBasic) {
    addTriples(
        ":London rdf:type :City . "
        ":London :temp 21 . "
        ":London :temp 22 . "
        ":London :temp 23 . "

        ":Oxford rdf:type :City . "
        ":Oxford :temp 11 . "
        ":Oxford :temp 12 . "
        ":Oxford :temp 13 . "
        ":Oxford :temp 14 . "

        ":Manchester rdf:type :City . "
    );
    addRules(
        "[?X, :readingNr, ?CNT] :- [?X, rdf:type, :City], AGGREGATE([?X, :temp, ?T] ON ?X BIND COUNT(?T) AS ?CNT) . "
    );
    if (m_processComponentsByLevels) {
        applyRules();
        ASSERT_QUERY("SELECT ?X ?Y WHERE { ?X :readingNr ?Y }",
            ":London 3 ."
            ":Oxford 4 ."
            ":Manchester 0 ."
        );
    }
}

TEST(testAtomsInRules) {
    // The following looks weird because we're not adding triples, but rules.
    // However, there is no real distinction, and any difference is historical.
    // The two functions will be unified in due course.
    addTriples(
        "[?X, rdf:type, :B] :- [?X, rdf:type, :A] . "
        "[:i, rdf:type, :A] . "
    );
    applyRules();
    ASSERT_QUERY("SELECT ?X WHERE { ?X rdf:type :B }",
        ":i ."
    );
}

TEST(testRulePlan) {
    addRules(
        "[?X,rdf:type,:B] :- [?X,rdf:type,:A], NOT EXISTS ?Y,?Z IN([?X,:R,?Y], [?Y,:S,?Z]) . "
    );
    std::ostringstream output;
    printRulePlan(output, m_prefixes, *m_dataStore);
    ASSERT_EQUAL(
        "[?X, rdf:type, :B] :- [?X, rdf:type, :A], NOT EXIST ?Y, ?Z IN([?X, :R, ?Y], [?Y, :S, ?Z]) .    [ADDED]\n"
        "      + [?X, rdf:type, :B]@0\n"
        "    - [?X, rdf:type, :A]@0, NOT EXIST ?Y, ?Z IN([?X, :R, ?Y], [?Y, :S, ?Z])^<=@0 .\n"
        "    - NOT EXISTS ?Y, ?Z IN([?X, :R, ?Y], [?Y, :S, ?Z]^<=)@0, [?X, rdf:type, :A]^<@0 .\n"
        "    - NOT EXISTS ?Y, ?Z IN([?Y, :S, ?Z], [?X, :R, ?Y]^<)@0, [?X, rdf:type, :A]^<@0 .\n"
        "\n",
        output.str()
    );
}

TEST(testStandardChase) {
    addRules(
        "[?X,:R,?Y], [?Y,rdf:type,:B] :- [?X,rdf:type,:A], FILTER(NOT EXISTS { SELECT ?X WHERE { [?X,:R,?Z], [?Z,rdf:type,:B] } }), BIND(SKOLEM(\"f\",?X) AS ?Y) . "
        "[?X,:R,?Y], [?Y,rdf:type,:C] :- [?X,rdf:type,:B], FILTER(NOT EXISTS { SELECT ?X WHERE { [?X,:R,?Z], [?Z,rdf:type,:C] } }), BIND(SKOLEM(\"g\",?X) AS ?Y) . "
    );
    addTriples(
        ":a rdf:type :A . "

        ":b rdf:type :A . "
        ":b :R :b1 . "
        ":b1 rdf:type :B . "

        ":c rdf:type :A . "
        ":c rdf:type :B . "
        ":c :R :c . "

        ":d rdf:type :A . "
        ":d rdf:type :B . "
        ":d rdf:type :C . "
        ":d :R :d . "
    );
    applyRules();
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":a rdf:type :A .\n"
        ":a :R _:f_12 .\n"
        "_:f_12 rdf:type :B .\n"
        "_:f_12 :R _:g_1036 .\n"
        "_:g_1036 rdf:type :C .\n"
        "\n"
        ":b rdf:type :A .\n"
        ":b :R :b1 .\n"
        ":b1 rdf:type :B .\n"
        ":b1 :R _:g_14 .\n"
        "_:g_14 rdf:type :C .\n"
        "\n"
        ":c rdf:type :A .\n"
        ":c rdf:type :B .\n"
        ":c :R :c .\n"
        ":c :R _:g_15 .\n"
        "_:g_15 rdf:type :C .\n"
        "\n"
        ":d rdf:type :A .\n"
        ":d rdf:type :B .\n"
        ":d rdf:type :C .\n"
        ":d :R :d .\n"

    );
}

#endif
