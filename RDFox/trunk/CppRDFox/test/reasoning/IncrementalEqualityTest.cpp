// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifdef WITH_TEST

#include <CppTest/TestCase.h>

// Define all other tests quites by copying IncrementalEqualityTest.FBF as the template.

SUITE(IncrementalEqualityTest);

__REGISTER(IncrementalEqualityTest, IncrementalEqualityTest__FBF);
__MAKE_SUITE(IncrementalEqualityTest__FBF, "FBF");

__REGISTER(IncrementalEqualityTest, IncrementalEqualityTest__DRed);
__MAKE_ADAPTER(IncrementalEqualityTest__DRed, "DRed", IncrementalEqualityTest__FBF) {
    testParameters.setBoolean("reason.use-DRed", true);
}

// Now define the IncrementalEqualityTest.FBF template suite; to avoid confusion, use a neutral fixture name.

#define TEST(testName)                                                                                          \
    __REGISTER(IncrementalEqualityTest__FBF, IncrementalEqualityTest__FBF##testName);                           \
    __MAKE_TEST(IncrementalEqualityTest__FBF##testName, #testName, : public IncrementalEqualityTestFixture)

#define TEST_UNA(testName)                                                                                      \
    __REGISTER(IncrementalEqualityTest__FBF, IncrementalEqualityTest__FBF##testName);                           \
    __MAKE_TEST(IncrementalEqualityTest__FBF##testName, #testName, : public IncrementalEqualityUNATestFixture)

#include "../querying/DataStoreTest.h"
#include "../../src/dictionary/Dictionary.h"

#include "../querying/DataStoreTest.h"

class IncrementalEqualityTestFixture : public DataStoreTest {

protected:

    virtual void initializeQueryParameters() {
        m_queryParameters.setBoolean("bushy", false);
        m_queryParameters.setBoolean("distinct", false);
        m_queryParameters.setBoolean("cardinality", false);
    }

public:

    IncrementalEqualityTestFixture(const EqualityAxiomatizationType equalityAxiomatizationType = EQUALITY_AXIOMATIZATION_NO_UNA) : DataStoreTest("par-complex-ww", equalityAxiomatizationType) {
    }

    void initializeEx(const CppTest::TestParameters& testParameters) {
        m_useDRed = testParameters.getBoolean("reason.use-DRed", false);
    }

};

class IncrementalEqualityUNATestFixture : public IncrementalEqualityTestFixture {

public:

    IncrementalEqualityUNATestFixture() : IncrementalEqualityTestFixture(EQUALITY_AXIOMATIZATION_UNA) {
    }

};

TEST(testBasicEquality) {
    addRules(
        ":C(?X) :- :A(?X), :B(?X) . "
    );
    addTriples(
        ":i1 a :A . "
        ":i2 a :B . "

        ":i1 :R :i3 . "
        ":i2 :S :i4 . "
    );
    applyRules();
    // Check with expanding equality.
    m_queryParameters.setString("domain", "IDB");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "
        ":A owl:sameAs :A . "
        ":B owl:sameAs :B . "
        ":R owl:sameAs :R . "
        ":S owl:sameAs :S . "

        ":i1 owl:sameAs :i1 . "
        ":i2 owl:sameAs :i2 . "
        ":i3 owl:sameAs :i3 . "
        ":i4 owl:sameAs :i4 . "

        ":i1 a :A . "
        ":i2 a :B . "

        ":i1 :R :i3 . "
        ":i2 :S :i4 . "
    );
    // Check without expanding equality.
    m_queryParameters.setString("domain", "IDBrep");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "
        ":A owl:sameAs :A . "
        ":B owl:sameAs :B . "
        ":R owl:sameAs :R . "
        ":S owl:sameAs :S . "

        ":i1 owl:sameAs :i1 . "
        ":i2 owl:sameAs :i2 . "
        ":i3 owl:sameAs :i3 . "
        ":i4 owl:sameAs :i4 . "

        ":i1 a :A . "
        ":i2 a :B . "

        ":i1 :R :i3 . "
        ":i2 :S :i4 . "
    );

    // Now add the equality
    forAddition(
        ":i1 owl:sameAs :i2 . "
    );
    applyRulesIncrementally(1);
    // Check with expanding equality.
    m_queryParameters.setString("domain", "IDB");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "
        ":A owl:sameAs :A . "
        ":B owl:sameAs :B . "
        ":C owl:sameAs :C . "
        ":R owl:sameAs :R . "
        ":S owl:sameAs :S . "

        ":i1 owl:sameAs :i1 . "
        ":i1 owl:sameAs :i2 . "
        ":i2 owl:sameAs :i1 . "
        ":i2 owl:sameAs :i2 . "
        ":i3 owl:sameAs :i3 . "
        ":i4 owl:sameAs :i4 . "

        ":i1 a :A . "
        ":i1 a :B . "
        ":i1 a :C . "
        ":i2 a :A . "
        ":i2 a :B . "
        ":i2 a :C . "

        ":i1 :R :i3 . "
        ":i2 :R :i3 . "
        ":i1 :S :i4 . "
        ":i2 :S :i4 . "
    );
    // Check without expanding equality.
    m_queryParameters.setString("domain", "IDBrep");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "
        ":A owl:sameAs :A . "
        ":B owl:sameAs :B . "
        ":C owl:sameAs :C . "
        ":R owl:sameAs :R . "
        ":S owl:sameAs :S . "

        ":i1 owl:sameAs :i1 . "
        ":i3 owl:sameAs :i3 . "
        ":i4 owl:sameAs :i4 . "

        ":i1 a :A . "
        ":i1 a :B . "
        ":i1 a :C . "

        ":i1 :R :i3 . "
        ":i1 :S :i4 . "
    );

    // Now delete the equality
    forDeletion(
        ":i1 owl:sameAs :i2 . "
    );
    applyRulesIncrementally(1);
    // Check with expanding equality.
    m_queryParameters.setString("domain", "IDB");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "
        ":A owl:sameAs :A . "
        ":B owl:sameAs :B . "
        ":R owl:sameAs :R . "
        ":S owl:sameAs :S . "

        ":i1 owl:sameAs :i1 . "
        ":i2 owl:sameAs :i2 . "
        ":i3 owl:sameAs :i3 . "
        ":i4 owl:sameAs :i4 . "

        ":i1 a :A . "
        ":i2 a :B . "

        ":i1 :R :i3 . "
        ":i2 :S :i4 . "
    );
    // Check without expanding equality.
    m_queryParameters.setString("domain", "IDBrep");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "
        ":A owl:sameAs :A . "
        ":B owl:sameAs :B . "
        ":R owl:sameAs :R . "
        ":S owl:sameAs :S . "

        ":i1 owl:sameAs :i1 . "
        ":i2 owl:sameAs :i2 . "
        ":i3 owl:sameAs :i3 . "
        ":i4 owl:sameAs :i4 . "

        ":i1 a :A . "
        ":i2 a :B . "

        ":i1 :R :i3 . "
        ":i2 :S :i4 . "
    );
}

TEST(testDiamond) {
    // Initial data loading
    addRules(
        "owl:sameAs(?Y2,?Y1) :- :S(?X,?Y1), :S(?X,?Y2) . "
    );
    addTriples(
        ":a :R :b1 . "

        ":c :S :b2 . "
        ":c :S :b1 . "
        ":d :S :b1 . "
        ":d :S :b3 . "
    );
    applyRules();
    // Check with expanding equality.
    m_queryParameters.setString("domain", "IDB");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        ":R owl:sameAs :R . "
        ":S owl:sameAs :S . "

        ":a owl:sameAs :a . "
        ":b1 owl:sameAs :b1 . "
        ":b1 owl:sameAs :b2 . "
        ":b1 owl:sameAs :b3 . "
        ":b2 owl:sameAs :b1 . "
        ":b2 owl:sameAs :b2 . "
        ":b2 owl:sameAs :b3 . "
        ":b3 owl:sameAs :b1 . "
        ":b3 owl:sameAs :b2 . "
        ":b3 owl:sameAs :b3 . "
        ":c owl:sameAs :c . "
        ":d owl:sameAs :d . "

        ":a :R :b1 . "
        ":a :R :b2 . "
        ":a :R :b3 . "

        ":c :S :b1 . "
        ":c :S :b2 . "
        ":c :S :b3 . "
        ":d :S :b1 . "
        ":d :S :b2 . "
        ":d :S :b3 . "
    );
    // Check without expanding equality.
    m_queryParameters.setString("domain", "IDBrep");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        ":R owl:sameAs :R . "
        ":S owl:sameAs :S . "

        ":a owl:sameAs :a . "
        ":b1 owl:sameAs :b1 . "
        ":c owl:sameAs :c . "
        ":d owl:sameAs :d . "

        ":a :R :b1 . "

        ":c :S :b1 . "
        ":d :S :b1 . "
    );

    // Now delete some tuples.
    forDeletion(
        ":a :R :b1 . "
    );
    applyRulesIncrementally();
    // Check with expanding equality.
    m_queryParameters.setString("domain", "IDB");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        ":S owl:sameAs :S . "

        ":b1 owl:sameAs :b1 . "
        ":b1 owl:sameAs :b2 . "
        ":b1 owl:sameAs :b3 . "
        ":b2 owl:sameAs :b1 . "
        ":b2 owl:sameAs :b2 . "
        ":b2 owl:sameAs :b3 . "
        ":b3 owl:sameAs :b1 . "
        ":b3 owl:sameAs :b2 . "
        ":b3 owl:sameAs :b3 . "
        ":c owl:sameAs :c . "
        ":d owl:sameAs :d . "

        ":c :S :b1 . "
        ":c :S :b2 . "
        ":c :S :b3 . "
        ":d :S :b1 . "
        ":d :S :b2 . "
        ":d :S :b3 . "
    );
    // Check without expanding equality.
    m_queryParameters.setString("domain", "IDBrep");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        ":S owl:sameAs :S . "

        ":b1 owl:sameAs :b1 . "
        ":c owl:sameAs :c . "
        ":d owl:sameAs :d . "

        ":c :S :b1 . "
        ":d :S :b1 . "
    );
}

TEST(testChainEquality) {
    addRules(
        "owl:sameAs(?Y1,?Y2) :- :B(?X), :R(?X,?Y1), :R(?X,?Y2) . "
        "owl:sameAs(?Y1,?Y2) :- :B(?X), :S(?X,?Y1), :S(?X,?Y2) . "
        ":B(?X) :- :A(?X), :C(?X) . "
    );
    addTriples(
        ":a0 :R :a1 . "
        ":a0 :R :b1 . "
        ":a1 :R :a2 . "
        ":a1 :R :b2 . "
        ":a2 :R :a3 . "
        ":a2 :R :b3 . "

        ":c0 :S :b1 . "
        ":c0 :S :c1 . "
        ":c1 :S :b2 . "
        ":c1 :S :c2 . "
        ":c2 :S :b3 . "
        ":c2 :S :c3 . "

        ":a0 a :A . "
        ":a1 a :A . "
        ":a2 a :A . "
        ":a3 a :A . "

        ":c0 a :C . "
        ":c1 a :C . "
        ":c2 a :C . "
        ":c3 a :C . "
    );
    applyRules();
    ASSERT_QUERY("SELECT ?X WHERE { ?X a :B }",
        ""
    );

    // Now add the equality
    forAddition(
        ":a0 owl:sameAs :b0 . "
        ":b0 owl:sameAs :c0 . "
    );
    applyRulesIncrementally(1);
    ASSERT_QUERY("SELECT ?X WHERE { ?X a :B }",
        ":a0 . "
        ":b0 . "
        ":c0 . "

        ":a1 . "
        ":b1 . "
        ":c1 . "

        ":a2 . "
        ":b2 . "
        ":c2 . "

        ":a3 . "
        ":b3 . "
        ":c3 . "
    );

    // Now remove some fact that will break the equality chain
    forDeletion(
        ":c2 a :C . "
    );
    applyRulesIncrementally(1);
    ASSERT_QUERY("SELECT ?X WHERE { ?X a :B }",
        ":a0 . "
        ":b0 . "
        ":c0 . "

        ":a1 . "
        ":b1 . "
        ":c1 . "
    );
}

TEST(testDanglingSameAsRemoval) {
    m_queryParameters.setString("domain", "IDBrep");

    addTriples(
        ":a :R :b . "
        ":c :R :d . "
        ":c :S :d . "
    );
    applyRules();
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":a :R :b . "
        ":c :R :d . "
        ":c :S :d . "
        ":a owl:sameAs :a . "
        ":b owl:sameAs :b . "
        ":c owl:sameAs :c . "
        ":d owl:sameAs :d . "
        ":R owl:sameAs :R . "
        ":S owl:sameAs :S . "
        "owl:sameAs owl:sameAs owl:sameAs . "
    );

    // Now remove some fact that will lead to the removal of some owl:sameAs triples
    forDeletion(
        ":a :R :b . "
        ":c :S :d . "
    );
    applyRulesIncrementally(1);
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":c :R :d . "
        ":c owl:sameAs :c . "
        ":d owl:sameAs :d . "
        ":R owl:sameAs :R . "
        "owl:sameAs owl:sameAs owl:sameAs . "
    );

    // At this point, [:c :R :d] is the only triple. So we make :c and :d the same.
    forAddition(
        ":c owl:sameAs :d . "
    );
    applyRulesIncrementally(1);
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":c :R :c . "
        ":c owl:sameAs :c . "
        ":R owl:sameAs :R . "
        "owl:sameAs owl:sameAs owl:sameAs . "
    );

    // Now delete [:c :R :d]; this should leave [:c owl:sameAs :c].
    forDeletion(
        ":c :R :d . "
    );
    applyRulesIncrementally(1);
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":c owl:sameAs :c . "
        "owl:sameAs owl:sameAs owl:sameAs . "
    );

    // Now delete [:c owl:sameAs :d].
    forDeletion(
        ":c owl:sameAs :d . "
    );
    applyRulesIncrementally(1);
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ""
    );
}

TEST(testInvalidEDBStopGap) {
    // This bug revealed an error in the applyDelRules() function of the incremental reasoning algorithm.
    // The original version of the algorithm DID NOT add a fact G to D if G \in E holds. This, however,
    // was wrong: since such G never got inserted into D, it was never considered in the rederiveDeleted()
    // function, which meant that EDB facts that were merged into G would not get reinstated. Thus, the bug
    // essentially occured if two EDB facts were merged into one: the merged fact would not be reinstated.
    //
    // The following test case reveals this error as follows. Initial materialisation derives triple (1),
    // and so EDB triple (2) is merged into EDB triple (3) since :P1 is chosen as the representative of :P2.
    //
    // (1)  [ :P1, owl:sameAs, :P2 ]
    // (2)  [ :P2, :isHeadOf,  :U ]
    // (3)  [ :P1, :isHeadOf,  :U ]
    //
    // Function overestimateDeleted() determines that :P1 and :P2 should be unmerged, and so it goes through
    // each fact containing :P1. The original version of applyDelRules() would not add fact (3) to D because
    // the fact was an EDB fact -- the reasoning was that the fact will be rederived anyway. Because of that,
    // however, function rederiveDeleted() never got to see a fact in D that represents (2), and so fact (2)
    // was never inserted into I. Consequently, :P1 and :P2 were not merged in the insertion phase, which
    // produced the incorrect result.

    // Load the rules and the data.
    addRules(
        "owl:sameAs(?Y1,?Y2) :- :teacherOf(?Y1,?X), :teacherOf(?Y2,?X) . "
        "owl:sameAs(?Y1,?Y2) :- :isHeadOf(?Y1,?X), :isHeadOf(?Y2,?X) . "
    );
    addTriples(
        ":P1 :isHeadOf  :U . "
        ":P2 :isHeadOf  :U . "
        ":P2 :teacherOf :C1 . "
        ":P2 :teacherOf :C2 . "
    );
    applyRules();
    // Check with expanding equality.
    m_queryParameters.setString("domain", "IDB");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":P1 :isHeadOf :U . "
        ":P1 :teacherOf :C1 . "
        ":P1 :teacherOf :C2 . "

        ":P2 :isHeadOf :U . "
        ":P2 :teacherOf :C1 . "
        ":P2 :teacherOf :C2 . "

        ":P1 owl:sameAs :P1 . "
        ":P1 owl:sameAs :P2 . "
        ":P2 owl:sameAs :P1 . "
        ":P2 owl:sameAs :P2 . "

        ":U owl:sameAs :U . "

        ":C1 owl:sameAs :C1 . "
        ":C2 owl:sameAs :C2 . "

        ":isHeadOf owl:sameAs :isHeadOf . "
        ":teacherOf owl:sameAs :teacherOf . "
        "owl:sameAs owl:sameAs owl:sameAs . "
    );
    // Check without expanding equality.
    m_queryParameters.setString("domain", "IDBrep");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":P1 :isHeadOf :U . "
        ":P1 :teacherOf :C1 . "
        ":P1 :teacherOf :C2 . "

        ":P1 owl:sameAs :P1 . "

        ":U owl:sameAs :U . "

        ":C1 owl:sameAs :C1 . "
        ":C2 owl:sameAs :C2 . "

        ":isHeadOf owl:sameAs :isHeadOf . "
        ":teacherOf owl:sameAs :teacherOf . "
        "owl:sameAs owl:sameAs owl:sameAs . "
    );

    // Now apply incremental reasoning.
    forDeletion(
        ":P2 :teacherOf :C2 . "
    );
    applyRulesIncrementally(1);
    // Check with expanding equality.
    m_queryParameters.setString("domain", "IDB");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":P1 :isHeadOf :U . "
        ":P1 :teacherOf :C1 . "

        ":P2 :isHeadOf :U . "
        ":P2 :teacherOf :C1 . "

        ":P1 owl:sameAs :P1 . "
        ":P1 owl:sameAs :P2 . "
        ":P2 owl:sameAs :P1 . "
        ":P2 owl:sameAs :P2 . "

        ":U owl:sameAs :U . "

        ":C1 owl:sameAs :C1 . "

        ":isHeadOf owl:sameAs :isHeadOf . "
        ":teacherOf owl:sameAs :teacherOf . "
        "owl:sameAs owl:sameAs owl:sameAs . "
    );
    // Check without expanding equality.
    m_queryParameters.setString("domain", "IDBrep");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":P1 :isHeadOf :U . "
        ":P1 :teacherOf :C1 . "

        ":P1 owl:sameAs :P1 . "

        ":U owl:sameAs :U . "

        ":C1 owl:sameAs :C1 . "

        ":isHeadOf owl:sameAs :isHeadOf . "
        ":teacherOf owl:sameAs :teacherOf . "
        "owl:sameAs owl:sameAs owl:sameAs . "
    );
}

TEST(testPrepareIncrementalBug) {
    // Load the rules and the data.
    addRules(
        "owl:sameAs(?X1,?X2) :- :like(?X,?X1), :like(?X,?X2) . "
    );
    addTriples(
        ":i1 :like :P . "
        ":i2 :like :S . "
        ":i2 :like :P . "
    );
    applyRules();
    // Check with expanding equality.
    m_queryParameters.setString("domain", "IDB");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":i1 :like :P . "
        ":i1 :like :S . "

        ":i2 :like :P . "
        ":i2 :like :S . "

        ":i1 owl:sameAs :i1 . "
        ":i2 owl:sameAs :i2 . "
        ":P owl:sameAs :P . "
        ":P owl:sameAs :S . "
        ":S owl:sameAs :P . "
        ":S owl:sameAs :S . "

        ":like owl:sameAs :like . "
        "owl:sameAs owl:sameAs owl:sameAs . "
    );
    // Check without expanding equality.
    m_queryParameters.setString("domain", "IDBrep");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":i1 :like :P . "

        ":i2 :like :P . "

        ":i1 owl:sameAs :i1 . "
        ":i2 owl:sameAs :i2 . "
        ":P owl:sameAs :P . "

        ":like owl:sameAs :like . "
        "owl:sameAs owl:sameAs owl:sameAs . "
    );


    // Now apply incremental reasoning.
    forDeletion(
        ":i2 :like :S . "
        ":i2 :like :P . "
    );
    applyRulesIncrementally(1);
    // Check with expanding equality.
    m_queryParameters.setString("domain", "IDB");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":i1 :like :P . "

        ":i1 owl:sameAs :i1 . "
        ":P owl:sameAs :P . "

        ":like owl:sameAs :like . "
        "owl:sameAs owl:sameAs owl:sameAs . "
    );
    // Check without expanding equality.
    m_queryParameters.setString("domain", "IDBrep");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":i1 :like :P . "

        ":i1 owl:sameAs :i1 . "
        ":P owl:sameAs :P . "

        ":like owl:sameAs :like . "
        "owl:sameAs owl:sameAs owl:sameAs . "
    );
}

TEST(testPreparationBug) {
    // Load the rules and the data.
    addRules(
        "owl:sameAs(?Y1,?Y2) :- :isTaughtBy(?X,?Y1), :isTaughtBy(?X,?Y2) . "
        ":isTaughtBy(?X,?Y) :- :teacherOf(?Y,?X) . "
    );
    addTriples(
        ":P :teacherOf :C . "
        ":C :isTaughtBy :S . "
    );
    applyRules();
    // Check with expanding equality.
    m_queryParameters.setString("domain", "IDB");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":P :teacherOf :C . "
        ":S :teacherOf :C . "

        ":C :isTaughtBy :P . "
        ":C :isTaughtBy :S . "

        ":C owl:sameAs :C . "
        ":P owl:sameAs :P . "
        ":P owl:sameAs :S . "
        ":S owl:sameAs :P . "
        ":S owl:sameAs :S . "

        ":isTaughtBy owl:sameAs :isTaughtBy . "
        ":teacherOf owl:sameAs :teacherOf . "
        "owl:sameAs owl:sameAs owl:sameAs . "
    );
    // Check without expanding equality.
    m_queryParameters.setString("domain", "IDBrep");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":P :teacherOf :C . "

        ":C :isTaughtBy :P . "

        ":C owl:sameAs :C . "
        ":P owl:sameAs :P . "

        ":isTaughtBy owl:sameAs :isTaughtBy . "
        ":teacherOf owl:sameAs :teacherOf . "
        "owl:sameAs owl:sameAs owl:sameAs . "
    );

    // Now apply incremental reasoning.
    forDeletion(
        ":P :teacherOf :C . "
    );
    forAddition(
        ":S :isFriendOf :F . "
    );
    applyRulesIncrementally(1);
    // Check with expanding equality.
    m_queryParameters.setString("domain", "IDB");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":C :isTaughtBy :S . "
        ":S :isFriendOf :F . "

        ":C owl:sameAs :C . "
        ":S owl:sameAs :S . "
        ":F owl:sameAs :F . "

        ":isTaughtBy owl:sameAs :isTaughtBy . "
        ":isFriendOf owl:sameAs :isFriendOf . "
        "owl:sameAs owl:sameAs owl:sameAs . "
    );
    // Check without expanding equality.
    m_queryParameters.setString("domain", "IDBrep");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":C :isTaughtBy :S . "
        ":S :isFriendOf :F . "

        ":C owl:sameAs :C . "
        ":S owl:sameAs :S . "
        ":F owl:sameAs :F . "

        ":isTaughtBy owl:sameAs :isTaughtBy . "
        ":isFriendOf owl:sameAs :isFriendOf . "
        "owl:sameAs owl:sameAs owl:sameAs . "
    );
}

TEST(testQueryArgumentsBufferNormalizationBug) {
    // Initial data loading
    addRules(
        "owl:sameAs(?Y1,?Y2) :- :S(?X,?Y2), :S(?X,?Y1) . "
        ":S(?X,:d) :- :R(?Y,?X) . "
        ":T(?X,?Y) :- :S(?X,?Y) . "
    );
    addTriples(
        ":a :R :b . "
        ":b :S :c . "
    );
    applyRules();
    // Check with expanding equality.
    m_queryParameters.setString("domain", "IDB");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        ":R owl:sameAs :R . "
        ":S owl:sameAs :S . "
        ":T owl:sameAs :T . "
        ":a owl:sameAs :a . "
        ":b owl:sameAs :b . "
        ":c owl:sameAs :c . "
        ":c owl:sameAs :d . "
        ":d owl:sameAs :c . "
        ":d owl:sameAs :d . "

        ":a :R :b . "
        ":b :S :c . "
        ":b :S :d . "
        ":b :T :c . "
        ":b :T :d . "
    );
    // Check without expanding equality.
    m_queryParameters.setString("domain", "IDBrep");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        ":R owl:sameAs :R . "
        ":S owl:sameAs :S . "
        ":T owl:sameAs :T . "
        ":a owl:sameAs :a . "
        ":b owl:sameAs :b . "
        ":d owl:sameAs :d . "

        ":a :R :b . "
        ":b :S :d . "
        ":b :T :d . "
    );

    // Now delete some tuples.
    forDeletion(
        ":a :R :b . "
    );
    applyRulesIncrementally();
    // Check with expanding equality.
    m_queryParameters.setString("domain", "IDB");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        ":S owl:sameAs :S . "
        ":T owl:sameAs :T . "
        ":b owl:sameAs :b . "
        ":c owl:sameAs :c . "

        ":b :S :c . "
        ":b :T :c . "
    );
    // Check without expanding equality.
    m_queryParameters.setString("domain", "IDBrep");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        ":S owl:sameAs :S . "
        ":T owl:sameAs :T . "
        ":b owl:sameAs :b . "
        ":c owl:sameAs :c . "

        ":b :S :c . "
        ":b :T :c . "
    );
}

TEST(testDanglingSameAsBug) {
    // Initial data loading
    addRules(
        "owl:sameAs(?Y1,?Y2) :- :R(?Y1,?X), :R(?Y2,?X) . "
        ":C(?X) :- :S(?X,?Y) . "
    );
    addTriples(
        ":a :S :b . "
        ":a :S :c . "
        ":a :R :d . "
    );
    applyRules();
    // Check with expanding equality.
    m_queryParameters.setString("domain", "IDB");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "
        ":R owl:sameAs :R . "
        ":S owl:sameAs :S . "
        ":C owl:sameAs :C . "
        ":a owl:sameAs :a . "
        ":b owl:sameAs :b . "
        ":c owl:sameAs :c . "
        ":d owl:sameAs :d . "

        ":a :S :b . "
        ":a :S :c . "
        ":a :R :d . "
        ":a rdf:type :C . "
    );
    // Check without expanding equality.
    m_queryParameters.setString("domain", "IDBrep");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "
        ":R owl:sameAs :R . "
        ":S owl:sameAs :S . "
        ":C owl:sameAs :C . "
        ":a owl:sameAs :a . "
        ":b owl:sameAs :b . "
        ":c owl:sameAs :c . "
        ":d owl:sameAs :d . "

        ":a :S :b . "
        ":a :S :c . "
        ":a :R :d . "
        ":a rdf:type :C . "
    );

    // Now delete some tuples.
    forDeletion(
        ":a :S :c . "
        ":a :R :d . "
    );
    applyRulesIncrementally();
    // Check with expanding equality.
    m_queryParameters.setString("domain", "IDB");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "
        ":S owl:sameAs :S . "
        ":C owl:sameAs :C . "
        ":a owl:sameAs :a . "
        ":b owl:sameAs :b . "

        ":a :S :b . "
        ":a rdf:type :C . "
    );
    // Check without expanding equality.
    m_queryParameters.setString("domain", "IDBrep");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "
        ":S owl:sameAs :S . "
        ":C owl:sameAs :C . "
        ":a owl:sameAs :a . "
        ":b owl:sameAs :b . "

        ":a :S :b . "
        ":a rdf:type :C . "
    );
}

TEST(testRuleIndexBug1) {
    // Initial data loading
    addRules(
        "owl:sameAs(?Y1,?Y2) :- :R(?X,?Y1), :R(?X,?Y2) . "
        ":R(?X,:a) :- :A(?X) . "
        ":R(?X,:b) :- :B(?X) . "
        ":R(?X,:c) :- :R(?X,:b) . "
        ":R(?X,:a) :- :R(?X,:b) . "
    );
    addTriples(
        ":i1 :R :d . "
        ":i1 rdf:type :A . "
        ":i2 :R :d . "
        ":i3 rdf:type :B . "
    );
    applyRules();
    // Check with expanding equality.
    m_queryParameters.setString("domain", "IDB");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "
        ":R owl:sameAs :R . "
        ":A owl:sameAs :A . "
        ":B owl:sameAs :B . "

        ":i1 owl:sameAs :i1 . "
        ":i2 owl:sameAs :i2 . "
        ":i3 owl:sameAs :i3 . "

        ":a owl:sameAs :a . "
        ":a owl:sameAs :b . "
        ":a owl:sameAs :c . "
        ":a owl:sameAs :d . "

        ":b owl:sameAs :a . "
        ":b owl:sameAs :b . "
        ":b owl:sameAs :c . "
        ":b owl:sameAs :d . "

        ":c owl:sameAs :a . "
        ":c owl:sameAs :b . "
        ":c owl:sameAs :c . "
        ":c owl:sameAs :d . "

        ":d owl:sameAs :a . "
        ":d owl:sameAs :b . "
        ":d owl:sameAs :c . "
        ":d owl:sameAs :d . "

        ":i1 :R :a . "
        ":i1 :R :b . "
        ":i1 :R :c . "
        ":i1 :R :d . "
        ":i1 rdf:type :A . "

        ":i2 :R :a . "
        ":i2 :R :b . "
        ":i2 :R :c . "
        ":i2 :R :d . "

        ":i3 :R :a . "
        ":i3 :R :b . "
        ":i3 :R :c . "
        ":i3 :R :d . "
        ":i3 rdf:type :B . "
    );
    // Check without expanding equality.
    m_queryParameters.setString("domain", "IDBrep");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "
        ":R owl:sameAs :R . "
        ":A owl:sameAs :A . "
        ":B owl:sameAs :B . "

        ":i1 owl:sameAs :i1 . "
        ":i2 owl:sameAs :i2 . "
        ":i3 owl:sameAs :i3 . "

        ":a owl:sameAs :a . "

        ":i1 :R :a . "
        ":i1 rdf:type :A . "

        ":i2 :R :a . "

        ":i3 :R :a . "
        ":i3 rdf:type :B . "
    );

    // Now delete some tuples.
    forDeletion(
        ":i1 :R :d . "
    );
    applyRulesIncrementally();
    // Check with expanding equality.
    m_queryParameters.setString("domain", "IDB");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "
        ":R owl:sameAs :R . "
        ":A owl:sameAs :A . "
        ":B owl:sameAs :B . "

        ":i1 owl:sameAs :i1 . "
        ":i2 owl:sameAs :i2 . "
        ":i3 owl:sameAs :i3 . "

        ":a owl:sameAs :a . "
        ":a owl:sameAs :b . "
        ":a owl:sameAs :c . "

        ":b owl:sameAs :a . "
        ":b owl:sameAs :b . "
        ":b owl:sameAs :c . "

        ":c owl:sameAs :a . "
        ":c owl:sameAs :b . "
        ":c owl:sameAs :c . "

        ":d owl:sameAs :d . "

        ":i1 :R :a . "
        ":i1 :R :b . "
        ":i1 :R :c . "
        ":i1 rdf:type :A . "

        ":i2 :R :d . "

        ":i3 :R :a . "
        ":i3 :R :b . "
        ":i3 :R :c . "
        ":i3 rdf:type :B . "
    );
    // Check without expanding equality.
    m_queryParameters.setString("domain", "IDBrep");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "
        ":R owl:sameAs :R . "
        ":A owl:sameAs :A . "
        ":B owl:sameAs :B . "

        ":i1 owl:sameAs :i1 . "
        ":i2 owl:sameAs :i2 . "
        ":i3 owl:sameAs :i3 . "

        ":a owl:sameAs :a . "

        ":d owl:sameAs :d . "

        ":i1 :R :a . "
        ":i1 rdf:type :A . "

        ":i2 :R :d . "

        ":i3 :R :a . "
        ":i3 rdf:type :B . "
    );
}

TEST(testRuleIndexBug2) {
    // Initial data loading
    addRules(
        "owl:sameAs(?Y1,?Y2) :- :isHeadOf(?Y1,?X), :isHeadOf(?Y2,?X) . "
        ":like(?X,:NI18) :- :BasketBallFan(?X) . "
        ":isHeadOf(?X,:NI21) :- :SwimmingFan(?X), :isHeadOf(?X,?X1) . "
        ":isHeadOf(?X,:NI21) :- :isHeadOf(?X,?X1) . "
        ":like(?X,?Y) :- :NR6(?X,?Y) . "
        "owl:sameAs(?Y1,?Y2) :- :like(?X,?Y1), :like(?X,?Y2) . "
        ":NR6(?X,:NI6) :- :SwimmingFan(?X) . "
        ":like(?X,:NI12) :- :like(?X,?X1) . "
    );
    addTriples(
        ":Chair4 :isHeadOf :Department4 . "
        ":AssistantProfessor5 :like :Rock_and_Roll . "
        ":Chair5 :isHeadOf :Department5 . "
        ":Lecturer2 rdf:type :BasketBallFan . "
        ":AssistantProfessor6 :like :Rock_and_Roll . "
        ":AssistantProfessor6 rdf:type :SwimmingFan . "
        ":Chair14 :isHeadOf :Department14 . "
        ":Chair14 :like :Tennis . "
        ":Chair14 rdf:type :SwimmingFan . "
        ":AssociateProfessor13 :like :Tennis . "
    );
    applyRules();
    // Check with expanding equality.
    m_queryParameters.setString("domain", "IDB");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":AssistantProfessor5 :like :NI12 . "
        ":AssistantProfessor5 :like :NI18 . "
        ":AssistantProfessor5 :like :NI6 . "
        ":AssistantProfessor5 :like :Rock_and_Roll . "
        ":AssistantProfessor5 :like :Tennis . "
        ":AssistantProfessor5 owl:sameAs :AssistantProfessor5 . "
        ":AssistantProfessor6 :like :NI12 . "
        ":AssistantProfessor6 :like :NI18 . "
        ":AssistantProfessor6 :like :NI6 . "
        ":AssistantProfessor6 :like :Rock_and_Roll . "
        ":AssistantProfessor6 :like :Tennis . "
        ":AssistantProfessor6 :NR6 :NI12 . "
        ":AssistantProfessor6 :NR6 :NI18 . "
        ":AssistantProfessor6 :NR6 :NI6 . "
        ":AssistantProfessor6 :NR6 :Rock_and_Roll . "
        ":AssistantProfessor6 :NR6 :Tennis . "
        ":AssistantProfessor6 owl:sameAs :AssistantProfessor6 . "
        ":AssistantProfessor6 rdf:type :SwimmingFan . "
        ":AssociateProfessor13 :like :NI12 . "
        ":AssociateProfessor13 :like :NI18 . "
        ":AssociateProfessor13 :like :NI6 . "
        ":AssociateProfessor13 :like :Rock_and_Roll . "
        ":AssociateProfessor13 :like :Tennis . "
        ":AssociateProfessor13 owl:sameAs :AssociateProfessor13 . "
        ":BasketBallFan owl:sameAs :BasketBallFan . "
        ":Chair14 :isHeadOf :Department14 . "
        ":Chair14 :isHeadOf :Department4 . "
        ":Chair14 :isHeadOf :Department5 . "
        ":Chair14 :isHeadOf :NI21 . "
        ":Chair14 :like :NI12 . "
        ":Chair14 :like :NI18 . "
        ":Chair14 :like :NI6 . "
        ":Chair14 :like :Rock_and_Roll . "
        ":Chair14 :like :Tennis . "
        ":Chair14 :NR6 :NI12 . "
        ":Chair14 :NR6 :NI18 . "
        ":Chair14 :NR6 :NI6 . "
        ":Chair14 :NR6 :Rock_and_Roll . "
        ":Chair14 :NR6 :Tennis . "
        ":Chair14 owl:sameAs :Chair14 . "
        ":Chair14 owl:sameAs :Chair4 . "
        ":Chair14 owl:sameAs :Chair5 . "
        ":Chair14 rdf:type :SwimmingFan . "
        ":Chair4 :isHeadOf :Department14 . "
        ":Chair4 :isHeadOf :Department4 . "
        ":Chair4 :isHeadOf :Department5 . "
        ":Chair4 :isHeadOf :NI21 . "
        ":Chair4 :like :NI12 . "
        ":Chair4 :like :NI18 . "
        ":Chair4 :like :NI6 . "
        ":Chair4 :like :Rock_and_Roll . "
        ":Chair4 :like :Tennis . "
        ":Chair4 :NR6 :NI12 . "
        ":Chair4 :NR6 :NI18 . "
        ":Chair4 :NR6 :NI6 . "
        ":Chair4 :NR6 :Rock_and_Roll . "
        ":Chair4 :NR6 :Tennis . "
        ":Chair4 owl:sameAs :Chair14 . "
        ":Chair4 owl:sameAs :Chair4 . "
        ":Chair4 owl:sameAs :Chair5 . "
        ":Chair4 rdf:type :SwimmingFan . "
        ":Chair5 :isHeadOf :Department14 . "
        ":Chair5 :isHeadOf :Department4 . "
        ":Chair5 :isHeadOf :Department5 . "
        ":Chair5 :isHeadOf :NI21 . "
        ":Chair5 :like :NI12 . "
        ":Chair5 :like :NI18 . "
        ":Chair5 :like :NI6 . "
        ":Chair5 :like :Rock_and_Roll . "
        ":Chair5 :like :Tennis . "
        ":Chair5 :NR6 :NI12 . "
        ":Chair5 :NR6 :NI18 . "
        ":Chair5 :NR6 :NI6 . "
        ":Chair5 :NR6 :Rock_and_Roll . "
        ":Chair5 :NR6 :Tennis . "
        ":Chair5 owl:sameAs :Chair14 . "
        ":Chair5 owl:sameAs :Chair4 . "
        ":Chair5 owl:sameAs :Chair5 . "
        ":Chair5 rdf:type :SwimmingFan . "
        ":Department14 owl:sameAs :Department14 . "
        ":Department4 owl:sameAs :Department4 . "
        ":Department5 owl:sameAs :Department5 . "
        ":isHeadOf owl:sameAs :isHeadOf . "
        ":Lecturer2 :like :NI12 . "
        ":Lecturer2 :like :NI18 . "
        ":Lecturer2 :like :NI6 . "
        ":Lecturer2 :like :Rock_and_Roll . "
        ":Lecturer2 :like :Tennis . "
        ":Lecturer2 owl:sameAs :Lecturer2 . "
        ":Lecturer2 rdf:type :BasketBallFan . "
        ":like owl:sameAs :like . "
        ":NI12 owl:sameAs :NI12 . "
        ":NI12 owl:sameAs :NI18 . "
        ":NI12 owl:sameAs :NI6 . "
        ":NI12 owl:sameAs :Rock_and_Roll . "
        ":NI12 owl:sameAs :Tennis . "
        ":NI18 owl:sameAs :NI12 . "
        ":NI18 owl:sameAs :NI18 . "
        ":NI18 owl:sameAs :NI6 . "
        ":NI18 owl:sameAs :Rock_and_Roll . "
        ":NI18 owl:sameAs :Tennis . "
        ":NI21 owl:sameAs :NI21 . "
        ":NI6 owl:sameAs :NI12 . "
        ":NI6 owl:sameAs :NI18 . "
        ":NI6 owl:sameAs :NI6 . "
        ":NI6 owl:sameAs :Rock_and_Roll . "
        ":NI6 owl:sameAs :Tennis . "
        ":NR6 owl:sameAs :NR6 . "
        ":Rock_and_Roll owl:sameAs :NI12 . "
        ":Rock_and_Roll owl:sameAs :NI18 . "
        ":Rock_and_Roll owl:sameAs :NI6 . "
        ":Rock_and_Roll owl:sameAs :Rock_and_Roll . "
        ":Rock_and_Roll owl:sameAs :Tennis . "
        ":SwimmingFan owl:sameAs :SwimmingFan . "
        ":Tennis owl:sameAs :NI12 . "
        ":Tennis owl:sameAs :NI18 . "
        ":Tennis owl:sameAs :NI6 . "
        ":Tennis owl:sameAs :Rock_and_Roll . "
        ":Tennis owl:sameAs :Tennis . "
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "
    );
    // Check without expanding equality.
    m_queryParameters.setString("domain", "IDBrep");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":AssistantProfessor5 :like :NI18 . "
        ":AssistantProfessor5 owl:sameAs :AssistantProfessor5 . "
        ":AssistantProfessor6 :like :NI18 . "
        ":AssistantProfessor6 :NR6 :NI18 . "
        ":AssistantProfessor6 owl:sameAs :AssistantProfessor6 . "
        ":AssistantProfessor6 rdf:type :SwimmingFan . "
        ":AssociateProfessor13 :like :NI18 . "
        ":AssociateProfessor13 owl:sameAs :AssociateProfessor13 . "
        ":BasketBallFan owl:sameAs :BasketBallFan . "
        ":Chair4 :isHeadOf :Department14 . "
        ":Chair4 :isHeadOf :Department4 . "
        ":Chair4 :isHeadOf :Department5 . "
        ":Chair4 :isHeadOf :NI21 . "
        ":Chair4 :like :NI18 . "
        ":Chair4 :NR6 :NI18 . "
        ":Chair4 owl:sameAs :Chair4 . "
        ":Chair4 rdf:type :SwimmingFan . "
        ":Department14 owl:sameAs :Department14 . "
        ":Department4 owl:sameAs :Department4 . "
        ":Department5 owl:sameAs :Department5 . "
        ":isHeadOf owl:sameAs :isHeadOf . "
        ":Lecturer2 :like :NI18 . "
        ":Lecturer2 owl:sameAs :Lecturer2 . "
        ":Lecturer2 rdf:type :BasketBallFan . "
        ":like owl:sameAs :like . "
        ":NI18 owl:sameAs :NI18 . "
        ":NI21 owl:sameAs :NI21 . "
        ":NR6 owl:sameAs :NR6 . "
        ":SwimmingFan owl:sameAs :SwimmingFan . "
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "
    );

    // Now delete some tuples.
    forDeletion(
        ":AssistantProfessor5 :like :Rock_and_Roll . "
        ":Chair5 :isHeadOf :Department5 . "
    );
    applyRulesIncrementally();
    // Check with expanding equality.
    m_queryParameters.setString("domain", "IDB");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":AssistantProfessor6 :like :NI12 . "
        ":AssistantProfessor6 :like :NI18 . "
        ":AssistantProfessor6 :like :NI6 . "
        ":AssistantProfessor6 :like :Rock_and_Roll . "
        ":AssistantProfessor6 :like :Tennis . "
        ":AssistantProfessor6 :NR6 :NI12 . "
        ":AssistantProfessor6 :NR6 :NI18 . "
        ":AssistantProfessor6 :NR6 :NI6 . "
        ":AssistantProfessor6 :NR6 :Rock_and_Roll . "
        ":AssistantProfessor6 :NR6 :Tennis . "
        ":AssistantProfessor6 owl:sameAs :AssistantProfessor6 . "
        ":AssistantProfessor6 rdf:type :SwimmingFan . "
        ":AssociateProfessor13 :like :NI12 . "
        ":AssociateProfessor13 :like :NI18 . "
        ":AssociateProfessor13 :like :NI6 . "
        ":AssociateProfessor13 :like :Rock_and_Roll . "
        ":AssociateProfessor13 :like :Tennis . "
        ":AssociateProfessor13 owl:sameAs :AssociateProfessor13 . "
        ":BasketBallFan owl:sameAs :BasketBallFan . "
        ":Chair14 :isHeadOf :Department14 . "
        ":Chair14 :isHeadOf :Department4 . "
        ":Chair14 :isHeadOf :NI21 . "
        ":Chair14 :like :NI12 . "
        ":Chair14 :like :NI18 . "
        ":Chair14 :like :NI6 . "
        ":Chair14 :like :Rock_and_Roll . "
        ":Chair14 :like :Tennis . "
        ":Chair14 :NR6 :NI12 . "
        ":Chair14 :NR6 :NI18 . "
        ":Chair14 :NR6 :NI6 . "
        ":Chair14 :NR6 :Rock_and_Roll . "
        ":Chair14 :NR6 :Tennis . "
        ":Chair14 owl:sameAs :Chair14 . "
        ":Chair14 owl:sameAs :Chair4 . "
        ":Chair14 rdf:type :SwimmingFan . "
        ":Chair4 :isHeadOf :Department14 . "
        ":Chair4 :isHeadOf :Department4 . "
        ":Chair4 :isHeadOf :NI21 . "
        ":Chair4 :like :NI12 . "
        ":Chair4 :like :NI18 . "
        ":Chair4 :like :NI6 . "
        ":Chair4 :like :Rock_and_Roll . "
        ":Chair4 :like :Tennis . "
        ":Chair4 :NR6 :NI12 . "
        ":Chair4 :NR6 :NI18 . "
        ":Chair4 :NR6 :NI6 . "
        ":Chair4 :NR6 :Rock_and_Roll . "
        ":Chair4 :NR6 :Tennis . "
        ":Chair4 owl:sameAs :Chair14 . "
        ":Chair4 owl:sameAs :Chair4 . "
        ":Chair4 rdf:type :SwimmingFan . "
        ":Department14 owl:sameAs :Department14 . "
        ":Department4 owl:sameAs :Department4 . "
        ":isHeadOf owl:sameAs :isHeadOf . "
        ":Lecturer2 :like :NI12 . "
        ":Lecturer2 :like :NI18 . "
        ":Lecturer2 :like :NI6 . "
        ":Lecturer2 :like :Rock_and_Roll . "
        ":Lecturer2 :like :Tennis . "
        ":Lecturer2 owl:sameAs :Lecturer2 . "
        ":Lecturer2 rdf:type :BasketBallFan . "
        ":like owl:sameAs :like . "
        ":NI12 owl:sameAs :NI12 . "
        ":NI12 owl:sameAs :NI18 . "
        ":NI12 owl:sameAs :NI6 . "
        ":NI12 owl:sameAs :Rock_and_Roll . "
        ":NI12 owl:sameAs :Tennis . "
        ":NI18 owl:sameAs :NI12 . "
        ":NI18 owl:sameAs :NI18 . "
        ":NI18 owl:sameAs :NI6 . "
        ":NI18 owl:sameAs :Rock_and_Roll . "
        ":NI18 owl:sameAs :Tennis . "
        ":NI21 owl:sameAs :NI21 . "
        ":NI6 owl:sameAs :NI12 . "
        ":NI6 owl:sameAs :NI18 . "
        ":NI6 owl:sameAs :NI6 . "
        ":NI6 owl:sameAs :Rock_and_Roll . "
        ":NI6 owl:sameAs :Tennis . "
        ":NR6 owl:sameAs :NR6 . "
        ":Rock_and_Roll owl:sameAs :NI12 . "
        ":Rock_and_Roll owl:sameAs :NI18 . "
        ":Rock_and_Roll owl:sameAs :NI6 . "
        ":Rock_and_Roll owl:sameAs :Rock_and_Roll . "
        ":Rock_and_Roll owl:sameAs :Tennis . "
        ":SwimmingFan owl:sameAs :SwimmingFan . "
        ":Tennis owl:sameAs :NI12 . "
        ":Tennis owl:sameAs :NI18 . "
        ":Tennis owl:sameAs :NI6 . "
        ":Tennis owl:sameAs :Rock_and_Roll . "
        ":Tennis owl:sameAs :Tennis . "
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "
    );
    // Check without expanding equality.
    m_queryParameters.setString("domain", "IDBrep");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":AssistantProfessor6 :like :NI18 . "
        ":AssistantProfessor6 :NR6 :NI18 . "
        ":AssistantProfessor6 owl:sameAs :AssistantProfessor6 . "
        ":AssistantProfessor6 rdf:type :SwimmingFan . "
        ":AssociateProfessor13 :like :NI18 . "
        ":AssociateProfessor13 owl:sameAs :AssociateProfessor13 . "
        ":BasketBallFan owl:sameAs :BasketBallFan . "
        ":Chair4 :isHeadOf :Department14 . "
        ":Chair4 :isHeadOf :Department4 . "
        ":Chair4 :isHeadOf :NI21 . "
        ":Chair4 :like :NI18 . "
        ":Chair4 :NR6 :NI18 . "
        ":Chair4 owl:sameAs :Chair4 . "
        ":Chair4 rdf:type :SwimmingFan . "
        ":Department14 owl:sameAs :Department14 . "
        ":Department4 owl:sameAs :Department4 . "
        ":isHeadOf owl:sameAs :isHeadOf . "
        ":Lecturer2 :like :NI18 . "
        ":Lecturer2 owl:sameAs :Lecturer2 . "
        ":Lecturer2 rdf:type :BasketBallFan . "
        ":like owl:sameAs :like . "
        ":NI18 owl:sameAs :NI18 . "
        ":NI21 owl:sameAs :NI21 . "
        ":NR6 owl:sameAs :NR6 . "
        ":SwimmingFan owl:sameAs :SwimmingFan . "
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "
    );
}

TEST(testSameAsBug) {
    // Initial data loading
    addRules(
        "owl:sameAs(?Y1,?Y2) :- :R(?Y1,?X), :R(?Y2,?X) . "
        ":R(?X,:b) :- :R(?X,?X1) . "
        "owl:sameAs(?Y1,?Y2) :- :S(?X,?Y1), :S(?X,?Y2) . "
        ":S(?X,:c) :- :S(?X,?X1) . "
    );
    addTriples(
        ":a1 :R :b1 . "
        ":a2 :R :b2 . "
        ":a2 :S :c2 . "
        ":a3 :S :c3 . "
    );
    applyRules();
    // Check with expanding equality.
    m_queryParameters.setString("domain", "IDB");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        ":R owl:sameAs :R . "
        ":S owl:sameAs :S . "

        ":a1 owl:sameAs :a1 . "
        ":a1 owl:sameAs :a2 . "
        ":a2 owl:sameAs :a1 . "
        ":a2 owl:sameAs :a2 . "
        ":a3 owl:sameAs :a3 . "

        ":b owl:sameAs :b . "
        ":b1 owl:sameAs :b1 . "
        ":b2 owl:sameAs :b2 . "

        ":c owl:sameAs :c . "
        ":c owl:sameAs :c2 . "
        ":c owl:sameAs :c3 . "
        ":c2 owl:sameAs :c . "
        ":c2 owl:sameAs :c2 . "
        ":c2 owl:sameAs :c3 . "
        ":c3 owl:sameAs :c . "
        ":c3 owl:sameAs :c2 . "
        ":c3 owl:sameAs :c3 . "

        ":a1 :R :b . "
        ":a1 :R :b1 . "
        ":a1 :R :b2 . "

        ":a2 :R :b . "
        ":a2 :R :b1 . "
        ":a2 :R :b2 . "

        ":a1 :S :c . "
        ":a1 :S :c2 . "
        ":a1 :S :c3 . "

        ":a2 :S :c . "
        ":a2 :S :c2 . "
        ":a2 :S :c3 . "

        ":a3 :S :c . "
        ":a3 :S :c2 . "
        ":a3 :S :c3 . "
    );
    // Check without expanding equality.
    m_queryParameters.setString("domain", "IDBrep");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        ":R owl:sameAs :R . "
        ":S owl:sameAs :S . "

        ":a1 owl:sameAs :a1 . "
        ":a3 owl:sameAs :a3 . "

        ":b owl:sameAs :b . "
        ":b1 owl:sameAs :b1 . "
        ":b2 owl:sameAs :b2 . "

        ":c owl:sameAs :c . "

        ":a1 :R :b . "
        ":a1 :R :b1 . "
        ":a1 :R :b2 . "
        ":a1 :S :c . "

        ":a3 :S :c . "
    );

    // Now delete some tuples.
    forDeletion(
        ":a3 :S :c3 . "
        ":a2 :R :b2 . "
    );
    applyRulesIncrementally();
    // Check with expanding equality.
    m_queryParameters.setString("domain", "IDB");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        ":R owl:sameAs :R . "
        ":S owl:sameAs :S . "

        ":a1 owl:sameAs :a1 . "
        ":a2 owl:sameAs :a2 . "

        ":b owl:sameAs :b . "
        ":b1 owl:sameAs :b1 . "

        ":c owl:sameAs :c . "
        ":c owl:sameAs :c2 . "
        ":c2 owl:sameAs :c . "
        ":c2 owl:sameAs :c2 . "

        ":a1 :R :b . "
        ":a1 :R :b1 . "

        ":a2 :S :c . "
        ":a2 :S :c2 . "
    );
    // Check without expanding equality.
    m_queryParameters.setString("domain", "IDBrep");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        ":R owl:sameAs :R . "
        ":S owl:sameAs :S . "

        ":a1 owl:sameAs :a1 . "
        ":a2 owl:sameAs :a2 . "

        ":b owl:sameAs :b . "
        ":b1 owl:sameAs :b1 . "

        ":c owl:sameAs :c . "

        ":a1 :R :b . "
        ":a1 :R :b1 . "

        ":a2 :S :c . "
    );
}

TEST(testContradiction1) {
    // Initial data loading
    addRules(
        "owl:sameAs(?Y1,?Y2) :- :A(?X), :R(?X,?Y1), :R(?X,?Y2) . "
    );
    addTriples(
        ":a :R :b . "
        ":a :R :c . "
        ":b owl:differentFrom :c . "
    );
    applyRules();
    // Check with expanding equality.
    m_queryParameters.setString("domain", "IDB");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "owl:differentFrom owl:sameAs owl:differentFrom . "
        ":R owl:sameAs :R . "
        ":a owl:sameAs :a . "
        ":b owl:sameAs :b . "
        ":c owl:sameAs :c . "

        ":a :R :b . "
        ":a :R :c . "
        ":b owl:differentFrom :c . "
    );
    // Check without expanding equality.
    m_queryParameters.setString("domain", "IDBrep");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "owl:differentFrom owl:sameAs owl:differentFrom . "
        ":R owl:sameAs :R . "
        ":a owl:sameAs :a . "
        ":b owl:sameAs :b . "
        ":c owl:sameAs :c . "

        ":a :R :b . "
        ":a :R :c . "
        ":b owl:differentFrom :c . "
    );

    // Now add some tuples.
    forAddition(
        ":a rdf:type :A . "
    );
    applyRulesIncrementally();
    // Check with expanding equality.
    m_queryParameters.setString("domain", "IDB");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "owl:differentFrom owl:sameAs owl:differentFrom . "
        "rdf:type owl:sameAs rdf:type . "
        "owl:Nothing owl:sameAs owl:Nothing . "
        ":A owl:sameAs :A . "
        ":R owl:sameAs :R . "
        ":a owl:sameAs :a . "
        ":b owl:sameAs :b . "
        ":b owl:sameAs :c . "
        ":c owl:sameAs :b . "
        ":c owl:sameAs :c . "

        ":a rdf:type :A . "

        ":a :R :b . "
        ":a :R :c . "
        ":b owl:differentFrom :b . "
        ":b owl:differentFrom :c . "
        ":c owl:differentFrom :b . "
        ":c owl:differentFrom :c . "
        ":b rdf:type owl:Nothing . "
        ":c rdf:type owl:Nothing . "
    );
    // Check without expanding equality.
    m_queryParameters.setString("domain", "IDBrep");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "owl:differentFrom owl:sameAs owl:differentFrom . "
        "rdf:type owl:sameAs rdf:type . "
        "owl:Nothing owl:sameAs owl:Nothing . "
        ":A owl:sameAs :A . "
        ":R owl:sameAs :R . "
        ":a owl:sameAs :a . "
        ":b owl:sameAs :b . "

         ":a rdf:type :A . "

        ":a :R :b . "
        ":b owl:differentFrom :b . "
        ":b rdf:type owl:Nothing . "
    );

    // Now revert these changes.
    forDeletion(
        ":a rdf:type :A . "
    );
    applyRulesIncrementally();
    // Check with expanding equality.
    m_queryParameters.setString("domain", "IDB");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "owl:differentFrom owl:sameAs owl:differentFrom . "
        ":R owl:sameAs :R . "
        ":a owl:sameAs :a . "
        ":b owl:sameAs :b . "
        ":c owl:sameAs :c . "

        ":a :R :b . "
        ":a :R :c . "
        ":b owl:differentFrom :c . "
    );
    // Check without expanding equality.
    m_queryParameters.setString("domain", "IDBrep");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "owl:differentFrom owl:sameAs owl:differentFrom . "
        ":R owl:sameAs :R . "
        ":a owl:sameAs :a . "
        ":b owl:sameAs :b . "
        ":c owl:sameAs :c . "

        ":a :R :b . "
        ":a :R :c . "
        ":b owl:differentFrom :c . "
    );
}

TEST(testContradiction2) {
    // Initial data loading
    addRules(
        "owl:sameAs(1,2) :- :A(?X) . "
    );
    addTriples(
        ""
    );
    applyRules();
    // Check with expanding equality.
    m_queryParameters.setString("domain", "IDB");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ""
    );
    // Check without expanding equality.
    m_queryParameters.setString("domain", "IDBrep");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ""
    );

    // Now add some tuples.
    forAddition(
        ":a rdf:type :A . "
    );
    applyRulesIncrementally();
    // Check with expanding equality.
    m_queryParameters.setString("domain", "IDB");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "
        "owl:Nothing owl:sameAs owl:Nothing . "
        ":A owl:sameAs :A . "
        ":a owl:sameAs :a . "
        "1 owl:sameAs 1 . "
        "1 owl:sameAs 2 . "
        "2 owl:sameAs 1 . "
        "2 owl:sameAs 2 . "

        ":a rdf:type :A . "

        "1 rdf:type owl:Nothing . "
        "2 rdf:type owl:Nothing . "
    );
    // Check without expanding equality.
    m_queryParameters.setString("domain", "IDBrep");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "
        "owl:Nothing owl:sameAs owl:Nothing . "
        ":A owl:sameAs :A . "
        ":a owl:sameAs :a . "
        "1 owl:sameAs 1 . "

        ":a rdf:type :A . "

        "1 rdf:type owl:Nothing . "
    );

    // Now revert these changes.
    forDeletion(
        ":a rdf:type :A . "
    );
    applyRulesIncrementally();
    // Check with expanding equality.
    m_queryParameters.setString("domain", "IDB");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ""
    );
    // Check without expanding equality.
    m_queryParameters.setString("domain", "IDBrep");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ""
    );
}

TEST(testRuleReevaluationDuringInsertionBug) {
    // Initial data loading -- the order of operations was important to demonstrate the bug.
    addTriples(
        ":a rdf:type :C . "
    );
    addRules(
        ":R(?X,:b) :- :A(?X) . "
        ":B(?X) :- :R(?X,:b) . "
    );
    applyRules();
    // Check with expanding equality.
    m_queryParameters.setString("domain", "IDB");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "

        ":C owl:sameAs :C . "
        ":a owl:sameAs :a . "

        ":a rdf:type :C . "
    );
    // Check without expanding equality.
    m_queryParameters.setString("domain", "IDBrep");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "

        ":C owl:sameAs :C . "
        ":a owl:sameAs :a . "

        ":a rdf:type :C . "
    );

    // Now add some tuples.
    forAddition(
        ":i rdf:type :A . "
        ":a owl:sameAs :b . "
    );
    applyRulesIncrementally();
    // Check with expanding equality.
    m_queryParameters.setString("domain", "IDB");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "

        ":R owl:sameAs :R . "
        ":A owl:sameAs :A . "
        ":B owl:sameAs :B . "
        ":C owl:sameAs :C . "
        ":i owl:sameAs :i . "
        ":a owl:sameAs :a . "
        ":a owl:sameAs :b . "
        ":b owl:sameAs :a . "
        ":b owl:sameAs :b . "

        ":a rdf:type :C . "
        ":b rdf:type :C . "
        ":i rdf:type :A . "
        ":i rdf:type :B . "
        ":i :R :a . "
        ":i :R :b . "
    );
    // Check without expanding equality.
    m_queryParameters.setString("domain", "IDBrep");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "

        ":R owl:sameAs :R . "
        ":A owl:sameAs :A . "
        ":B owl:sameAs :B . "
        ":C owl:sameAs :C . "
        ":i owl:sameAs :i . "
        ":a owl:sameAs :a . "

        ":a rdf:type :C . "
        ":i rdf:type :A . "
        ":i rdf:type :B . "
        ":i :R :a . "
    );
}

TEST(testRestoredSameAs) {
    // The order of operations is important.
    addTriples(
        ":a rdf:type :C . "
        ":a owl:sameAs :b . "
        ":b rdf:type :C . "
    );
    applyRules();
    // Check with expanding equality.
    m_queryParameters.setString("domain", "IDB");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "

        ":C owl:sameAs :C . "
        ":a owl:sameAs :a . "
        ":a owl:sameAs :b . "
        ":b owl:sameAs :a . "
        ":b owl:sameAs :b . "

        ":a rdf:type :C . "
        ":b rdf:type :C . "
    );
    // Check without expanding equality.
    m_queryParameters.setString("domain", "IDBrep");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "

        ":C owl:sameAs :C . "
        ":a owl:sameAs :a . "

        ":a rdf:type :C . "
    );

    // Now delete some tuples.
    forDeletion(
        ":a owl:sameAs :b . "
    );
    applyRulesIncrementally();
    // Check with expanding equality.
    m_queryParameters.setString("domain", "IDB");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "

        ":C owl:sameAs :C . "
        ":a owl:sameAs :a . "
        ":b owl:sameAs :b . "

        ":a rdf:type :C . "
        ":b rdf:type :C . "
    );
    // Check without expanding equality.
    m_queryParameters.setString("domain", "IDBrep");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "

        ":C owl:sameAs :C . "
        ":a owl:sameAs :a . "
        ":b owl:sameAs :b . "

        ":a rdf:type :C . "
        ":b rdf:type :C . "
    );
}

TEST(testSupportingFactsPrototypeBug) {
    // The order of operations is important.
    addTriples(
        ":a :R :b . "
        ":a :S :b . "
        ":S owl:sameAs :T . "
    );
    addRules(
        ":C(?X) :- :R(?X,?Y) . "
        ":C(?X) :- :T(?X,?Y) . "
    );
    applyRules();
    // Check with expanding equality.
    m_queryParameters.setString("domain", "IDB");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "

        ":a owl:sameAs :a . "
        ":b owl:sameAs :b . "

        ":C owl:sameAs :C . "
        ":R owl:sameAs :R . "
        ":S owl:sameAs :S . "
        ":S owl:sameAs :T . "
        ":T owl:sameAs :S . "
        ":T owl:sameAs :T . "

        ":a :R :b . "
        ":a :S :b . "
        ":a :T :b . "
        ":a rdf:type :C . "
    );
    // Check without expanding equality.
    m_queryParameters.setString("domain", "IDBrep");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "

        ":a owl:sameAs :a . "
        ":b owl:sameAs :b . "

        ":C owl:sameAs :C . "
        ":R owl:sameAs :R . "
        ":S owl:sameAs :S . "

        ":a :R :b . "
        ":a :S :b . "
        ":a rdf:type :C . "
    );

    // Now delete some tuples.
    forDeletion(
        ":a :R :b . "
    );
    applyRulesIncrementally();
    // Check with expanding equality.
    m_queryParameters.setString("domain", "IDB");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "

        ":a owl:sameAs :a . "
        ":b owl:sameAs :b . "

        ":C owl:sameAs :C . "
        ":S owl:sameAs :S . "
        ":S owl:sameAs :T . "
        ":T owl:sameAs :S . "
        ":T owl:sameAs :T . "

        ":a :S :b . "
        ":a :T :b . "
        ":a rdf:type :C . "
    );
    // Check without expanding equality.
    m_queryParameters.setString("domain", "IDBrep");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
         "owl:sameAs owl:sameAs owl:sameAs . "
         "rdf:type owl:sameAs rdf:type . "

         ":a owl:sameAs :a . "
         ":b owl:sameAs :b . "

         ":C owl:sameAs :C . "
         ":S owl:sameAs :S . "

         ":a :S :b . "
         ":a rdf:type :C . "
    );
}

TEST(testEquivalenceClass) {
    addTriples(
        ":a owl:sameAs :b . "
        ":a rdf:type :A . "
        ":b rdf:type :A . "
        ":c rdf:type :C . "
    );
    addRules(
        ":A(:a) :- :C(:c) . "
    );
    applyRules();
    // Check with expanding equality.
    m_queryParameters.setString("domain", "IDB");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "
        ":A owl:sameAs :A . "
        ":C owl:sameAs :C . "

        ":a owl:sameAs :a . "
        ":a owl:sameAs :b . "
        ":b owl:sameAs :a . "
        ":b owl:sameAs :b . "
        ":c owl:sameAs :c . "

        ":a rdf:type :A . "
        ":b rdf:type :A . "
        ":c rdf:type :C . "
    );
    // Check without expanding equality.
    m_queryParameters.setString("domain", "IDBrep");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "
        ":A owl:sameAs :A . "
        ":C owl:sameAs :C . "

        ":a owl:sameAs :a . "
        ":c owl:sameAs :c . "

        ":a rdf:type :A . "
        ":c rdf:type :C . "
    );

    // Now delete some tuples.
    forDeletion(
        ":c rdf:type :C . "
    );
    applyRulesIncrementally();
    // Check with expanding equality.
    m_queryParameters.setString("domain", "IDB");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "
        ":A owl:sameAs :A . "

        ":a owl:sameAs :a . "
        ":a owl:sameAs :b . "
        ":b owl:sameAs :a . "
        ":b owl:sameAs :b . "

        ":a rdf:type :A . "
        ":b rdf:type :A . "
    );
    // Check without expanding equality.
    m_queryParameters.setString("domain", "IDBrep");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "
        ":A owl:sameAs :A . "

        ":a owl:sameAs :a . "

        ":a rdf:type :A . "
    );
}

TEST(testSameAsInCheckProvability) {
    addTriples(
        ":a rdf:type :A . "
        ":b rdf:type :B . "
        ":c rdf:type :C . "
        ":b owl:sameAs :c . "
    );
    addRules(
        ":C(:b) :- :A(:a) . "
        ":D(:b) :- :B(:b), :C(:b) . "
    );
    applyRules();
    // Check with expanding equality.
    m_queryParameters.setString("domain", "IDB");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "
        ":A owl:sameAs :A . "
        ":B owl:sameAs :B . "
        ":C owl:sameAs :C . "
        ":D owl:sameAs :D . "

        ":a owl:sameAs :a . "
        ":b owl:sameAs :b . "
        ":b owl:sameAs :c . "
        ":c owl:sameAs :b . "
        ":c owl:sameAs :c . "

        ":a rdf:type :A . "
        ":b rdf:type :B . "
        ":b rdf:type :C . "
        ":b rdf:type :D . "
        ":c rdf:type :B . "
        ":c rdf:type :C . "
        ":c rdf:type :D . "
    );
    // Check without expanding equality.
    m_queryParameters.setString("domain", "IDBrep");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "
        ":A owl:sameAs :A . "
        ":B owl:sameAs :B . "
        ":C owl:sameAs :C . "
        ":D owl:sameAs :D . "

        ":a owl:sameAs :a . "
        ":b owl:sameAs :b . "

        ":a rdf:type :A . "
        ":b rdf:type :B . "
        ":b rdf:type :C . "
        ":b rdf:type :D . "
    );

    // Now delete some tuples.
    forDeletion(
        ":a rdf:type :A . "
    );
    applyRulesIncrementally();
    // Check with expanding equality.
    m_queryParameters.setString("domain", "IDB");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "
        ":B owl:sameAs :B . "
        ":C owl:sameAs :C . "
        ":D owl:sameAs :D . "

        ":b owl:sameAs :b . "
        ":b owl:sameAs :c . "
        ":c owl:sameAs :b . "
        ":c owl:sameAs :c . "

        ":b rdf:type :B . "
        ":b rdf:type :C . "
        ":b rdf:type :D . "
        ":c rdf:type :B . "
        ":c rdf:type :C . "
        ":c rdf:type :D . "
    );
    // Check without expanding equality.
    m_queryParameters.setString("domain", "IDBrep");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "
        ":B owl:sameAs :B . "
        ":C owl:sameAs :C . "
        ":D owl:sameAs :D . "

        ":b owl:sameAs :b . "

        ":b rdf:type :B . "
        ":b rdf:type :C . "
        ":b rdf:type :D . "
    );
}

TEST(testUpdateConstantsInRules) {
    addTriples(
        ":a rdf:type :C . "
        ":a owl:sameAs :b . "
    );
    addRules(
        ":A(:b) :- :C(?X) . "
    );
    applyRules();
    // Check with expanding equality.
    m_queryParameters.setString("domain", "IDB");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "
        ":A owl:sameAs :A . "
        ":C owl:sameAs :C . "

        ":a owl:sameAs :a . "
        ":a owl:sameAs :b . "
        ":b owl:sameAs :a . "
        ":b owl:sameAs :b . "

        ":a rdf:type :A . "
        ":b rdf:type :A . "
        ":a rdf:type :C . "
        ":b rdf:type :C . "
    );
    // Check without expanding equality.
    m_queryParameters.setString("domain", "IDBrep");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "
        ":A owl:sameAs :A . "
        ":C owl:sameAs :C . "

        ":a owl:sameAs :a . "

        ":a rdf:type :A . "
        ":a rdf:type :C . "
    );

    // Now delete some tuples.
    forDeletion(
        ":a owl:sameAs :b . "
    );
    forAddition(
        ":c rdf:type :C . "
    );
    applyRulesIncrementally();
    // Check with expanding equality.
    m_queryParameters.setString("domain", "IDB");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "
        ":A owl:sameAs :A . "
        ":C owl:sameAs :C . "

        ":a owl:sameAs :a . "
        ":b owl:sameAs :b . "
        ":c owl:sameAs :c . "

        ":b rdf:type :A . "
        ":a rdf:type :C . "
        ":c rdf:type :C . "
    );
    // Check without expanding equality.
    m_queryParameters.setString("domain", "IDBrep");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "
        ":A owl:sameAs :A . "
        ":C owl:sameAs :C . "

        ":a owl:sameAs :a . "
        ":b owl:sameAs :b . "
        ":c owl:sameAs :c . "

        ":b rdf:type :A . "
        ":a rdf:type :C . "
        ":c rdf:type :C . "
    );
}

TEST(testSameAsInRuleBodies) {
    // Only reflexive equalities are derived in this test: equality is used in the rules
    // to determine the universe. Hence, there is no point in checking expanded query results.
    m_queryParameters.setString("domain", "IDBrep");
    addTriples(
        ":a1 rdf:type :A . "
        ":a2 rdf:type :A . "
    );
    addRules(
        ":B(?X) :- owl:sameAs(?X,?X) . "
    );
    applyRules();
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":a1 rdf:type :A . "
        ":a2 rdf:type :A . "

        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "
        ":A owl:sameAs :A . "
        ":B owl:sameAs :B . "
        ":a1 owl:sameAs :a1 . "
        ":a2 owl:sameAs :a2 . "

        "owl:sameAs rdf:type :B . "
        "rdf:type rdf:type :B . "
        ":A rdf:type :B . "
        ":B rdf:type :B . "
        ":a1 rdf:type :B . "
        ":a2 rdf:type :B . "
    );

    // First deletion
    forDeletion(
        ":a1 rdf:type :A . "
    );
    applyRulesIncrementally();
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":a2 rdf:type :A . "

        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "
        ":A owl:sameAs :A . "
        ":B owl:sameAs :B . "
        ":a2 owl:sameAs :a2 . "

        "owl:sameAs rdf:type :B . "
        "rdf:type rdf:type :B . "
        ":A rdf:type :B . "
        ":B rdf:type :B . "
        ":a2 rdf:type :B . "
    );

    // Second deletion
    forDeletion(
        ":a2 rdf:type :A . "
    );
    applyRulesIncrementally();
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ""
    );
}

TEST(testReflexivityInBackwardChaining) {
    addTriples(
        ":a rdf:type :A . "
        ":b2 rdf:type :B . "
        ":c rdf:type :C . "
    );
    addRules(
        "owl:sameAs(:b1,:b2) :- :A(:a) . "
        ":C(:c) :- :B(:b1) . "
    );
    applyRules();
    // Check with expanding equality.
    m_queryParameters.setString("domain", "IDB");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "
        ":A owl:sameAs :A . "
        ":B owl:sameAs :B . "
        ":C owl:sameAs :C . "

        ":a owl:sameAs :a . "
        ":b1 owl:sameAs :b1 . "
        ":b1 owl:sameAs :b2 . "
        ":b2 owl:sameAs :b1 . "
        ":b2 owl:sameAs :b2 . "
        ":c owl:sameAs :c . "

        ":a rdf:type :A . "
        ":b1 rdf:type :B . "
        ":b2 rdf:type :B . "
        ":c rdf:type :C . "
    );
    // Check without expanding equality.
    m_queryParameters.setString("domain", "IDBrep");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "
        ":A owl:sameAs :A . "
        ":B owl:sameAs :B . "
        ":C owl:sameAs :C . "

        ":a owl:sameAs :a . "
        ":b2 owl:sameAs :b2 . "
        ":c owl:sameAs :c . "

        ":a rdf:type :A . "
        ":b2 rdf:type :B . "
        ":c rdf:type :C . "
    );

    // Now delete some tuples.
    forDeletion(
        ":c rdf:type :C . "
    );
    applyRulesIncrementally();
    // Check with expanding equality.
    m_queryParameters.setString("domain", "IDB");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "
        ":A owl:sameAs :A . "
        ":B owl:sameAs :B . "
        ":C owl:sameAs :C . "

        ":a owl:sameAs :a . "
        ":b1 owl:sameAs :b1 . "
        ":b1 owl:sameAs :b2 . "
        ":b2 owl:sameAs :b1 . "
        ":b2 owl:sameAs :b2 . "
        ":c owl:sameAs :c . "

        ":a rdf:type :A . "
        ":b1 rdf:type :B . "
        ":b2 rdf:type :B . "
        ":c rdf:type :C . "
    );
    // Check without expanding equality.
    m_queryParameters.setString("domain", "IDBrep");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "
        ":A owl:sameAs :A . "
        ":B owl:sameAs :B . "
        ":C owl:sameAs :C . "

        ":a owl:sameAs :a . "
        ":b2 owl:sameAs :b2 . "
        ":c owl:sameAs :c . "

        ":a rdf:type :A . "
        ":b2 rdf:type :B . "
        ":c rdf:type :C . "
    );
}

TEST(testPivotlessRules) {
    addTriples(
        ":a rdf:type :A . "
        ":b rdf:type :B . "
        ":a :R :b . "
    );
    addRules(
        "[:a, owl:sameAs, :b] :- . "
        "owl:sameAs(?X,?Y) :- :R(?X,?Y) . "
        ":C(?X) :- :A(?X), :B(?X) . "
    );
    applyRules();
    // Check with expanding equality.
    m_queryParameters.setString("domain", "IDB");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "
        ":R owl:sameAs :R . "
        ":A owl:sameAs :A . "
        ":B owl:sameAs :B . "
        ":C owl:sameAs :C . "

        ":a owl:sameAs :a . "
        ":a owl:sameAs :b . "
        ":b owl:sameAs :a . "
        ":b owl:sameAs :b . "

        ":a :R :a . "
        ":a :R :b . "
        ":b :R :a . "
        ":b :R :b . "

        ":a rdf:type :A . "
        ":a rdf:type :B . "
        ":a rdf:type :C . "

        ":b rdf:type :A . "
        ":b rdf:type :B . "
        ":b rdf:type :C . "
    );
    // Check without expanding equality.
    m_queryParameters.setString("domain", "IDBrep");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "
        ":R owl:sameAs :R . "
        ":A owl:sameAs :A . "
        ":B owl:sameAs :B . "
        ":C owl:sameAs :C . "

        ":a owl:sameAs :a . "

        ":a :R :a . "

        ":a rdf:type :A . "
        ":a rdf:type :B . "
        ":a rdf:type :C . "
    );

    // Now delete some tuples.
    forDeletion(
        ":a :R :b . "
    );
    applyRulesIncrementally();
    // Check with expanding equality.
    m_queryParameters.setString("domain", "IDB");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "
        ":A owl:sameAs :A . "
        ":B owl:sameAs :B . "
        ":C owl:sameAs :C . "

        ":a owl:sameAs :a . "
        ":a owl:sameAs :b . "
        ":b owl:sameAs :a . "
        ":b owl:sameAs :b . "

        ":a rdf:type :A . "
        ":a rdf:type :B . "
        ":a rdf:type :C . "

        ":b rdf:type :A . "
        ":b rdf:type :B . "
        ":b rdf:type :C . "
    );
    // Check without expanding equality.
    m_queryParameters.setString("domain", "IDBrep");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "
        ":A owl:sameAs :A . "
        ":B owl:sameAs :B . "
        ":C owl:sameAs :C . "

        ":a owl:sameAs :a . "

        ":a rdf:type :A . "
        ":a rdf:type :B . "
        ":a rdf:type :C . "
    );

    // Now delete the pivotless rule
    removeRules(
        "[:a, owl:sameAs, :b] :- . "
    );
    applyRulesIncrementally();
    // Check with expanding equality.
    m_queryParameters.setString("domain", "IDB");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "
        ":A owl:sameAs :A . "
        ":B owl:sameAs :B . "

        ":a owl:sameAs :a . "
        ":b owl:sameAs :b . "

        ":a rdf:type :A . "

        ":b rdf:type :B . "
    );
    // Check without expanding equality.
    m_queryParameters.setString("domain", "IDBrep");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "
        ":A owl:sameAs :A . "
        ":B owl:sameAs :B . "

        ":a owl:sameAs :a . "
        ":b owl:sameAs :b . "

        ":a rdf:type :A . "

        ":b rdf:type :B . "
    );
}

TEST(testAddDeleteEquality) {
    addTriples(
        ":a rdf:type :A . "
        ":b rdf:type :A . "
        ":c rdf:type :A . "
        ":b owl:sameAs :c . "
    );
    applyRules();
    // Check with expanding equality.
    m_queryParameters.setString("domain", "IDB");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "
        ":A owl:sameAs :A . "

        ":a rdf:type :A . "
        ":b rdf:type :A . "
        ":c rdf:type :A . "

        ":a owl:sameAs :a . "
        ":b owl:sameAs :b . "
        ":b owl:sameAs :c . "
        ":c owl:sameAs :b . "
        ":c owl:sameAs :c . "
    );
    // Check without expanding equality.
    m_queryParameters.setString("domain", "IDBrep");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "
        ":A owl:sameAs :A . "

        ":a rdf:type :A . "
        ":b rdf:type :A . "

        ":a owl:sameAs :a . "
        ":b owl:sameAs :b . "
    );
    
    // Now delete some tuples.
    forDeletion(
        ":b owl:sameAs :c . "
    );
    forAddition(
        ":a owl:sameAs :b . "
    );
    if (m_useDRed)
        ASSERT_APPLY_RULES_INCREMENTALLY(
            "============================================================\n"
            "  0:    Applying deletion rules to tuples from previous levels\n"
            "  0:    Applying recursive deletion rules\n"
            "  0:        Extracted possibly deleted tuple [ :b, owl:sameAs, :b ]\n"
            "  0:            Applying deletion rules to [ :b, owl:sameAs, :b ]\n"
            "  0:                Propagating replacament of :b to [ :b, owl:sameAs, :b ]    { not added }\n"
            "  0:                Propagating replacament of :b to [ :b, rdf:type, :A ]    { added }\n"
            "  0:                Propagating replacament of :b to [ :b, owl:sameAs, :b ]    { not added }\n"
            "  0:                Derived reflexive tuple [ :b , owl:sameAs, :b ]    { not added }\n"
            "  0:                Derived reflexive tuple [ owl:sameAs , owl:sameAs, owl:sameAs ]    { added }\n"
            "  0:        Extracted possibly deleted tuple [ :b, rdf:type, :A ]\n"
            "  0:            Applying deletion rules to [ :b, rdf:type, :A ]\n"
            "  0:                Derived reflexive tuple [ rdf:type , owl:sameAs, rdf:type ]    { added }\n"
            "  0:                Derived reflexive tuple [ :A , owl:sameAs, :A ]    { added }\n"
            "  0:        Extracted possibly deleted tuple [ owl:sameAs, owl:sameAs, owl:sameAs ]\n"
            "  0:            Applying deletion rules to [ owl:sameAs, owl:sameAs, owl:sameAs ]\n"
            "  0:        Extracted possibly deleted tuple [ rdf:type, owl:sameAs, rdf:type ]\n"
            "  0:            Applying deletion rules to [ rdf:type, owl:sameAs, rdf:type ]\n"
            "  0:        Extracted possibly deleted tuple [ :A, owl:sameAs, :A ]\n"
            "  0:            Applying deletion rules to [ :A, owl:sameAs, :A ]\n"
            "  0:    Updating equality manager\n"
            "  0:    Checking provability of [ :b, owl:sameAs, :b ]    { added to checked }\n"
            "  0:    Checking provability of [ :b, rdf:type, :A ]    { added to checked }\n"
            "  0:    Checking provability of [ owl:sameAs, owl:sameAs, owl:sameAs ]    { added to checked }\n"
            "  0:        Matched reflexive owl:sameAs rule instance [ owl:sameAs, owl:sameAs, owl:sameAs ] :- [ :a, owl:sameAs, :a ] .\n"
            "  0:            Backward chaining stopped because the tuple was proved\n"
            "  0:    Checking provability of [ rdf:type, owl:sameAs, rdf:type ]    { added to checked }\n"
            "  0:        Matched reflexive owl:sameAs rule instance [ rdf:type, owl:sameAs, rdf:type ] :- [ :a, rdf:type, :A ] .\n"
            "  0:            Backward chaining stopped because the tuple was proved\n"
            "  0:    Checking provability of [ :A, owl:sameAs, :A ]    { added to checked }\n"
            "  0:        Matched reflexive owl:sameAs rule instance [ :A, owl:sameAs, :A ] :- [ :a, rdf:type, :A ] .\n"
            "  0:            Backward chaining stopped because the tuple was proved\n"
            "  0:    Updating equality manager\n"
            "  0:    Applying insertion rules to tuples from previous levels\n"
            "  0:    Applying recursive insertion rules\n"
            "  0:        Extracted current tuple [ :a, owl:sameAs, :b ]\n"
            "  0:            Tuple added [ :a, owl:sameAs, :b ]    { added }\n"
            "  0:            Merged :b -> :a    { successful }\n"
            "  0:        Normalizing constant :b\n"
            "  0:            Tuple [ :a, owl:sameAs, :b ] was normalized to [ :a, owl:sameAs, :a ]    { added }\n"
            "  0:        Extracted current tuple [ :b, rdf:type, :A ]\n"
            "  0:            Current tuple normalized to [ :a, rdf:type, :A ]    { added }\n"
            "  0:        Extracted current tuple [ :c, rdf:type, :A ]\n"
            "  0:            Tuple added [ :c, rdf:type, :A ]    { added }\n"
            "  0:            Derived reflexive tuple [ :c , owl:sameAs, :c ]    { added }\n"
            "  0:            Derived reflexive tuple [ rdf:type , owl:sameAs, rdf:type ]    { not added }\n"
            "  0:            Derived reflexive tuple [ :A , owl:sameAs, :A ]    { not added }\n"
            "  0:        Extracted current tuple [ owl:sameAs, owl:sameAs, owl:sameAs ]\n"
            "  0:            Tuple added [ owl:sameAs, owl:sameAs, owl:sameAs ]    { added }\n"
            "  0:            Derived reflexive tuple [ owl:sameAs , owl:sameAs, owl:sameAs ]    { not added }\n"
            "  0:        Extracted current tuple [ rdf:type, owl:sameAs, rdf:type ]\n"
            "  0:            Tuple added [ rdf:type, owl:sameAs, rdf:type ]    { added }\n"
            "  0:        Extracted current tuple [ :A, owl:sameAs, :A ]\n"
            "  0:            Tuple added [ :A, owl:sameAs, :A ]    { added }\n"
            "  0:        Extracted current tuple [ :c, owl:sameAs, :c ]\n"
            "  0:            Tuple added [ :c, owl:sameAs, :c ]    { added }\n"
            "============================================================\n"
            "  0:    Propagating deleted and proved tuples into the store\n"
            "  0:        Tuple deleted [ :b, owl:sameAs, :b ]    { deleted }\n"
            "  0:        Tuple deleted [ :b, rdf:type, :A ]    { deleted }\n"
            "  0:        Tuple added [ :c, rdf:type, :A ]    { added }\n"
            "  0:        Tuple added [ :c, owl:sameAs, :c ]    { added }\n"
        );
    else
        ASSERT_APPLY_RULES_INCREMENTALLY(
            "============================================================\n"
            "  0:    Applying deletion rules to tuples from previous levels\n"
            "  0:    Applying recursive deletion rules\n"
            "  0:        Extracted possibly deleted tuple [ :b, owl:sameAs, :b ]\n"
            "  0:            Checking provability of [ :b, owl:sameAs, :b ]    { added to checked }\n"
            "  0:                Derived reflexive tuple [ :b, owl:sameAs, :b ] directly from EDB    { added }\n"
            "  0:                Derived reflexive tuple [ :c, owl:sameAs, :c ] directly from EDB    { added }\n"
            "  0:                Processing the proved list\n"
            "  0:                    Extracted current tuple [ :b, owl:sameAs, :b ]\n"
            "  0:                        Derived reflexive tuple [ :b , owl:sameAs, :b ]    { not added }\n"
            "  0:                        Delaying reflexive tuple [ owl:sameAs, owl:sameAs, owl:sameAs ]    { added }\n"
            "  0:                    Extracted current tuple [ :c, owl:sameAs, :c ]\n"
            "  0:                        Derived reflexive tuple [ :c , owl:sameAs, :c ]    { not added }\n"
            "  0:                All consequences of reflexivity rules proved for [ :b, owl:sameAs, :b ]\n"
            "  0:                Checking replacement owl:sameAs rule for [ :b, owl:sameAs, :b ] at position 0\n"
            "  0:                    Checking provability of [ :b, owl:sameAs, :b ]    { not added to checked }\n"
            "  0:            Applying deletion rules to [ :b, owl:sameAs, :b ]\n"
            "  0:                Propagating replacament of :b to [ :b, owl:sameAs, :b ]    { not added }\n"
            "  0:                Propagating replacament of :b to [ :b, rdf:type, :A ]    { added }\n"
            "  0:                Propagating replacament of :b to [ :b, owl:sameAs, :b ]    { not added }\n"
            "  0:                Derived reflexive tuple [ :b , owl:sameAs, :b ]    { not added }\n"
            "  0:                Derived reflexive tuple [ owl:sameAs , owl:sameAs, owl:sameAs ]    { added }\n"
            "  0:        Extracted possibly deleted tuple [ :b, rdf:type, :A ]\n"
            "  0:            Checking provability of [ :b, rdf:type, :A ]    { added to checked }\n"
            "  0:                Derived tuple [ :b, rdf:type, :A ]    { from EDB, not from delayed, not from previous level, added }\n"
            "  0:                Derived tuple [ :c, rdf:type, :A ]    { from EDB, not from delayed, not from previous level, added }\n"
            "  0:                Processing the proved list\n"
            "  0:                    Extracted current tuple [ :b, rdf:type, :A ]\n"
            "  0:                        Delaying reflexive tuple [ rdf:type, owl:sameAs, rdf:type ]    { added }\n"
            "  0:                        Delaying reflexive tuple [ :A, owl:sameAs, :A ]    { added }\n"
            "  0:                    Extracted current tuple [ :c, rdf:type, :A ]\n"
            "  0:                Backward chaining stopped because the tuple was proved\n"
            "  0:            Possibly deleted tuple proved\n"
            "  0:        Extracted possibly deleted tuple [ owl:sameAs, owl:sameAs, owl:sameAs ]\n"
            "  0:            Checking provability of [ owl:sameAs, owl:sameAs, owl:sameAs ]    { added to checked }\n"
            "  0:                Derived reflexive tuple [ owl:sameAs, owl:sameAs, owl:sameAs ] directly from EDB    { added }\n"
            "  0:                Derived tuple [ owl:sameAs, owl:sameAs, owl:sameAs ]    { not from EDB, from delayed, not from previous level, not added }\n"
            "  0:                Processing the proved list\n"
            "  0:                    Extracted current tuple [ owl:sameAs, owl:sameAs, owl:sameAs ]\n"
            "  0:                Backward chaining stopped because the tuple was proved\n"
            "  0:            Possibly deleted tuple proved\n"
            "  0:    Updating equality manager\n"
            "  0:        Copying the equivalence class for :b\n"
            "  0:        Copying the equivalence class for owl:sameAs\n"
            "  0:    Applying insertion rules to tuples from previous levels\n"
            "  0:    Applying recursive insertion rules\n"
            "  0:        Extracted current tuple [ :a, owl:sameAs, :b ]\n"
            "  0:            Tuple added [ :a, owl:sameAs, :b ]    { added }\n"
            "  0:            Merged :b -> :a    { successful }\n"
            "  0:        Normalizing constant :b\n"
            "  0:            Tuple [ :b, owl:sameAs, :b ] was normalized to [ :a, owl:sameAs, :a ]    { added }\n"
            "  0:            Tuple [ :b, rdf:type, :A ] was normalized to [ :a, rdf:type, :A ]    { added }\n"
            "  0:            Tuple [ :a, owl:sameAs, :b ] was normalized to [ :a, owl:sameAs, :a ]    { not added }\n"
            "============================================================\n"
            "  0:    Propagating deleted and proved tuples into the store\n"
            "  0:        Tuple deleted [ :b, owl:sameAs, :b ]    { deleted }\n"
            "  0:        Tuple added [ :c, owl:sameAs, :c ]    { added }\n"
            "  0:        Tuple added [ :c, rdf:type, :A ]    { added }\n"
            "  0:        Tuple added [ owl:sameAs, owl:sameAs, owl:sameAs ]    { not added }\n"
        );
    // Check with expanding equality.
    m_queryParameters.setString("domain", "IDB");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "
        ":A owl:sameAs :A . "

        ":a rdf:type :A . "
        ":b rdf:type :A . "
        ":c rdf:type :A . "

        ":a owl:sameAs :a . "
        ":a owl:sameAs :b . "
        ":b owl:sameAs :a . "
        ":b owl:sameAs :b . "
        ":c owl:sameAs :c . "
    );
    // Check without expanding equality.
    m_queryParameters.setString("domain", "IDBrep");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "
        ":A owl:sameAs :A . "

        ":a rdf:type :A . "
        ":c rdf:type :A . "

        ":a owl:sameAs :a . "
        ":c owl:sameAs :c . "
    );
}

TEST(testDeleteAddInterplay) {
    addTriples(
        ":a rdf:type :A . "
        ":b rdf:type :B . "
        ":c rdf:type :C . "
    );
    addRules(
        "owl:sameAs(?X,?Y) :- :R(?X,?Y) . "
        "owl:sameAs(?X,?Y) :- :S(?X,?Y) . "
        ":D(?X) :- :A(?X), :B(?X) . "
        ":E(?X) :- :B(?X), :C(?X) . "
    );
    applyRules();
    // Check with expanding equality.
    m_queryParameters.setString("domain", "IDB");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "
        ":A owl:sameAs :A . "
        ":B owl:sameAs :B . "
        ":C owl:sameAs :C . "

        ":a rdf:type :A . "
        ":b rdf:type :B . "
        ":c rdf:type :C . "

        ":a owl:sameAs :a . "
        ":b owl:sameAs :b . "
        ":c owl:sameAs :c . "
    );
    // Check without expanding equality.
    m_queryParameters.setString("domain", "IDBrep");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "
        ":A owl:sameAs :A . "
        ":B owl:sameAs :B . "
        ":C owl:sameAs :C . "

        ":a rdf:type :A . "
        ":b rdf:type :B . "
        ":c rdf:type :C . "

        ":a owl:sameAs :a . "
        ":b owl:sameAs :b . "
        ":c owl:sameAs :c . "
    );
    
    // Merge :a and :b
    forAddition(
        ":a :R :b . "
    );
    if (m_useDRed)
        ASSERT_APPLY_RULES_INCREMENTALLY(
            "============================================================\n"
            "  0:    Applying deletion rules to tuples from previous levels\n"
            "  0:    Applying recursive deletion rules\n"
            "  0:    Updating equality manager\n"
            "  0:    Updating equality manager\n"
            "  0:    Applying insertion rules to tuples from previous levels\n"
            "  0:    Applying recursive insertion rules\n"
            "  0:        Extracted current tuple [ :a, :R, :b ]\n"
            "  0:            Tuple added [ :a, :R, :b ]    { added }\n"
            "  0:            Derived reflexive tuple [ :a , owl:sameAs, :a ]    { added }\n"
            "  0:            Derived reflexive tuple [ :R , owl:sameAs, :R ]    { added }\n"
            "  0:            Derived reflexive tuple [ :b , owl:sameAs, :b ]    { added }\n"
            "  0:            Matched atom [?X, :R, ?Y] to tuple [ :a, :R, :b ]\n"
            "  0:                Matched rule [?X, owl:sameAs, ?Y] :- [?X, :R, ?Y] .\n"
            "  0:                    Derived tuple [ :a, owl:sameAs, :b ]    { normal, added }\n"
            "  0:                    Merged :b -> :a    { successful }\n"
            "  0:        Normalizing constant :b\n"
            "  0:            Tuple [ :b, owl:sameAs, :b ] was normalized to [ :a, owl:sameAs, :a ]    { not added }\n"
            "  0:            Tuple [ :b, rdf:type, :B ] was normalized to [ :a, rdf:type, :B ]    { added }\n"
            "  0:            Tuple [ :a, :R, :b ] was normalized to [ :a, :R, :a ]    { added }\n"
            "  0:        Extracted current tuple [ :R, owl:sameAs, :R ]\n"
            "  0:            Tuple added [ :R, owl:sameAs, :R ]    { added }\n"
            "  0:            Derived reflexive tuple [ owl:sameAs , owl:sameAs, owl:sameAs ]    { added }\n"
            "  0:        Extracted current tuple [ :b, owl:sameAs, :b ]\n"
            "  0:            Current tuple normalized to [ :a, owl:sameAs, :a ]    { not added }\n"
            "  0:        Extracted current tuple [ :a, owl:sameAs, :b ]\n"
            "  0:            Current tuple normalized to [ :a, owl:sameAs, :a ]    { not added }\n"
            "  0:        Extracted current tuple [ :a, rdf:type, :B ]\n"
            "  0:            Tuple added [ :a, rdf:type, :B ]    { added }\n"
            "  0:            Derived reflexive tuple [ rdf:type , owl:sameAs, rdf:type ]    { added }\n"
            "  0:            Derived reflexive tuple [ :B , owl:sameAs, :B ]    { added }\n"
            "  0:            Matched atom [?X, rdf:type, :B] to tuple [ :a, rdf:type, :B ]\n"
            "  0:                Matching atom [?X, rdf:type, :C]\n"
            "  0:                Matching atom [?X, rdf:type, :A]\n"
            "  0:                    Matched atom [?X, rdf:type, :A] to tuple [ :a, rdf:type, :A ]\n"
            "  0:                        Matched rule [?X, rdf:type, :D] :- [?X, rdf:type, :A], [?X, rdf:type, :B] .\n"
            "  0:                            Derived tuple [ :a, rdf:type, :D ]    { normal, added }\n"
            "  0:        Extracted current tuple [ :a, :R, :a ]\n"
            "  0:            Tuple added [ :a, :R, :a ]    { added }\n"
            "  0:            Matched atom [?X, :R, ?Y] to tuple [ :a, :R, :a ]\n"
            "  0:                Matched rule [?X, owl:sameAs, ?Y] :- [?X, :R, ?Y] .\n"
            "  0:                    Derived tuple [ :a, owl:sameAs, :a ]    { normal, not added }\n"
            "  0:        Extracted current tuple [ :a, rdf:type, :D ]\n"
            "  0:            Tuple added [ :a, rdf:type, :D ]    { added }\n"
            "  0:            Derived reflexive tuple [ :D , owl:sameAs, :D ]    { added }\n"
            "  0:        Extracted current tuple [ :D, owl:sameAs, :D ]\n"
            "  0:            Tuple added [ :D, owl:sameAs, :D ]    { added }\n"
            "============================================================\n"
            "  0:    Propagating deleted and proved tuples into the store\n"
            "  0:        Tuple added [ :R, owl:sameAs, :R ]    { added }\n"
            "  0:        Tuple added [ :a, rdf:type, :B ]    { added }\n"
            "  0:        Tuple added [ :a, :R, :a ]    { added }\n"
            "  0:        Tuple added [ :a, rdf:type, :D ]    { added }\n"
            "  0:        Tuple added [ :D, owl:sameAs, :D ]    { added }\n"
        );
    else
        ASSERT_APPLY_RULES_INCREMENTALLY(
            "============================================================\n"
            "  0:    Applying deletion rules to tuples from previous levels\n"
            "  0:    Applying recursive deletion rules\n"
            "  0:    Updating equality manager\n"
            "  0:    Applying insertion rules to tuples from previous levels\n"
            "  0:    Applying recursive insertion rules\n"
            "  0:        Extracted current tuple [ :a, :R, :b ]\n"
            "  0:            Tuple added [ :a, :R, :b ]    { added }\n"
            "  0:            Derived reflexive tuple [ :a , owl:sameAs, :a ]    { added }\n"
            "  0:            Derived reflexive tuple [ :R , owl:sameAs, :R ]    { added }\n"
            "  0:            Derived reflexive tuple [ :b , owl:sameAs, :b ]    { added }\n"
            "  0:            Matched atom [?X, :R, ?Y] to tuple [ :a, :R, :b ]\n"
            "  0:                Matched rule [?X, owl:sameAs, ?Y] :- [?X, :R, ?Y] .\n"
            "  0:                    Derived tuple [ :a, owl:sameAs, :b ]    { normal, added }\n"
            "  0:                    Merged :b -> :a    { successful }\n"
            "  0:        Normalizing constant :b\n"
            "  0:            Tuple [ :b, owl:sameAs, :b ] was normalized to [ :a, owl:sameAs, :a ]    { not added }\n"
            "  0:            Tuple [ :b, rdf:type, :B ] was normalized to [ :a, rdf:type, :B ]    { added }\n"
            "  0:            Tuple [ :a, :R, :b ] was normalized to [ :a, :R, :a ]    { added }\n"
            "  0:        Extracted current tuple [ :R, owl:sameAs, :R ]\n"
            "  0:            Tuple added [ :R, owl:sameAs, :R ]    { added }\n"
            "  0:            Derived reflexive tuple [ owl:sameAs , owl:sameAs, owl:sameAs ]    { added }\n"
            "  0:        Extracted current tuple [ :b, owl:sameAs, :b ]\n"
            "  0:            Current tuple normalized to [ :a, owl:sameAs, :a ]    { not added }\n"
            "  0:        Extracted current tuple [ :a, owl:sameAs, :b ]\n"
            "  0:            Current tuple normalized to [ :a, owl:sameAs, :a ]    { not added }\n"
            "  0:        Extracted current tuple [ :a, rdf:type, :B ]\n"
            "  0:            Tuple added [ :a, rdf:type, :B ]    { added }\n"
            "  0:            Derived reflexive tuple [ rdf:type , owl:sameAs, rdf:type ]    { added }\n"
            "  0:            Derived reflexive tuple [ :B , owl:sameAs, :B ]    { added }\n"
            "  0:            Matched atom [?X, rdf:type, :B] to tuple [ :a, rdf:type, :B ]\n"
            "  0:                Matching atom [?X, rdf:type, :C]\n"
            "  0:                Matching atom [?X, rdf:type, :A]\n"
            "  0:                    Matched atom [?X, rdf:type, :A] to tuple [ :a, rdf:type, :A ]\n"
            "  0:                        Matched rule [?X, rdf:type, :D] :- [?X, rdf:type, :A], [?X, rdf:type, :B] .\n"
            "  0:                            Derived tuple [ :a, rdf:type, :D ]    { normal, added }\n"
            "  0:        Extracted current tuple [ :a, :R, :a ]\n"
            "  0:            Tuple added [ :a, :R, :a ]    { added }\n"
            "  0:            Matched atom [?X, :R, ?Y] to tuple [ :a, :R, :a ]\n"
            "  0:                Matched rule [?X, owl:sameAs, ?Y] :- [?X, :R, ?Y] .\n"
            "  0:                    Derived tuple [ :a, owl:sameAs, :a ]    { normal, not added }\n"
            "  0:        Extracted current tuple [ :a, rdf:type, :D ]\n"
            "  0:            Tuple added [ :a, rdf:type, :D ]    { added }\n"
            "  0:            Derived reflexive tuple [ :D , owl:sameAs, :D ]    { added }\n"
            "  0:        Extracted current tuple [ :D, owl:sameAs, :D ]\n"
            "  0:            Tuple added [ :D, owl:sameAs, :D ]    { added }\n"
            "============================================================\n"
            "  0:    Propagating deleted and proved tuples into the store\n"
            "  0:        Tuple added [ :R, owl:sameAs, :R ]    { added }\n"
            "  0:        Tuple added [ :a, rdf:type, :B ]    { added }\n"
            "  0:        Tuple added [ :a, :R, :a ]    { added }\n"
            "  0:        Tuple added [ :a, rdf:type, :D ]    { added }\n"
            "  0:        Tuple added [ :D, owl:sameAs, :D ]    { added }\n"
        );
    // Check with expanding equality.
    m_queryParameters.setString("domain", "IDB");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "
        ":A owl:sameAs :A . "
        ":B owl:sameAs :B . "
        ":C owl:sameAs :C . "
        ":D owl:sameAs :D . "
        ":R owl:sameAs :R . "

        ":a rdf:type :A . "
        ":a rdf:type :B . "
        ":a rdf:type :D . "
        ":b rdf:type :A . "
        ":b rdf:type :B . "
        ":b rdf:type :D . "
        ":c rdf:type :C . "

        ":a :R :a . "
        ":a :R :b . "
        ":b :R :a . "
        ":b :R :b . "

        ":a owl:sameAs :a . "
        ":a owl:sameAs :b . "
        ":b owl:sameAs :a . "
        ":b owl:sameAs :b . "
        ":c owl:sameAs :c . "
    );
    // Check without expanding equality.
    m_queryParameters.setString("domain", "IDBrep");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "
        ":A owl:sameAs :A . "
        ":B owl:sameAs :B . "
        ":C owl:sameAs :C . "
        ":D owl:sameAs :D . "
        ":R owl:sameAs :R . "

        ":a rdf:type :A . "
        ":a rdf:type :B . "
        ":a rdf:type :D . "
        ":c rdf:type :C . "

        ":a :R :a . "

        ":a owl:sameAs :a . "
        ":c owl:sameAs :c . "
    );
    
    // Now umerge :a and :b, but merge :b and :c.
    forDeletion(
        ":a :R :b . "
    );
    forAddition(
        ":b :S :c . "
    );
    if (m_useDRed)
        ASSERT_APPLY_RULES_INCREMENTALLY(
            "============================================================\n"
            "  0:    Applying deletion rules to tuples from previous levels\n"
            "  0:    Applying recursive deletion rules\n"
            "  0:        Extracted possibly deleted tuple [ :a, :R, :a ]\n"
            "  0:            Applying deletion rules to [ :a, :R, :a ]\n"
            "  0:                Derived reflexive tuple [ :a , owl:sameAs, :a ]    { added }\n"
            "  0:                Derived reflexive tuple [ :R , owl:sameAs, :R ]    { added }\n"
            "  0:                Matched atom [?X, :R, ?Y] to tuple [ :a, :R, :a ]\n"
            "  0:                    Matched rule [?X, owl:sameAs, ?Y] :- [?X, :R, ?Y] .\n"
            "  0:                        Derived tuple [ :a, owl:sameAs, :a ]    { normal, not added }\n"
            "  0:        Extracted possibly deleted tuple [ :a, owl:sameAs, :a ]\n"
            "  0:            Applying deletion rules to [ :a, owl:sameAs, :a ]\n"
            "  0:                Propagating replacament of :a to [ :a, :R, :a ]    { not added }\n"
            "  0:                Propagating replacament of :a to [ :a, owl:sameAs, :a ]    { not added }\n"
            "  0:                Propagating replacament of :a to [ :a, rdf:type, :A ]    { added }\n"
            "  0:                Propagating replacament of :a to [ :a, rdf:type, :D ]    { added }\n"
            "  0:                Propagating replacament of :a to [ :a, rdf:type, :B ]    { added }\n"
            "  0:                Propagating replacament of :a to [ :a, :R, :a ]    { not added }\n"
            "  0:                Propagating replacament of :a to [ :a, owl:sameAs, :a ]    { not added }\n"
            "  0:                Derived reflexive tuple [ owl:sameAs , owl:sameAs, owl:sameAs ]    { added }\n"
            "  0:        Extracted possibly deleted tuple [ :R, owl:sameAs, :R ]\n"
            "  0:            Applying deletion rules to [ :R, owl:sameAs, :R ]\n"
            "  0:        Extracted possibly deleted tuple [ :a, rdf:type, :A ]\n"
            "  0:            Applying deletion rules to [ :a, rdf:type, :A ]\n"
            "  0:                Derived reflexive tuple [ rdf:type , owl:sameAs, rdf:type ]    { added }\n"
            "  0:                Derived reflexive tuple [ :A , owl:sameAs, :A ]    { added }\n"
            "  0:                Matched atom [?X, rdf:type, :A] to tuple [ :a, rdf:type, :A ]\n"
            "  0:                    Matching atom [?X, rdf:type, :B]\n"
            "  0:                        Matched atom [?X, rdf:type, :B] to tuple [ :a, rdf:type, :B ]\n"
            "  0:                            Matched rule [?X, rdf:type, :D] :- [?X, rdf:type, :A], [?X, rdf:type, :B] .\n"
            "  0:                                Derived tuple [ :a, rdf:type, :D ]    { normal, not added }\n"
            "  0:        Extracted possibly deleted tuple [ :a, rdf:type, :D ]\n"
            "  0:            Applying deletion rules to [ :a, rdf:type, :D ]\n"
            "  0:                Derived reflexive tuple [ :D , owl:sameAs, :D ]    { added }\n"
            "  0:        Extracted possibly deleted tuple [ :a, rdf:type, :B ]\n"
            "  0:            Applying deletion rules to [ :a, rdf:type, :B ]\n"
            "  0:                Derived reflexive tuple [ :B , owl:sameAs, :B ]    { added }\n"
            "  0:                Matched atom [?X, rdf:type, :B] to tuple [ :a, rdf:type, :B ]\n"
            "  0:                    Matching atom [?X, rdf:type, :C]\n"
            "  0:                    Matching atom [?X, rdf:type, :A]\n"
            "  0:        Extracted possibly deleted tuple [ owl:sameAs, owl:sameAs, owl:sameAs ]\n"
            "  0:            Applying deletion rules to [ owl:sameAs, owl:sameAs, owl:sameAs ]\n"
            "  0:        Extracted possibly deleted tuple [ rdf:type, owl:sameAs, rdf:type ]\n"
            "  0:            Applying deletion rules to [ rdf:type, owl:sameAs, rdf:type ]\n"
            "  0:        Extracted possibly deleted tuple [ :A, owl:sameAs, :A ]\n"
            "  0:            Applying deletion rules to [ :A, owl:sameAs, :A ]\n"
            "  0:        Extracted possibly deleted tuple [ :D, owl:sameAs, :D ]\n"
            "  0:            Applying deletion rules to [ :D, owl:sameAs, :D ]\n"
            "  0:        Extracted possibly deleted tuple [ :B, owl:sameAs, :B ]\n"
            "  0:            Applying deletion rules to [ :B, owl:sameAs, :B ]\n"
            "  0:    Updating equality manager\n"
            "  0:    Checking provability of [ :a, :R, :a ]    { added to checked }\n"
            "  0:    Checking provability of [ :a, owl:sameAs, :a ]    { added to checked }\n"
            "  0:        Checking recursive rule [?X, owl:sameAs, ?Y] :- [?X, :S, ?Y] .\n"
            "  0:        Checking recursive rule [?X, owl:sameAs, ?Y] :- [?X, :R, ?Y] .\n"
            "  0:        Checking recursive rule [?X, owl:sameAs, ?Y] :- [?X, :S, ?Y] .\n"
            "  0:        Checking recursive rule [?X, owl:sameAs, ?Y] :- [?X, :R, ?Y] .\n"
            "  0:        Checking recursive rule [?X, owl:sameAs, ?Y] :- [?X, :S, ?Y] .\n"
            "  0:        Checking recursive rule [?X, owl:sameAs, ?Y] :- [?X, :R, ?Y] .\n"
            "  0:        Checking recursive rule [?X, owl:sameAs, ?Y] :- [?X, :S, ?Y] .\n"
            "  0:        Checking recursive rule [?X, owl:sameAs, ?Y] :- [?X, :R, ?Y] .\n"
            "  0:    Checking provability of [ :R, owl:sameAs, :R ]    { added to checked }\n"
            "  0:        Checking recursive rule [?X, owl:sameAs, ?Y] :- [?X, :S, ?Y] .\n"
            "  0:        Checking recursive rule [?X, owl:sameAs, ?Y] :- [?X, :R, ?Y] .\n"
            "  0:    Checking provability of [ :a, rdf:type, :A ]    { added to checked }\n"
            "  0:    Checking provability of [ :a, rdf:type, :D ]    { added to checked }\n"
            "  0:        Checking recursive rule [?X, rdf:type, :D] :- [?X, rdf:type, :A], [?X, rdf:type, :B] .\n"
            "  0:        Checking recursive rule [?X, rdf:type, :D] :- [?X, rdf:type, :A], [?X, rdf:type, :B] .\n"
            "  0:    Checking provability of [ :a, rdf:type, :B ]    { added to checked }\n"
            "  0:    Checking provability of [ owl:sameAs, owl:sameAs, owl:sameAs ]    { added to checked }\n"
            "  0:        Checking recursive rule [?X, owl:sameAs, ?Y] :- [?X, :S, ?Y] .\n"
            "  0:        Checking recursive rule [?X, owl:sameAs, ?Y] :- [?X, :R, ?Y] .\n"
            "  0:        Matched reflexive owl:sameAs rule instance [ owl:sameAs, owl:sameAs, owl:sameAs ] :- [ :C, owl:sameAs, :C ] .\n"
            "  0:            Backward chaining stopped because the tuple was proved\n"
            "  0:    Checking provability of [ rdf:type, owl:sameAs, rdf:type ]    { added to checked }\n"
            "  0:        Checking recursive rule [?X, owl:sameAs, ?Y] :- [?X, :S, ?Y] .\n"
            "  0:        Checking recursive rule [?X, owl:sameAs, ?Y] :- [?X, :R, ?Y] .\n"
            "  0:        Matched reflexive owl:sameAs rule instance [ rdf:type, owl:sameAs, rdf:type ] :- [ :c, rdf:type, :C ] .\n"
            "  0:            Backward chaining stopped because the tuple was proved\n"
            "  0:    Checking provability of [ :A, owl:sameAs, :A ]    { added to checked }\n"
            "  0:        Checking recursive rule [?X, owl:sameAs, ?Y] :- [?X, :S, ?Y] .\n"
            "  0:        Checking recursive rule [?X, owl:sameAs, ?Y] :- [?X, :R, ?Y] .\n"
            "  0:    Checking provability of [ :D, owl:sameAs, :D ]    { added to checked }\n"
            "  0:        Checking recursive rule [?X, owl:sameAs, ?Y] :- [?X, :S, ?Y] .\n"
            "  0:        Checking recursive rule [?X, owl:sameAs, ?Y] :- [?X, :R, ?Y] .\n"
            "  0:    Checking provability of [ :B, owl:sameAs, :B ]    { added to checked }\n"
            "  0:        Checking recursive rule [?X, owl:sameAs, ?Y] :- [?X, :S, ?Y] .\n"
            "  0:        Checking recursive rule [?X, owl:sameAs, ?Y] :- [?X, :R, ?Y] .\n"
            "  0:    Updating equality manager\n"
            "  0:    Applying insertion rules to tuples from previous levels\n"
            "  0:    Applying recursive insertion rules\n"
            "  0:        Extracted current tuple [ :b, :S, :c ]\n"
            "  0:            Tuple added [ :b, :S, :c ]    { added }\n"
            "  0:            Derived reflexive tuple [ :b , owl:sameAs, :b ]    { added }\n"
            "  0:            Derived reflexive tuple [ :S , owl:sameAs, :S ]    { added }\n"
            "  0:            Derived reflexive tuple [ :c , owl:sameAs, :c ]    { added }\n"
            "  0:            Matched atom [?X, :S, ?Y] to tuple [ :b, :S, :c ]\n"
            "  0:                Matched rule [?X, owl:sameAs, ?Y] :- [?X, :S, ?Y] .\n"
            "  0:                    Derived tuple [ :b, owl:sameAs, :c ]    { normal, added }\n"
            "  0:                    Merged :c -> :b    { successful }\n"
            "  0:        Normalizing constant :c\n"
            "  0:            Tuple [ :c, owl:sameAs, :c ] was normalized to [ :b, owl:sameAs, :b ]    { not added }\n"
            "  0:            Tuple [ :c, rdf:type, :C ] was normalized to [ :b, rdf:type, :C ]    { added }\n"
            "  0:            Tuple [ :b, :S, :c ] was normalized to [ :b, :S, :b ]    { added }\n"
            "  0:        Extracted current tuple [ :a, rdf:type, :A ]\n"
            "  0:            Tuple added [ :a, rdf:type, :A ]    { added }\n"
            "  0:            Derived reflexive tuple [ :a , owl:sameAs, :a ]    { added }\n"
            "  0:            Derived reflexive tuple [ rdf:type , owl:sameAs, rdf:type ]    { not added }\n"
            "  0:            Derived reflexive tuple [ :A , owl:sameAs, :A ]    { added }\n"
            "  0:            Matched atom [?X, rdf:type, :A] to tuple [ :a, rdf:type, :A ]\n"
            "  0:                Matching atom [?X, rdf:type, :B]\n"
            "  0:        Extracted current tuple [ :b, rdf:type, :B ]\n"
            "  0:            Tuple added [ :b, rdf:type, :B ]    { added }\n"
            "  0:            Derived reflexive tuple [ :B , owl:sameAs, :B ]    { added }\n"
            "  0:            Matched atom [?X, rdf:type, :B] to tuple [ :b, rdf:type, :B ]\n"
            "  0:                Matching atom [?X, rdf:type, :C]\n"
            "  0:                Matching atom [?X, rdf:type, :A]\n"
            "  0:        Extracted current tuple [ owl:sameAs, owl:sameAs, owl:sameAs ]\n"
            "  0:            Tuple added [ owl:sameAs, owl:sameAs, owl:sameAs ]    { added }\n"
            "  0:            Derived reflexive tuple [ owl:sameAs , owl:sameAs, owl:sameAs ]    { not added }\n"
            "  0:        Extracted current tuple [ rdf:type, owl:sameAs, rdf:type ]\n"
            "  0:            Tuple added [ rdf:type, owl:sameAs, rdf:type ]    { added }\n"
            "  0:        Extracted current tuple [ :b, owl:sameAs, :b ]\n"
            "  0:            Tuple added [ :b, owl:sameAs, :b ]    { added }\n"
            "  0:        Extracted current tuple [ :S, owl:sameAs, :S ]\n"
            "  0:            Tuple added [ :S, owl:sameAs, :S ]    { added }\n"
            "  0:        Extracted current tuple [ :c, owl:sameAs, :c ]\n"
            "  0:            Current tuple normalized to [ :b, owl:sameAs, :b ]    { not added }\n"
            "  0:        Extracted current tuple [ :b, owl:sameAs, :c ]\n"
            "  0:            Current tuple normalized to [ :b, owl:sameAs, :b ]    { not added }\n"
            "  0:        Extracted current tuple [ :b, rdf:type, :C ]\n"
            "  0:            Tuple added [ :b, rdf:type, :C ]    { added }\n"
            "  0:            Derived reflexive tuple [ :C , owl:sameAs, :C ]    { added }\n"
            "  0:            Matched atom [?X, rdf:type, :C] to tuple [ :b, rdf:type, :C ]\n"
            "  0:                Matching atom [?X, rdf:type, :B]\n"
            "  0:                    Matched atom [?X, rdf:type, :B] to tuple [ :b, rdf:type, :B ]\n"
            "  0:                        Matched rule [?X, rdf:type, :E] :- [?X, rdf:type, :B], [?X, rdf:type, :C] .\n"
            "  0:                            Derived tuple [ :b, rdf:type, :E ]    { normal, added }\n"
            "  0:        Extracted current tuple [ :b, :S, :b ]\n"
            "  0:            Tuple added [ :b, :S, :b ]    { added }\n"
            "  0:            Matched atom [?X, :S, ?Y] to tuple [ :b, :S, :b ]\n"
            "  0:                Matched rule [?X, owl:sameAs, ?Y] :- [?X, :S, ?Y] .\n"
            "  0:                    Derived tuple [ :b, owl:sameAs, :b ]    { normal, not added }\n"
            "  0:        Extracted current tuple [ :a, owl:sameAs, :a ]\n"
            "  0:            Tuple added [ :a, owl:sameAs, :a ]    { added }\n"
            "  0:        Extracted current tuple [ :A, owl:sameAs, :A ]\n"
            "  0:            Tuple added [ :A, owl:sameAs, :A ]    { added }\n"
            "  0:        Extracted current tuple [ :B, owl:sameAs, :B ]\n"
            "  0:            Tuple added [ :B, owl:sameAs, :B ]    { added }\n"
            "  0:        Extracted current tuple [ :b, rdf:type, :E ]\n"
            "  0:            Tuple added [ :b, rdf:type, :E ]    { added }\n"
            "  0:            Derived reflexive tuple [ :E , owl:sameAs, :E ]    { added }\n"
            "  0:        Extracted current tuple [ :E, owl:sameAs, :E ]\n"
            "  0:            Tuple added [ :E, owl:sameAs, :E ]    { added }\n"
            "============================================================\n"
            "  0:    Propagating deleted and proved tuples into the store\n"
            "  0:        Tuple deleted [ :a, :R, :a ]    { deleted }\n"
            "  0:        Tuple deleted [ :R, owl:sameAs, :R ]    { deleted }\n"
            "  0:        Tuple deleted [ :a, rdf:type, :D ]    { deleted }\n"
            "  0:        Tuple deleted [ :a, rdf:type, :B ]    { deleted }\n"
            "  0:        Tuple deleted [ :D, owl:sameAs, :D ]    { deleted }\n"
            "  0:        Tuple added [ :b, rdf:type, :B ]    { added }\n"
            "  0:        Tuple added [ :b, owl:sameAs, :b ]    { added }\n"
            "  0:        Tuple added [ :S, owl:sameAs, :S ]    { added }\n"
            "  0:        Tuple added [ :b, rdf:type, :C ]    { added }\n"
            "  0:        Tuple added [ :b, :S, :b ]    { added }\n"
            "  0:        Tuple added [ :b, rdf:type, :E ]    { added }\n"
            "  0:        Tuple added [ :E, owl:sameAs, :E ]    { added }\n"
        );
    else
        ASSERT_APPLY_RULES_INCREMENTALLY(
            "============================================================\n"
            "  0:    Applying deletion rules to tuples from previous levels\n"
            "  0:    Applying recursive deletion rules\n"
            "  0:        Extracted possibly deleted tuple [ :a, :R, :a ]\n"
            "  0:            Checking provability of [ :a, :R, :a ]    { added to checked }\n"
            "  0:                Checking replacement owl:sameAs rule for [ :a, owl:sameAs, :a ] at position 0\n"
            "  0:                    Checking provability of [ :a, owl:sameAs, :a ]    { added to checked }\n"
            "  0:                        Derived reflexive tuple [ :a, owl:sameAs, :a ] directly from EDB    { added }\n"
            "  0:                        Derived reflexive tuple [ :b, owl:sameAs, :b ] directly from EDB    { added }\n"
            "  0:                        Processing the proved list\n"
            "  0:                            Extracted current tuple [ :a, owl:sameAs, :a ]\n"
            "  0:                                Derived reflexive tuple [ :a , owl:sameAs, :a ]    { not added }\n"
            "  0:                                Delaying reflexive tuple [ owl:sameAs, owl:sameAs, owl:sameAs ]    { added }\n"
            "  0:                            Extracted current tuple [ :b, owl:sameAs, :b ]\n"
            "  0:                                Derived reflexive tuple [ :b , owl:sameAs, :b ]    { not added }\n"
            "  0:                        All consequences of reflexivity rules proved for [ :a, owl:sameAs, :a ]\n"
            "  0:                        Checking recursive rule [?X, owl:sameAs, ?Y] :- [?X, :S, ?Y] .\n"
            "  0:                        Checking recursive rule [?X, owl:sameAs, ?Y] :- [?X, :R, ?Y] .\n"
            "  0:                            Matched recursive rule instance [ :a, owl:sameAs, :a ] :- [ :a, :R, :a ]* .\n"
            "  0:                                Checking body atom [ :a, :R, :a ]\n"
            "  0:                                    Checking provability of [ :a, :R, :a ]    { not added to checked }\n"
            "  0:            Tuple disproved [ :a, :R, :a ]    { added }\n"
            "  0:            Applying deletion rules to [ :a, :R, :a ]\n"
            "  0:                Derived reflexive tuple [ :a , owl:sameAs, :a ]    { added }\n"
            "  0:                Derived reflexive tuple [ :R , owl:sameAs, :R ]    { added }\n"
            "  0:                Matched atom [?X, :R, ?Y] to tuple [ :a, :R, :a ]\n"
            "  0:                    Matched rule [?X, owl:sameAs, ?Y] :- [?X, :R, ?Y] .\n"
            "  0:                        Derived tuple [ :a, owl:sameAs, :a ]    { normal, not added }\n"
            "  0:        Extracted possibly deleted tuple [ :a, owl:sameAs, :a ]\n"
            "  0:            Checking provability of [ :a, owl:sameAs, :a ]    { not added to checked }\n"
            "  0:            Applying deletion rules to [ :a, owl:sameAs, :a ]\n"
            "  0:                Propagating replacament of :a to [ :a, :R, :a ]    { not added }\n"
            "  0:                Propagating replacament of :a to [ :a, owl:sameAs, :a ]    { not added }\n"
            "  0:                Propagating replacament of :a to [ :a, rdf:type, :A ]    { added }\n"
            "  0:                Propagating replacament of :a to [ :a, rdf:type, :D ]    { added }\n"
            "  0:                Propagating replacament of :a to [ :a, rdf:type, :B ]    { added }\n"
            "  0:                Propagating replacament of :a to [ :a, :R, :a ]    { not added }\n"
            "  0:                Propagating replacament of :a to [ :a, owl:sameAs, :a ]    { not added }\n"
            "  0:                Derived reflexive tuple [ owl:sameAs , owl:sameAs, owl:sameAs ]    { added }\n"
            "  0:        Extracted possibly deleted tuple [ :R, owl:sameAs, :R ]\n"
            "  0:            Checking provability of [ :R, owl:sameAs, :R ]    { added to checked }\n"
            "  0:                Matched reflexive owl:sameAs rule instance [ :R, owl:sameAs, :R ] :- [ :R, owl:sameAs, :R ] .\n"
            "  0:                    Checking provability of [ :R, owl:sameAs, :R ]    { not added to checked }\n"
            "  0:                Matched reflexive owl:sameAs rule instance [ :R, owl:sameAs, :R ] :- [ :R, owl:sameAs, :R ] .\n"
            "  0:                    Checking provability of [ :R, owl:sameAs, :R ]    { not added to checked }\n"
            "  0:                Checking recursive rule [?X, owl:sameAs, ?Y] :- [?X, :S, ?Y] .\n"
            "  0:                Checking recursive rule [?X, owl:sameAs, ?Y] :- [?X, :R, ?Y] .\n"
            "  0:            Tuple disproved [ :R, owl:sameAs, :R ]    { added }\n"
            "  0:            Applying deletion rules to [ :R, owl:sameAs, :R ]\n"
            "  0:        Extracted possibly deleted tuple [ :a, rdf:type, :A ]\n"
            "  0:            Checking provability of [ :a, rdf:type, :A ]    { added to checked }\n"
            "  0:                Derived tuple [ :a, rdf:type, :A ]    { from EDB, not from delayed, not from previous level, added }\n"
            "  0:                Processing the proved list\n"
            "  0:                    Extracted current tuple [ :a, rdf:type, :A ]\n"
            "  0:                        Delaying reflexive tuple [ rdf:type, owl:sameAs, rdf:type ]    { added }\n"
            "  0:                        Delaying reflexive tuple [ :A, owl:sameAs, :A ]    { added }\n"
            "  0:                        Matched atom [?X, rdf:type, :A] to tuple [ :a, rdf:type, :A ]\n"
            "  0:                            Matching atom [?X, rdf:type, :B]\n"
            "  0:            Applying deletion rules to [ :a, rdf:type, :A ]\n"
            "  0:                Derived reflexive tuple [ rdf:type , owl:sameAs, rdf:type ]    { added }\n"
            "  0:                Derived reflexive tuple [ :A , owl:sameAs, :A ]    { added }\n"
            "  0:                Matched atom [?X, rdf:type, :A] to tuple [ :a, rdf:type, :A ]\n"
            "  0:                    Matching atom [?X, rdf:type, :B]\n"
            "  0:                        Matched atom [?X, rdf:type, :B] to tuple [ :a, rdf:type, :B ]\n"
            "  0:                            Matched rule [?X, rdf:type, :D] :- [?X, rdf:type, :A], [?X, rdf:type, :B] .\n"
            "  0:                                Derived tuple [ :a, rdf:type, :D ]    { normal, not added }\n"
            "  0:        Extracted possibly deleted tuple [ :a, rdf:type, :D ]\n"
            "  0:            Checking provability of [ :a, rdf:type, :D ]    { added to checked }\n"
            "  0:                Checking recursive rule [?X, rdf:type, :D] :- [?X, rdf:type, :A], [?X, rdf:type, :B] .\n"
            "  0:                    Matched recursive rule instance [ :a, rdf:type, :D ] :- [ :a, rdf:type, :A ]*, [ :a, rdf:type, :B ]* .\n"
            "  0:                        Checking body atom [ :a, rdf:type, :A ]\n"
            "  0:                            Checking provability of [ :a, rdf:type, :A ]    { not added to checked }\n"
            "  0:                        Checking body atom [ :a, rdf:type, :B ]\n"
            "  0:                            Checking provability of [ :a, rdf:type, :B ]    { added to checked }\n"
            "  0:                                Derived tuple [ :b, rdf:type, :B ]    { from EDB, not from delayed, not from previous level, added }\n"
            "  0:                                Processing the proved list\n"
            "  0:                                    Extracted current tuple [ :b, rdf:type, :B ]\n"
            "  0:                                        Delaying reflexive tuple [ :B, owl:sameAs, :B ]    { added }\n"
            "  0:                                        Matched atom [?X, rdf:type, :B] to tuple [ :a, rdf:type, :B ]\n"
            "  0:                                            Matching atom [?X, rdf:type, :C]\n"
            "  0:                                            Matching atom [?X, rdf:type, :A]\n"
            "  0:            Tuple disproved [ :a, rdf:type, :D ]    { added }\n"
            "  0:            Applying deletion rules to [ :a, rdf:type, :D ]\n"
            "  0:                Derived reflexive tuple [ :D , owl:sameAs, :D ]    { added }\n"
            "  0:        Extracted possibly deleted tuple [ :a, rdf:type, :B ]\n"
            "  0:            Checking provability of [ :a, rdf:type, :B ]    { not added to checked }\n"
            "  0:            Applying deletion rules to [ :a, rdf:type, :B ]\n"
            "  0:                Derived reflexive tuple [ :B , owl:sameAs, :B ]    { added }\n"
            "  0:                Matched atom [?X, rdf:type, :B] to tuple [ :a, rdf:type, :B ]\n"
            "  0:                    Matching atom [?X, rdf:type, :C]\n"
            "  0:                    Matching atom [?X, rdf:type, :A]\n"
            "  0:        Extracted possibly deleted tuple [ owl:sameAs, owl:sameAs, owl:sameAs ]\n"
            "  0:            Checking provability of [ owl:sameAs, owl:sameAs, owl:sameAs ]    { added to checked }\n"
            "  0:                Derived tuple [ owl:sameAs, owl:sameAs, owl:sameAs ]    { not from EDB, from delayed, not from previous level, added }\n"
            "  0:                Processing the proved list\n"
            "  0:                    Extracted current tuple [ owl:sameAs, owl:sameAs, owl:sameAs ]\n"
            "  0:                Backward chaining stopped because the tuple was proved\n"
            "  0:            Possibly deleted tuple proved\n"
            "  0:        Extracted possibly deleted tuple [ rdf:type, owl:sameAs, rdf:type ]\n"
            "  0:            Checking provability of [ rdf:type, owl:sameAs, rdf:type ]    { added to checked }\n"
            "  0:                Derived reflexive tuple [ rdf:type, owl:sameAs, rdf:type ] directly from EDB    { added }\n"
            "  0:                Derived tuple [ rdf:type, owl:sameAs, rdf:type ]    { not from EDB, from delayed, not from previous level, not added }\n"
            "  0:                Processing the proved list\n"
            "  0:                    Extracted current tuple [ rdf:type, owl:sameAs, rdf:type ]\n"
            "  0:                Backward chaining stopped because the tuple was proved\n"
            "  0:            Possibly deleted tuple proved\n"
            "  0:        Extracted possibly deleted tuple [ :A, owl:sameAs, :A ]\n"
            "  0:            Checking provability of [ :A, owl:sameAs, :A ]    { added to checked }\n"
            "  0:                Derived reflexive tuple [ :A, owl:sameAs, :A ] directly from EDB    { added }\n"
            "  0:                Derived tuple [ :A, owl:sameAs, :A ]    { not from EDB, from delayed, not from previous level, not added }\n"
            "  0:                Processing the proved list\n"
            "  0:                    Extracted current tuple [ :A, owl:sameAs, :A ]\n"
            "  0:                Backward chaining stopped because the tuple was proved\n"
            "  0:            Possibly deleted tuple proved\n"
            "  0:        Extracted possibly deleted tuple [ :D, owl:sameAs, :D ]\n"
            "  0:            Checking provability of [ :D, owl:sameAs, :D ]    { added to checked }\n"
            "  0:                Matched reflexive owl:sameAs rule instance [ :D, owl:sameAs, :D ] :- [ :D, owl:sameAs, :D ] .\n"
            "  0:                    Checking provability of [ :D, owl:sameAs, :D ]    { not added to checked }\n"
            "  0:                Matched reflexive owl:sameAs rule instance [ :D, owl:sameAs, :D ] :- [ :D, owl:sameAs, :D ] .\n"
            "  0:                    Checking provability of [ :D, owl:sameAs, :D ]    { not added to checked }\n"
            "  0:                Checking recursive rule [?X, owl:sameAs, ?Y] :- [?X, :S, ?Y] .\n"
            "  0:                Checking recursive rule [?X, owl:sameAs, ?Y] :- [?X, :R, ?Y] .\n"
            "  0:            Tuple disproved [ :D, owl:sameAs, :D ]    { added }\n"
            "  0:            Applying deletion rules to [ :D, owl:sameAs, :D ]\n"
            "  0:        Extracted possibly deleted tuple [ :B, owl:sameAs, :B ]\n"
            "  0:            Checking provability of [ :B, owl:sameAs, :B ]    { added to checked }\n"
            "  0:                Derived reflexive tuple [ :B, owl:sameAs, :B ] directly from EDB    { added }\n"
            "  0:                Derived tuple [ :B, owl:sameAs, :B ]    { not from EDB, from delayed, not from previous level, not added }\n"
            "  0:                Processing the proved list\n"
            "  0:                    Extracted current tuple [ :B, owl:sameAs, :B ]\n"
            "  0:                Backward chaining stopped because the tuple was proved\n"
            "  0:            Possibly deleted tuple proved\n"
            "  0:    Updating equality manager\n"
            "  0:        Copying the equivalence class for :a\n"
            "  0:        Copying the equivalence class for :R\n"
            "  0:        Copying the equivalence class for owl:sameAs\n"
            "  0:        Copying the equivalence class for rdf:type\n"
            "  0:        Copying the equivalence class for :A\n"
            "  0:        Copying the equivalence class for :D\n"
            "  0:        Copying the equivalence class for :B\n"
            "  0:    Applying insertion rules to tuples from previous levels\n"
            "  0:    Applying recursive insertion rules\n"
            "  0:        Extracted current tuple [ :b, :S, :c ]\n"
            "  0:            Tuple added [ :b, :S, :c ]    { added }\n"
            "  0:            Derived reflexive tuple [ :b , owl:sameAs, :b ]    { added }\n"
            "  0:            Derived reflexive tuple [ :S , owl:sameAs, :S ]    { added }\n"
            "  0:            Derived reflexive tuple [ :c , owl:sameAs, :c ]    { added }\n"
            "  0:            Matched atom [?X, :S, ?Y] to tuple [ :b, :S, :c ]\n"
            "  0:                Matched rule [?X, owl:sameAs, ?Y] :- [?X, :S, ?Y] .\n"
            "  0:                    Derived tuple [ :b, owl:sameAs, :c ]    { normal, added }\n"
            "  0:                    Merged :c -> :b    { successful }\n"
            "  0:        Normalizing constant :c\n"
            "  0:            Tuple [ :c, owl:sameAs, :c ] was normalized to [ :b, owl:sameAs, :b ]    { not added }\n"
            "  0:            Tuple [ :c, rdf:type, :C ] was normalized to [ :b, rdf:type, :C ]    { added }\n"
            "  0:            Tuple [ :b, :S, :c ] was normalized to [ :b, :S, :b ]    { added }\n"
            "  0:        Extracted current tuple [ :S, owl:sameAs, :S ]\n"
            "  0:            Tuple added [ :S, owl:sameAs, :S ]    { added }\n"
            "  0:            Derived reflexive tuple [ owl:sameAs , owl:sameAs, owl:sameAs ]    { added }\n"
            "  0:        Extracted current tuple [ :c, owl:sameAs, :c ]\n"
            "  0:            Current tuple normalized to [ :b, owl:sameAs, :b ]    { not added }\n"
            "  0:        Extracted current tuple [ :b, owl:sameAs, :c ]\n"
            "  0:            Current tuple normalized to [ :b, owl:sameAs, :b ]    { not added }\n"
            "  0:        Extracted current tuple [ :b, rdf:type, :C ]\n"
            "  0:            Tuple added [ :b, rdf:type, :C ]    { added }\n"
            "  0:            Derived reflexive tuple [ rdf:type , owl:sameAs, rdf:type ]    { added }\n"
            "  0:            Derived reflexive tuple [ :C , owl:sameAs, :C ]    { added }\n"
            "  0:            Matched atom [?X, rdf:type, :C] to tuple [ :b, rdf:type, :C ]\n"
            "  0:                Matching atom [?X, rdf:type, :B]\n"
            "  0:                    Matched atom [?X, rdf:type, :B] to tuple [ :b, rdf:type, :B ]\n"
            "  0:                        Matched rule [?X, rdf:type, :E] :- [?X, rdf:type, :B], [?X, rdf:type, :C] .\n"
            "  0:                            Derived tuple [ :b, rdf:type, :E ]    { normal, added }\n"
            "  0:        Extracted current tuple [ :b, :S, :b ]\n"
            "  0:            Tuple added [ :b, :S, :b ]    { added }\n"
            "  0:            Matched atom [?X, :S, ?Y] to tuple [ :b, :S, :b ]\n"
            "  0:                Matched rule [?X, owl:sameAs, ?Y] :- [?X, :S, ?Y] .\n"
            "  0:                    Derived tuple [ :b, owl:sameAs, :b ]    { normal, not added }\n"
            "  0:        Extracted current tuple [ :b, rdf:type, :E ]\n"
            "  0:            Tuple added [ :b, rdf:type, :E ]    { added }\n"
            "  0:            Derived reflexive tuple [ :E , owl:sameAs, :E ]    { added }\n"
            "  0:        Extracted current tuple [ :E, owl:sameAs, :E ]\n"
            "  0:            Tuple added [ :E, owl:sameAs, :E ]    { added }\n"
            "============================================================\n"
            "  0:    Propagating deleted and proved tuples into the store\n"
            "  0:        Tuple deleted [ :a, :R, :a ]    { deleted }\n"
            "  0:        Tuple deleted [ :a, owl:sameAs, :a ]    { deleted }\n"
            "  0:        Tuple deleted [ :R, owl:sameAs, :R ]    { deleted }\n"
            "  0:        Tuple deleted [ :a, rdf:type, :A ]    { deleted }\n"
            "  0:        Tuple deleted [ :a, rdf:type, :D ]    { deleted }\n"
            "  0:        Tuple deleted [ :a, rdf:type, :B ]    { deleted }\n"
            "  0:        Tuple deleted [ :D, owl:sameAs, :D ]    { deleted }\n"
            "  0:        Tuple added [ :a, owl:sameAs, :a ]    { added }\n"
            "  0:        Tuple added [ :b, owl:sameAs, :b ]    { added }\n"
            "  0:        Tuple added [ :a, rdf:type, :A ]    { added }\n"
            "  0:        Tuple added [ :b, rdf:type, :B ]    { added }\n"
            "  0:        Tuple added [ owl:sameAs, owl:sameAs, owl:sameAs ]    { not added }\n"
            "  0:        Tuple added [ rdf:type, owl:sameAs, rdf:type ]    { not added }\n"
            "  0:        Tuple added [ :A, owl:sameAs, :A ]    { not added }\n"
            "  0:        Tuple added [ :B, owl:sameAs, :B ]    { not added }\n"
            "  0:        Tuple added [ :S, owl:sameAs, :S ]    { added }\n"
            "  0:        Tuple added [ :b, rdf:type, :C ]    { added }\n"
            "  0:        Tuple added [ :b, :S, :b ]    { added }\n"
            "  0:        Tuple added [ :b, rdf:type, :E ]    { added }\n"
            "  0:        Tuple added [ :E, owl:sameAs, :E ]    { added }\n"
        );
    // Check with expanding equality.
    m_queryParameters.setString("domain", "IDB");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "
        ":A owl:sameAs :A . "
        ":B owl:sameAs :B . "
        ":C owl:sameAs :C . "
        ":E owl:sameAs :E . "
        ":S owl:sameAs :S . "

        ":a rdf:type :A . "
        ":b rdf:type :B . "
        ":b rdf:type :C . "
        ":b rdf:type :E . "
        ":c rdf:type :B . "
        ":c rdf:type :C . "
        ":c rdf:type :E . "

        ":b :S :b . "
        ":b :S :c . "
        ":c :S :b . "
        ":c :S :c . "

        ":a owl:sameAs :a . "
        ":b owl:sameAs :b . "
        ":b owl:sameAs :c . "
        ":c owl:sameAs :b . "
        ":c owl:sameAs :c . "
    );
    // Check without expanding equality.
    m_queryParameters.setString("domain", "IDBrep");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "
        ":A owl:sameAs :A . "
        ":B owl:sameAs :B . "
        ":C owl:sameAs :C . "
        ":E owl:sameAs :E . "
        ":S owl:sameAs :S . "

        ":a rdf:type :A . "
        ":b rdf:type :B . "
        ":b rdf:type :C . "
        ":b rdf:type :E . "

        ":b :S :b . "

        ":a owl:sameAs :a . "
        ":b owl:sameAs :b . "
    );
}

TEST(testDeleteAddInterplayWithRules) {
    addTriples(
        ":a1 rdf:type :A . "
        ":b1 rdf:type :B . "
        ":a1 owl:sameAs :b1 . "

        ":a2 rdf:type :A . "
        ":b2 rdf:type :B . "
        ":a2 :R :b2 . "

        ":a3 rdf:type :A . "
        ":b3 rdf:type :B . "

        ":a4 rdf:type :A . "
        ":b4 rdf:type :B . "
        ":a4 :S :b4 . "
    );
    addRules(
        ":C(?X) :- :A(?X), :B(?X) . "
        "owl:sameAs(?X,?Y) :- :R(?X,?Y) . "
    );
    applyRules();
    // Check with expanding equality.
    m_queryParameters.setString("domain", "IDB");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "
        ":A owl:sameAs :A . "
        ":B owl:sameAs :B . "
        ":C owl:sameAs :C . "
        ":R owl:sameAs :R . "
        ":S owl:sameAs :S . "

        ":a1 rdf:type :A . "
        ":b1 rdf:type :A . "
        ":a1 rdf:type :B . "
        ":b1 rdf:type :B . "
        ":a1 rdf:type :C . "
        ":b1 rdf:type :C . "
        ":a1 owl:sameAs :a1 . "
        ":a1 owl:sameAs :b1 . "
        ":b1 owl:sameAs :a1 . "
        ":b1 owl:sameAs :b1 . "

        ":a2 rdf:type :A . "
        ":b2 rdf:type :A . "
        ":a2 rdf:type :B . "
        ":b2 rdf:type :B . "
        ":a2 rdf:type :C . "
        ":b2 rdf:type :C . "
        ":a2 :R :a2 . "
        ":a2 :R :b2 . "
        ":b2 :R :a2 . "
        ":b2 :R :b2 . "
        ":a2 owl:sameAs :a2 . "
        ":a2 owl:sameAs :b2 . "
        ":b2 owl:sameAs :a2 . "
        ":b2 owl:sameAs :b2 . "

        ":a3 rdf:type :A . "
        ":b3 rdf:type :B . "
        ":a3 owl:sameAs :a3 . "
        ":b3 owl:sameAs :b3 . "

        ":a4 rdf:type :A . "
        ":b4 rdf:type :B . "
        ":a4 :S :b4 . "
        ":a4 owl:sameAs :a4 . "
        ":b4 owl:sameAs :b4 . "
    );
    // Check without expanding equality.
    m_queryParameters.setString("domain", "IDBrep");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "
        ":A owl:sameAs :A . "
        ":B owl:sameAs :B . "
        ":C owl:sameAs :C . "
        ":R owl:sameAs :R . "
        ":S owl:sameAs :S . "

        ":a1 rdf:type :A . "
        ":a1 rdf:type :B . "
        ":a1 rdf:type :C . "
        ":a1 owl:sameAs :a1 . "

        ":a2 rdf:type :A . "
        ":a2 rdf:type :B . "
        ":a2 rdf:type :C . "
        ":a2 :R :a2 . "
        ":a2 owl:sameAs :a2 . "

        ":a3 rdf:type :A . "
        ":b3 rdf:type :B . "
        ":a3 owl:sameAs :a3 . "
        ":b3 owl:sameAs :b3 . "

        ":a4 rdf:type :A . "
        ":b4 rdf:type :B . "
        ":a4 :S :b4 . "
        ":a4 owl:sameAs :a4 . "
        ":b4 owl:sameAs :b4 . "
    );

    // Delete/add facts/rules
    forDeletion(
        ":a1 owl:sameAs :b1 . "
    );
    forDeletion(
        "owl:sameAs(?X,?Y) :- :R(?X,?Y) . "
    );
    forAddition(
        ":a3 owl:sameAs :b3 . "
    );
    forAddition(
        "owl:sameAs(?X,?Y) :- :S(?X,?Y) . "
    );
    // Apply changes
    if (m_useDRed)
        ASSERT_APPLY_RULES_INCREMENTALLY(
            "  0:    Evaluating deleted rule [?X, owl:sameAs, ?Y] :- [?X, :R, ?Y] .\n"
            "  0:        Matched rule [?X, owl:sameAs, ?Y] :- [?X, :R, ?Y] .\n"
            "  0:            Derived tuple [ :a2, owl:sameAs, :a2 ]    { normal, added }\n"
            "============================================================\n"
            "  0:    Applying deletion rules to tuples from previous levels\n"
            "  0:    Applying recursive deletion rules\n"
            "  0:        Extracted possibly deleted tuple [ :a2, owl:sameAs, :a2 ]\n"
            "  0:            Applying deletion rules to [ :a2, owl:sameAs, :a2 ]\n"
            "  0:                Propagating replacament of :a2 to [ :a2, owl:sameAs, :a2 ]    { not added }\n"
            "  0:                Propagating replacament of :a2 to [ :a2, :R, :a2 ]    { added }\n"
            "  0:                Propagating replacament of :a2 to [ :a2, rdf:type, :A ]    { added }\n"
            "  0:                Propagating replacament of :a2 to [ :a2, rdf:type, :C ]    { added }\n"
            "  0:                Propagating replacament of :a2 to [ :a2, rdf:type, :B ]    { added }\n"
            "  0:                Propagating replacament of :a2 to [ :a2, :R, :a2 ]    { not added }\n"
            "  0:                Propagating replacament of :a2 to [ :a2, owl:sameAs, :a2 ]    { not added }\n"
            "  0:                Derived reflexive tuple [ :a2 , owl:sameAs, :a2 ]    { not added }\n"
            "  0:                Derived reflexive tuple [ owl:sameAs , owl:sameAs, owl:sameAs ]    { added }\n"
            "  0:        Extracted possibly deleted tuple [ :a1, owl:sameAs, :a1 ]\n"
            "  0:            Applying deletion rules to [ :a1, owl:sameAs, :a1 ]\n"
            "  0:                Propagating replacament of :a1 to [ :a1, owl:sameAs, :a1 ]    { not added }\n"
            "  0:                Propagating replacament of :a1 to [ :a1, rdf:type, :A ]    { added }\n"
            "  0:                Propagating replacament of :a1 to [ :a1, rdf:type, :C ]    { added }\n"
            "  0:                Propagating replacament of :a1 to [ :a1, rdf:type, :B ]    { added }\n"
            "  0:                Propagating replacament of :a1 to [ :a1, owl:sameAs, :a1 ]    { not added }\n"
            "  0:                Derived reflexive tuple [ :a1 , owl:sameAs, :a1 ]    { not added }\n"
            "  0:        Extracted possibly deleted tuple [ :a2, :R, :a2 ]\n"
            "  0:            Applying deletion rules to [ :a2, :R, :a2 ]\n"
            "  0:                Derived reflexive tuple [ :R , owl:sameAs, :R ]    { added }\n"
            "  0:        Extracted possibly deleted tuple [ :a2, rdf:type, :A ]\n"
            "  0:            Applying deletion rules to [ :a2, rdf:type, :A ]\n"
            "  0:                Derived reflexive tuple [ rdf:type , owl:sameAs, rdf:type ]    { added }\n"
            "  0:                Derived reflexive tuple [ :A , owl:sameAs, :A ]    { added }\n"
            "  0:                Matched atom [?X, rdf:type, :A] to tuple [ :a2, rdf:type, :A ]\n"
            "  0:                    Matching atom [?X, rdf:type, :B]\n"
            "  0:                        Matched atom [?X, rdf:type, :B] to tuple [ :a2, rdf:type, :B ]\n"
            "  0:                            Matched rule [?X, rdf:type, :C] :- [?X, rdf:type, :A], [?X, rdf:type, :B] .\n"
            "  0:                                Derived tuple [ :a2, rdf:type, :C ]    { normal, not added }\n"
            "  0:        Extracted possibly deleted tuple [ :a2, rdf:type, :C ]\n"
            "  0:            Applying deletion rules to [ :a2, rdf:type, :C ]\n"
            "  0:                Derived reflexive tuple [ :C , owl:sameAs, :C ]    { added }\n"
            "  0:        Extracted possibly deleted tuple [ :a2, rdf:type, :B ]\n"
            "  0:            Applying deletion rules to [ :a2, rdf:type, :B ]\n"
            "  0:                Derived reflexive tuple [ :B , owl:sameAs, :B ]    { added }\n"
            "  0:                Matched atom [?X, rdf:type, :B] to tuple [ :a2, rdf:type, :B ]\n"
            "  0:                    Matching atom [?X, rdf:type, :A]\n"
            "  0:        Extracted possibly deleted tuple [ owl:sameAs, owl:sameAs, owl:sameAs ]\n"
            "  0:            Applying deletion rules to [ owl:sameAs, owl:sameAs, owl:sameAs ]\n"
            "  0:        Extracted possibly deleted tuple [ :a1, rdf:type, :A ]\n"
            "  0:            Applying deletion rules to [ :a1, rdf:type, :A ]\n"
            "  0:                Matched atom [?X, rdf:type, :A] to tuple [ :a1, rdf:type, :A ]\n"
            "  0:                    Matching atom [?X, rdf:type, :B]\n"
            "  0:                        Matched atom [?X, rdf:type, :B] to tuple [ :a1, rdf:type, :B ]\n"
            "  0:                            Matched rule [?X, rdf:type, :C] :- [?X, rdf:type, :A], [?X, rdf:type, :B] .\n"
            "  0:                                Derived tuple [ :a1, rdf:type, :C ]    { normal, not added }\n"
            "  0:        Extracted possibly deleted tuple [ :a1, rdf:type, :C ]\n"
            "  0:            Applying deletion rules to [ :a1, rdf:type, :C ]\n"
            "  0:        Extracted possibly deleted tuple [ :a1, rdf:type, :B ]\n"
            "  0:            Applying deletion rules to [ :a1, rdf:type, :B ]\n"
            "  0:                Matched atom [?X, rdf:type, :B] to tuple [ :a1, rdf:type, :B ]\n"
            "  0:                    Matching atom [?X, rdf:type, :A]\n"
            "  0:        Extracted possibly deleted tuple [ :R, owl:sameAs, :R ]\n"
            "  0:            Applying deletion rules to [ :R, owl:sameAs, :R ]\n"
            "  0:        Extracted possibly deleted tuple [ rdf:type, owl:sameAs, rdf:type ]\n"
            "  0:            Applying deletion rules to [ rdf:type, owl:sameAs, rdf:type ]\n"
            "  0:        Extracted possibly deleted tuple [ :A, owl:sameAs, :A ]\n"
            "  0:            Applying deletion rules to [ :A, owl:sameAs, :A ]\n"
            "  0:        Extracted possibly deleted tuple [ :C, owl:sameAs, :C ]\n"
            "  0:            Applying deletion rules to [ :C, owl:sameAs, :C ]\n"
            "  0:        Extracted possibly deleted tuple [ :B, owl:sameAs, :B ]\n"
            "  0:            Applying deletion rules to [ :B, owl:sameAs, :B ]\n"
            "  0:    Updating equality manager\n"
            "  0:    Checking provability of [ :a2, owl:sameAs, :a2 ]    { added to checked }\n"
            "  0:    Checking provability of [ :a1, owl:sameAs, :a1 ]    { added to checked }\n"
            "  0:    Checking provability of [ :a2, :R, :a2 ]    { added to checked }\n"
            "  0:    Checking provability of [ :a2, rdf:type, :A ]    { added to checked }\n"
            "  0:    Checking provability of [ :a2, rdf:type, :C ]    { added to checked }\n"
            "  0:        Checking recursive rule [?X, rdf:type, :C] :- [?X, rdf:type, :A], [?X, rdf:type, :B] .\n"
            "  0:        Checking recursive rule [?X, rdf:type, :C] :- [?X, rdf:type, :A], [?X, rdf:type, :B] .\n"
            "  0:    Checking provability of [ :a2, rdf:type, :B ]    { added to checked }\n"
            "  0:    Checking provability of [ owl:sameAs, owl:sameAs, owl:sameAs ]    { added to checked }\n"
            "  0:        Matched reflexive owl:sameAs rule instance [ owl:sameAs, owl:sameAs, owl:sameAs ] :- [ :S, owl:sameAs, :S ] .\n"
            "  0:            Backward chaining stopped because the tuple was proved\n"
            "  0:    Checking provability of [ :a1, rdf:type, :A ]    { added to checked }\n"
            "  0:    Checking provability of [ :a1, rdf:type, :C ]    { added to checked }\n"
            "  0:        Checking recursive rule [?X, rdf:type, :C] :- [?X, rdf:type, :A], [?X, rdf:type, :B] .\n"
            "  0:        Checking recursive rule [?X, rdf:type, :C] :- [?X, rdf:type, :A], [?X, rdf:type, :B] .\n"
            "  0:    Checking provability of [ :a1, rdf:type, :B ]    { added to checked }\n"
            "  0:    Checking provability of [ :R, owl:sameAs, :R ]    { added to checked }\n"
            "  0:    Checking provability of [ rdf:type, owl:sameAs, rdf:type ]    { added to checked }\n"
            "  0:        Matched reflexive owl:sameAs rule instance [ rdf:type, owl:sameAs, rdf:type ] :- [ :b4, rdf:type, :B ] .\n"
            "  0:            Backward chaining stopped because the tuple was proved\n"
            "  0:    Checking provability of [ :A, owl:sameAs, :A ]    { added to checked }\n"
            "  0:        Matched reflexive owl:sameAs rule instance [ :A, owl:sameAs, :A ] :- [ :a4, rdf:type, :A ] .\n"
            "  0:            Backward chaining stopped because the tuple was proved\n"
            "  0:    Checking provability of [ :C, owl:sameAs, :C ]    { added to checked }\n"
            "  0:    Checking provability of [ :B, owl:sameAs, :B ]    { added to checked }\n"
            "  0:        Matched reflexive owl:sameAs rule instance [ :B, owl:sameAs, :B ] :- [ :b4, rdf:type, :B ] .\n"
            "  0:            Backward chaining stopped because the tuple was proved\n"
            "  0:    Updating equality manager\n"
            "  0:    Evaluating inserted rule [?X, owl:sameAs, ?Y] :- [?X, :S, ?Y] .\n"
            "  0:        Matched rule [?X, owl:sameAs, ?Y] :- [?X, :S, ?Y] .\n"
            "  0:            Derived tuple [ :a4, owl:sameAs, :b4 ]    { normal, added }\n"
            "  0:    Applying insertion rules to tuples from previous levels\n"
            "  0:    Applying recursive insertion rules\n"
            "  0:        Extracted current tuple [ :a3, owl:sameAs, :b3 ]\n"
            "  0:            Tuple added [ :a3, owl:sameAs, :b3 ]    { added }\n"
            "  0:            Merged :b3 -> :a3    { successful }\n"
            "  0:        Normalizing constant :b3\n"
            "  0:            Tuple [ :b3, owl:sameAs, :b3 ] was normalized to [ :a3, owl:sameAs, :a3 ]    { added }\n"
            "  0:            Tuple [ :b3, rdf:type, :B ] was normalized to [ :a3, rdf:type, :B ]    { added }\n"
            "  0:            Tuple [ :a3, owl:sameAs, :b3 ] was normalized to [ :a3, owl:sameAs, :a3 ]    { not added }\n"
            "  0:        Extracted current tuple [ :a2, :R, :b2 ]\n"
            "  0:            Tuple added [ :a2, :R, :b2 ]    { added }\n"
            "  0:            Derived reflexive tuple [ :a2 , owl:sameAs, :a2 ]    { added }\n"
            "  0:            Derived reflexive tuple [ :R , owl:sameAs, :R ]    { added }\n"
            "  0:            Derived reflexive tuple [ :b2 , owl:sameAs, :b2 ]    { added }\n"
            "  0:        Extracted current tuple [ :a2, rdf:type, :A ]\n"
            "  0:            Tuple added [ :a2, rdf:type, :A ]    { added }\n"
            "  0:            Derived reflexive tuple [ rdf:type , owl:sameAs, rdf:type ]    { not added }\n"
            "  0:            Derived reflexive tuple [ :A , owl:sameAs, :A ]    { not added }\n"
            "  0:            Matched atom [?X, rdf:type, :A] to tuple [ :a2, rdf:type, :A ]\n"
            "  0:                Matching atom [?X, rdf:type, :B]\n"
            "  0:        Extracted current tuple [ :b2, rdf:type, :B ]\n"
            "  0:            Tuple added [ :b2, rdf:type, :B ]    { added }\n"
            "  0:            Derived reflexive tuple [ :B , owl:sameAs, :B ]    { not added }\n"
            "  0:            Matched atom [?X, rdf:type, :B] to tuple [ :b2, rdf:type, :B ]\n"
            "  0:                Matching atom [?X, rdf:type, :A]\n"
            "  0:        Extracted current tuple [ owl:sameAs, owl:sameAs, owl:sameAs ]\n"
            "  0:            Tuple added [ owl:sameAs, owl:sameAs, owl:sameAs ]    { added }\n"
            "  0:            Derived reflexive tuple [ owl:sameAs , owl:sameAs, owl:sameAs ]    { not added }\n"
            "  0:        Extracted current tuple [ :a1, rdf:type, :A ]\n"
            "  0:            Tuple added [ :a1, rdf:type, :A ]    { added }\n"
            "  0:            Derived reflexive tuple [ :a1 , owl:sameAs, :a1 ]    { added }\n"
            "  0:            Matched atom [?X, rdf:type, :A] to tuple [ :a1, rdf:type, :A ]\n"
            "  0:                Matching atom [?X, rdf:type, :B]\n"
            "  0:        Extracted current tuple [ :b1, rdf:type, :B ]\n"
            "  0:            Tuple added [ :b1, rdf:type, :B ]    { added }\n"
            "  0:            Derived reflexive tuple [ :b1 , owl:sameAs, :b1 ]    { added }\n"
            "  0:            Matched atom [?X, rdf:type, :B] to tuple [ :b1, rdf:type, :B ]\n"
            "  0:                Matching atom [?X, rdf:type, :A]\n"
            "  0:        Extracted current tuple [ rdf:type, owl:sameAs, rdf:type ]\n"
            "  0:            Tuple added [ rdf:type, owl:sameAs, rdf:type ]    { added }\n"
            "  0:        Extracted current tuple [ :A, owl:sameAs, :A ]\n"
            "  0:            Tuple added [ :A, owl:sameAs, :A ]    { added }\n"
            "  0:        Extracted current tuple [ :B, owl:sameAs, :B ]\n"
            "  0:            Tuple added [ :B, owl:sameAs, :B ]    { added }\n"
            "  0:        Extracted current tuple [ :a4, owl:sameAs, :b4 ]\n"
            "  0:            Tuple added [ :a4, owl:sameAs, :b4 ]    { added }\n"
            "  0:            Merged :b4 -> :a4    { successful }\n"
            "  0:        Normalizing constant :b4\n"
            "  0:            Tuple [ :b4, owl:sameAs, :b4 ] was normalized to [ :a4, owl:sameAs, :a4 ]    { added }\n"
            "  0:            Tuple [ :b4, rdf:type, :B ] was normalized to [ :a4, rdf:type, :B ]    { added }\n"
            "  0:            Tuple [ :a4, owl:sameAs, :b4 ] was normalized to [ :a4, owl:sameAs, :a4 ]    { not added }\n"
            "  0:            Tuple [ :a4, :S, :b4 ] was normalized to [ :a4, :S, :a4 ]    { added }\n"
            "  0:        Extracted current tuple [ :a3, rdf:type, :B ]\n"
            "  0:            Tuple added [ :a3, rdf:type, :B ]    { added }\n"
            "  0:            Derived reflexive tuple [ :a3 , owl:sameAs, :a3 ]    { not added }\n"
            "  0:            Matched atom [?X, rdf:type, :B] to tuple [ :a3, rdf:type, :B ]\n"
            "  0:                Matching atom [?X, rdf:type, :A]\n"
            "  0:                    Matched atom [?X, rdf:type, :A] to tuple [ :a3, rdf:type, :A ]\n"
            "  0:                        Matched rule [?X, rdf:type, :C] :- [?X, rdf:type, :A], [?X, rdf:type, :B] .\n"
            "  0:                            Derived tuple [ :a3, rdf:type, :C ]    { normal, added }\n"
            "  0:        Extracted current tuple [ :a2, owl:sameAs, :a2 ]\n"
            "  0:            Tuple added [ :a2, owl:sameAs, :a2 ]    { added }\n"
            "  0:        Extracted current tuple [ :R, owl:sameAs, :R ]\n"
            "  0:            Tuple added [ :R, owl:sameAs, :R ]    { added }\n"
            "  0:        Extracted current tuple [ :b2, owl:sameAs, :b2 ]\n"
            "  0:            Tuple added [ :b2, owl:sameAs, :b2 ]    { added }\n"
            "  0:        Extracted current tuple [ :a1, owl:sameAs, :a1 ]\n"
            "  0:            Tuple added [ :a1, owl:sameAs, :a1 ]    { added }\n"
            "  0:        Extracted current tuple [ :b1, owl:sameAs, :b1 ]\n"
            "  0:            Tuple added [ :b1, owl:sameAs, :b1 ]    { added }\n"
            "  0:        Extracted current tuple [ :a4, rdf:type, :B ]\n"
            "  0:            Tuple added [ :a4, rdf:type, :B ]    { added }\n"
            "  0:            Derived reflexive tuple [ :a4 , owl:sameAs, :a4 ]    { not added }\n"
            "  0:            Matched atom [?X, rdf:type, :B] to tuple [ :a4, rdf:type, :B ]\n"
            "  0:                Matching atom [?X, rdf:type, :A]\n"
            "  0:                    Matched atom [?X, rdf:type, :A] to tuple [ :a4, rdf:type, :A ]\n"
            "  0:                        Matched rule [?X, rdf:type, :C] :- [?X, rdf:type, :A], [?X, rdf:type, :B] .\n"
            "  0:                            Derived tuple [ :a4, rdf:type, :C ]    { normal, added }\n"
            "  0:        Extracted current tuple [ :a4, :S, :a4 ]\n"
            "  0:            Tuple added [ :a4, :S, :a4 ]    { added }\n"
            "  0:            Derived reflexive tuple [ :S , owl:sameAs, :S ]    { added }\n"
            "  0:            Matched atom [?X, :S, ?Y] to tuple [ :a4, :S, :a4 ]\n"
            "  0:                Matched rule [?X, owl:sameAs, ?Y] :- [?X, :S, ?Y] .\n"
            "  0:                    Derived tuple [ :a4, owl:sameAs, :a4 ]    { normal, not added }\n"
            "  0:        Extracted current tuple [ :a3, rdf:type, :C ]\n"
            "  0:            Tuple added [ :a3, rdf:type, :C ]    { added }\n"
            "  0:            Derived reflexive tuple [ :C , owl:sameAs, :C ]    { added }\n"
            "  0:        Extracted current tuple [ :a4, rdf:type, :C ]\n"
            "  0:            Tuple added [ :a4, rdf:type, :C ]    { added }\n"
            "  0:        Extracted current tuple [ :C, owl:sameAs, :C ]\n"
            "  0:            Tuple added [ :C, owl:sameAs, :C ]    { added }\n"
            "============================================================\n"
            "  0:    Propagating deleted and proved tuples into the store\n"
            "  0:        Tuple deleted [ :a2, :R, :a2 ]    { deleted }\n"
            "  0:        Tuple deleted [ :a2, rdf:type, :C ]    { deleted }\n"
            "  0:        Tuple deleted [ :a2, rdf:type, :B ]    { deleted }\n"
            "  0:        Tuple deleted [ :a1, rdf:type, :C ]    { deleted }\n"
            "  0:        Tuple deleted [ :a1, rdf:type, :B ]    { deleted }\n"
            "  0:        Tuple added [ :a2, :R, :b2 ]    { added }\n"
            "  0:        Tuple added [ :b2, rdf:type, :B ]    { added }\n"
            "  0:        Tuple added [ :b1, rdf:type, :B ]    { added }\n"
            "  0:        Tuple added [ :a3, rdf:type, :B ]    { added }\n"
            "  0:        Tuple added [ :b2, owl:sameAs, :b2 ]    { added }\n"
            "  0:        Tuple added [ :b1, owl:sameAs, :b1 ]    { added }\n"
            "  0:        Tuple added [ :a4, rdf:type, :B ]    { added }\n"
            "  0:        Tuple added [ :a4, :S, :a4 ]    { added }\n"
            "  0:        Tuple added [ :a3, rdf:type, :C ]    { added }\n"
            "  0:        Tuple added [ :a4, rdf:type, :C ]    { added }\n"
        );
    else
        ASSERT_APPLY_RULES_INCREMENTALLY(
            "  0:    Evaluating deleted rule [?X, owl:sameAs, ?Y] :- [?X, :R, ?Y] .\n"
            "  0:        Matched rule [?X, owl:sameAs, ?Y] :- [?X, :R, ?Y] .\n"
            "  0:            Derived tuple [ :a2, owl:sameAs, :a2 ]    { normal, added }\n"
            "============================================================\n"
            "  0:    Applying deletion rules to tuples from previous levels\n"
            "  0:    Applying recursive deletion rules\n"
            "  0:        Extracted possibly deleted tuple [ :a2, owl:sameAs, :a2 ]\n"
            "  0:            Checking provability of [ :a2, owl:sameAs, :a2 ]    { added to checked }\n"
            "  0:                Derived reflexive tuple [ :a2, owl:sameAs, :a2 ] directly from EDB    { added }\n"
            "  0:                Derived reflexive tuple [ :b2, owl:sameAs, :b2 ] directly from EDB    { added }\n"
            "  0:                Processing the proved list\n"
            "  0:                    Extracted current tuple [ :a2, owl:sameAs, :a2 ]\n"
            "  0:                        Derived reflexive tuple [ :a2 , owl:sameAs, :a2 ]    { not added }\n"
            "  0:                        Delaying reflexive tuple [ owl:sameAs, owl:sameAs, owl:sameAs ]    { added }\n"
            "  0:                    Extracted current tuple [ :b2, owl:sameAs, :b2 ]\n"
            "  0:                        Derived reflexive tuple [ :b2 , owl:sameAs, :b2 ]    { not added }\n"
            "  0:                All consequences of reflexivity rules proved for [ :a2, owl:sameAs, :a2 ]\n"
            "  0:                Checking replacement owl:sameAs rule for [ :a2, owl:sameAs, :a2 ] at position 0\n"
            "  0:                    Checking provability of [ :a2, owl:sameAs, :a2 ]    { not added to checked }\n"
            "  0:            Applying deletion rules to [ :a2, owl:sameAs, :a2 ]\n"
            "  0:                Propagating replacament of :a2 to [ :a2, owl:sameAs, :a2 ]    { not added }\n"
            "  0:                Propagating replacament of :a2 to [ :a2, :R, :a2 ]    { added }\n"
            "  0:                Propagating replacament of :a2 to [ :a2, rdf:type, :A ]    { added }\n"
            "  0:                Propagating replacament of :a2 to [ :a2, rdf:type, :C ]    { added }\n"
            "  0:                Propagating replacament of :a2 to [ :a2, rdf:type, :B ]    { added }\n"
            "  0:                Propagating replacament of :a2 to [ :a2, :R, :a2 ]    { not added }\n"
            "  0:                Propagating replacament of :a2 to [ :a2, owl:sameAs, :a2 ]    { not added }\n"
            "  0:                Derived reflexive tuple [ :a2 , owl:sameAs, :a2 ]    { not added }\n"
            "  0:                Derived reflexive tuple [ owl:sameAs , owl:sameAs, owl:sameAs ]    { added }\n"
            "  0:        Extracted possibly deleted tuple [ :a1, owl:sameAs, :a1 ]\n"
            "  0:            Checking provability of [ :a1, owl:sameAs, :a1 ]    { added to checked }\n"
            "  0:                Derived reflexive tuple [ :a1, owl:sameAs, :a1 ] directly from EDB    { added }\n"
            "  0:                Derived reflexive tuple [ :b1, owl:sameAs, :b1 ] directly from EDB    { added }\n"
            "  0:                Processing the proved list\n"
            "  0:                    Extracted current tuple [ :a1, owl:sameAs, :a1 ]\n"
            "  0:                        Derived reflexive tuple [ :a1 , owl:sameAs, :a1 ]    { not added }\n"
            "  0:                    Extracted current tuple [ :b1, owl:sameAs, :b1 ]\n"
            "  0:                        Derived reflexive tuple [ :b1 , owl:sameAs, :b1 ]    { not added }\n"
            "  0:                All consequences of reflexivity rules proved for [ :a1, owl:sameAs, :a1 ]\n"
            "  0:                Checking replacement owl:sameAs rule for [ :a1, owl:sameAs, :a1 ] at position 0\n"
            "  0:                    Checking provability of [ :a1, owl:sameAs, :a1 ]    { not added to checked }\n"
            "  0:            Applying deletion rules to [ :a1, owl:sameAs, :a1 ]\n"
            "  0:                Propagating replacament of :a1 to [ :a1, owl:sameAs, :a1 ]    { not added }\n"
            "  0:                Propagating replacament of :a1 to [ :a1, rdf:type, :A ]    { added }\n"
            "  0:                Propagating replacament of :a1 to [ :a1, rdf:type, :C ]    { added }\n"
            "  0:                Propagating replacament of :a1 to [ :a1, rdf:type, :B ]    { added }\n"
            "  0:                Propagating replacament of :a1 to [ :a1, owl:sameAs, :a1 ]    { not added }\n"
            "  0:                Derived reflexive tuple [ :a1 , owl:sameAs, :a1 ]    { not added }\n"
            "  0:        Extracted possibly deleted tuple [ :a2, :R, :a2 ]\n"
            "  0:            Checking provability of [ :a2, :R, :a2 ]    { added to checked }\n"
            "  0:                Derived tuple [ :a2, :R, :b2 ]    { from EDB, not from delayed, not from previous level, added }\n"
            "  0:                Processing the proved list\n"
            "  0:                    Extracted current tuple [ :a2, :R, :b2 ]\n"
            "  0:                        Delaying reflexive tuple [ :R, owl:sameAs, :R ]    { added }\n"
            "  0:            Applying deletion rules to [ :a2, :R, :a2 ]\n"
            "  0:                Derived reflexive tuple [ :R , owl:sameAs, :R ]    { added }\n"
            "  0:        Extracted possibly deleted tuple [ :a2, rdf:type, :A ]\n"
            "  0:            Checking provability of [ :a2, rdf:type, :A ]    { added to checked }\n"
            "  0:                Derived tuple [ :a2, rdf:type, :A ]    { from EDB, not from delayed, not from previous level, added }\n"
            "  0:                Processing the proved list\n"
            "  0:                    Extracted current tuple [ :a2, rdf:type, :A ]\n"
            "  0:                        Delaying reflexive tuple [ rdf:type, owl:sameAs, rdf:type ]    { added }\n"
            "  0:                        Delaying reflexive tuple [ :A, owl:sameAs, :A ]    { added }\n"
            "  0:                        Matched atom [?X, rdf:type, :A] to tuple [ :a2, rdf:type, :A ]\n"
            "  0:                            Matching atom [?X, rdf:type, :B]\n"
            "  0:            Applying deletion rules to [ :a2, rdf:type, :A ]\n"
            "  0:                Derived reflexive tuple [ rdf:type , owl:sameAs, rdf:type ]    { added }\n"
            "  0:                Derived reflexive tuple [ :A , owl:sameAs, :A ]    { added }\n"
            "  0:                Matched atom [?X, rdf:type, :A] to tuple [ :a2, rdf:type, :A ]\n"
            "  0:                    Matching atom [?X, rdf:type, :B]\n"
            "  0:                        Matched atom [?X, rdf:type, :B] to tuple [ :a2, rdf:type, :B ]\n"
            "  0:                            Matched rule [?X, rdf:type, :C] :- [?X, rdf:type, :A], [?X, rdf:type, :B] .\n"
            "  0:                                Derived tuple [ :a2, rdf:type, :C ]    { normal, not added }\n"
            "  0:        Extracted possibly deleted tuple [ :a2, rdf:type, :C ]\n"
            "  0:            Checking provability of [ :a2, rdf:type, :C ]    { added to checked }\n"
            "  0:                Checking recursive rule [?X, rdf:type, :C] :- [?X, rdf:type, :A], [?X, rdf:type, :B] .\n"
            "  0:                    Matched recursive rule instance [ :a2, rdf:type, :C ] :- [ :a2, rdf:type, :A ]*, [ :a2, rdf:type, :B ]* .\n"
            "  0:                        Checking body atom [ :a2, rdf:type, :A ]\n"
            "  0:                            Checking provability of [ :a2, rdf:type, :A ]    { not added to checked }\n"
            "  0:                        Checking body atom [ :a2, rdf:type, :B ]\n"
            "  0:                            Checking provability of [ :a2, rdf:type, :B ]    { added to checked }\n"
            "  0:                                Derived tuple [ :b2, rdf:type, :B ]    { from EDB, not from delayed, not from previous level, added }\n"
            "  0:                                Processing the proved list\n"
            "  0:                                    Extracted current tuple [ :b2, rdf:type, :B ]\n"
            "  0:                                        Delaying reflexive tuple [ :B, owl:sameAs, :B ]    { added }\n"
            "  0:                                        Matched atom [?X, rdf:type, :B] to tuple [ :a2, rdf:type, :B ]\n"
            "  0:                                            Matching atom [?X, rdf:type, :A]\n"
            "  0:            Tuple disproved [ :a2, rdf:type, :C ]    { added }\n"
            "  0:            Applying deletion rules to [ :a2, rdf:type, :C ]\n"
            "  0:                Derived reflexive tuple [ :C , owl:sameAs, :C ]    { added }\n"
            "  0:        Extracted possibly deleted tuple [ :a2, rdf:type, :B ]\n"
            "  0:            Checking provability of [ :a2, rdf:type, :B ]    { not added to checked }\n"
            "  0:            Applying deletion rules to [ :a2, rdf:type, :B ]\n"
            "  0:                Derived reflexive tuple [ :B , owl:sameAs, :B ]    { added }\n"
            "  0:                Matched atom [?X, rdf:type, :B] to tuple [ :a2, rdf:type, :B ]\n"
            "  0:                    Matching atom [?X, rdf:type, :A]\n"
            "  0:        Extracted possibly deleted tuple [ owl:sameAs, owl:sameAs, owl:sameAs ]\n"
            "  0:            Checking provability of [ owl:sameAs, owl:sameAs, owl:sameAs ]    { added to checked }\n"
            "  0:                Derived reflexive tuple [ owl:sameAs, owl:sameAs, owl:sameAs ] directly from EDB    { added }\n"
            "  0:                Derived tuple [ owl:sameAs, owl:sameAs, owl:sameAs ]    { not from EDB, from delayed, not from previous level, not added }\n"
            "  0:                Processing the proved list\n"
            "  0:                    Extracted current tuple [ owl:sameAs, owl:sameAs, owl:sameAs ]\n"
            "  0:                Backward chaining stopped because the tuple was proved\n"
            "  0:            Possibly deleted tuple proved\n"
            "  0:        Extracted possibly deleted tuple [ :a1, rdf:type, :A ]\n"
            "  0:            Checking provability of [ :a1, rdf:type, :A ]    { added to checked }\n"
            "  0:                Derived tuple [ :a1, rdf:type, :A ]    { from EDB, not from delayed, not from previous level, added }\n"
            "  0:                Processing the proved list\n"
            "  0:                    Extracted current tuple [ :a1, rdf:type, :A ]\n"
            "  0:                        Matched atom [?X, rdf:type, :A] to tuple [ :a2, rdf:type, :A ]\n"
            "  0:                            Matching atom [?X, rdf:type, :B]\n"
            "  0:            Applying deletion rules to [ :a1, rdf:type, :A ]\n"
            "  0:                Matched atom [?X, rdf:type, :A] to tuple [ :a1, rdf:type, :A ]\n"
            "  0:                    Matching atom [?X, rdf:type, :B]\n"
            "  0:                        Matched atom [?X, rdf:type, :B] to tuple [ :a1, rdf:type, :B ]\n"
            "  0:                            Matched rule [?X, rdf:type, :C] :- [?X, rdf:type, :A], [?X, rdf:type, :B] .\n"
            "  0:                                Derived tuple [ :a1, rdf:type, :C ]    { normal, not added }\n"
            "  0:        Extracted possibly deleted tuple [ :a1, rdf:type, :C ]\n"
            "  0:            Checking provability of [ :a1, rdf:type, :C ]    { added to checked }\n"
            "  0:                Checking recursive rule [?X, rdf:type, :C] :- [?X, rdf:type, :A], [?X, rdf:type, :B] .\n"
            "  0:                    Matched recursive rule instance [ :a1, rdf:type, :C ] :- [ :a1, rdf:type, :A ]*, [ :a1, rdf:type, :B ]* .\n"
            "  0:                        Checking body atom [ :a1, rdf:type, :A ]\n"
            "  0:                            Checking provability of [ :a1, rdf:type, :A ]    { not added to checked }\n"
            "  0:                        Checking body atom [ :a1, rdf:type, :B ]\n"
            "  0:                            Checking provability of [ :a1, rdf:type, :B ]    { added to checked }\n"
            "  0:                                Derived tuple [ :b1, rdf:type, :B ]    { from EDB, not from delayed, not from previous level, added }\n"
            "  0:                                Processing the proved list\n"
            "  0:                                    Extracted current tuple [ :b1, rdf:type, :B ]\n"
            "  0:                                        Matched atom [?X, rdf:type, :B] to tuple [ :a1, rdf:type, :B ]\n"
            "  0:                                            Matching atom [?X, rdf:type, :A]\n"
            "  0:            Tuple disproved [ :a1, rdf:type, :C ]    { added }\n"
            "  0:            Applying deletion rules to [ :a1, rdf:type, :C ]\n"
            "  0:        Extracted possibly deleted tuple [ :a1, rdf:type, :B ]\n"
            "  0:            Checking provability of [ :a1, rdf:type, :B ]    { not added to checked }\n"
            "  0:            Applying deletion rules to [ :a1, rdf:type, :B ]\n"
            "  0:                Matched atom [?X, rdf:type, :B] to tuple [ :a1, rdf:type, :B ]\n"
            "  0:                    Matching atom [?X, rdf:type, :A]\n"
            "  0:        Extracted possibly deleted tuple [ :R, owl:sameAs, :R ]\n"
            "  0:            Checking provability of [ :R, owl:sameAs, :R ]    { added to checked }\n"
            "  0:                Derived reflexive tuple [ :R, owl:sameAs, :R ] directly from EDB    { added }\n"
            "  0:                Derived tuple [ :R, owl:sameAs, :R ]    { not from EDB, from delayed, not from previous level, not added }\n"
            "  0:                Processing the proved list\n"
            "  0:                    Extracted current tuple [ :R, owl:sameAs, :R ]\n"
            "  0:                Backward chaining stopped because the tuple was proved\n"
            "  0:            Possibly deleted tuple proved\n"
            "  0:        Extracted possibly deleted tuple [ rdf:type, owl:sameAs, rdf:type ]\n"
            "  0:            Checking provability of [ rdf:type, owl:sameAs, rdf:type ]    { added to checked }\n"
            "  0:                Derived reflexive tuple [ rdf:type, owl:sameAs, rdf:type ] directly from EDB    { added }\n"
            "  0:                Derived tuple [ rdf:type, owl:sameAs, rdf:type ]    { not from EDB, from delayed, not from previous level, not added }\n"
            "  0:                Processing the proved list\n"
            "  0:                    Extracted current tuple [ rdf:type, owl:sameAs, rdf:type ]\n"
            "  0:                Backward chaining stopped because the tuple was proved\n"
            "  0:            Possibly deleted tuple proved\n"
            "  0:        Extracted possibly deleted tuple [ :A, owl:sameAs, :A ]\n"
            "  0:            Checking provability of [ :A, owl:sameAs, :A ]    { added to checked }\n"
            "  0:                Derived reflexive tuple [ :A, owl:sameAs, :A ] directly from EDB    { added }\n"
            "  0:                Derived tuple [ :A, owl:sameAs, :A ]    { not from EDB, from delayed, not from previous level, not added }\n"
            "  0:                Processing the proved list\n"
            "  0:                    Extracted current tuple [ :A, owl:sameAs, :A ]\n"
            "  0:                Backward chaining stopped because the tuple was proved\n"
            "  0:            Possibly deleted tuple proved\n"
            "  0:        Extracted possibly deleted tuple [ :C, owl:sameAs, :C ]\n"
            "  0:            Checking provability of [ :C, owl:sameAs, :C ]    { added to checked }\n"
            "  0:                Matched reflexive owl:sameAs rule instance [ :C, owl:sameAs, :C ] :- [ :C, owl:sameAs, :C ] .\n"
            "  0:                    Checking provability of [ :C, owl:sameAs, :C ]    { not added to checked }\n"
            "  0:                Matched reflexive owl:sameAs rule instance [ :C, owl:sameAs, :C ] :- [ :C, owl:sameAs, :C ] .\n"
            "  0:                    Checking provability of [ :C, owl:sameAs, :C ]    { not added to checked }\n"
            "  0:            Tuple disproved [ :C, owl:sameAs, :C ]    { added }\n"
            "  0:            Applying deletion rules to [ :C, owl:sameAs, :C ]\n"
            "  0:        Extracted possibly deleted tuple [ :B, owl:sameAs, :B ]\n"
            "  0:            Checking provability of [ :B, owl:sameAs, :B ]    { added to checked }\n"
            "  0:                Derived reflexive tuple [ :B, owl:sameAs, :B ] directly from EDB    { added }\n"
            "  0:                Derived tuple [ :B, owl:sameAs, :B ]    { not from EDB, from delayed, not from previous level, not added }\n"
            "  0:                Processing the proved list\n"
            "  0:                    Extracted current tuple [ :B, owl:sameAs, :B ]\n"
            "  0:                Backward chaining stopped because the tuple was proved\n"
            "  0:            Possibly deleted tuple proved\n"
            "  0:    Updating equality manager\n"
            "  0:        Copying the equivalence class for :a2\n"
            "  0:        Copying the equivalence class for :a1\n"
            "  0:        Copying the equivalence class for owl:sameAs\n"
            "  0:        Copying the equivalence class for :R\n"
            "  0:        Copying the equivalence class for rdf:type\n"
            "  0:        Copying the equivalence class for :A\n"
            "  0:        Copying the equivalence class for :C\n"
            "  0:        Copying the equivalence class for :B\n"
            "  0:    Evaluating inserted rule [?X, owl:sameAs, ?Y] :- [?X, :S, ?Y] .\n"
            "  0:        Matched rule [?X, owl:sameAs, ?Y] :- [?X, :S, ?Y] .\n"
            "  0:            Derived tuple [ :a4, owl:sameAs, :b4 ]    { normal, added }\n"
            "  0:    Applying insertion rules to tuples from previous levels\n"
            "  0:    Applying recursive insertion rules\n"
            "  0:        Extracted current tuple [ :a3, owl:sameAs, :b3 ]\n"
            "  0:            Tuple added [ :a3, owl:sameAs, :b3 ]    { added }\n"
            "  0:            Merged :b3 -> :a3    { successful }\n"
            "  0:        Normalizing constant :b3\n"
            "  0:            Tuple [ :b3, owl:sameAs, :b3 ] was normalized to [ :a3, owl:sameAs, :a3 ]    { added }\n"
            "  0:            Tuple [ :b3, rdf:type, :B ] was normalized to [ :a3, rdf:type, :B ]    { added }\n"
            "  0:            Tuple [ :a3, owl:sameAs, :b3 ] was normalized to [ :a3, owl:sameAs, :a3 ]    { not added }\n"
            "  0:        Extracted current tuple [ :a4, owl:sameAs, :b4 ]\n"
            "  0:            Tuple added [ :a4, owl:sameAs, :b4 ]    { added }\n"
            "  0:            Merged :b4 -> :a4    { successful }\n"
            "  0:        Normalizing constant :b4\n"
            "  0:            Tuple [ :b4, owl:sameAs, :b4 ] was normalized to [ :a4, owl:sameAs, :a4 ]    { added }\n"
            "  0:            Tuple [ :b4, rdf:type, :B ] was normalized to [ :a4, rdf:type, :B ]    { added }\n"
            "  0:            Tuple [ :a4, owl:sameAs, :b4 ] was normalized to [ :a4, owl:sameAs, :a4 ]    { not added }\n"
            "  0:            Tuple [ :a4, :S, :b4 ] was normalized to [ :a4, :S, :a4 ]    { added }\n"
            "  0:        Extracted current tuple [ :a3, rdf:type, :B ]\n"
            "  0:            Tuple added [ :a3, rdf:type, :B ]    { added }\n"
            "  0:            Derived reflexive tuple [ :a3 , owl:sameAs, :a3 ]    { not added }\n"
            "  0:            Derived reflexive tuple [ rdf:type , owl:sameAs, rdf:type ]    { added }\n"
            "  0:            Derived reflexive tuple [ :B , owl:sameAs, :B ]    { added }\n"
            "  0:            Matched atom [?X, rdf:type, :B] to tuple [ :a3, rdf:type, :B ]\n"
            "  0:                Matching atom [?X, rdf:type, :A]\n"
            "  0:                    Matched atom [?X, rdf:type, :A] to tuple [ :a3, rdf:type, :A ]\n"
            "  0:                        Matched rule [?X, rdf:type, :C] :- [?X, rdf:type, :A], [?X, rdf:type, :B] .\n"
            "  0:                            Derived tuple [ :a3, rdf:type, :C ]    { normal, added }\n"
            "  0:        Extracted current tuple [ :a4, rdf:type, :B ]\n"
            "  0:            Tuple added [ :a4, rdf:type, :B ]    { added }\n"
            "  0:            Derived reflexive tuple [ :a4 , owl:sameAs, :a4 ]    { not added }\n"
            "  0:            Matched atom [?X, rdf:type, :B] to tuple [ :a4, rdf:type, :B ]\n"
            "  0:                Matching atom [?X, rdf:type, :A]\n"
            "  0:                    Matched atom [?X, rdf:type, :A] to tuple [ :a4, rdf:type, :A ]\n"
            "  0:                        Matched rule [?X, rdf:type, :C] :- [?X, rdf:type, :A], [?X, rdf:type, :B] .\n"
            "  0:                            Derived tuple [ :a4, rdf:type, :C ]    { normal, added }\n"
            "  0:        Extracted current tuple [ :a4, :S, :a4 ]\n"
            "  0:            Tuple added [ :a4, :S, :a4 ]    { added }\n"
            "  0:            Derived reflexive tuple [ :S , owl:sameAs, :S ]    { added }\n"
            "  0:            Matched atom [?X, :S, ?Y] to tuple [ :a4, :S, :a4 ]\n"
            "  0:                Matched rule [?X, owl:sameAs, ?Y] :- [?X, :S, ?Y] .\n"
            "  0:                    Derived tuple [ :a4, owl:sameAs, :a4 ]    { normal, not added }\n"
            "  0:        Extracted current tuple [ :a3, rdf:type, :C ]\n"
            "  0:            Tuple added [ :a3, rdf:type, :C ]    { added }\n"
            "  0:            Derived reflexive tuple [ :C , owl:sameAs, :C ]    { added }\n"
            "  0:        Extracted current tuple [ :a4, rdf:type, :C ]\n"
            "  0:            Tuple added [ :a4, rdf:type, :C ]    { added }\n"
            "  0:        Extracted current tuple [ :C, owl:sameAs, :C ]\n"
            "  0:            Tuple added [ :C, owl:sameAs, :C ]    { added }\n"
            "  0:            Derived reflexive tuple [ owl:sameAs , owl:sameAs, owl:sameAs ]    { added }\n"
            "============================================================\n"
            "  0:    Propagating deleted and proved tuples into the store\n"
            "  0:        Tuple deleted [ :a2, owl:sameAs, :a2 ]    { deleted }\n"
            "  0:        Tuple deleted [ :a1, owl:sameAs, :a1 ]    { deleted }\n"
            "  0:        Tuple deleted [ :a2, :R, :a2 ]    { deleted }\n"
            "  0:        Tuple deleted [ :a2, rdf:type, :A ]    { deleted }\n"
            "  0:        Tuple deleted [ :a2, rdf:type, :C ]    { deleted }\n"
            "  0:        Tuple deleted [ :a2, rdf:type, :B ]    { deleted }\n"
            "  0:        Tuple deleted [ :a1, rdf:type, :A ]    { deleted }\n"
            "  0:        Tuple deleted [ :a1, rdf:type, :C ]    { deleted }\n"
            "  0:        Tuple deleted [ :a1, rdf:type, :B ]    { deleted }\n"
            "  0:        Tuple added [ :a2, owl:sameAs, :a2 ]    { added }\n"
            "  0:        Tuple added [ :b2, owl:sameAs, :b2 ]    { added }\n"
            "  0:        Tuple added [ :a1, owl:sameAs, :a1 ]    { added }\n"
            "  0:        Tuple added [ :b1, owl:sameAs, :b1 ]    { added }\n"
            "  0:        Tuple added [ :a2, :R, :b2 ]    { added }\n"
            "  0:        Tuple added [ :a2, rdf:type, :A ]    { added }\n"
            "  0:        Tuple added [ :b2, rdf:type, :B ]    { added }\n"
            "  0:        Tuple added [ owl:sameAs, owl:sameAs, owl:sameAs ]    { not added }\n"
            "  0:        Tuple added [ :a1, rdf:type, :A ]    { added }\n"
            "  0:        Tuple added [ :b1, rdf:type, :B ]    { added }\n"
            "  0:        Tuple added [ :R, owl:sameAs, :R ]    { not added }\n"
            "  0:        Tuple added [ rdf:type, owl:sameAs, rdf:type ]    { not added }\n"
            "  0:        Tuple added [ :A, owl:sameAs, :A ]    { not added }\n"
            "  0:        Tuple added [ :B, owl:sameAs, :B ]    { not added }\n"
            "  0:        Tuple added [ :a3, rdf:type, :B ]    { added }\n"
            "  0:        Tuple added [ :a4, rdf:type, :B ]    { added }\n"
            "  0:        Tuple added [ :a4, :S, :a4 ]    { added }\n"
            "  0:        Tuple added [ :a3, rdf:type, :C ]    { added }\n"
            "  0:        Tuple added [ :a4, rdf:type, :C ]    { added }\n"
        );

    // Check with expanding equality.
    m_queryParameters.setString("domain", "IDB");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "
        ":A owl:sameAs :A . "
        ":B owl:sameAs :B . "
        ":C owl:sameAs :C . "
        ":R owl:sameAs :R . "
        ":S owl:sameAs :S . "

        ":a1 rdf:type :A . "
        ":b1 rdf:type :B . "
        ":a1 owl:sameAs :a1 . "
        ":b1 owl:sameAs :b1 . "

        ":a2 rdf:type :A . "
        ":b2 rdf:type :B . "
        ":a2 :R :b2 . "
        ":a2 owl:sameAs :a2 . "
        ":b2 owl:sameAs :b2 . "

        ":a3 rdf:type :A . "
        ":b3 rdf:type :A . "
        ":a3 rdf:type :B . "
        ":b3 rdf:type :B . "
        ":a3 rdf:type :C . "
        ":b3 rdf:type :C . "
        ":a3 owl:sameAs :a3 . "
        ":a3 owl:sameAs :b3 . "
        ":b3 owl:sameAs :a3 . "
        ":b3 owl:sameAs :b3 . "

        ":a4 rdf:type :A . "
        ":b4 rdf:type :A . "
        ":a4 rdf:type :B . "
        ":b4 rdf:type :B . "
        ":a4 rdf:type :C . "
        ":b4 rdf:type :C . "
        ":a4 :S :a4 . "
        ":a4 :S :b4 . "
        ":b4 :S :a4 . "
        ":b4 :S :b4 . "
        ":a4 owl:sameAs :a4 . "
        ":a4 owl:sameAs :b4 . "
        ":b4 owl:sameAs :a4 . "
        ":b4 owl:sameAs :b4 . "
    );
    // Check without expanding equality.
    m_queryParameters.setString("domain", "IDBrep");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "
        ":A owl:sameAs :A . "
        ":B owl:sameAs :B . "
        ":C owl:sameAs :C . "
        ":R owl:sameAs :R . "
        ":S owl:sameAs :S . "

        ":a1 rdf:type :A . "
        ":b1 rdf:type :B . "
        ":a1 owl:sameAs :a1 . "
        ":b1 owl:sameAs :b1 . "

        ":a2 rdf:type :A . "
        ":b2 rdf:type :B . "
        ":a2 :R :b2 . "
        ":a2 owl:sameAs :a2 . "
        ":b2 owl:sameAs :b2 . "

        ":a3 rdf:type :A . "
        ":a3 rdf:type :B . "
        ":a3 rdf:type :C . "
        ":a3 owl:sameAs :a3 . "

        ":a4 rdf:type :A . "
        ":a4 rdf:type :B . "
        ":a4 rdf:type :C . "
        ":a4 :S :a4 . "
        ":a4 owl:sameAs :a4 . "
    );
}

TEST_UNA(testUNA) {
    addTriples(
        ":a rdf:type :A . "
        ":a :R :a1 . "
        ":a :R :a2 . "

        ":b rdf:type :A . "
        ":b :R _:b1 . "
        ":b :R :b2 . "

        ":c :R :c1 . "
        ":c :R :c2 . "

        ":d :R _:d1 . "
        ":d :R :d2 . "

        ":e rdf:type :A . "
        ":e :R _:e1 . "
        ":e :R 2 . "
    );
    addRules(
        "[?Y1, owl:sameAs, ?Y2] :- [?X, rdf:type, :A], [?X, :R, ?Y1], [?X, :R, ?Y2] . "
    );
    applyRules();
    // Check with expanding equality.
    m_queryParameters.setString("domain", "IDB");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "
        ":R owl:sameAs :R . "
        ":A owl:sameAs :A . "
        "owl:Nothing owl:sameAs owl:Nothing . "

        ":a rdf:type :A . "
        ":a :R :a1 . "
        ":a1 rdf:type owl:Nothing . "
        ":a :R :a2 . "
        ":a2 rdf:type owl:Nothing . "
        ":a owl:sameAs :a . "
        ":a1 owl:sameAs :a1 . "
        ":a1 owl:sameAs :a2 . "
        ":a2 owl:sameAs :a1 . "
        ":a2 owl:sameAs :a2 . "

        ":b rdf:type :A . "
        ":b :R _:b1 . "
        ":b :R :b2 . "
        ":b owl:sameAs :b . "
        "_:b1 owl:sameAs _:b1 . "
        "_:b1 owl:sameAs :b2 . "
        ":b2 owl:sameAs _:b1 . "
        ":b2 owl:sameAs :b2 . "

        ":c :R :c1 . "
        ":c :R :c2 . "
        ":c owl:sameAs :c . "
        ":c1 owl:sameAs :c1 . "
        ":c2 owl:sameAs :c2 . "

        ":d :R _:d1 . "
        ":d :R :d2 . "
        ":d owl:sameAs :d . "
        "_:d1 owl:sameAs _:d1 . "
        ":d2 owl:sameAs :d2 . "

        ":e rdf:type :A . "
        ":e :R _:e1 . "
        ":e :R 2 . "
        ":e owl:sameAs :e . "
        "_:e1 owl:sameAs _:e1 . "
        "_:e1 owl:sameAs 2 . "
        "2 owl:sameAs _:e1 . "
        "2 owl:sameAs 2 . "
    );
    // Check without expanding equality.
    m_queryParameters.setString("domain", "IDBrep");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "
        ":R owl:sameAs :R . "
        ":A owl:sameAs :A . "
        "owl:Nothing owl:sameAs owl:Nothing . "

        ":a rdf:type :A . "
        ":a :R :a1 . "
        ":a1 rdf:type owl:Nothing . "
        ":a owl:sameAs :a . "
        ":a1 owl:sameAs :a1 . "

        ":b rdf:type :A . "
        ":b :R :b2 . "
        ":b owl:sameAs :b . "
        ":b2 owl:sameAs :b2 . "

        ":c :R :c1 . "
        ":c :R :c2 . "
        ":c owl:sameAs :c . "
        ":c1 owl:sameAs :c1 . "
        ":c2 owl:sameAs :c2 . "

        ":d :R _:d1 . "
        ":d :R :d2 . "
        ":d owl:sameAs :d . "
        "_:d1 owl:sameAs _:d1 . "
        ":d2 owl:sameAs :d2 . "

        ":e rdf:type :A . "
        ":e :R 2 . "
        ":e owl:sameAs :e . "
        "2 owl:sameAs 2 . "
    );

    // Now delete and add some tuples.
    forDeletion(
        ":a rdf:type :A . "
        ":b rdf:type :A . "
        ":e rdf:type :A . "
    );
    forAddition(
        ":c rdf:type :A . "
        ":d rdf:type :A . "
    );
    applyRulesIncrementally();
    // Check with expanding equality.
    m_queryParameters.setString("domain", "IDB");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "
        ":R owl:sameAs :R . "
        ":A owl:sameAs :A . "
        "owl:Nothing owl:sameAs owl:Nothing . "

        ":a :R :a1 . "
        ":a :R :a2 . "
        ":a owl:sameAs :a . "
        ":a1 owl:sameAs :a1 . "
        ":a2 owl:sameAs :a2 . "

        ":b :R _:b1 . "
        ":b :R :b2 . "
        ":b owl:sameAs :b . "
        "_:b1 owl:sameAs _:b1 . "
        ":b2 owl:sameAs :b2 . "

        ":c rdf:type :A . "
        ":c :R :c1 . "
        ":c1 rdf:type owl:Nothing . "
        ":c :R :c2 . "
        ":c2 rdf:type owl:Nothing . "
        ":c owl:sameAs :c . "
        ":c1 owl:sameAs :c1 . "
        ":c1 owl:sameAs :c2 . "
        ":c2 owl:sameAs :c1 . "
        ":c2 owl:sameAs :c2 . "

        ":d rdf:type :A . "
        ":d :R _:d1 . "
        ":d :R :d2 . "
        ":d owl:sameAs :d . "
        "_:d1 owl:sameAs _:d1 . "
        "_:d1 owl:sameAs :d2 . "
        ":d2 owl:sameAs _:d1 . "
        ":d2 owl:sameAs :d2 . "

        ":e :R _:e1 . "
        ":e :R 2 . "
        ":e owl:sameAs :e . "
        "_:e1 owl:sameAs _:e1 . "
        "2 owl:sameAs 2 . "
    );
    // Check without expanding equality.
    m_queryParameters.setString("domain", "IDBrep");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "
        ":R owl:sameAs :R . "
        ":A owl:sameAs :A . "
        "owl:Nothing owl:sameAs owl:Nothing . "

        ":a :R :a1 . "
        ":a :R :a2 . "
        ":a owl:sameAs :a . "
        ":a1 owl:sameAs :a1 . "
        ":a2 owl:sameAs :a2 . "

        ":b :R _:b1 . "
        ":b :R :b2 . "
        ":b owl:sameAs :b . "
        "_:b1 owl:sameAs _:b1 . "
        ":b2 owl:sameAs :b2 . "

        ":c rdf:type :A . "
        ":c :R :c1 . "
        ":c1 rdf:type owl:Nothing . "
        ":c owl:sameAs :c . "
        ":c1 owl:sameAs :c1 . "

        ":d rdf:type :A . "
        ":d :R :d2 . "
        ":d owl:sameAs :d . "
        ":d2 owl:sameAs :d2 . "

        ":e :R _:e1 . "
        ":e :R 2 . "
        ":e owl:sameAs :e . "
        "_:e1 owl:sameAs _:e1 . "
        "2 owl:sameAs 2 . "
    );

    // Now restore the original state.
    forAddition(
        ":a rdf:type :A . "
        ":b rdf:type :A . "
        ":e rdf:type :A . "
    );
    forDeletion(
        ":c rdf:type :A . "
        ":d rdf:type :A . "
    );
    applyRulesIncrementally();
    // Check with expanding equality.
    m_queryParameters.setString("domain", "IDB");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "
        ":R owl:sameAs :R . "
        ":A owl:sameAs :A . "
        "owl:Nothing owl:sameAs owl:Nothing . "

        ":a rdf:type :A . "
        ":a :R :a1 . "
        ":a1 rdf:type owl:Nothing . "
        ":a :R :a2 . "
        ":a2 rdf:type owl:Nothing . "
        ":a owl:sameAs :a . "
        ":a1 owl:sameAs :a1 . "
        ":a1 owl:sameAs :a2 . "
        ":a2 owl:sameAs :a1 . "
        ":a2 owl:sameAs :a2 . "

        ":b rdf:type :A . "
        ":b :R _:b1 . "
        ":b :R :b2 . "
        ":b owl:sameAs :b . "
        "_:b1 owl:sameAs _:b1 . "
        "_:b1 owl:sameAs :b2 . "
        ":b2 owl:sameAs _:b1 . "
        ":b2 owl:sameAs :b2 . "

        ":c :R :c1 . "
        ":c :R :c2 . "
        ":c owl:sameAs :c . "
        ":c1 owl:sameAs :c1 . "
        ":c2 owl:sameAs :c2 . "

        ":d :R _:d1 . "
        ":d :R :d2 . "
        ":d owl:sameAs :d . "
        "_:d1 owl:sameAs _:d1 . "
        ":d2 owl:sameAs :d2 . "

        ":e rdf:type :A . "
        ":e :R _:e1 . "
        ":e :R 2 . "
        ":e owl:sameAs :e . "
        "_:e1 owl:sameAs _:e1 . "
        "_:e1 owl:sameAs 2 . "
        "2 owl:sameAs _:e1 . "
        "2 owl:sameAs 2 . "
    );
    // Check without expanding equality.
    m_queryParameters.setString("domain", "IDBrep");
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        "owl:sameAs owl:sameAs owl:sameAs . "
        "rdf:type owl:sameAs rdf:type . "
        ":R owl:sameAs :R . "
        ":A owl:sameAs :A . "
        "owl:Nothing owl:sameAs owl:Nothing . "

        ":a rdf:type :A . "
        ":a :R :a1 . "
        ":a1 rdf:type owl:Nothing . "
        ":a owl:sameAs :a . "
        ":a1 owl:sameAs :a1 . "

        ":b rdf:type :A . "
        ":b :R :b2 . "
        ":b owl:sameAs :b . "
        ":b2 owl:sameAs :b2 . "

        ":c :R :c1 . "
        ":c :R :c2 . "
        ":c owl:sameAs :c . "
        ":c1 owl:sameAs :c1 . "
        ":c2 owl:sameAs :c2 . "

        ":d :R _:d1 . "
        ":d :R :d2 . "
        ":d owl:sameAs :d . "
        "_:d1 owl:sameAs _:d1 . "
        ":d2 owl:sameAs :d2 . "

        ":e rdf:type :A . "
        ":e :R 2 . "
        ":e owl:sameAs :e . "
        "2 owl:sameAs 2 . "
    );
}

#endif
