// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifdef WITH_TEST

#include <CppTest/TestCase.h>

#include "../querying/DataStoreTest.h"

// Define all other tests quites by copying IncrementalTest.FBF.byLevels as the template.

SUITE(IncrementalTest);

__REGISTER(IncrementalTest, IncrementalTest__FBF);
__MAKE_SUITE(IncrementalTest__FBF, "FBF");

__REGISTER(IncrementalTest__FBF, IncrementalTest__FBF__byLevels);
__MAKE_SUITE(IncrementalTest__FBF__byLevels, "byLevels");

__REGISTER(IncrementalTest, IncrementalTest__DRed);
__MAKE_SUITE(IncrementalTest__DRed, "DRed");

__REGISTER(IncrementalTest__FBF, IncrementalTest__FBF__noLevels);
__MAKE_ADAPTER(IncrementalTest__FBF__noLevels, "noLevels", IncrementalTest__FBF__byLevels) {
    testParameters.setBoolean("reason.by-Levels", false);
}

__REGISTER(IncrementalTest__DRed, IncrementalTest__DRed__byLevels);
__MAKE_ADAPTER(IncrementalTest__DRed__byLevels, "byLevels", IncrementalTest__FBF__byLevels) {
    testParameters.setBoolean("reason.use-DRed", true);
}

__REGISTER(IncrementalTest__DRed, IncrementalTest__DRed__noLevels);
__MAKE_ADAPTER(IncrementalTest__DRed__noLevels, "noLevels", IncrementalTest__FBF__byLevels) {
    testParameters.setBoolean("reason.use-DRed", true);
    testParameters.setBoolean("reason.by-Levels", false);
}

// Now define the IncrementalTest.FBF.byLevels template suite; to avoid confusion, use a neutral fixture name.

#define TEST(testName)                                                                                  \
    __REGISTER(IncrementalTest__FBF__byLevels, IncrementalTest__FBF__byLevels##testName);               \
    __MAKE_TEST(IncrementalTest__FBF__byLevels##testName, #testName, : public IncrementalTestFixture)

class IncrementalTestFixture : public DataStoreTest {

protected:

    virtual void initializeQueryParameters() {
        m_queryParameters.setBoolean("bushy", false);
        m_queryParameters.setBoolean("distinct", false);
        m_queryParameters.setBoolean("cardinality", false);
    }

public:

    IncrementalTestFixture() : DataStoreTest("par-complex-ww", EQUALITY_AXIOMATIZATION_OFF) {
    }

    void initializeEx(const CppTest::TestParameters& testParameters) {
        m_processComponentsByLevels = testParameters.getBoolean("reason.by-Levels", true);
        m_useDRed = testParameters.getBoolean("reason.use-DRed", false);
    }

};

TEST(testBasic) {
    // Initial data loading
    addRules(
        ":C(?X) :- :A(?X) . "
        ":C(?X) :- :B(?X) . "
        ":D(?X) :- :C(?X) . "
    );
    addTriples(
        ":i1 a :A . "
        ":i2 a :B . "
        ":i3 a :A . "
        ":i3 a :B . "
    );
    applyRules();
    ASSERT_QUERY("SELECT ?X WHERE { ?X a :C }",
        ":i1 . "
        ":i2 . "
        ":i3 . "
    );
    ASSERT_QUERY("SELECT ?X WHERE { ?X a :D }",
        ":i1 . "
        ":i2 . "
        ":i3 . "
    );

    // Now delete some tuples.
    forDeletion(
        ":i1 a :A . "
        ":i3 a :A . "
    );
    applyRulesIncrementally();
    ASSERT_QUERY("SELECT ?X WHERE { ?X a :C }",
        ":i2 . "
        ":i3 . "
    );
    ASSERT_QUERY("SELECT ?X WHERE { ?X a :D }",
        ":i2 . "
        ":i3 . "
    );

    // Now add and delete some tuples
    forDeletion(
        ":i2 a :B . "
    );
    forAddition(
        ":i4 a :A . "
    );
    applyRulesIncrementally();
    ASSERT_QUERY("SELECT ?X WHERE { ?X a :C }",
        ":i3 . "
        ":i4 . "
    );
    ASSERT_QUERY("SELECT ?X WHERE { ?X a :D }",
        ":i3 . "
        ":i4 . "
    );

    // Delete and add the same tuple
    forDeletion(
        ":i4 a :A . "
    );
    forAddition(
        ":i4 a :A . "
    );
    applyRulesIncrementally();
    ASSERT_QUERY("SELECT ?X WHERE { ?X a :C }",
        ":i3 . "
        ":i4 . "
    );
    ASSERT_QUERY("SELECT ?X WHERE { ?X a :D }",
        ":i3 . "
        ":i4 . "
    );
}

TEST(testCircular) {
    // Initial data loading
    addRules(
        ":B(?X) :- :A(?X) . "
        ":C(?X) :- :B(?X) . "
        ":D(?X) :- :C(?X) . "
        ":B(?X) :- :D(?X) . "
    );
    addTriples(
        ":i1 a :A . "
        ":i2 a :A . "

        ":i3 a :B . "
    );
    applyRules();
    ASSERT_QUERY("SELECT ?X WHERE { ?X a :B }",
        ":i1 . "
        ":i2 . "
        ":i3 . "
    );
    ASSERT_QUERY("SELECT ?X WHERE { ?X a :C }",
        ":i1 . "
        ":i2 . "
        ":i3 . "
    );
    ASSERT_QUERY("SELECT ?X WHERE { ?X a :D }",
        ":i1 . "
        ":i2 . "
        ":i3 . "
    );

    // Now add and delete some tuples
    forDeletion(
        ":i1 a :A . "
        ":i3 a :B . "
    );
    forAddition(
        ":i3 a :A . "
    );
    applyRulesIncrementally();
    ASSERT_QUERY("SELECT ?X WHERE { ?X a :B }",
        ":i2 . "
        ":i3 . "
    );
    ASSERT_QUERY("SELECT ?X WHERE { ?X a :C }",
        ":i2 . "
        ":i3 . "
    );
    ASSERT_QUERY("SELECT ?X WHERE { ?X a :D }",
        ":i2 . "
        ":i3 . "
    );
}

TEST(testNoChange) {
    addRules(
        ":B(?X) :- :A(?X) . "
    );
    addTriples(
        ":i1 a :A . "
        ":i2 a :B . "
    );
    applyRules();
    ASSERT_QUERY("SELECT ?X WHERE { ?X a :A }",
        ":i1 . "
    );
    ASSERT_QUERY("SELECT ?X WHERE { ?X a :B }",
        ":i1 . "
        ":i2 . "
    );

    // Perform changes that don't affect the EDB
    forDeletion(
        ":i2 a :A . "
    );
    forAddition(
        ":i2 a :B . "
    );
    applyRulesIncrementally();
    ASSERT_QUERY("SELECT ?X WHERE { ?X a :A }",
        ":i1 . "
    );
    ASSERT_QUERY("SELECT ?X WHERE { ?X a :B }",
        ":i1 . "
        ":i2 . "
    );
}

TEST(testChangeRules) {
    addRules(
        ":B(?X) :- :A(?X) . "
        ":C(?X) :- :B(?X) . "
        ":D(?X) :- :C(?X) . "
        ":E(?X) :- :D(?X) . "
        ":C(?X) :- :E(?X) . "
    );
    addTriples(
        ":i1 a :A . "
        ":i2 a :C . "
    );
    applyRules();
    ASSERT_QUERY("SELECT ?X WHERE { ?X a :E }",
        ":i1 . "
        ":i2 . "
    );
    ASSERT_QUERY("SELECT ?X WHERE { ?X a :F }",
        ""
    );

    // Change the rules
    removeRules(
        ":C(?X) :- :B(?X) . "
    );
    addRules(
        ":F(?X) :- :E(?X) . "
    );
    applyRulesIncrementally();
    ASSERT_QUERY("SELECT ?X WHERE { ?X a :E }",
        ":i2 . "
    );
    ASSERT_QUERY("SELECT ?X WHERE { ?X a :F }",
        ":i2 . "
    );
}

TEST(testBackwardChainingBug) {
    addRules(
         ":A(?X) :- :B(?X) . "
         ":C(?X) :- :A(?X) . "
         ":A(?X) :- :C(?X) . "
     );
    addTriples(
           ":i a :A . "
           ":i a :B . "
       );
    applyRules();
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
         ":i a :A . "
         ":i a :B . "
         ":i a :C . "
     );
    
    // remove :i a :A
    forDeletion(
        ":i a :A . "
    );
    applyRulesIncrementally();
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":i a :A . "
        ":i a :B . "
        ":i a :C . "
    );
}

// Tests with equality

TEST(testNonrepetitionDeletionProvedBug) {
    addRules(
        "[?S1, ?P, ?O] :- [?S, ?P, ?O], [?S, owl:sameAs, ?S1] . "
    );
    addTriples(
        ":i2 :S :i4 . "
        ":i1 owl:sameAs :i1 . "
        ":i1 owl:sameAs :i2 . "
    );
    applyRules();

    ASSERT_QUERY("SELECT ?X ?Y WHERE { ?X owl:sameAs ?Y }",
        ":i1 :i1 . "
        ":i1 :i2 . "
        ":i2 :i1 . "
        ":i2 :i2 . "
    );
    ASSERT_QUERY("SELECT ?X ?Y WHERE { ?X :S ?Y }",
        ":i1 :i4 . "
        ":i2 :i4 . "
    );

    // Now delete the equality
    forDeletion(
        ":i1 owl:sameAs :i2 . "
    );
    applyRulesIncrementally(1);
    ASSERT_QUERY("SELECT ?X ?Y WHERE { ?X owl:sameAs ?Y }",
        ":i1 :i1 . "
    );
    ASSERT_QUERY("SELECT ?X ?Y WHERE { ?X :S ?Y }",
        ":i2 :i4 . "
    );
}

TEST(testBasicEquality) {
    addRules(
        "[?S1, ?P, ?O] :- [?S, ?P, ?O], [?S, owl:sameAs, ?S1] . "
        "[?S, ?P1, ?O] :- [?S, ?P, ?O], [?P, owl:sameAs, ?P1] . "
        "[?S, ?P, ?O1] :- [?S, ?P, ?O], [?O, owl:sameAs, ?O1] . "
        "[?S, owl:sameAs, ?S] :- [?S, ?P, ?O] . "
        "[?P, owl:sameAs, ?P] :- [?S, ?P, ?O] . "
        "[?O, owl:sameAs, ?O] :- [?S, ?P, ?O] . "
        ":C(?X) :- :A(?X), :B(?X) . "
    );
    addTriples(
        ":i1 a :A . "
        ":i2 a :B . "

        ":i1 :R :i3 . "
        ":i2 :S :i4 . "
    );
    applyRules();
    ASSERT_QUERY("SELECT ?X WHERE { ?X a :C }",
        ""
    );
    ASSERT_QUERY("SELECT ?X ?Y WHERE { ?X :R ?Y }",
        ":i1 :i3 . "
    );
    ASSERT_QUERY("SELECT ?X ?Y WHERE { ?X :S ?Y }",
        ":i2 :i4 . "
    );

    // Now add the equality
    forAddition(
        ":i1 owl:sameAs :i2 . "
    );
    applyRulesIncrementally(1);
    ASSERT_QUERY("SELECT ?X WHERE { ?X a :A }",
        ":i1 . "
        ":i2 . "
    );
    ASSERT_QUERY("SELECT ?X WHERE { ?X a :B }",
        ":i1 . "
        ":i2 . "
    );
    ASSERT_QUERY("SELECT ?X WHERE { ?X a :C }",
        ":i1 . "
        ":i2 . "
    );
    ASSERT_QUERY("SELECT ?X ?Y WHERE { ?X :R ?Y }",
        ":i1 :i3 . "
        ":i2 :i3 . "
    );
    ASSERT_QUERY("SELECT ?X ?Y WHERE { ?X :S ?Y }",
        ":i1 :i4 . "
        ":i2 :i4 . "
    );

    // Now delete the equality
    forDeletion(
        ":i1 owl:sameAs :i2 . "
    );
    applyRulesIncrementally(1);
    ASSERT_QUERY("SELECT ?X WHERE { ?X a :A }",
        ":i1 . "
    );
    ASSERT_QUERY("SELECT ?X WHERE { ?X a :B }",
        ":i2 . "
    );
    ASSERT_QUERY("SELECT ?X WHERE { ?X a :C }",
        ""
    );
    ASSERT_QUERY("SELECT ?X ?Y WHERE { ?X :R ?Y }",
        ":i1 :i3 . "
    );
    ASSERT_QUERY("SELECT ?X ?Y WHERE { ?X :S ?Y }",
        ":i2 :i4 . "
    );
}

TEST(testChainEquality) {
    addRules(
         "[?S1, ?P, ?O] :- [?S, ?P, ?O], [?S, owl:sameAs, ?S1] . "
         "[?S, ?P1, ?O] :- [?S, ?P, ?O], [?P, owl:sameAs, ?P1] . "
         "[?S, ?P, ?O1] :- [?S, ?P, ?O], [?O, owl:sameAs, ?O1] . "
         "[?S, owl:sameAs, ?S] :- [?S, ?P, ?O] . "
         "[?P, owl:sameAs, ?P] :- [?S, ?P, ?O] . "
         "[?O, owl:sameAs, ?O] :- [?S, ?P, ?O] . "
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
         "[?S1, ?P, ?O] :- [?S, ?P, ?O], [?S, owl:sameAs, ?S1] . "
         "[?S, ?P1, ?O] :- [?S, ?P, ?O], [?P, owl:sameAs, ?P1] . "
         "[?S, ?P, ?O1] :- [?S, ?P, ?O], [?O, owl:sameAs, ?O1] . "
         "[?S, owl:sameAs, ?S] :- [?S, ?P, ?O] . "
         "[?P, owl:sameAs, ?P] :- [?S, ?P, ?O] . "
         "[?O, owl:sameAs, ?O] :- [?S, ?P, ?O] . "
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

    // Now apply incremental reasoning.
    forDeletion(
        ":P2 :teacherOf :C2 . "
    );
    applyRulesIncrementally(1);

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
}

TEST(testPrepareIncrementalBug) {
    // Load the rules and the data.
    addRules(
         "[?S1, ?P, ?O] :- [?S, ?P, ?O], [?S, owl:sameAs, ?S1] . "
         "[?S, ?P1, ?O] :- [?S, ?P, ?O], [?P, owl:sameAs, ?P1] . "
         "[?S, ?P, ?O1] :- [?S, ?P, ?O], [?O, owl:sameAs, ?O1] . "
         "[?S, owl:sameAs, ?S] :- [?S, ?P, ?O] . "
         "[?P, owl:sameAs, ?P] :- [?S, ?P, ?O] . "
         "[?O, owl:sameAs, ?O] :- [?S, ?P, ?O] . "
        "owl:sameAs(?X1,?X2) :- :like(?X,?X1), :like(?X,?X2) . "
    );
    addTriples(
        ":i1 :like :P . "
        ":i2 :like :S . "
        ":i2 :like :P . "
    );
    applyRules();

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

    // Now apply incremental reasoning.
    forDeletion(
        ":i2 :like :S . "
        ":i2 :like :P . "
    );
    applyRulesIncrementally(1);

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
         "[?S1, ?P, ?O] :- [?S, ?P, ?O], [?S, owl:sameAs, ?S1] . "
         "[?S, ?P1, ?O] :- [?S, ?P, ?O], [?P, owl:sameAs, ?P1] . "
         "[?S, ?P, ?O1] :- [?S, ?P, ?O], [?O, owl:sameAs, ?O1] . "
         "[?S, owl:sameAs, ?S] :- [?S, ?P, ?O] . "
         "[?P, owl:sameAs, ?P] :- [?S, ?P, ?O] . "
         "[?O, owl:sameAs, ?O] :- [?S, ?P, ?O] . "
        "owl:sameAs(?Y1,?Y2) :- :isTaughtBy(?X,?Y1), :isTaughtBy(?X,?Y2) . "
        ":isTaughtBy(?X,?Y) :- :teacherOf(?Y,?X) . "
    );
    addTriples(
        ":P :teacherOf :C . "
        ":C :isTaughtBy :S . "
    );
    applyRules();

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
    
    // Now apply incremental reasoning.
    forDeletion(
        ":P :teacherOf :C . "
    );
    forAddition(
        ":S :isFriendOf :F . "
    );
    applyRulesIncrementally(1);

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

TEST(testHeadEqualitiesBug) {
    // Load the rules and the data.
    addRules(
        ":R(?X,?X) :- :A(?X) . "
    );
    addTriples(
        ":i1 a :A . "
        ":i1 :R :i2 . "
    );
    applyRules();
    ASSERT_QUERY("SELECT ?X ?Y WHERE { ?X :R ?Y }",
        ":i1 :i1 . "
        ":i1 :i2 . "
    );

    // Now delete some tuples.
    forDeletion(
        ":i1 :R :i2 . "
    );
    applyRulesIncrementally();
    ASSERT_QUERY("SELECT ?X ?Y WHERE { ?X :R ?Y }",
        ":i1 :i1 . "
    );
}

TEST(testRuleLifeCycle) {
    // Load the rules and the data.
    addRules(
        ":B(?X) :- :A(?X) . "
    );
    addTriples(
        ":i1 a :A . "
    );
    applyRules();
    ASSERT_QUERY("SELECT ?X WHERE { ?X rdf:type :B }",
        ":i1 . "
    );

    // Now delete a non-existent rule, and delete and add a rule.
    removeRules(
        ":C(?X) :- :A(?X) . "
    );
    removeRules(
        ":B(?X) :- :A(?X) . "
    );
    addRules(
        ":B(?X) :- :A(?X) . "
    );
    applyRulesIncrementally();
    ASSERT_QUERY("SELECT ?X WHERE { ?X rdf:type :B }",
        ":i1 . "
    );

    // Now add a rule that is already there
    addRules(
        ":B(?X) :- :A(?X) . "
    );
    applyRulesIncrementally();
    ASSERT_QUERY("SELECT ?X WHERE { ?X rdf:type :B }",
        ":i1 . "
    );
}

TEST(testComponentsBug) {
    // This test is for a fixed bug in the RuleIndex rule application, where the component level of the
    // ruleInfo object wasn't checked once all of its body atoms have been mathced. Hence, for example,
    // if you have two rules with the same bodies that derive facts for different component levels, then
    // both rules will be considered in each of the component levels
    
    addRules(
        ":B(?X) :- :A(?X) . "
        ":C(?X) :- :A(?X) . "
        ":C(?X) :- :B(?X) . "
        ":C(?X) :- :D(?X) . "
        ":D(?X) :- :C(?X) . "
    );
    addTriples(
        ":a rdf:type :A . "
    );
    applyRules();
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":a rdf:type :A . "
        ":a rdf:type :B . "
        ":a rdf:type :C . "
        ":a rdf:type :D . "
    );
    forDeletion(
        ":a rdf:type :A . "
    );
    applyRulesIncrementally();
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }", "");
}

TEST(testAddPivotlessRules) {
    // Load the rules and the data.
    addRules(
        ":C(?X) :- :A(?X), :B(?X) . "
    );
    addTriples(
        ":i1 rdf:type :A . "
    );
    applyRules();
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":i1 rdf:type :A . "
    );

    // Now add some rules and facts
    addRules(
        ":B(:i1) :- . "
        ":A(:i2) :- FILTER(1 <= 1) . "
    );
    forAddition(
        ":i2 rdf:type :B . "
    );
    applyRulesIncrementally();
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":i1 rdf:type :A . "
        ":i1 rdf:type :B . "
        ":i1 rdf:type :C . "
        ":i2 rdf:type :A . "
        ":i2 rdf:type :B . "
        ":i2 rdf:type :C . "
    );
}

TEST(testPivotlessRules) {
    // Load the rules and the data.
    addRules(
        ":C(:i) :- . "
        ":B(?X) :- :A(?X) . "
        ":C(?X) :- :B(?X) . "
    );
    addTriples(
        ":i rdf:type :A . "
    );
    applyRules();
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":i rdf:type :A . "
        ":i rdf:type :B . "
        ":i rdf:type :C . "
    );

    forDeletion(
        ":i rdf:type :A . "
    );
    applyRulesIncrementally();
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":i rdf:type :C . "
    );

    removeRules(
        ":C(:i) :- . "
    );
    applyRulesIncrementally();
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ""
    );
}

TEST(testComponents1) {
    addRules(
        ":B(?X) :- :A(?X) . "
        ":C(?X) :- :B(?X) . "
        ":D(?X) :- :C(?X) . "
    );
    addTriples(
        ":a rdf:type :A . "
        ":a rdf:type :C . "
    );
    applyRules();
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
         ":a rdf:type :A . "
         ":a rdf:type :B . "
         ":a rdf:type :C . "
         ":a rdf:type :D . "
    );

    // Now apply incremental reasoning.
    forDeletion(
        ":a rdf:type :C . "
    );
    if (m_useDRed) {
        if (m_processComponentsByLevels)
            ASSERT_APPLY_RULES_INCREMENTALLY(
                "== LEVEL   0 ===============================================\n"
                "  0:    Applying deletion rules to tuples from previous levels\n"
                "  0:    Applying recursive deletion rules\n"
                "  0:    Applying insertion rules to tuples from previous levels\n"
                "  0:    Applying recursive insertion rules\n"
                "============================================================\n"
                "== LEVEL   1 ===============================================\n"
                "  0:    Applying deletion rules to tuples from previous levels\n"
                "  0:    Applying recursive deletion rules\n"
                "  0:    Applying insertion rules to tuples from previous levels\n"
                "  0:    Applying recursive insertion rules\n"
                "============================================================\n"
                "== LEVEL   2 ===============================================\n"
                "  0:    Applying deletion rules to tuples from previous levels\n"
                "  0:    Applying recursive deletion rules\n"
                "  0:        Extracted possibly deleted tuple [ :a, rdf:type, :C ]\n"
                "  0:            Applying deletion rules to [ :a, rdf:type, :C ]\n"
                "  0:    Checking provability of [ :a, rdf:type, :C ]    { added to checked }\n"
                "  0:        Checking nonrecursive rule [?X, rdf:type, :C] :- [?X, rdf:type, :B] .\n"
                "  0:            Matched nonrecursive rule instance [ :a, rdf:type, :C ] :- [ :a, rdf:type, :B ] .\n"
                "  0:    Applying insertion rules to tuples from previous levels\n"
                "  0:    Applying recursive insertion rules\n"
                "  0:        Extracted current tuple [ :a, rdf:type, :C ]\n"
                "  0:            Tuple added [ :a, rdf:type, :C ]    { added }\n"
                "============================================================\n"
                "== LEVEL   3 ===============================================\n"
                "  0:    Applying deletion rules to tuples from previous levels\n"
                "  0:    Applying recursive deletion rules\n"
                "  0:    Applying insertion rules to tuples from previous levels\n"
                "  0:    Applying recursive insertion rules\n"
                "============================================================\n"
                "  0:    Propagating deleted and proved tuples into the store for level 0\n"
                "  0:    Propagating deleted and proved tuples into the store for level 1\n"
                "  0:    Propagating deleted and proved tuples into the store for level 2\n"
                "  0:    Propagating deleted and proved tuples into the store for level 3\n"
            );
        else
            ASSERT_APPLY_RULES_INCREMENTALLY(
                "============================================================\n"
                "  0:    Applying deletion rules to tuples from previous levels\n"
                "  0:    Applying recursive deletion rules\n"
                "  0:        Extracted possibly deleted tuple [ :a, rdf:type, :C ]\n"
                "  0:            Applying deletion rules to [ :a, rdf:type, :C ]\n"
                "  0:                Matched atom [?X, rdf:type, :C] to tuple [ :a, rdf:type, :C ]\n"
                "  0:                    Matched rule [?X, rdf:type, :D] :- [?X, rdf:type, :C] .\n"
                "  0:                        Derived tuple [ :a, rdf:type, :D ]    { normal, added }\n"
                "  0:        Extracted possibly deleted tuple [ :a, rdf:type, :D ]\n"
                "  0:            Applying deletion rules to [ :a, rdf:type, :D ]\n"
                "  0:    Checking provability of [ :a, rdf:type, :C ]    { added to checked }\n"
                "  0:        Checking recursive rule [?X, rdf:type, :C] :- [?X, rdf:type, :B] .\n"
                "  0:            Matched recursive rule instance [ :a, rdf:type, :C ] :- [ :a, rdf:type, :B ] .\n"
                "  0:    Checking provability of [ :a, rdf:type, :D ]    { added to checked }\n"
                "  0:        Checking recursive rule [?X, rdf:type, :D] :- [?X, rdf:type, :C] .\n"
                "  0:    Applying insertion rules to tuples from previous levels\n"
                "  0:    Applying recursive insertion rules\n"
                "  0:        Extracted current tuple [ :a, rdf:type, :C ]\n"
                "  0:            Tuple added [ :a, rdf:type, :C ]    { added }\n"
                "  0:            Matched atom [?X, rdf:type, :C] to tuple [ :a, rdf:type, :C ]\n"
                "  0:                Matched rule [?X, rdf:type, :D] :- [?X, rdf:type, :C] .\n"
                "  0:                    Derived tuple [ :a, rdf:type, :D ]    { normal, added }\n"
                "  0:        Extracted current tuple [ :a, rdf:type, :D ]\n"
                "  0:            Tuple added [ :a, rdf:type, :D ]    { added }\n"
                "============================================================\n"
                "  0:    Propagating deleted and proved tuples into the store\n"
            );
    }
    else {
        if (m_processComponentsByLevels)
            ASSERT_APPLY_RULES_INCREMENTALLY(
                "== LEVEL   0 ===============================================\n"
                "  0:    Applying deletion rules to tuples from previous levels\n"
                "  0:    Applying recursive deletion rules\n"
                "  0:    Applying insertion rules to tuples from previous levels\n"
                "  0:    Applying recursive insertion rules\n"
                "============================================================\n"
                "== LEVEL   1 ===============================================\n"
                "  0:    Applying deletion rules to tuples from previous levels\n"
                "  0:    Applying recursive deletion rules\n"
                "  0:    Applying insertion rules to tuples from previous levels\n"
                "  0:    Applying recursive insertion rules\n"
                "============================================================\n"
                "== LEVEL   2 ===============================================\n"
                "  0:    Applying deletion rules to tuples from previous levels\n"
                "  0:    Applying recursive deletion rules\n"
                "  0:        Extracted possibly deleted tuple [ :a, rdf:type, :C ]\n"
                "  0:            Checking provability of [ :a, rdf:type, :C ]    { added to checked }\n"
                "  0:                Checking nonrecursive rule [?X, rdf:type, :C] :- [?X, rdf:type, :B] .\n"
                "  0:                    Matched nonrecursive rule instance [ :a, rdf:type, :C ] :- [ :a, rdf:type, :B ] .\n"
                "  0:                Derived tuple [ :a, rdf:type, :C ]    { not from EDB, not from delayed, from previous level, added }\n"
                "  0:                Processing the proved list\n"
                "  0:                    Extracted current tuple [ :a, rdf:type, :C ]\n"
                "  0:                Backward chaining stopped because the tuple was proved\n"
                "  0:            Possibly deleted tuple proved\n"
                "  0:    Applying insertion rules to tuples from previous levels\n"
                "  0:    Applying recursive insertion rules\n"
                "============================================================\n"
                "== LEVEL   3 ===============================================\n"
                "  0:    Applying deletion rules to tuples from previous levels\n"
                "  0:    Applying recursive deletion rules\n"
                "  0:    Applying insertion rules to tuples from previous levels\n"
                "  0:    Applying recursive insertion rules\n"
                "============================================================\n"
                "  0:    Propagating deleted and proved tuples into the store for level 0\n"
                "  0:    Propagating deleted and proved tuples into the store for level 1\n"
                "  0:    Propagating deleted and proved tuples into the store for level 2\n"
                "  0:    Propagating deleted and proved tuples into the store for level 3\n"
            );
        else
            ASSERT_APPLY_RULES_INCREMENTALLY(
                "============================================================\n"
                "  0:    Applying deletion rules to tuples from previous levels\n"
                "  0:    Applying recursive deletion rules\n"
                "  0:        Extracted possibly deleted tuple [ :a, rdf:type, :C ]\n"
                "  0:            Checking provability of [ :a, rdf:type, :C ]    { added to checked }\n"
                "  0:                Checking recursive rule [?X, rdf:type, :C] :- [?X, rdf:type, :B] .\n"
                "  0:                    Matched recursive rule instance [ :a, rdf:type, :C ] :- [ :a, rdf:type, :B ] .\n"
                "  0:                        Checking body atom [ :a, rdf:type, :B ]\n"
                "  0:                            Checking provability of [ :a, rdf:type, :B ]    { added to checked }\n"
                "  0:                                Checking recursive rule [?X, rdf:type, :B] :- [?X, rdf:type, :A] .\n"
                "  0:                                    Matched recursive rule instance [ :a, rdf:type, :B ] :- [ :a, rdf:type, :A ] .\n"
                "  0:                                        Checking body atom [ :a, rdf:type, :A ]\n"
                "  0:                                            Checking provability of [ :a, rdf:type, :A ]    { added to checked }\n"
                "  0:                                                Derived tuple [ :a, rdf:type, :A ]    { from EDB, not from delayed, not from previous level, added }\n"
                "  0:                                                Processing the proved list\n"
                "  0:                                                    Extracted current tuple [ :a, rdf:type, :A ]\n"
                "  0:                                                        Matched atom [?X, rdf:type, :A] to tuple [ :a, rdf:type, :A ]\n"
                "  0:                                                            Matched rule [?X, rdf:type, :B] :- [?X, rdf:type, :A] .\n"
                "  0:                                                                Derived tuple [ :a, rdf:type, :B ]    { normal, added }\n"
                "  0:                                                    Extracted current tuple [ :a, rdf:type, :B ]\n"
                "  0:                                                        Matched atom [?X, rdf:type, :B] to tuple [ :a, rdf:type, :B ]\n"
                "  0:                                                            Matched rule [?X, rdf:type, :C] :- [?X, rdf:type, :B] .\n"
                "  0:                                                                Derived tuple [ :a, rdf:type, :C ]    { normal, added }\n"
                "  0:                                                    Extracted current tuple [ :a, rdf:type, :C ]\n"
                "  0:                                                        Matched atom [?X, rdf:type, :C] to tuple [ :a, rdf:type, :C ]\n"
                "  0:                                                            Matched rule [?X, rdf:type, :D] :- [?X, rdf:type, :C] .\n"
                "  0:                                                                Delaying tuple [ :a, rdf:type, :D ]    { added }\n"
                "  0:                                                Backward chaining stopped because the tuple was proved\n"
                "  0:                                        Backward chaining stopped because the tuple was proved\n"
                "  0:                        Backward chaining stopped because the tuple was proved\n"
                "  0:            Possibly deleted tuple proved\n"
                "  0:    Applying insertion rules to tuples from previous levels\n"
                "  0:    Applying recursive insertion rules\n"
                "============================================================\n"
                "  0:    Propagating deleted and proved tuples into the store\n"
            );
    }
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":a rdf:type :A . "
        ":a rdf:type :B . "
        ":a rdf:type :C . "
        ":a rdf:type :D . "
    );
}

TEST(testComponents2) {
    addRules(
        ":B(?X) :- :A(?X) . "
        ":C(?X) :- :B(?X) . "
        ":D(?X) :- :C(?X) . "
        ":E(?X) :- :D(?X) . "
        ":C(?X) :- :F(?X) . "
    );
    addTriples(
        ":a rdf:type :A . "
    );
    applyRules();
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":a rdf:type :A . "
        ":a rdf:type :B . "
        ":a rdf:type :C . "
        ":a rdf:type :D . "
        ":a rdf:type :E . "
    );
    
    // Combine deletion with addition.
    forDeletion(
        ":a rdf:type :A . "
    );
    forAddition(
        ":a rdf:type :F . "
    );
    if (m_useDRed) {
        if (m_processComponentsByLevels)
            ASSERT_APPLY_RULES_INCREMENTALLY(
                "== LEVEL   0 ===============================================\n"
                "  0:    Applying deletion rules to tuples from previous levels\n"
                "  0:    Applying recursive deletion rules\n"
                "  0:        Extracted possibly deleted tuple [ :a, rdf:type, :A ]\n"
                "  0:            Applying deletion rules to [ :a, rdf:type, :A ]\n"
                "  0:    Checking provability of [ :a, rdf:type, :A ]    { added to checked }\n"
                "  0:    Applying insertion rules to tuples from previous levels\n"
                "  0:    Applying recursive insertion rules\n"
                "  0:        Extracted current tuple [ :a, rdf:type, :F ]\n"
                "  0:            Tuple added [ :a, rdf:type, :F ]    { added }\n"
                "============================================================\n"
                "== LEVEL   1 ===============================================\n"
                "  0:    Applying deletion rules to tuples from previous levels\n"
                "  0:        Applying deletion rules to [ :a, rdf:type, :A ]\n"
                "  0:            Matched atom [?X, rdf:type, :A] to tuple [ :a, rdf:type, :A ]\n"
                "  0:                Matched rule [?X, rdf:type, :B] :- [?X, rdf:type, :A] .\n"
                "  0:                    Derived tuple [ :a, rdf:type, :B ]    { normal, added }\n"
                "  0:    Applying recursive deletion rules\n"
                "  0:        Extracted possibly deleted tuple [ :a, rdf:type, :B ]\n"
                "  0:            Applying deletion rules to [ :a, rdf:type, :B ]\n"
                "  0:    Checking provability of [ :a, rdf:type, :B ]    { added to checked }\n"
                "  0:        Checking nonrecursive rule [?X, rdf:type, :B] :- [?X, rdf:type, :A] .\n"
                "  0:    Applying insertion rules to tuples from previous levels\n"
                "  0:        Extracted current tuple [ :a, rdf:type, :F ]\n"
                "  0:    Applying recursive insertion rules\n"
                "============================================================\n"
                "== LEVEL   2 ===============================================\n"
                "  0:    Applying deletion rules to tuples from previous levels\n"
                "  0:        Applying deletion rules to [ :a, rdf:type, :A ]\n"
                "  0:        Applying deletion rules to [ :a, rdf:type, :B ]\n"
                "  0:            Matched atom [?X, rdf:type, :B] to tuple [ :a, rdf:type, :B ]\n"
                "  0:                Matched rule [?X, rdf:type, :C] :- [?X, rdf:type, :B] .\n"
                "  0:                    Derived tuple [ :a, rdf:type, :C ]    { normal, added }\n"
                "  0:    Applying recursive deletion rules\n"
                "  0:        Extracted possibly deleted tuple [ :a, rdf:type, :C ]\n"
                "  0:            Applying deletion rules to [ :a, rdf:type, :C ]\n"
                "  0:    Checking provability of [ :a, rdf:type, :C ]    { added to checked }\n"
                "  0:        Checking nonrecursive rule [?X, rdf:type, :C] :- [?X, rdf:type, :F] .\n"
                "  0:        Checking nonrecursive rule [?X, rdf:type, :C] :- [?X, rdf:type, :B] .\n"
                "  0:    Applying insertion rules to tuples from previous levels\n"
                "  0:        Extracted current tuple [ :a, rdf:type, :F ]\n"
                "  0:            Matched atom [?X, rdf:type, :F] to tuple [ :a, rdf:type, :F ]\n"
                "  0:                Matched rule [?X, rdf:type, :C] :- [?X, rdf:type, :F] .\n"
                "  0:                    Derived tuple [ :a, rdf:type, :C ]    { normal, added }\n"
                "  0:    Applying recursive insertion rules\n"
                "  0:        Extracted current tuple [ :a, rdf:type, :C ]\n"
                "  0:            Tuple added [ :a, rdf:type, :C ]    { added }\n"
                "============================================================\n"
                "== LEVEL   3 ===============================================\n"
                "  0:    Applying deletion rules to tuples from previous levels\n"
                "  0:        Applying deletion rules to [ :a, rdf:type, :A ]\n"
                "  0:        Applying deletion rules to [ :a, rdf:type, :B ]\n"
                "  0:    Applying recursive deletion rules\n"
                "  0:    Applying insertion rules to tuples from previous levels\n"
                "  0:        Extracted current tuple [ :a, rdf:type, :F ]\n"
                "  0:    Applying recursive insertion rules\n"
                "============================================================\n"
                "== LEVEL   4 ===============================================\n"
                "  0:    Applying deletion rules to tuples from previous levels\n"
                "  0:        Applying deletion rules to [ :a, rdf:type, :A ]\n"
                "  0:        Applying deletion rules to [ :a, rdf:type, :B ]\n"
                "  0:    Applying recursive deletion rules\n"
                "  0:    Applying insertion rules to tuples from previous levels\n"
                "  0:        Extracted current tuple [ :a, rdf:type, :F ]\n"
                "  0:    Applying recursive insertion rules\n"
                "============================================================\n"
                "  0:    Propagating deleted and proved tuples into the store for level 0\n"
                "  0:        Tuple deleted [ :a, rdf:type, :A ]    { deleted }\n"
                "  0:        Tuple added [ :a, rdf:type, :F ]    { added }\n"
                "  0:    Propagating deleted and proved tuples into the store for level 1\n"
                "  0:        Tuple deleted [ :a, rdf:type, :B ]    { deleted }\n"
                "  0:    Propagating deleted and proved tuples into the store for level 2\n"
                "  0:    Propagating deleted and proved tuples into the store for level 3\n"
                "  0:    Propagating deleted and proved tuples into the store for level 4\n"
            );
        else
            ASSERT_APPLY_RULES_INCREMENTALLY(
                "============================================================\n"
                "  0:    Applying deletion rules to tuples from previous levels\n"
                "  0:    Applying recursive deletion rules\n"
                "  0:        Extracted possibly deleted tuple [ :a, rdf:type, :A ]\n"
                "  0:            Applying deletion rules to [ :a, rdf:type, :A ]\n"
                "  0:                Matched atom [?X, rdf:type, :A] to tuple [ :a, rdf:type, :A ]\n"
                "  0:                    Matched rule [?X, rdf:type, :B] :- [?X, rdf:type, :A] .\n"
                "  0:                        Derived tuple [ :a, rdf:type, :B ]    { normal, added }\n"
                "  0:        Extracted possibly deleted tuple [ :a, rdf:type, :B ]\n"
                "  0:            Applying deletion rules to [ :a, rdf:type, :B ]\n"
                "  0:                Matched atom [?X, rdf:type, :B] to tuple [ :a, rdf:type, :B ]\n"
                "  0:                    Matched rule [?X, rdf:type, :C] :- [?X, rdf:type, :B] .\n"
                "  0:                        Derived tuple [ :a, rdf:type, :C ]    { normal, added }\n"
                "  0:        Extracted possibly deleted tuple [ :a, rdf:type, :C ]\n"
                "  0:            Applying deletion rules to [ :a, rdf:type, :C ]\n"
                "  0:                Matched atom [?X, rdf:type, :C] to tuple [ :a, rdf:type, :C ]\n"
                "  0:                    Matched rule [?X, rdf:type, :D] :- [?X, rdf:type, :C] .\n"
                "  0:                        Derived tuple [ :a, rdf:type, :D ]    { normal, added }\n"
                "  0:        Extracted possibly deleted tuple [ :a, rdf:type, :D ]\n"
                "  0:            Applying deletion rules to [ :a, rdf:type, :D ]\n"
                "  0:                Matched atom [?X, rdf:type, :D] to tuple [ :a, rdf:type, :D ]\n"
                "  0:                    Matched rule [?X, rdf:type, :E] :- [?X, rdf:type, :D] .\n"
                "  0:                        Derived tuple [ :a, rdf:type, :E ]    { normal, added }\n"
                "  0:        Extracted possibly deleted tuple [ :a, rdf:type, :E ]\n"
                "  0:            Applying deletion rules to [ :a, rdf:type, :E ]\n"
                "  0:    Checking provability of [ :a, rdf:type, :A ]    { added to checked }\n"
                "  0:    Checking provability of [ :a, rdf:type, :B ]    { added to checked }\n"
                "  0:        Checking recursive rule [?X, rdf:type, :B] :- [?X, rdf:type, :A] .\n"
                "  0:    Checking provability of [ :a, rdf:type, :C ]    { added to checked }\n"
                "  0:        Checking recursive rule [?X, rdf:type, :C] :- [?X, rdf:type, :F] .\n"
                "  0:        Checking recursive rule [?X, rdf:type, :C] :- [?X, rdf:type, :B] .\n"
                "  0:    Checking provability of [ :a, rdf:type, :D ]    { added to checked }\n"
                "  0:        Checking recursive rule [?X, rdf:type, :D] :- [?X, rdf:type, :C] .\n"
                "  0:    Checking provability of [ :a, rdf:type, :E ]    { added to checked }\n"
                "  0:        Checking recursive rule [?X, rdf:type, :E] :- [?X, rdf:type, :D] .\n"
                "  0:    Applying insertion rules to tuples from previous levels\n"
                "  0:    Applying recursive insertion rules\n"
                "  0:        Extracted current tuple [ :a, rdf:type, :F ]\n"
                "  0:            Tuple added [ :a, rdf:type, :F ]    { added }\n"
                "  0:            Matched atom [?X, rdf:type, :F] to tuple [ :a, rdf:type, :F ]\n"
                "  0:                Matched rule [?X, rdf:type, :C] :- [?X, rdf:type, :F] .\n"
                "  0:                    Derived tuple [ :a, rdf:type, :C ]    { normal, added }\n"
                "  0:        Extracted current tuple [ :a, rdf:type, :C ]\n"
                "  0:            Tuple added [ :a, rdf:type, :C ]    { added }\n"
                "  0:            Matched atom [?X, rdf:type, :C] to tuple [ :a, rdf:type, :C ]\n"
                "  0:                Matched rule [?X, rdf:type, :D] :- [?X, rdf:type, :C] .\n"
                "  0:                    Derived tuple [ :a, rdf:type, :D ]    { normal, added }\n"
                "  0:        Extracted current tuple [ :a, rdf:type, :D ]\n"
                "  0:            Tuple added [ :a, rdf:type, :D ]    { added }\n"
                "  0:            Matched atom [?X, rdf:type, :D] to tuple [ :a, rdf:type, :D ]\n"
                "  0:                Matched rule [?X, rdf:type, :E] :- [?X, rdf:type, :D] .\n"
                "  0:                    Derived tuple [ :a, rdf:type, :E ]    { normal, added }\n"
                "  0:        Extracted current tuple [ :a, rdf:type, :E ]\n"
                "  0:            Tuple added [ :a, rdf:type, :E ]    { added }\n"
                "============================================================\n"
                "  0:    Propagating deleted and proved tuples into the store\n"
                "  0:        Tuple deleted [ :a, rdf:type, :A ]    { deleted }\n"
                "  0:        Tuple deleted [ :a, rdf:type, :B ]    { deleted }\n"
                "  0:        Tuple added [ :a, rdf:type, :F ]    { added }\n"
            );
    }
    else {
        if (m_processComponentsByLevels)
            ASSERT_APPLY_RULES_INCREMENTALLY(
                "== LEVEL   0 ===============================================\n"
                "  0:    Applying deletion rules to tuples from previous levels\n"
                "  0:    Applying recursive deletion rules\n"
                "  0:        Extracted possibly deleted tuple [ :a, rdf:type, :A ]\n"
                "  0:            Checking provability of [ :a, rdf:type, :A ]    { added to checked }\n"
                "  0:            Tuple disproved [ :a, rdf:type, :A ]    { added }\n"
                "  0:            Applying deletion rules to [ :a, rdf:type, :A ]\n"
                "  0:    Applying insertion rules to tuples from previous levels\n"
                "  0:    Applying recursive insertion rules\n"
                "  0:        Extracted current tuple [ :a, rdf:type, :F ]\n"
                "  0:            Tuple added [ :a, rdf:type, :F ]    { added }\n"
                "============================================================\n"
                "== LEVEL   1 ===============================================\n"
                "  0:    Applying deletion rules to tuples from previous levels\n"
                "  0:        Applying deletion rules to [ :a, rdf:type, :A ]\n"
                "  0:            Matched atom [?X, rdf:type, :A] to tuple [ :a, rdf:type, :A ]\n"
                "  0:                Matched rule [?X, rdf:type, :B] :- [?X, rdf:type, :A] .\n"
                "  0:                    Derived tuple [ :a, rdf:type, :B ]    { normal, added }\n"
                "  0:    Applying recursive deletion rules\n"
                "  0:        Extracted possibly deleted tuple [ :a, rdf:type, :B ]\n"
                "  0:            Checking provability of [ :a, rdf:type, :B ]    { added to checked }\n"
                "  0:                Checking nonrecursive rule [?X, rdf:type, :B] :- [?X, rdf:type, :A] .\n"
                "  0:            Tuple disproved [ :a, rdf:type, :B ]    { added }\n"
                "  0:            Applying deletion rules to [ :a, rdf:type, :B ]\n"
                "  0:    Applying insertion rules to tuples from previous levels\n"
                "  0:        Extracted current tuple [ :a, rdf:type, :F ]\n"
                "  0:    Applying recursive insertion rules\n"
                "============================================================\n"
                "== LEVEL   2 ===============================================\n"
                "  0:    Applying deletion rules to tuples from previous levels\n"
                "  0:        Applying deletion rules to [ :a, rdf:type, :A ]\n"
                "  0:        Applying deletion rules to [ :a, rdf:type, :B ]\n"
                "  0:            Matched atom [?X, rdf:type, :B] to tuple [ :a, rdf:type, :B ]\n"
                "  0:                Matched rule [?X, rdf:type, :C] :- [?X, rdf:type, :B] .\n"
                "  0:                    Derived tuple [ :a, rdf:type, :C ]    { normal, added }\n"
                "  0:    Applying recursive deletion rules\n"
                "  0:        Extracted possibly deleted tuple [ :a, rdf:type, :C ]\n"
                "  0:            Checking provability of [ :a, rdf:type, :C ]    { added to checked }\n"
                "  0:                Checking nonrecursive rule [?X, rdf:type, :C] :- [?X, rdf:type, :F] .\n"
                "  0:                Checking nonrecursive rule [?X, rdf:type, :C] :- [?X, rdf:type, :B] .\n"
                "  0:            Tuple disproved [ :a, rdf:type, :C ]    { added }\n"
                "  0:            Applying deletion rules to [ :a, rdf:type, :C ]\n"
                "  0:    Applying insertion rules to tuples from previous levels\n"
                "  0:        Extracted current tuple [ :a, rdf:type, :F ]\n"
                "  0:            Matched atom [?X, rdf:type, :F] to tuple [ :a, rdf:type, :F ]\n"
                "  0:                Matched rule [?X, rdf:type, :C] :- [?X, rdf:type, :F] .\n"
                "  0:                    Derived tuple [ :a, rdf:type, :C ]    { normal, added }\n"
                "  0:    Applying recursive insertion rules\n"
                "  0:        Extracted current tuple [ :a, rdf:type, :C ]\n"
                "  0:            Tuple added [ :a, rdf:type, :C ]    { added }\n"
                "============================================================\n"
                "== LEVEL   3 ===============================================\n"
                "  0:    Applying deletion rules to tuples from previous levels\n"
                "  0:        Applying deletion rules to [ :a, rdf:type, :A ]\n"
                "  0:        Applying deletion rules to [ :a, rdf:type, :B ]\n"
                "  0:    Applying recursive deletion rules\n"
                "  0:    Applying insertion rules to tuples from previous levels\n"
                "  0:        Extracted current tuple [ :a, rdf:type, :F ]\n"
                "  0:    Applying recursive insertion rules\n"
                "============================================================\n"
                "== LEVEL   4 ===============================================\n"
                "  0:    Applying deletion rules to tuples from previous levels\n"
                "  0:        Applying deletion rules to [ :a, rdf:type, :A ]\n"
                "  0:        Applying deletion rules to [ :a, rdf:type, :B ]\n"
                "  0:    Applying recursive deletion rules\n"
                "  0:    Applying insertion rules to tuples from previous levels\n"
                "  0:        Extracted current tuple [ :a, rdf:type, :F ]\n"
                "  0:    Applying recursive insertion rules\n"
                "============================================================\n"
                "  0:    Propagating deleted and proved tuples into the store for level 0\n"
                "  0:        Tuple deleted [ :a, rdf:type, :A ]    { deleted }\n"
                "  0:        Tuple added [ :a, rdf:type, :F ]    { added }\n"
                "  0:    Propagating deleted and proved tuples into the store for level 1\n"
                "  0:        Tuple deleted [ :a, rdf:type, :B ]    { deleted }\n"
                "  0:    Propagating deleted and proved tuples into the store for level 2\n"
                "  0:    Propagating deleted and proved tuples into the store for level 3\n"
                "  0:    Propagating deleted and proved tuples into the store for level 4\n"
            );
        else
            ASSERT_APPLY_RULES_INCREMENTALLY(
                "============================================================\n"
                "  0:    Applying deletion rules to tuples from previous levels\n"
                "  0:    Applying recursive deletion rules\n"
                "  0:        Extracted possibly deleted tuple [ :a, rdf:type, :A ]\n"
                "  0:            Checking provability of [ :a, rdf:type, :A ]    { added to checked }\n"
                "  0:            Tuple disproved [ :a, rdf:type, :A ]    { added }\n"
                "  0:            Applying deletion rules to [ :a, rdf:type, :A ]\n"
                "  0:                Matched atom [?X, rdf:type, :A] to tuple [ :a, rdf:type, :A ]\n"
                "  0:                    Matched rule [?X, rdf:type, :B] :- [?X, rdf:type, :A] .\n"
                "  0:                        Derived tuple [ :a, rdf:type, :B ]    { normal, added }\n"
                "  0:        Extracted possibly deleted tuple [ :a, rdf:type, :B ]\n"
                "  0:            Checking provability of [ :a, rdf:type, :B ]    { added to checked }\n"
                "  0:                Checking recursive rule [?X, rdf:type, :B] :- [?X, rdf:type, :A] .\n"
                "  0:            Tuple disproved [ :a, rdf:type, :B ]    { added }\n"
                "  0:            Applying deletion rules to [ :a, rdf:type, :B ]\n"
                "  0:                Matched atom [?X, rdf:type, :B] to tuple [ :a, rdf:type, :B ]\n"
                "  0:                    Matched rule [?X, rdf:type, :C] :- [?X, rdf:type, :B] .\n"
                "  0:                        Derived tuple [ :a, rdf:type, :C ]    { normal, added }\n"
                "  0:        Extracted possibly deleted tuple [ :a, rdf:type, :C ]\n"
                "  0:            Checking provability of [ :a, rdf:type, :C ]    { added to checked }\n"
                "  0:                Checking recursive rule [?X, rdf:type, :C] :- [?X, rdf:type, :F] .\n"
                "  0:                Checking recursive rule [?X, rdf:type, :C] :- [?X, rdf:type, :B] .\n"
                "  0:            Tuple disproved [ :a, rdf:type, :C ]    { added }\n"
                "  0:            Applying deletion rules to [ :a, rdf:type, :C ]\n"
                "  0:                Matched atom [?X, rdf:type, :C] to tuple [ :a, rdf:type, :C ]\n"
                "  0:                    Matched rule [?X, rdf:type, :D] :- [?X, rdf:type, :C] .\n"
                "  0:                        Derived tuple [ :a, rdf:type, :D ]    { normal, added }\n"
                "  0:        Extracted possibly deleted tuple [ :a, rdf:type, :D ]\n"
                "  0:            Checking provability of [ :a, rdf:type, :D ]    { added to checked }\n"
                "  0:                Checking recursive rule [?X, rdf:type, :D] :- [?X, rdf:type, :C] .\n"
                "  0:            Tuple disproved [ :a, rdf:type, :D ]    { added }\n"
                "  0:            Applying deletion rules to [ :a, rdf:type, :D ]\n"
                "  0:                Matched atom [?X, rdf:type, :D] to tuple [ :a, rdf:type, :D ]\n"
                "  0:                    Matched rule [?X, rdf:type, :E] :- [?X, rdf:type, :D] .\n"
                "  0:                        Derived tuple [ :a, rdf:type, :E ]    { normal, added }\n"
                "  0:        Extracted possibly deleted tuple [ :a, rdf:type, :E ]\n"
                "  0:            Checking provability of [ :a, rdf:type, :E ]    { added to checked }\n"
                "  0:                Checking recursive rule [?X, rdf:type, :E] :- [?X, rdf:type, :D] .\n"
                "  0:            Tuple disproved [ :a, rdf:type, :E ]    { added }\n"
                "  0:            Applying deletion rules to [ :a, rdf:type, :E ]\n"
                "  0:    Applying insertion rules to tuples from previous levels\n"
                "  0:    Applying recursive insertion rules\n"
                "  0:        Extracted current tuple [ :a, rdf:type, :F ]\n"
                "  0:            Tuple added [ :a, rdf:type, :F ]    { added }\n"
                "  0:            Matched atom [?X, rdf:type, :F] to tuple [ :a, rdf:type, :F ]\n"
                "  0:                Matched rule [?X, rdf:type, :C] :- [?X, rdf:type, :F] .\n"
                "  0:                    Derived tuple [ :a, rdf:type, :C ]    { normal, added }\n"
                "  0:        Extracted current tuple [ :a, rdf:type, :C ]\n"
                "  0:            Tuple added [ :a, rdf:type, :C ]    { added }\n"
                "  0:            Matched atom [?X, rdf:type, :C] to tuple [ :a, rdf:type, :C ]\n"
                "  0:                Matched rule [?X, rdf:type, :D] :- [?X, rdf:type, :C] .\n"
                "  0:                    Derived tuple [ :a, rdf:type, :D ]    { normal, added }\n"
                "  0:        Extracted current tuple [ :a, rdf:type, :D ]\n"
                "  0:            Tuple added [ :a, rdf:type, :D ]    { added }\n"
                "  0:            Matched atom [?X, rdf:type, :D] to tuple [ :a, rdf:type, :D ]\n"
                "  0:                Matched rule [?X, rdf:type, :E] :- [?X, rdf:type, :D] .\n"
                "  0:                    Derived tuple [ :a, rdf:type, :E ]    { normal, added }\n"
                "  0:        Extracted current tuple [ :a, rdf:type, :E ]\n"
                "  0:            Tuple added [ :a, rdf:type, :E ]    { added }\n"
                "============================================================\n"
                "  0:    Propagating deleted and proved tuples into the store\n"
                "  0:        Tuple deleted [ :a, rdf:type, :A ]    { deleted }\n"
                "  0:        Tuple deleted [ :a, rdf:type, :B ]    { deleted }\n"
                "  0:        Tuple added [ :a, rdf:type, :F ]    { added }\n"
            );
    }
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":a rdf:type :F . "
        ":a rdf:type :C . "
        ":a rdf:type :D . "
        ":a rdf:type :E . "
    );
}

TEST(testComponentsRecursive) {
    addRules(
        ":B(?X) :- :A(?X) . "
        ":B(?Y) :- :B(?X), :R(?X,?Y) . "
    );
    addTriples(
        ":i1 :R :i2 . "
        ":i2 :R :i3 . "
        ":i3 :R :i4 . "
        ":i4 :R :i5 . "

        ":i1 rdf:type :A . "
        ":i3 rdf:type :A . "
    );
    applyRules();
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":i1 :R :i2 . "
        ":i2 :R :i3 . "
        ":i3 :R :i4 . "
        ":i4 :R :i5 . "

        ":i1 rdf:type :A . "
        ":i3 rdf:type :A . "

        ":i1 rdf:type :B . "
        ":i2 rdf:type :B . "
        ":i3 rdf:type :B . "
        ":i4 rdf:type :B . "
        ":i5 rdf:type :B . "
    );
    // Now remove some redundant derivations in the chain.
    forDeletion(
        ":i3 rdf:type :A . "
    );
    if (m_useDRed) {
        if (m_processComponentsByLevels)
            ASSERT_APPLY_RULES_INCREMENTALLY(
                "== LEVEL   0 ===============================================\n"
                "  0:    Applying deletion rules to tuples from previous levels\n"
                "  0:    Applying recursive deletion rules\n"
                "  0:        Extracted possibly deleted tuple [ :i3, rdf:type, :A ]\n"
                "  0:            Applying deletion rules to [ :i3, rdf:type, :A ]\n"
                "  0:    Checking provability of [ :i3, rdf:type, :A ]    { added to checked }\n"
                "  0:    Applying insertion rules to tuples from previous levels\n"
                "  0:    Applying recursive insertion rules\n"
                "============================================================\n"
                "== LEVEL   1 ===============================================\n"
                "  0:    Applying deletion rules to tuples from previous levels\n"
                "  0:        Applying deletion rules to [ :i3, rdf:type, :A ]\n"
                "  0:            Matched atom [?X, rdf:type, :A] to tuple [ :i3, rdf:type, :A ]\n"
                "  0:                Matched rule [?X, rdf:type, :B] :- [?X, rdf:type, :A] .\n"
                "  0:                    Derived tuple [ :i3, rdf:type, :B ]    { normal, added }\n"
                "  0:    Applying recursive deletion rules\n"
                "  0:        Extracted possibly deleted tuple [ :i3, rdf:type, :B ]\n"
                "  0:            Applying deletion rules to [ :i3, rdf:type, :B ]\n"
                "  0:                Matched atom [?X, rdf:type, :B] to tuple [ :i3, rdf:type, :B ]\n"
                "  0:                    Matching atom [?X, :R, ?Y]\n"
                "  0:                        Matched atom [?X, :R, ?Y] to tuple [ :i3, :R, :i4 ]\n"
                "  0:                            Matched rule [?Y, rdf:type, :B] :- [?X, rdf:type, :B], [?X, :R, ?Y] .\n"
                "  0:                                Derived tuple [ :i4, rdf:type, :B ]    { normal, added }\n"
                "  0:        Extracted possibly deleted tuple [ :i4, rdf:type, :B ]\n"
                "  0:            Applying deletion rules to [ :i4, rdf:type, :B ]\n"
                "  0:                Matched atom [?X, rdf:type, :B] to tuple [ :i4, rdf:type, :B ]\n"
                "  0:                    Matching atom [?X, :R, ?Y]\n"
                "  0:                        Matched atom [?X, :R, ?Y] to tuple [ :i4, :R, :i5 ]\n"
                "  0:                            Matched rule [?Y, rdf:type, :B] :- [?X, rdf:type, :B], [?X, :R, ?Y] .\n"
                "  0:                                Derived tuple [ :i5, rdf:type, :B ]    { normal, added }\n"
                "  0:        Extracted possibly deleted tuple [ :i5, rdf:type, :B ]\n"
                "  0:            Applying deletion rules to [ :i5, rdf:type, :B ]\n"
                "  0:                Matched atom [?X, rdf:type, :B] to tuple [ :i5, rdf:type, :B ]\n"
                "  0:                    Matching atom [?X, :R, ?Y]\n"
                "  0:    Checking provability of [ :i3, rdf:type, :B ]    { added to checked }\n"
                "  0:        Checking recursive rule [?Y, rdf:type, :B] :- [?X, rdf:type, :B], [?X, :R, ?Y] .\n"
                "  0:            Matched recursive rule instance [ :i3, rdf:type, :B ] :- [ :i2, :R, :i3 ], [ :i2, rdf:type, :B ]* .\n"
                "  0:    Checking provability of [ :i4, rdf:type, :B ]    { added to checked }\n"
                "  0:        Checking recursive rule [?Y, rdf:type, :B] :- [?X, rdf:type, :B], [?X, :R, ?Y] .\n"
                "  0:        Checking nonrecursive rule [?X, rdf:type, :B] :- [?X, rdf:type, :A] .\n"
                "  0:    Checking provability of [ :i5, rdf:type, :B ]    { added to checked }\n"
                "  0:        Checking recursive rule [?Y, rdf:type, :B] :- [?X, rdf:type, :B], [?X, :R, ?Y] .\n"
                "  0:        Checking nonrecursive rule [?X, rdf:type, :B] :- [?X, rdf:type, :A] .\n"
                "  0:    Applying insertion rules to tuples from previous levels\n"
                "  0:    Applying recursive insertion rules\n"
                "  0:        Extracted current tuple [ :i3, rdf:type, :B ]\n"
                "  0:            Tuple added [ :i3, rdf:type, :B ]    { added }\n"
                "  0:            Matched atom [?X, rdf:type, :B] to tuple [ :i3, rdf:type, :B ]\n"
                "  0:                Matching atom [?X, :R, ?Y]\n"
                "  0:                    Matched atom [?X, :R, ?Y] to tuple [ :i3, :R, :i4 ]\n"
                "  0:                        Matched rule [?Y, rdf:type, :B] :- [?X, rdf:type, :B], [?X, :R, ?Y] .\n"
                "  0:                            Derived tuple [ :i4, rdf:type, :B ]    { normal, added }\n"
                "  0:        Extracted current tuple [ :i4, rdf:type, :B ]\n"
                "  0:            Tuple added [ :i4, rdf:type, :B ]    { added }\n"
                "  0:            Matched atom [?X, rdf:type, :B] to tuple [ :i4, rdf:type, :B ]\n"
                "  0:                Matching atom [?X, :R, ?Y]\n"
                "  0:                    Matched atom [?X, :R, ?Y] to tuple [ :i4, :R, :i5 ]\n"
                "  0:                        Matched rule [?Y, rdf:type, :B] :- [?X, rdf:type, :B], [?X, :R, ?Y] .\n"
                "  0:                            Derived tuple [ :i5, rdf:type, :B ]    { normal, added }\n"
                "  0:        Extracted current tuple [ :i5, rdf:type, :B ]\n"
                "  0:            Tuple added [ :i5, rdf:type, :B ]    { added }\n"
                "  0:            Matched atom [?X, rdf:type, :B] to tuple [ :i5, rdf:type, :B ]\n"
                "  0:                Matching atom [?X, :R, ?Y]\n"
                "============================================================\n"
                "  0:    Propagating deleted and proved tuples into the store for level 0\n"
                "  0:        Tuple deleted [ :i3, rdf:type, :A ]    { deleted }\n"
                "  0:    Propagating deleted and proved tuples into the store for level 1\n"
            );
        else
            ASSERT_APPLY_RULES_INCREMENTALLY(
                "============================================================\n"
                "  0:    Applying deletion rules to tuples from previous levels\n"
                "  0:    Applying recursive deletion rules\n"
                "  0:        Extracted possibly deleted tuple [ :i3, rdf:type, :A ]\n"
                "  0:            Applying deletion rules to [ :i3, rdf:type, :A ]\n"
                "  0:                Matched atom [?X, rdf:type, :A] to tuple [ :i3, rdf:type, :A ]\n"
                "  0:                    Matched rule [?X, rdf:type, :B] :- [?X, rdf:type, :A] .\n"
                "  0:                        Derived tuple [ :i3, rdf:type, :B ]    { normal, added }\n"
                "  0:        Extracted possibly deleted tuple [ :i3, rdf:type, :B ]\n"
                "  0:            Applying deletion rules to [ :i3, rdf:type, :B ]\n"
                "  0:                Matched atom [?X, rdf:type, :B] to tuple [ :i3, rdf:type, :B ]\n"
                "  0:                    Matching atom [?X, :R, ?Y]\n"
                "  0:                        Matched atom [?X, :R, ?Y] to tuple [ :i3, :R, :i4 ]\n"
                "  0:                            Matched rule [?Y, rdf:type, :B] :- [?X, rdf:type, :B], [?X, :R, ?Y] .\n"
                "  0:                                Derived tuple [ :i4, rdf:type, :B ]    { normal, added }\n"
                "  0:        Extracted possibly deleted tuple [ :i4, rdf:type, :B ]\n"
                "  0:            Applying deletion rules to [ :i4, rdf:type, :B ]\n"
                "  0:                Matched atom [?X, rdf:type, :B] to tuple [ :i4, rdf:type, :B ]\n"
                "  0:                    Matching atom [?X, :R, ?Y]\n"
                "  0:                        Matched atom [?X, :R, ?Y] to tuple [ :i4, :R, :i5 ]\n"
                "  0:                            Matched rule [?Y, rdf:type, :B] :- [?X, rdf:type, :B], [?X, :R, ?Y] .\n"
                "  0:                                Derived tuple [ :i5, rdf:type, :B ]    { normal, added }\n"
                "  0:        Extracted possibly deleted tuple [ :i5, rdf:type, :B ]\n"
                "  0:            Applying deletion rules to [ :i5, rdf:type, :B ]\n"
                "  0:                Matched atom [?X, rdf:type, :B] to tuple [ :i5, rdf:type, :B ]\n"
                "  0:                    Matching atom [?X, :R, ?Y]\n"
                "  0:    Checking provability of [ :i3, rdf:type, :A ]    { added to checked }\n"
                "  0:    Checking provability of [ :i3, rdf:type, :B ]    { added to checked }\n"
                "  0:        Checking recursive rule [?Y, rdf:type, :B] :- [?X, rdf:type, :B], [?X, :R, ?Y] .\n"
                "  0:            Matched recursive rule instance [ :i3, rdf:type, :B ] :- [ :i2, :R, :i3 ], [ :i2, rdf:type, :B ]* .\n"
                "  0:    Checking provability of [ :i4, rdf:type, :B ]    { added to checked }\n"
                "  0:        Checking recursive rule [?Y, rdf:type, :B] :- [?X, rdf:type, :B], [?X, :R, ?Y] .\n"
                "  0:        Checking recursive rule [?X, rdf:type, :B] :- [?X, rdf:type, :A] .\n"
                "  0:    Checking provability of [ :i5, rdf:type, :B ]    { added to checked }\n"
                "  0:        Checking recursive rule [?Y, rdf:type, :B] :- [?X, rdf:type, :B], [?X, :R, ?Y] .\n"
                "  0:        Checking recursive rule [?X, rdf:type, :B] :- [?X, rdf:type, :A] .\n"
                "  0:    Applying insertion rules to tuples from previous levels\n"
                "  0:    Applying recursive insertion rules\n"
                "  0:        Extracted current tuple [ :i3, rdf:type, :B ]\n"
                "  0:            Tuple added [ :i3, rdf:type, :B ]    { added }\n"
                "  0:            Matched atom [?X, rdf:type, :B] to tuple [ :i3, rdf:type, :B ]\n"
                "  0:                Matching atom [?X, :R, ?Y]\n"
                "  0:                    Matched atom [?X, :R, ?Y] to tuple [ :i3, :R, :i4 ]\n"
                "  0:                        Matched rule [?Y, rdf:type, :B] :- [?X, rdf:type, :B], [?X, :R, ?Y] .\n"
                "  0:                            Derived tuple [ :i4, rdf:type, :B ]    { normal, added }\n"
                "  0:        Extracted current tuple [ :i4, rdf:type, :B ]\n"
                "  0:            Tuple added [ :i4, rdf:type, :B ]    { added }\n"
                "  0:            Matched atom [?X, rdf:type, :B] to tuple [ :i4, rdf:type, :B ]\n"
                "  0:                Matching atom [?X, :R, ?Y]\n"
                "  0:                    Matched atom [?X, :R, ?Y] to tuple [ :i4, :R, :i5 ]\n"
                "  0:                        Matched rule [?Y, rdf:type, :B] :- [?X, rdf:type, :B], [?X, :R, ?Y] .\n"
                "  0:                            Derived tuple [ :i5, rdf:type, :B ]    { normal, added }\n"
                "  0:        Extracted current tuple [ :i5, rdf:type, :B ]\n"
                "  0:            Tuple added [ :i5, rdf:type, :B ]    { added }\n"
                "  0:            Matched atom [?X, rdf:type, :B] to tuple [ :i5, rdf:type, :B ]\n"
                "  0:                Matching atom [?X, :R, ?Y]\n"
                "============================================================\n"
                "  0:    Propagating deleted and proved tuples into the store\n"
                "  0:        Tuple deleted [ :i3, rdf:type, :A ]    { deleted }\n"
            );
    }
    else {
        if (m_processComponentsByLevels)
            ASSERT_APPLY_RULES_INCREMENTALLY(
                "== LEVEL   0 ===============================================\n"
                "  0:    Applying deletion rules to tuples from previous levels\n"
                "  0:    Applying recursive deletion rules\n"
                "  0:        Extracted possibly deleted tuple [ :i3, rdf:type, :A ]\n"
                "  0:            Checking provability of [ :i3, rdf:type, :A ]    { added to checked }\n"
                "  0:            Tuple disproved [ :i3, rdf:type, :A ]    { added }\n"
                "  0:            Applying deletion rules to [ :i3, rdf:type, :A ]\n"
                "  0:    Applying insertion rules to tuples from previous levels\n"
                "  0:    Applying recursive insertion rules\n"
                "============================================================\n"
                "== LEVEL   1 ===============================================\n"
                "  0:    Applying deletion rules to tuples from previous levels\n"
                "  0:        Applying deletion rules to [ :i3, rdf:type, :A ]\n"
                "  0:            Matched atom [?X, rdf:type, :A] to tuple [ :i3, rdf:type, :A ]\n"
                "  0:                Matched rule [?X, rdf:type, :B] :- [?X, rdf:type, :A] .\n"
                "  0:                    Derived tuple [ :i3, rdf:type, :B ]    { normal, added }\n"
                "  0:    Applying recursive deletion rules\n"
                "  0:        Extracted possibly deleted tuple [ :i3, rdf:type, :B ]\n"
                "  0:            Checking provability of [ :i3, rdf:type, :B ]    { added to checked }\n"
                "  0:                Checking nonrecursive rule [?X, rdf:type, :B] :- [?X, rdf:type, :A] .\n"
                "  0:                Checking recursive rule [?Y, rdf:type, :B] :- [?X, rdf:type, :B], [?X, :R, ?Y] .\n"
                "  0:                    Matched recursive rule instance [ :i3, rdf:type, :B ] :- [ :i2, :R, :i3 ], [ :i2, rdf:type, :B ]* .\n"
                "  0:                        Checking body atom [ :i2, rdf:type, :B ]\n"
                "  0:                            Checking provability of [ :i2, rdf:type, :B ]    { added to checked }\n"
                "  0:                                Checking nonrecursive rule [?X, rdf:type, :B] :- [?X, rdf:type, :A] .\n"
                "  0:                                Checking recursive rule [?Y, rdf:type, :B] :- [?X, rdf:type, :B], [?X, :R, ?Y] .\n"
                "  0:                                    Matched recursive rule instance [ :i2, rdf:type, :B ] :- [ :i1, :R, :i2 ], [ :i1, rdf:type, :B ]* .\n"
                "  0:                                        Checking body atom [ :i1, rdf:type, :B ]\n"
                "  0:                                            Checking provability of [ :i1, rdf:type, :B ]    { added to checked }\n"
                "  0:                                                Checking nonrecursive rule [?X, rdf:type, :B] :- [?X, rdf:type, :A] .\n"
                "  0:                                                    Matched nonrecursive rule instance [ :i1, rdf:type, :B ] :- [ :i1, rdf:type, :A ] .\n"
                "  0:                                                Derived tuple [ :i1, rdf:type, :B ]    { not from EDB, not from delayed, from previous level, added }\n"
                "  0:                                                Processing the proved list\n"
                "  0:                                                    Extracted current tuple [ :i1, rdf:type, :B ]\n"
                "  0:                                                        Matched atom [?X, rdf:type, :B] to tuple [ :i3, rdf:type, :B ]\n"
                "  0:                                                            Matching atom [?X, :R, ?Y]\n"
                "  0:                                                                Matched atom [?X, :R, ?Y] to tuple [ :i3, :R, :i5 ]\n"
                "  0:                                                                    Matched rule [?Y, rdf:type, :B] :- [?X, rdf:type, :B], [?X, :R, ?Y] .\n"
                "  0:                                                                        Derived tuple [ :i2, rdf:type, :B ]    { normal, added }\n"
                "  0:                                                    Extracted current tuple [ :i2, rdf:type, :B ]\n"
                "  0:                                                        Matched atom [?X, rdf:type, :B] to tuple [ :i3, rdf:type, :B ]\n"
                "  0:                                                            Matching atom [?X, :R, ?Y]\n"
                "  0:                                                                Matched atom [?X, :R, ?Y] to tuple [ :i3, :R, :i5 ]\n"
                "  0:                                                                    Matched rule [?Y, rdf:type, :B] :- [?X, rdf:type, :B], [?X, :R, ?Y] .\n"
                "  0:                                                                        Derived tuple [ :i3, rdf:type, :B ]    { normal, added }\n"
                "  0:                                                    Extracted current tuple [ :i3, rdf:type, :B ]\n"
                "  0:                                                        Matched atom [?X, rdf:type, :B] to tuple [ :i3, rdf:type, :B ]\n"
                "  0:                                                            Matching atom [?X, :R, ?Y]\n"
                "  0:                                                                Matched atom [?X, :R, ?Y] to tuple [ :i3, :R, :i5 ]\n"
                "  0:                                                                    Matched rule [?Y, rdf:type, :B] :- [?X, rdf:type, :B], [?X, :R, ?Y] .\n"
                "  0:                                                                        Delaying tuple [ :i4, rdf:type, :B ]    { added }\n"
                "  0:                                                Backward chaining stopped because the tuple was proved\n"
                "  0:                                        Backward chaining stopped because the tuple was proved\n"
                "  0:                        Backward chaining stopped because the tuple was proved\n"
                "  0:            Possibly deleted tuple proved\n"
                "  0:    Applying insertion rules to tuples from previous levels\n"
                "  0:    Applying recursive insertion rules\n"
                "============================================================\n"
                "  0:    Propagating deleted and proved tuples into the store for level 0\n"
                "  0:        Tuple deleted [ :i3, rdf:type, :A ]    { deleted }\n"
                "  0:    Propagating deleted and proved tuples into the store for level 1\n"
            );
        else
            ASSERT_APPLY_RULES_INCREMENTALLY(
                "============================================================\n"
                "  0:    Applying deletion rules to tuples from previous levels\n"
                "  0:    Applying recursive deletion rules\n"
                "  0:        Extracted possibly deleted tuple [ :i3, rdf:type, :A ]\n"
                "  0:            Checking provability of [ :i3, rdf:type, :A ]    { added to checked }\n"
                "  0:            Tuple disproved [ :i3, rdf:type, :A ]    { added }\n"
                "  0:            Applying deletion rules to [ :i3, rdf:type, :A ]\n"
                "  0:                Matched atom [?X, rdf:type, :A] to tuple [ :i3, rdf:type, :A ]\n"
                "  0:                    Matched rule [?X, rdf:type, :B] :- [?X, rdf:type, :A] .\n"
                "  0:                        Derived tuple [ :i3, rdf:type, :B ]    { normal, added }\n"
                "  0:        Extracted possibly deleted tuple [ :i3, rdf:type, :B ]\n"
                "  0:            Checking provability of [ :i3, rdf:type, :B ]    { added to checked }\n"
                "  0:                Checking recursive rule [?Y, rdf:type, :B] :- [?X, rdf:type, :B], [?X, :R, ?Y] .\n"
                "  0:                    Matched recursive rule instance [ :i3, rdf:type, :B ] :- [ :i2, :R, :i3 ], [ :i2, rdf:type, :B ]* .\n"
                "  0:                        Checking body atom [ :i2, :R, :i3 ]\n"
                "  0:                            Checking provability of [ :i2, :R, :i3 ]    { added to checked }\n"
                "  0:                                Derived tuple [ :i2, :R, :i3 ]    { from EDB, not from delayed, not from previous level, added }\n"
                "  0:                                Processing the proved list\n"
                "  0:                                    Extracted current tuple [ :i2, :R, :i3 ]\n"
                "  0:                                        Matched atom [?X, :R, ?Y] to tuple [ :i3, :R, :i5 ]\n"
                "  0:                                            Matching atom [?X, rdf:type, :B]\n"
                "  0:                                Backward chaining stopped because the tuple was proved\n"
                "  0:                        Checking body atom [ :i2, rdf:type, :B ]\n"
                "  0:                            Checking provability of [ :i2, rdf:type, :B ]    { added to checked }\n"
                "  0:                                Checking recursive rule [?Y, rdf:type, :B] :- [?X, rdf:type, :B], [?X, :R, ?Y] .\n"
                "  0:                                    Matched recursive rule instance [ :i2, rdf:type, :B ] :- [ :i1, :R, :i2 ], [ :i1, rdf:type, :B ]* .\n"
                "  0:                                        Checking body atom [ :i1, :R, :i2 ]\n"
                "  0:                                            Checking provability of [ :i1, :R, :i2 ]    { added to checked }\n"
                "  0:                                                Derived tuple [ :i1, :R, :i2 ]    { from EDB, not from delayed, not from previous level, added }\n"
                "  0:                                                Processing the proved list\n"
                "  0:                                                    Extracted current tuple [ :i1, :R, :i2 ]\n"
                "  0:                                                        Matched atom [?X, :R, ?Y] to tuple [ :i3, :R, :i5 ]\n"
                "  0:                                                            Matching atom [?X, rdf:type, :B]\n"
                "  0:                                                Backward chaining stopped because the tuple was proved\n"
                "  0:                                        Checking body atom [ :i1, rdf:type, :B ]\n"
                "  0:                                            Checking provability of [ :i1, rdf:type, :B ]    { added to checked }\n"
                "  0:                                                Checking recursive rule [?Y, rdf:type, :B] :- [?X, rdf:type, :B], [?X, :R, ?Y] .\n"
                "  0:                                                Checking recursive rule [?X, rdf:type, :B] :- [?X, rdf:type, :A] .\n"
                "  0:                                                    Matched recursive rule instance [ :i1, rdf:type, :B ] :- [ :i1, rdf:type, :A ] .\n"
                "  0:                                                        Checking body atom [ :i1, rdf:type, :A ]\n"
                "  0:                                                            Checking provability of [ :i1, rdf:type, :A ]    { added to checked }\n"
                "  0:                                                                Derived tuple [ :i1, rdf:type, :A ]    { from EDB, not from delayed, not from previous level, added }\n"
                "  0:                                                                Processing the proved list\n"
                "  0:                                                                    Extracted current tuple [ :i1, rdf:type, :A ]\n"
                "  0:                                                                        Matched atom [?X, rdf:type, :A] to tuple [ :i3, rdf:type, :A ]\n"
                "  0:                                                                            Matched rule [?X, rdf:type, :B] :- [?X, rdf:type, :A] .\n"
                "  0:                                                                                Derived tuple [ :i1, rdf:type, :B ]    { normal, added }\n"
                "  0:                                                                    Extracted current tuple [ :i1, rdf:type, :B ]\n"
                "  0:                                                                        Matched atom [?X, rdf:type, :B] to tuple [ :i3, rdf:type, :B ]\n"
                "  0:                                                                            Matching atom [?X, :R, ?Y]\n"
                "  0:                                                                                Matched atom [?X, :R, ?Y] to tuple [ :i3, :R, :i5 ]\n"
                "  0:                                                                                    Matched rule [?Y, rdf:type, :B] :- [?X, rdf:type, :B], [?X, :R, ?Y] .\n"
                "  0:                                                                                        Derived tuple [ :i2, rdf:type, :B ]    { normal, added }\n"
                "  0:                                                                    Extracted current tuple [ :i2, rdf:type, :B ]\n"
                "  0:                                                                        Matched atom [?X, rdf:type, :B] to tuple [ :i3, rdf:type, :B ]\n"
                "  0:                                                                            Matching atom [?X, :R, ?Y]\n"
                "  0:                                                                                Matched atom [?X, :R, ?Y] to tuple [ :i3, :R, :i5 ]\n"
                "  0:                                                                                    Matched rule [?Y, rdf:type, :B] :- [?X, rdf:type, :B], [?X, :R, ?Y] .\n"
                "  0:                                                                                        Derived tuple [ :i3, rdf:type, :B ]    { normal, added }\n"
                "  0:                                                                    Extracted current tuple [ :i3, rdf:type, :B ]\n"
                "  0:                                                                        Matched atom [?X, rdf:type, :B] to tuple [ :i3, rdf:type, :B ]\n"
                "  0:                                                                            Matching atom [?X, :R, ?Y]\n"
                "  0:                                                                Backward chaining stopped because the tuple was proved\n"
                "  0:                                                        Backward chaining stopped because the tuple was proved\n"
                "  0:                                        Backward chaining stopped because the tuple was proved\n"
                "  0:                        Backward chaining stopped because the tuple was proved\n"
                "  0:            Possibly deleted tuple proved\n"
                "  0:    Applying insertion rules to tuples from previous levels\n"
                "  0:    Applying recursive insertion rules\n"
                "============================================================\n"
                "  0:    Propagating deleted and proved tuples into the store\n"
                "  0:        Tuple deleted [ :i3, rdf:type, :A ]    { deleted }\n"
            );
    }
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":i1 :R :i2 . "
        ":i2 :R :i3 . "
        ":i3 :R :i4 . "
        ":i4 :R :i5 . "

        ":i1 rdf:type :A . "

        ":i1 rdf:type :B . "
        ":i2 rdf:type :B . "
        ":i3 rdf:type :B . "
        ":i4 rdf:type :B . "
        ":i5 rdf:type :B . "
    );
}

TEST(testMultipleHeads) {
    addRules(
        ":A(?Y), :B(?Y) :- :R(?X,?Y), :A(?X) . "
    );
    addTriples(
        ":a :R :b . "
        ":b :R :c . "
        ":c :R :d . "
        ":d :R :e . "
        ":e :R :f . "

        ":a rdf:type :A ."
    );
    applyRules();
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":a :R :b . "
        ":b :R :c . "
        ":c :R :d . "
        ":d :R :e . "
        ":e :R :f . "

        ":a rdf:type :A ."
        ":b rdf:type :A ."
        ":c rdf:type :A ."
        ":d rdf:type :A ."
        ":e rdf:type :A ."
        ":f rdf:type :A ."

        ":b rdf:type :B ."
        ":c rdf:type :B ."
        ":d rdf:type :B ."
        ":e rdf:type :B ."
        ":f rdf:type :B ."
    );
    // Move the inferred part later in the chain.
    forDeletion(
        ":a rdf:type :A . "
    );
    forAddition(
        ":c rdf:type :A . "
    );
    applyRulesIncrementally();
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":a :R :b . "
        ":b :R :c . "
        ":c :R :d . "
        ":d :R :e . "
        ":e :R :f . "

        ":c rdf:type :A ."
        ":d rdf:type :A ."
        ":e rdf:type :A ."
        ":f rdf:type :A ."

        ":d rdf:type :B ."
        ":e rdf:type :B ."
        ":f rdf:type :B ."
    );
    // Restore the original state.
    forDeletion(
        ":c rdf:type :A . "
    );
    forAddition(
        ":a rdf:type :A . "
    );
    applyRulesIncrementally();
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":a :R :b . "
        ":b :R :c . "
        ":c :R :d . "
        ":d :R :e . "
        ":e :R :f . "

        ":a rdf:type :A ."
        ":b rdf:type :A ."
        ":c rdf:type :A ."
        ":d rdf:type :A ."
        ":e rdf:type :A ."
        ":f rdf:type :A ."

        ":b rdf:type :B ."
        ":c rdf:type :B ."
        ":d rdf:type :B ."
        ":e rdf:type :B ."
        ":f rdf:type :B ."
    );
}

TEST(testDisprovedBug) {
    addRules(
        ":C(?X) :- :A(?X) . "
        ":C(?X) :- :B(?X) . "
        ":E(?X) :- :C(?X) . "
        ":E(?X) :- :D(?X) . "
    );
    addTriples(
        ":i rdf:type :A . "
        ":i rdf:type :D . "
    );
    applyRules();
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":i rdf:type :A . "
        ":i rdf:type :C . "
        ":i rdf:type :D . "
        ":i rdf:type :E . "
    );
    // Now delete some tuples.
    forDeletion(
        ":i rdf:type :A . "
        ":i rdf:type :D . "
    );
    forAddition(
        ":i rdf:type :B . "
    );
    applyRulesIncrementally();
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":i rdf:type :B . "
        ":i rdf:type :C . "
        ":i rdf:type :E . "
    );
}

TEST(testBindSkolemBug) {
    addRules(
        ":dist_A(?V,?A), :dist_B(?V,?B), :dist_D(?V,?D) :- "
        "    BIND(SKOLEM(\"dist\",?A,?B,?D) as ?V), "
        "    :pos(?A,?PA), :pos(?B,?PB), "
        "    FILTER(?PA < ?PB), "
        "    BIND(?PB - ?PA as ?D). "
    );
    addTriples(
        ":a :pos 836.796 . "
        ":b :pos 866.796 . "
    );
    applyRules();
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":a :pos 836.796 . "
        ":b :pos 866.796 . "
        "_:dist_11_13_1035 :dist_A :a . "
        "_:dist_11_13_1035 :dist_B :b . "
        "_:dist_11_13_1035 :dist_D 30.0 . "
    );
    // Now delete some tuples.
    forDeletion(
        ":a :pos 836.796 . "
    );
    applyRulesIncrementally();
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":b :pos 866.796 . "
    );
}

TEST(testProvingPreviousLevelsBug) {
    addRules(
        ":A(?X) :- :R(?X,?Y) . "
        ":A(?X) :- :S(?X,?Y) . "
        ":B(?X) :- :A(?X), :T(?X,?Y) . "
        ":A(?X) :- :B(?X) . "
    );
    addTriples(
        ":a :R :b . "
        ":a :S :b . "
        ":a :T :c . "
    );
    applyRules();
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":a :R :b . "
        ":a :S :b ."
        ":a :T :c . "
        ":a rdf:type :A . "
        ":a rdf:type :B . "
    );
    // Now delete some tuples
    forDeletion(
        ":a :S :b . "
        ":a :T :c . "
    );
    applyRulesIncrementally();
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":a :R :b . "
        ":a rdf:type :A . "
    );
}

TEST(testNonrepetitionPositive) {
    addRules(
        "[?X,rdf:type,:C] :- [?X,rdf:type,:A], [?X,rdf:type,:B] . "
    );
    addTriples(
        ":a rdf:type :A . "
        ":a rdf:type :B . "
    );
    applyRules();
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":a rdf:type :A . "
        ":a rdf:type :B . "
        ":a rdf:type :C . "
    );
    // This introduces potential repetition in both addition and deletion!
    forDeletion(
        ":a rdf:type :A . "
        ":a rdf:type :B . "
    );
    forAddition(
        ":b rdf:type :A . "
        ":b rdf:type :B . "
    );
    if (m_useDRed) {
        if (m_processComponentsByLevels)
            ASSERT_APPLY_RULES_INCREMENTALLY(
                "== LEVEL   0 ===============================================\n"
                "  0:    Applying deletion rules to tuples from previous levels\n"
                "  0:    Applying recursive deletion rules\n"
                "  0:        Extracted possibly deleted tuple [ :a, rdf:type, :A ]\n"
                "  0:            Applying deletion rules to [ :a, rdf:type, :A ]\n"
                "  0:        Extracted possibly deleted tuple [ :a, rdf:type, :B ]\n"
                "  0:            Applying deletion rules to [ :a, rdf:type, :B ]\n"
                "  0:    Checking provability of [ :a, rdf:type, :A ]    { added to checked }\n"
                "  0:    Checking provability of [ :a, rdf:type, :B ]    { added to checked }\n"
                "  0:    Applying insertion rules to tuples from previous levels\n"
                "  0:    Applying recursive insertion rules\n"
                "  0:        Extracted current tuple [ :b, rdf:type, :A ]\n"
                "  0:            Tuple added [ :b, rdf:type, :A ]    { added }\n"
                "  0:        Extracted current tuple [ :b, rdf:type, :B ]\n"
                "  0:            Tuple added [ :b, rdf:type, :B ]    { added }\n"
                "============================================================\n"
                "== LEVEL   1 ===============================================\n"
                "  0:    Applying deletion rules to tuples from previous levels\n"
                "  0:        Applying deletion rules to [ :a, rdf:type, :A ]\n"
                "  0:            Matched atom [?X, rdf:type, :A] to tuple [ :a, rdf:type, :A ]\n"
                "  0:                Matching atom [?X, rdf:type, :B]\n"
                "  0:                    Matched atom [?X, rdf:type, :B] to tuple [ :a, rdf:type, :B ]\n"
                "  0:                        Matched rule [?X, rdf:type, :C] :- [?X, rdf:type, :A], [?X, rdf:type, :B] .\n"
                "  0:                            Derived tuple [ :a, rdf:type, :C ]    { normal, added }\n"
                "  0:        Applying deletion rules to [ :a, rdf:type, :B ]\n"
                "  0:            Matched atom [?X, rdf:type, :B] to tuple [ :a, rdf:type, :B ]\n"
                "  0:                Matching atom [?X, rdf:type, :A]\n"
                "  0:    Applying recursive deletion rules\n"
                "  0:        Extracted possibly deleted tuple [ :a, rdf:type, :C ]\n"
                "  0:            Applying deletion rules to [ :a, rdf:type, :C ]\n"
                "  0:    Checking provability of [ :a, rdf:type, :C ]    { added to checked }\n"
                "  0:        Checking nonrecursive rule [?X, rdf:type, :C] :- [?X, rdf:type, :A], [?X, rdf:type, :B] .\n"
                "  0:    Applying insertion rules to tuples from previous levels\n"
                "  0:        Extracted current tuple [ :b, rdf:type, :A ]\n"
                "  0:            Matched atom [?X, rdf:type, :A] to tuple [ :b, rdf:type, :A ]\n"
                "  0:                Matching atom [?X, rdf:type, :B]\n"
                "  0:                    Matched atom [?X, rdf:type, :B] to tuple [ :b, rdf:type, :B ]\n"
                "  0:                        Matched rule [?X, rdf:type, :C] :- [?X, rdf:type, :A], [?X, rdf:type, :B] .\n"
                "  0:                            Derived tuple [ :b, rdf:type, :C ]    { normal, added }\n"
                "  0:        Extracted current tuple [ :b, rdf:type, :B ]\n"
                "  0:            Matched atom [?X, rdf:type, :B] to tuple [ :b, rdf:type, :B ]\n"
                "  0:                Matching atom [?X, rdf:type, :A]\n"
                "  0:    Applying recursive insertion rules\n"
                "  0:        Extracted current tuple [ :b, rdf:type, :C ]\n"
                "  0:            Tuple added [ :b, rdf:type, :C ]    { added }\n"
                "============================================================\n"
                "  0:    Propagating deleted and proved tuples into the store for level 0\n"
                "  0:        Tuple deleted [ :a, rdf:type, :A ]    { deleted }\n"
                "  0:        Tuple deleted [ :a, rdf:type, :B ]    { deleted }\n"
                "  0:        Tuple added [ :b, rdf:type, :A ]    { added }\n"
                "  0:        Tuple added [ :b, rdf:type, :B ]    { added }\n"
                "  0:    Propagating deleted and proved tuples into the store for level 1\n"
                "  0:        Tuple deleted [ :a, rdf:type, :C ]    { deleted }\n"
                "  0:        Tuple added [ :b, rdf:type, :C ]    { added }\n"
            );
        else
            ASSERT_APPLY_RULES_INCREMENTALLY(
                "============================================================\n"
                "  0:    Applying deletion rules to tuples from previous levels\n"
                "  0:    Applying recursive deletion rules\n"
                "  0:        Extracted possibly deleted tuple [ :a, rdf:type, :A ]\n"
                "  0:            Applying deletion rules to [ :a, rdf:type, :A ]\n"
                "  0:                Matched atom [?X, rdf:type, :A] to tuple [ :a, rdf:type, :A ]\n"
                "  0:                    Matching atom [?X, rdf:type, :B]\n"
                "  0:                        Matched atom [?X, rdf:type, :B] to tuple [ :a, rdf:type, :B ]\n"
                "  0:                            Matched rule [?X, rdf:type, :C] :- [?X, rdf:type, :A], [?X, rdf:type, :B] .\n"
                "  0:                                Derived tuple [ :a, rdf:type, :C ]    { normal, added }\n"
                "  0:        Extracted possibly deleted tuple [ :a, rdf:type, :B ]\n"
                "  0:            Applying deletion rules to [ :a, rdf:type, :B ]\n"
                "  0:                Matched atom [?X, rdf:type, :B] to tuple [ :a, rdf:type, :B ]\n"
                "  0:                    Matching atom [?X, rdf:type, :A]\n"
                "  0:        Extracted possibly deleted tuple [ :a, rdf:type, :C ]\n"
                "  0:            Applying deletion rules to [ :a, rdf:type, :C ]\n"
                "  0:    Checking provability of [ :a, rdf:type, :A ]    { added to checked }\n"
                "  0:    Checking provability of [ :a, rdf:type, :B ]    { added to checked }\n"
                "  0:    Checking provability of [ :a, rdf:type, :C ]    { added to checked }\n"
                "  0:        Checking recursive rule [?X, rdf:type, :C] :- [?X, rdf:type, :A], [?X, rdf:type, :B] .\n"
                "  0:    Applying insertion rules to tuples from previous levels\n"
                "  0:    Applying recursive insertion rules\n"
                "  0:        Extracted current tuple [ :b, rdf:type, :A ]\n"
                "  0:            Tuple added [ :b, rdf:type, :A ]    { added }\n"
                "  0:            Matched atom [?X, rdf:type, :A] to tuple [ :b, rdf:type, :A ]\n"
                "  0:                Matching atom [?X, rdf:type, :B]\n"
                "  0:        Extracted current tuple [ :b, rdf:type, :B ]\n"
                "  0:            Tuple added [ :b, rdf:type, :B ]    { added }\n"
                "  0:            Matched atom [?X, rdf:type, :B] to tuple [ :b, rdf:type, :B ]\n"
                "  0:                Matching atom [?X, rdf:type, :A]\n"
                "  0:                    Matched atom [?X, rdf:type, :A] to tuple [ :b, rdf:type, :A ]\n"
                "  0:                        Matched rule [?X, rdf:type, :C] :- [?X, rdf:type, :A], [?X, rdf:type, :B] .\n"
                "  0:                            Derived tuple [ :b, rdf:type, :C ]    { normal, added }\n"
                "  0:        Extracted current tuple [ :b, rdf:type, :C ]\n"
                "  0:            Tuple added [ :b, rdf:type, :C ]    { added }\n"
                "============================================================\n"
                "  0:    Propagating deleted and proved tuples into the store\n"
                "  0:        Tuple deleted [ :a, rdf:type, :A ]    { deleted }\n"
                "  0:        Tuple deleted [ :a, rdf:type, :B ]    { deleted }\n"
                "  0:        Tuple deleted [ :a, rdf:type, :C ]    { deleted }\n"
                "  0:        Tuple added [ :b, rdf:type, :A ]    { added }\n"
                "  0:        Tuple added [ :b, rdf:type, :B ]    { added }\n"
                "  0:        Tuple added [ :b, rdf:type, :C ]    { added }\n"
            );
    }
    else {
        if (m_processComponentsByLevels)
            ASSERT_APPLY_RULES_INCREMENTALLY(
                "== LEVEL   0 ===============================================\n"
                "  0:    Applying deletion rules to tuples from previous levels\n"
                "  0:    Applying recursive deletion rules\n"
                "  0:        Extracted possibly deleted tuple [ :a, rdf:type, :A ]\n"
                "  0:            Checking provability of [ :a, rdf:type, :A ]    { added to checked }\n"
                "  0:            Tuple disproved [ :a, rdf:type, :A ]    { added }\n"
                "  0:            Applying deletion rules to [ :a, rdf:type, :A ]\n"
                "  0:        Extracted possibly deleted tuple [ :a, rdf:type, :B ]\n"
                "  0:            Checking provability of [ :a, rdf:type, :B ]    { added to checked }\n"
                "  0:            Tuple disproved [ :a, rdf:type, :B ]    { added }\n"
                "  0:            Applying deletion rules to [ :a, rdf:type, :B ]\n"
                "  0:    Applying insertion rules to tuples from previous levels\n"
                "  0:    Applying recursive insertion rules\n"
                "  0:        Extracted current tuple [ :b, rdf:type, :A ]\n"
                "  0:            Tuple added [ :b, rdf:type, :A ]    { added }\n"
                "  0:        Extracted current tuple [ :b, rdf:type, :B ]\n"
                "  0:            Tuple added [ :b, rdf:type, :B ]    { added }\n"
                "============================================================\n"
                "== LEVEL   1 ===============================================\n"
                "  0:    Applying deletion rules to tuples from previous levels\n"
                "  0:        Applying deletion rules to [ :a, rdf:type, :A ]\n"
                "  0:            Matched atom [?X, rdf:type, :A] to tuple [ :a, rdf:type, :A ]\n"
                "  0:                Matching atom [?X, rdf:type, :B]\n"
                "  0:                    Matched atom [?X, rdf:type, :B] to tuple [ :a, rdf:type, :B ]\n"
                "  0:                        Matched rule [?X, rdf:type, :C] :- [?X, rdf:type, :A], [?X, rdf:type, :B] .\n"
                "  0:                            Derived tuple [ :a, rdf:type, :C ]    { normal, added }\n"
                "  0:        Applying deletion rules to [ :a, rdf:type, :B ]\n"
                "  0:            Matched atom [?X, rdf:type, :B] to tuple [ :a, rdf:type, :B ]\n"
                "  0:                Matching atom [?X, rdf:type, :A]\n"
                "  0:    Applying recursive deletion rules\n"
                "  0:        Extracted possibly deleted tuple [ :a, rdf:type, :C ]\n"
                "  0:            Checking provability of [ :a, rdf:type, :C ]    { added to checked }\n"
                "  0:                Checking nonrecursive rule [?X, rdf:type, :C] :- [?X, rdf:type, :A], [?X, rdf:type, :B] .\n"
                "  0:            Tuple disproved [ :a, rdf:type, :C ]    { added }\n"
                "  0:            Applying deletion rules to [ :a, rdf:type, :C ]\n"
                "  0:    Applying insertion rules to tuples from previous levels\n"
                "  0:        Extracted current tuple [ :b, rdf:type, :A ]\n"
                "  0:            Matched atom [?X, rdf:type, :A] to tuple [ :b, rdf:type, :A ]\n"
                "  0:                Matching atom [?X, rdf:type, :B]\n"
                "  0:                    Matched atom [?X, rdf:type, :B] to tuple [ :b, rdf:type, :B ]\n"
                "  0:                        Matched rule [?X, rdf:type, :C] :- [?X, rdf:type, :A], [?X, rdf:type, :B] .\n"
                "  0:                            Derived tuple [ :b, rdf:type, :C ]    { normal, added }\n"
                "  0:        Extracted current tuple [ :b, rdf:type, :B ]\n"
                "  0:            Matched atom [?X, rdf:type, :B] to tuple [ :b, rdf:type, :B ]\n"
                "  0:                Matching atom [?X, rdf:type, :A]\n"
                "  0:    Applying recursive insertion rules\n"
                "  0:        Extracted current tuple [ :b, rdf:type, :C ]\n"
                "  0:            Tuple added [ :b, rdf:type, :C ]    { added }\n"
                "============================================================\n"
                "  0:    Propagating deleted and proved tuples into the store for level 0\n"
                "  0:        Tuple deleted [ :a, rdf:type, :A ]    { deleted }\n"
                "  0:        Tuple deleted [ :a, rdf:type, :B ]    { deleted }\n"
                "  0:        Tuple added [ :b, rdf:type, :A ]    { added }\n"
                "  0:        Tuple added [ :b, rdf:type, :B ]    { added }\n"
                "  0:    Propagating deleted and proved tuples into the store for level 1\n"
                "  0:        Tuple deleted [ :a, rdf:type, :C ]    { deleted }\n"
                "  0:        Tuple added [ :b, rdf:type, :C ]    { added }\n"
            );
        else
            ASSERT_APPLY_RULES_INCREMENTALLY(
                "============================================================\n"
                "  0:    Applying deletion rules to tuples from previous levels\n"
                "  0:    Applying recursive deletion rules\n"
                "  0:        Extracted possibly deleted tuple [ :a, rdf:type, :A ]\n"
                "  0:            Checking provability of [ :a, rdf:type, :A ]    { added to checked }\n"
                "  0:            Tuple disproved [ :a, rdf:type, :A ]    { added }\n"
                "  0:            Applying deletion rules to [ :a, rdf:type, :A ]\n"
                "  0:                Matched atom [?X, rdf:type, :A] to tuple [ :a, rdf:type, :A ]\n"
                "  0:                    Matching atom [?X, rdf:type, :B]\n"
                "  0:                        Matched atom [?X, rdf:type, :B] to tuple [ :a, rdf:type, :B ]\n"
                "  0:                            Matched rule [?X, rdf:type, :C] :- [?X, rdf:type, :A], [?X, rdf:type, :B] .\n"
                "  0:                                Derived tuple [ :a, rdf:type, :C ]    { normal, added }\n"
                "  0:        Extracted possibly deleted tuple [ :a, rdf:type, :B ]\n"
                "  0:            Checking provability of [ :a, rdf:type, :B ]    { added to checked }\n"
                "  0:            Tuple disproved [ :a, rdf:type, :B ]    { added }\n"
                "  0:            Applying deletion rules to [ :a, rdf:type, :B ]\n"
                "  0:                Matched atom [?X, rdf:type, :B] to tuple [ :a, rdf:type, :B ]\n"
                "  0:                    Matching atom [?X, rdf:type, :A]\n"
                "  0:        Extracted possibly deleted tuple [ :a, rdf:type, :C ]\n"
                "  0:            Checking provability of [ :a, rdf:type, :C ]    { added to checked }\n"
                "  0:                Checking recursive rule [?X, rdf:type, :C] :- [?X, rdf:type, :A], [?X, rdf:type, :B] .\n"
                "  0:            Tuple disproved [ :a, rdf:type, :C ]    { added }\n"
                "  0:            Applying deletion rules to [ :a, rdf:type, :C ]\n"
                "  0:    Applying insertion rules to tuples from previous levels\n"
                "  0:    Applying recursive insertion rules\n"
                "  0:        Extracted current tuple [ :b, rdf:type, :A ]\n"
                "  0:            Tuple added [ :b, rdf:type, :A ]    { added }\n"
                "  0:            Matched atom [?X, rdf:type, :A] to tuple [ :b, rdf:type, :A ]\n"
                "  0:                Matching atom [?X, rdf:type, :B]\n"
                "  0:        Extracted current tuple [ :b, rdf:type, :B ]\n"
                "  0:            Tuple added [ :b, rdf:type, :B ]    { added }\n"
                "  0:            Matched atom [?X, rdf:type, :B] to tuple [ :b, rdf:type, :B ]\n"
                "  0:                Matching atom [?X, rdf:type, :A]\n"
                "  0:                    Matched atom [?X, rdf:type, :A] to tuple [ :b, rdf:type, :A ]\n"
                "  0:                        Matched rule [?X, rdf:type, :C] :- [?X, rdf:type, :A], [?X, rdf:type, :B] .\n"
                "  0:                            Derived tuple [ :b, rdf:type, :C ]    { normal, added }\n"
                "  0:        Extracted current tuple [ :b, rdf:type, :C ]\n"
                "  0:            Tuple added [ :b, rdf:type, :C ]    { added }\n"
                "============================================================\n"
                "  0:    Propagating deleted and proved tuples into the store\n"
                "  0:        Tuple deleted [ :a, rdf:type, :A ]    { deleted }\n"
                "  0:        Tuple deleted [ :a, rdf:type, :B ]    { deleted }\n"
                "  0:        Tuple deleted [ :a, rdf:type, :C ]    { deleted }\n"
                "  0:        Tuple added [ :b, rdf:type, :A ]    { added }\n"
                "  0:        Tuple added [ :b, rdf:type, :B ]    { added }\n"
                "  0:        Tuple added [ :b, rdf:type, :C ]    { added }\n"
            );
    }
    // Check the result.
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":b rdf:type :A . "
        ":b rdf:type :B . "
        ":b rdf:type :C . "
    );
}

#endif
