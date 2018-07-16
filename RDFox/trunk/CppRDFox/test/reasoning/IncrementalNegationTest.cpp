// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifdef WITH_TEST

#include <CppTest/TestCase.h>

#include "../querying/DataStoreTest.h"

// Define all other tests quites by copying IncrementalTest.FBF.byLevels as the template.

SUITE(IncrementalNegationTest);

__REGISTER(IncrementalNegationTest, IncrementalNegationTest__FBF);
__MAKE_SUITE(IncrementalNegationTest__FBF, "FBF");

__REGISTER(IncrementalNegationTest, IncrementalNegationTest__DRed);
__MAKE_ADAPTER(IncrementalNegationTest__DRed, "DRed", IncrementalNegationTest__FBF) {
    testParameters.setBoolean("reason.use-DRed", true);
}

// Now define the IncrementalNegationTest.FBF template suite; to avoid confusion, use a neutral fixture name.

#define TEST(testName)                                                                                  \
    __REGISTER(IncrementalNegationTest__FBF, IncrementalNegationTest__FBF##testName);               \
    __MAKE_TEST(IncrementalNegationTest__FBF##testName, #testName, : public IncrementalNegationTestFixture)

class IncrementalNegationTestFixture : public DataStoreTest {

protected:

    virtual void initializeQueryParameters() {
        m_queryParameters.setBoolean("bushy", false);
        m_queryParameters.setBoolean("distinct", false);
        m_queryParameters.setBoolean("cardinality", false);
    }

public:

    IncrementalNegationTestFixture() : DataStoreTest("par-complex-ww", EQUALITY_AXIOMATIZATION_OFF) {
        m_processComponentsByLevels = true;
    }

    void initializeEx(const CppTest::TestParameters& testParameters) {
        m_useDRed = testParameters.getBoolean("reason.use-DRed", false);
    }

};

TEST(testNegationBasic) {
    addRules(
        ":B(?X) :- :O(?X), NOT :A(?X) . "
        ":C(?X) :- :O(?X), NOT :B(?X) . "
        ":D(?X) :- :O(?X), NOT :C(?X) . "
        ":E(?X) :- :O(?X), NOT :D(?X) . "
        ":F(?X) :- :O(?X), NOT :E(?X) . "
    );
    addTriples(
        ":a rdf:type :O . "
        ":b rdf:type :A . "
    );
    applyRules();
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":a rdf:type :O . "
        ":a rdf:type :B . "
        ":a rdf:type :D . "
        ":a rdf:type :F . "

        ":b rdf:type :A . "
    );
    // Apply addition.
    forAddition(
        ":a rdf:type :A . "
        ":b rdf:type :O . "
        ":c rdf:type :O . "
    );
    applyRulesIncrementally();
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":a rdf:type :O . "
        ":a rdf:type :A . "
        ":a rdf:type :C . "
        ":a rdf:type :E . "

        ":b rdf:type :O . "
        ":b rdf:type :A . "
        ":b rdf:type :C . "
        ":b rdf:type :E . "

        ":c rdf:type :O . "
        ":c rdf:type :B . "
        ":c rdf:type :D . "
        ":c rdf:type :F . "
    );
    // Apply deletion.
    forDeletion(
        ":a rdf:type :A . "
        ":b rdf:type :O . "
        ":c rdf:type :O . "
    );
    applyRulesIncrementally();
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":a rdf:type :O . "
        ":a rdf:type :B . "
        ":a rdf:type :D . "
        ":a rdf:type :F . "

        ":b rdf:type :A . "
    );
}

TEST(testNegationOfFullConjunction) {
    addRules(
        "[?X,:T,?Y] :- [?X,:R,?Y], NOT([?X,:S,?Y], [?Y,rdf:type,:A]) . "
    );
    addTriples(
        ":a1 :R :a2 . "

        ":b1 :R :b2 . "
        ":b1 :S :b2 . "

        ":c1 :R :c2 . "
        ":c2 rdf:type :A . "

        ":d1 :R :d2 . "
        ":d1 :S :d2 . "
        ":d2 rdf:type :A . "

        ":e1 :R :e2 . "
        ":e1 :S :e2 . "
        ":e2 rdf:type :A . "
    );
    applyRules();
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":a1 :R :a2 . "
        ":a1 :T :a2 . "

        ":b1 :R :b2 . "
        ":b1 :S :b2 . "
        ":b1 :T :b2 . "

        ":c1 :R :c2 . "
        ":c2 rdf:type :A . "
        ":c1 :T :c2 . "

        ":d1 :R :d2 . "
        ":d1 :S :d2 . "
        ":d2 rdf:type :A . "

        ":e1 :R :e2 . "
        ":e1 :S :e2 . "
        ":e2 rdf:type :A . "
    );
    // Apply changes.
    forDeletion(
        ":d1 :S :d2 . "

        ":e2 rdf:type :A . "
    );
    forAddition(
        ":a1 :S :a2 . "
        ":a2 rdf:type :A . "

        ":b2 rdf:type :A . "

        ":c1 :S :c2 . "
    );
    applyRulesIncrementally();
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":a1 :R :a2 . "
        ":a1 :S :a2 . "
        ":a2 rdf:type :A . "

        ":b1 :R :b2 . "
        ":b1 :S :b2 . "
        ":b2 rdf:type :A . "

        ":c1 :R :c2 . "
        ":c1 :S :c2 . "
        ":c2 rdf:type :A . "

        ":d1 :R :d2 . "
        ":d2 rdf:type :A . "
        ":d1 :T :d2 . "

        ":e1 :R :e2 . "
        ":e1 :S :e2 . "
        ":e1 :T :e2 . "
    );
    // Restore original state.
    forDeletion(
        ":a1 :S :a2 . "
        ":a2 rdf:type :A . "

        ":b2 rdf:type :A . "

        ":c1 :S :c2 . "
    );
    forAddition(
        ":d1 :S :d2 . "

        ":e2 rdf:type :A . "
    );
    applyRulesIncrementally();
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":a1 :R :a2 . "
        ":a1 :T :a2 . "

        ":b1 :R :b2 . "
        ":b1 :S :b2 . "
        ":b1 :T :b2 . "

        ":c1 :R :c2 . "
        ":c2 rdf:type :A . "
        ":c1 :T :c2 . "

        ":d1 :R :d2 . "
        ":d1 :S :d2 . "
        ":d2 rdf:type :A . "

        ":e1 :R :e2 . "
        ":e1 :S :e2 . "
        ":e2 rdf:type :A . "
    );
}

TEST(testNegationExists) {
    addRules(
        "[?X,rdf:type,:C] :- [?X,rdf:type,:A], NOT EXISTS ?Y IN ([?X,:R,?Y], [?Y,rdf:type,:B]) . "
    );
    addTriples(
        ":a rdf:type :A . "

        ":b rdf:type :A . "
        ":b :R :b1 . "
        ":b1 rdf:type :B . "

        ":c rdf:type :A . "
        ":c :R :c1 . "
        ":c1 rdf:type :B . "

        ":d rdf:type :A . "
        ":d :R :d1 . "
        ":d1 rdf:type :B . "
        ":d :R :d2 . "
        ":d2 rdf:type :B . "

        ":e rdf:type :A . "
        ":e :R :e1 . "
        ":e1 rdf:type :B . "
        ":e :R :e2 . "
        ":e2 rdf:type :B . "

        ":f rdf:type :A . "
        ":f :R :f1 . "

        ":g rdf:type :A . "
        ":g1 rdf:type :B . "

        ":h rdf:type :A . "
        ":h1 rdf:type :B . "
        ":h2 rdf:type :B . "
    );
    applyRules();
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":a rdf:type :A . "
        ":a rdf:type :C . "

        ":b rdf:type :A . "
        ":b :R :b1 . "
        ":b1 rdf:type :B . "

        ":c rdf:type :A . "
        ":c :R :c1 . "
        ":c1 rdf:type :B . "

        ":d rdf:type :A . "
        ":d :R :d1 . "
        ":d1 rdf:type :B . "
        ":d :R :d2 . "
        ":d2 rdf:type :B . "

        ":e rdf:type :A . "
        ":e :R :e1 . "
        ":e1 rdf:type :B . "
        ":e :R :e2 . "
        ":e2 rdf:type :B . "

        ":f rdf:type :A . "
        ":f :R :f1 . "
        ":f rdf:type :C . "

        ":g rdf:type :A . "
        ":g1 rdf:type :B . "
        ":g rdf:type :C . "

        ":h rdf:type :A . "
        ":h1 rdf:type :B . "
        ":h2 rdf:type :B . "
        ":h rdf:type :C . "
    );
    // Apply changes.
    forDeletion(
        ":b :R :b1 . "
        ":c1 rdf:type :B . "
        ":d :R :d1 . "
        ":e1 rdf:type :B . "
    );
    forAddition(
        ":f1 rdf:type :B . "
        ":g :R :g1 . "
        ":h :R :h1 . "
        ":h :R :h2 . "
    );
    applyRulesIncrementally();
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":a rdf:type :A . "
        ":a rdf:type :C . "

        ":b rdf:type :A . "
        ":b1 rdf:type :B . "
        ":b rdf:type :C . "

        ":c rdf:type :A . "
        ":c :R :c1 . "
        ":c rdf:type :C . "

        ":d rdf:type :A . "
        ":d1 rdf:type :B . "
        ":d :R :d2 . "
        ":d2 rdf:type :B . "

        ":e rdf:type :A . "
        ":e :R :e1 . "
        ":e :R :e2 . "
        ":e2 rdf:type :B . "

        ":f rdf:type :A . "
        ":f :R :f1 . "
        ":f1 rdf:type :B . "

        ":g rdf:type :A . "
        ":g :R :g1 . "
        ":g1 rdf:type :B . "

        ":h rdf:type :A . "
        ":h :R :h1 . "
        ":h1 rdf:type :B . "
        ":h :R :h2 . "
        ":h2 rdf:type :B . "
    );
    // Restore original state.
    forDeletion(
        ":f1 rdf:type :B . "
        ":g :R :g1 . "
        ":h :R :h1 . "
        ":h :R :h2 . "
    );
    forAddition(
        ":b :R :b1 . "
        ":c1 rdf:type :B . "
        ":d :R :d1 . "
        ":e1 rdf:type :B . "
    );
    applyRulesIncrementally();
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":a rdf:type :A . "
        ":a rdf:type :C . "

        ":b rdf:type :A . "
        ":b :R :b1 . "
        ":b1 rdf:type :B . "

        ":c rdf:type :A . "
        ":c :R :c1 . "
        ":c1 rdf:type :B . "

        ":d rdf:type :A . "
        ":d :R :d1 . "
        ":d1 rdf:type :B . "
        ":d :R :d2 . "
        ":d2 rdf:type :B . "

        ":e rdf:type :A . "
        ":e :R :e1 . "
        ":e1 rdf:type :B . "
        ":e :R :e2 . "
        ":e2 rdf:type :B . "

        ":f rdf:type :A . "
        ":f :R :f1 . "
        ":f rdf:type :C . "

        ":g rdf:type :A . "
        ":g1 rdf:type :B . "
        ":g rdf:type :C . "

        ":h rdf:type :A . "
        ":h1 rdf:type :B . "
        ":h2 rdf:type :B . "
        ":h rdf:type :C . "
    );
}

TEST(testNegationRecursive) {
    addRules(
        ":A(?Y) :- :A(?X), :R(?X,?Y), NOT :B(?X) . "
    );
    addTriples(
        ":i1 :R :i2 . "
        ":i2 :R :i3 . "
        ":i3 :R :i4 . "
        ":i4 :R :i5 . "
        ":i5 :R :i6 . "

        ":i1 rdf:type :A . "
    );
    applyRules();
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":i1 :R :i2 . "
        ":i2 :R :i3 . "
        ":i3 :R :i4 . "
        ":i4 :R :i5 . "
        ":i5 :R :i6 . "

        ":i1 rdf:type :A . "
        ":i2 rdf:type :A . "
        ":i3 rdf:type :A . "
        ":i4 rdf:type :A . "
        ":i5 rdf:type :A . "
        ":i6 rdf:type :A . "
    );
    // Apply addition, which leads to deletion.
    forAddition(
        ":i3 rdf:type :B . "
    );
    applyRulesIncrementally();
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":i1 :R :i2 . "
        ":i2 :R :i3 . "
        ":i3 :R :i4 . "
        ":i4 :R :i5 . "
        ":i5 :R :i6 . "

        ":i1 rdf:type :A . "
        ":i2 rdf:type :A . "
        ":i3 rdf:type :A . "

        ":i3 rdf:type :B . "
    );
    // Apply deletion and addition, which leads some changes.
    forDeletion(
        ":i3 rdf:type :B . "
    );
    forAddition(
        ":i4 rdf:type :B . "
    );
    applyRulesIncrementally();
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":i1 :R :i2 . "
        ":i2 :R :i3 . "
        ":i3 :R :i4 . "
        ":i4 :R :i5 . "
        ":i5 :R :i6 . "

        ":i1 rdf:type :A . "
        ":i2 rdf:type :A . "
        ":i3 rdf:type :A . "
        ":i4 rdf:type :A . "

        ":i4 rdf:type :B . "
    );
    // Apply changes so that the chain breaks earlier
    forDeletion(
        ":i4 rdf:type :B . "
    );
    forAddition(
        ":i2 rdf:type :B . "
    );
    applyRulesIncrementally();
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":i1 :R :i2 . "
        ":i2 :R :i3 . "
        ":i3 :R :i4 . "
        ":i4 :R :i5 . "
        ":i5 :R :i6 . "

        ":i1 rdf:type :A . "
        ":i2 rdf:type :A . "

        ":i2 rdf:type :B . "
    );
}

TEST(testAddRemoveRulesWithNegation) {
    addRules(
        ":B(?X) :- :O(?X), NOT :A(?X) . "
        ":C(?X) :- :O(?X), NOT :B(?X) . "
        ":D(?X) :- :O(?X), NOT :C(?X) . "
        ":E(?X) :- :O(?X), NOT :D(?X) . "
        ":F(?X) :- :O(?X), NOT :E(?X) . "
    );
    addTriples(
        ":a rdf:type :O . "
        ":b rdf:type :A . "
    );
    applyRules();
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":a rdf:type :O . "
        ":a rdf:type :B . "
        ":a rdf:type :D . "
        ":a rdf:type :F . "

        ":b rdf:type :A . "
    );
    // Now remove a rule in the middle.
    removeRules(
        ":D(?X) :- :O(?X), NOT :C(?X) . "
    );
    applyRulesIncrementally();
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":a rdf:type :O . "
        ":a rdf:type :B . "
        ":a rdf:type :E . "

        ":b rdf:type :A . "
    );
    // Restore a rule in the middle.
    addRules(
        ":D(?X) :- :O(?X), NOT :C(?X) . "
    );
    applyRulesIncrementally();
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":a rdf:type :O . "
        ":a rdf:type :B . "
        ":a rdf:type :D . "
        ":a rdf:type :F . "

        ":b rdf:type :A . "
    );
}

TEST(testNonrepetitionNegativeSingleAtom) {
    addRules(
        "[?X,rdf:type,:D] :- [?X,rdf:type,:A], NOT [?X,rdf:type,:B], NOT [?X,rdf:type,:C] . "
    );
    addTriples(
        ":a rdf:type :A . "

        ":b rdf:type :A . "
        ":b rdf:type :B . "
        ":b rdf:type :C . "
    );
    applyRules();
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":a rdf:type :A . "
        ":a rdf:type :D . "

        ":b rdf:type :A . "
        ":b rdf:type :B . "
        ":b rdf:type :C . "
    );
    // This introduces potential repetition in both addition and deletion! We need two negative atoms because negative atoms can be before pivot only for negative atoms.
    forDeletion(
        ":b rdf:type :B . "
        ":b rdf:type :C . "
    );
    forAddition(
        ":a rdf:type :B . "
        ":a rdf:type :C . "
    );
    if (m_useDRed)
        ASSERT_APPLY_RULES_INCREMENTALLY(
            "== LEVEL   0 ===============================================\n"
            "  0:    Applying deletion rules to tuples from previous levels\n"
            "  0:    Applying recursive deletion rules\n"
            "  0:        Extracted possibly deleted tuple [ :b, rdf:type, :B ]\n"
            "  0:            Applying deletion rules to [ :b, rdf:type, :B ]\n"
            "  0:        Extracted possibly deleted tuple [ :b, rdf:type, :C ]\n"
            "  0:            Applying deletion rules to [ :b, rdf:type, :C ]\n"
            "  0:    Checking provability of [ :b, rdf:type, :B ]    { added to checked }\n"
            "  0:    Checking provability of [ :b, rdf:type, :C ]    { added to checked }\n"
            "  0:    Applying insertion rules to tuples from previous levels\n"
            "  0:    Applying recursive insertion rules\n"
            "  0:        Extracted current tuple [ :a, rdf:type, :B ]\n"
            "  0:            Tuple added [ :a, rdf:type, :B ]    { added }\n"
            "  0:        Extracted current tuple [ :a, rdf:type, :C ]\n"
            "  0:            Tuple added [ :a, rdf:type, :C ]    { added }\n"
            "============================================================\n"
            "== LEVEL   1 ===============================================\n"
            "  0:    Applying deletion rules to tuples from previous levels\n"
            "  0:        Applying deletion rules to [ :b, rdf:type, :B ]\n"
            "  0:        Applying deletion rules to [ :b, rdf:type, :C ]\n"
            "  0:        Applying deletion rules to [ :a, rdf:type, :B ]\n"
            "  0:            Matched atom NOT [?X, rdf:type, :B] to tuple [ :a ]\n"
            "  0:                Matching atom [?X, rdf:type, :A]\n"
            "  0:                    Matched atom [?X, rdf:type, :A] to tuple [ :a, rdf:type, :A ]\n"
            "  0:                        Matching atom NOT [?X, rdf:type, :C]\n"
            "  0:                            Matched atom NOT [?X, rdf:type, :C] to tuple [ :a ]\n"
            "  0:                                Matched rule [?X, rdf:type, :D] :- [?X, rdf:type, :A], NOT [?X, rdf:type, :B], NOT [?X, rdf:type, :C] .\n"
            "  0:                                    Derived tuple [ :a, rdf:type, :D ]    { normal, added }\n"
            "  0:        Applying deletion rules to [ :a, rdf:type, :C ]\n"
            "  0:            Matched atom NOT [?X, rdf:type, :C] to tuple [ :a ]\n"
            "  0:                Matching atom [?X, rdf:type, :A]\n"
            "  0:                    Matched atom [?X, rdf:type, :A] to tuple [ :a, rdf:type, :A ]\n"
            "  0:                        Matching atom NOT [?X, rdf:type, :B]\n"
            "  0:    Applying recursive deletion rules\n"
            "  0:        Extracted possibly deleted tuple [ :a, rdf:type, :D ]\n"
            "  0:            Applying deletion rules to [ :a, rdf:type, :D ]\n"
            "  0:    Checking provability of [ :a, rdf:type, :D ]    { added to checked }\n"
            "  0:        Checking nonrecursive rule [?X, rdf:type, :D] :- [?X, rdf:type, :A], NOT [?X, rdf:type, :B], NOT [?X, rdf:type, :C] .\n"
            "  0:    Applying insertion rules to tuples from previous levels\n"
            "  0:        Extracted current tuple [ :a, rdf:type, :B ]\n"
            "  0:        Extracted current tuple [ :a, rdf:type, :C ]\n"
            "  0:        Extracted current tuple [ :b, rdf:type, :B ]\n"
            "  0:            Matched atom NOT [?X, rdf:type, :B] to tuple [ :b ]\n"
            "  0:                Matching atom [?X, rdf:type, :A]\n"
            "  0:                    Matched atom [?X, rdf:type, :A] to tuple [ :b, rdf:type, :A ]\n"
            "  0:                        Matching atom NOT [?X, rdf:type, :C]\n"
            "  0:                            Matched atom NOT [?X, rdf:type, :C] to tuple [ :b ]\n"
            "  0:                                Matched rule [?X, rdf:type, :D] :- [?X, rdf:type, :A], NOT [?X, rdf:type, :B], NOT [?X, rdf:type, :C] .\n"
            "  0:                                    Derived tuple [ :b, rdf:type, :D ]    { normal, added }\n"
            "  0:        Extracted current tuple [ :b, rdf:type, :C ]\n"
            "  0:            Matched atom NOT [?X, rdf:type, :C] to tuple [ :b ]\n"
            "  0:                Matching atom [?X, rdf:type, :A]\n"
            "  0:                    Matched atom [?X, rdf:type, :A] to tuple [ :b, rdf:type, :A ]\n"
            "  0:                        Matching atom NOT [?X, rdf:type, :B]\n"
            "  0:        Extracted current tuple [ :a, rdf:type, :D ]\n"
            "  0:    Applying recursive insertion rules\n"
            "  0:        Extracted current tuple [ :b, rdf:type, :D ]\n"
            "  0:            Tuple added [ :b, rdf:type, :D ]    { added }\n"
            "============================================================\n"
            "  0:    Propagating deleted and proved tuples into the store for level 0\n"
            "  0:        Tuple deleted [ :b, rdf:type, :B ]    { deleted }\n"
            "  0:        Tuple deleted [ :b, rdf:type, :C ]    { deleted }\n"
            "  0:        Tuple added [ :a, rdf:type, :B ]    { added }\n"
            "  0:        Tuple added [ :a, rdf:type, :C ]    { added }\n"
            "  0:    Propagating deleted and proved tuples into the store for level 1\n"
            "  0:        Tuple deleted [ :a, rdf:type, :D ]    { deleted }\n"
            "  0:        Tuple added [ :b, rdf:type, :D ]    { added }\n"
        );
    else
        ASSERT_APPLY_RULES_INCREMENTALLY(
            "== LEVEL   0 ===============================================\n"
            "  0:    Applying deletion rules to tuples from previous levels\n"
            "  0:    Applying recursive deletion rules\n"
            "  0:        Extracted possibly deleted tuple [ :b, rdf:type, :B ]\n"
            "  0:            Checking provability of [ :b, rdf:type, :B ]    { added to checked }\n"
            "  0:            Tuple disproved [ :b, rdf:type, :B ]    { added }\n"
            "  0:            Applying deletion rules to [ :b, rdf:type, :B ]\n"
            "  0:        Extracted possibly deleted tuple [ :b, rdf:type, :C ]\n"
            "  0:            Checking provability of [ :b, rdf:type, :C ]    { added to checked }\n"
            "  0:            Tuple disproved [ :b, rdf:type, :C ]    { added }\n"
            "  0:            Applying deletion rules to [ :b, rdf:type, :C ]\n"
            "  0:    Applying insertion rules to tuples from previous levels\n"
            "  0:    Applying recursive insertion rules\n"
            "  0:        Extracted current tuple [ :a, rdf:type, :B ]\n"
            "  0:            Tuple added [ :a, rdf:type, :B ]    { added }\n"
            "  0:        Extracted current tuple [ :a, rdf:type, :C ]\n"
            "  0:            Tuple added [ :a, rdf:type, :C ]    { added }\n"
            "============================================================\n"
            "== LEVEL   1 ===============================================\n"
            "  0:    Applying deletion rules to tuples from previous levels\n"
            "  0:        Applying deletion rules to [ :b, rdf:type, :B ]\n"
            "  0:        Applying deletion rules to [ :b, rdf:type, :C ]\n"
            "  0:        Applying deletion rules to [ :a, rdf:type, :B ]\n"
            "  0:            Matched atom NOT [?X, rdf:type, :B] to tuple [ :a ]\n"
            "  0:                Matching atom [?X, rdf:type, :A]\n"
            "  0:                    Matched atom [?X, rdf:type, :A] to tuple [ :a, rdf:type, :A ]\n"
            "  0:                        Matching atom NOT [?X, rdf:type, :C]\n"
            "  0:                            Matched atom NOT [?X, rdf:type, :C] to tuple [ :a ]\n"
            "  0:                                Matched rule [?X, rdf:type, :D] :- [?X, rdf:type, :A], NOT [?X, rdf:type, :B], NOT [?X, rdf:type, :C] .\n"
            "  0:                                    Derived tuple [ :a, rdf:type, :D ]    { normal, added }\n"
            "  0:        Applying deletion rules to [ :a, rdf:type, :C ]\n"
            "  0:            Matched atom NOT [?X, rdf:type, :C] to tuple [ :a ]\n"
            "  0:                Matching atom [?X, rdf:type, :A]\n"
            "  0:                    Matched atom [?X, rdf:type, :A] to tuple [ :a, rdf:type, :A ]\n"
            "  0:                        Matching atom NOT [?X, rdf:type, :B]\n"
            "  0:    Applying recursive deletion rules\n"
            "  0:        Extracted possibly deleted tuple [ :a, rdf:type, :D ]\n"
            "  0:            Checking provability of [ :a, rdf:type, :D ]    { added to checked }\n"
            "  0:                Checking nonrecursive rule [?X, rdf:type, :D] :- [?X, rdf:type, :A], NOT [?X, rdf:type, :B], NOT [?X, rdf:type, :C] .\n"
            "  0:            Tuple disproved [ :a, rdf:type, :D ]    { added }\n"
            "  0:            Applying deletion rules to [ :a, rdf:type, :D ]\n"
            "  0:    Applying insertion rules to tuples from previous levels\n"
            "  0:        Extracted current tuple [ :a, rdf:type, :B ]\n"
            "  0:        Extracted current tuple [ :a, rdf:type, :C ]\n"
            "  0:        Extracted current tuple [ :b, rdf:type, :B ]\n"
            "  0:            Matched atom NOT [?X, rdf:type, :B] to tuple [ :b ]\n"
            "  0:                Matching atom [?X, rdf:type, :A]\n"
            "  0:                    Matched atom [?X, rdf:type, :A] to tuple [ :b, rdf:type, :A ]\n"
            "  0:                        Matching atom NOT [?X, rdf:type, :C]\n"
            "  0:                            Matched atom NOT [?X, rdf:type, :C] to tuple [ :b ]\n"
            "  0:                                Matched rule [?X, rdf:type, :D] :- [?X, rdf:type, :A], NOT [?X, rdf:type, :B], NOT [?X, rdf:type, :C] .\n"
            "  0:                                    Derived tuple [ :b, rdf:type, :D ]    { normal, added }\n"
            "  0:        Extracted current tuple [ :b, rdf:type, :C ]\n"
            "  0:            Matched atom NOT [?X, rdf:type, :C] to tuple [ :b ]\n"
            "  0:                Matching atom [?X, rdf:type, :A]\n"
            "  0:                    Matched atom [?X, rdf:type, :A] to tuple [ :b, rdf:type, :A ]\n"
            "  0:                        Matching atom NOT [?X, rdf:type, :B]\n"
            "  0:        Extracted current tuple [ :a, rdf:type, :D ]\n"
            "  0:    Applying recursive insertion rules\n"
            "  0:        Extracted current tuple [ :b, rdf:type, :D ]\n"
            "  0:            Tuple added [ :b, rdf:type, :D ]    { added }\n"
            "============================================================\n"
            "  0:    Propagating deleted and proved tuples into the store for level 0\n"
            "  0:        Tuple deleted [ :b, rdf:type, :B ]    { deleted }\n"
            "  0:        Tuple deleted [ :b, rdf:type, :C ]    { deleted }\n"
            "  0:        Tuple added [ :a, rdf:type, :B ]    { added }\n"
            "  0:        Tuple added [ :a, rdf:type, :C ]    { added }\n"
            "  0:    Propagating deleted and proved tuples into the store for level 1\n"
            "  0:        Tuple deleted [ :a, rdf:type, :D ]    { deleted }\n"
            "  0:        Tuple added [ :b, rdf:type, :D ]    { added }\n"
        );
    // Check the result.
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":a rdf:type :A . "
        ":a rdf:type :B . "
        ":a rdf:type :C . "

        ":b rdf:type :A . "
        ":b rdf:type :D . "
    );
}

TEST(testNonrepetitionNegativeMultipleAtoms) {
    // This test documents that the incremental algorithms do not ensure the nonrepetition property for the case when the rules
    // contain negative atoms with conjunctions. Refer to AbstractDeletionTaskImpl.h and InsertionTask.cpp for more information.
    addRules(
        "[?X,rdf:type,:D] :- [?X,rdf:type,:A], NOT EXISTS ?Y IN ([?X,:R,?Y], [?Y,rdf:type,:B]), NOT EXISTS ?Z IN ([?X,:S,?Z], [?Z,rdf:type,:C]) . "
    );
    addTriples(
        ":a rdf:type :A . "

        ":b rdf:type :A . "
        ":b :R :b1 . "
        ":b1 rdf:type :B . "
        ":b :S :b2 . "
        ":b2 rdf:type :C . "

        ":c rdf:type :A . "
        ":c :R :c1 . "
        ":c1 rdf:type :B . "
        ":c :S :c2 . "
        ":c2 rdf:type :C . "

        ":d rdf:type :A . "
        ":d :R :d1 . "
        ":d1 rdf:type :B . "
        ":d :R :d2 . "
        ":d2 rdf:type :B . "
        ":d :S :d3 . "
        ":d3 rdf:type :C . "
        ":d :S :d4 . "
        ":d4 rdf:type :C . "
    );
    applyRules();
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":a rdf:type :A . "
        ":a rdf:type :D . "

        ":b rdf:type :A . "
        ":b :R :b1 . "
        ":b1 rdf:type :B . "
        ":b :S :b2 . "
        ":b2 rdf:type :C . "

        ":c rdf:type :A . "
        ":c :R :c1 . "
        ":c1 rdf:type :B . "
        ":c :S :c2 . "
        ":c2 rdf:type :C . "

        ":d rdf:type :A . "
        ":d :R :d1 . "
        ":d1 rdf:type :B . "
        ":d :R :d2 . "
        ":d2 rdf:type :B . "
        ":d :S :d3 . "
        ":d3 rdf:type :C . "
        ":d :S :d4 . "
        ":d4 rdf:type :C . "
    );
    // This introduces potential repetition in both addition and deletion! We need two negative atoms because negative atoms can be before pivot only for negative atoms.
    forDeletion(
        ":b :R :b1 . "
        ":b :S :b2 . "

        ":d :R :d1 . "
        ":d :S :d3 . "
    );
    forAddition(
        ":a :R :a1 . "
        ":a1 rdf:type :B . "
        ":a :S :a2 . "
        ":a2 rdf:type :C . "

        ":c :R :c3 . "
        ":c3 rdf:type :B . "
        ":c :S :c4 . "
        ":c4 rdf:type :C . "
    );
    if (m_useDRed)
        ASSERT_APPLY_RULES_INCREMENTALLY(
            "== LEVEL   0 ===============================================\n"
            "  0:    Applying deletion rules to tuples from previous levels\n"
            "  0:    Applying recursive deletion rules\n"
            "  0:        Extracted possibly deleted tuple [ :b, :R, :b1 ]\n"
            "  0:            Applying deletion rules to [ :b, :R, :b1 ]\n"
            "  0:        Extracted possibly deleted tuple [ :b, :S, :b2 ]\n"
            "  0:            Applying deletion rules to [ :b, :S, :b2 ]\n"
            "  0:        Extracted possibly deleted tuple [ :d, :R, :d1 ]\n"
            "  0:            Applying deletion rules to [ :d, :R, :d1 ]\n"
            "  0:        Extracted possibly deleted tuple [ :d, :S, :d3 ]\n"
            "  0:            Applying deletion rules to [ :d, :S, :d3 ]\n"
            "  0:    Checking provability of [ :b, :R, :b1 ]    { added to checked }\n"
            "  0:    Checking provability of [ :b, :S, :b2 ]    { added to checked }\n"
            "  0:    Checking provability of [ :d, :R, :d1 ]    { added to checked }\n"
            "  0:    Checking provability of [ :d, :S, :d3 ]    { added to checked }\n"
            "  0:    Applying insertion rules to tuples from previous levels\n"
            "  0:    Applying recursive insertion rules\n"
            "  0:        Extracted current tuple [ :a, :R, :a1 ]\n"
            "  0:            Tuple added [ :a, :R, :a1 ]    { added }\n"
            "  0:        Extracted current tuple [ :a1, rdf:type, :B ]\n"
            "  0:            Tuple added [ :a1, rdf:type, :B ]    { added }\n"
            "  0:        Extracted current tuple [ :a, :S, :a2 ]\n"
            "  0:            Tuple added [ :a, :S, :a2 ]    { added }\n"
            "  0:        Extracted current tuple [ :a2, rdf:type, :C ]\n"
            "  0:            Tuple added [ :a2, rdf:type, :C ]    { added }\n"
            "  0:        Extracted current tuple [ :c, :R, :c3 ]\n"
            "  0:            Tuple added [ :c, :R, :c3 ]    { added }\n"
            "  0:        Extracted current tuple [ :c3, rdf:type, :B ]\n"
            "  0:            Tuple added [ :c3, rdf:type, :B ]    { added }\n"
            "  0:        Extracted current tuple [ :c, :S, :c4 ]\n"
            "  0:            Tuple added [ :c, :S, :c4 ]    { added }\n"
            "  0:        Extracted current tuple [ :c4, rdf:type, :C ]\n"
            "  0:            Tuple added [ :c4, rdf:type, :C ]    { added }\n"
            "============================================================\n"
            "== LEVEL   1 ===============================================\n"
            "  0:    Applying deletion rules to tuples from previous levels\n"
            "  0:        Applying deletion rules to [ :b, :R, :b1 ]\n"
            "  0:        Applying deletion rules to [ :b, :S, :b2 ]\n"
            "  0:        Applying deletion rules to [ :d, :R, :d1 ]\n"
            "  0:        Applying deletion rules to [ :d, :S, :d3 ]\n"
            "  0:        Applying deletion rules to [ :a, :R, :a1 ]\n"
            "  0:            Matched atom NOT EXISTS ?Y IN([?X, :R, ?Y], [?Y, rdf:type, :B]) to tuple [ :a ]\n"
            "  0:                Matching atom [?X, rdf:type, :A]\n"
            "  0:                    Matched atom [?X, rdf:type, :A] to tuple [ :a, rdf:type, :A ]\n"
            "  0:                        Matching atom NOT EXISTS ?Z IN([?X, :S, ?Z], [?Z, rdf:type, :C])\n"
            "  0:                            Matched atom NOT EXISTS ?Z IN([?X, :S, ?Z], [?Z, rdf:type, :C]) to tuple [ :a ]\n"
            "  0:                                Matched rule [?X, rdf:type, :D] :- [?X, rdf:type, :A], NOT EXISTS ?Y IN([?X, :R, ?Y], [?Y, rdf:type, :B]), NOT EXISTS ?Z IN([?X, :S, ?Z], [?Z, rdf:type, :C]) .\n"
            "  0:                                    Derived tuple [ :a, rdf:type, :D ]    { normal, added }\n"
            "  0:        Applying deletion rules to [ :a1, rdf:type, :B ]\n"
            "  0:        Applying deletion rules to [ :a, :S, :a2 ]\n"
            "  0:            Matched atom NOT EXISTS ?Z IN([?X, :S, ?Z], [?Z, rdf:type, :C]) to tuple [ :a ]\n"
            "  0:                Matching atom [?X, rdf:type, :A]\n"
            "  0:                    Matched atom [?X, rdf:type, :A] to tuple [ :a, rdf:type, :A ]\n"
            "  0:                        Matching atom NOT EXISTS ?Y IN([?X, :R, ?Y], [?Y, rdf:type, :B])\n"
            "  0:                            Matched atom NOT EXISTS ?Y IN([?X, :R, ?Y], [?Y, rdf:type, :B]) to tuple [ :a ]\n"
            "  0:                                Matched rule [?X, rdf:type, :D] :- [?X, rdf:type, :A], NOT EXISTS ?Y IN([?X, :R, ?Y], [?Y, rdf:type, :B]), NOT EXISTS ?Z IN([?X, :S, ?Z], [?Z, rdf:type, :C]) .\n"
            "  0:                                    Derived tuple [ :a, rdf:type, :D ]    { normal, not added }\n"
            "  0:        Applying deletion rules to [ :a2, rdf:type, :C ]\n"
            "  0:        Applying deletion rules to [ :c, :R, :c3 ]\n"
            "  0:        Applying deletion rules to [ :c3, rdf:type, :B ]\n"
            "  0:        Applying deletion rules to [ :c, :S, :c4 ]\n"
            "  0:        Applying deletion rules to [ :c4, rdf:type, :C ]\n"
            "  0:    Applying recursive deletion rules\n"
            "  0:        Extracted possibly deleted tuple [ :a, rdf:type, :D ]\n"
            "  0:            Applying deletion rules to [ :a, rdf:type, :D ]\n"
            "  0:    Checking provability of [ :a, rdf:type, :D ]    { added to checked }\n"
            "  0:        Checking nonrecursive rule [?X, rdf:type, :D] :- [?X, rdf:type, :A], NOT EXISTS ?Y IN([?X, :R, ?Y], [?Y, rdf:type, :B]), NOT EXISTS ?Z IN([?X, :S, ?Z], [?Z, rdf:type, :C]) .\n"
            "  0:    Applying insertion rules to tuples from previous levels\n"
            "  0:        Extracted current tuple [ :a, :R, :a1 ]\n"
            "  0:        Extracted current tuple [ :a1, rdf:type, :B ]\n"
            "  0:        Extracted current tuple [ :a, :S, :a2 ]\n"
            "  0:        Extracted current tuple [ :a2, rdf:type, :C ]\n"
            "  0:        Extracted current tuple [ :c, :R, :c3 ]\n"
            "  0:        Extracted current tuple [ :c3, rdf:type, :B ]\n"
            "  0:        Extracted current tuple [ :c, :S, :c4 ]\n"
            "  0:        Extracted current tuple [ :c4, rdf:type, :C ]\n"
            "  0:        Extracted current tuple [ :b, :R, :b1 ]\n"
            "  0:            Matched atom NOT EXISTS ?Y IN([?X, :R, ?Y], [?Y, rdf:type, :B]) to tuple [ :b ]\n"
            "  0:                Matching atom [?X, rdf:type, :A]\n"
            "  0:                    Matched atom [?X, rdf:type, :A] to tuple [ :b, rdf:type, :A ]\n"
            "  0:                        Matching atom NOT EXISTS ?Z IN([?X, :S, ?Z], [?Z, rdf:type, :C])\n"
            "  0:                            Matched atom NOT EXISTS ?Z IN([?X, :S, ?Z], [?Z, rdf:type, :C]) to tuple [ :b ]\n"
            "  0:                                Matched rule [?X, rdf:type, :D] :- [?X, rdf:type, :A], NOT EXISTS ?Y IN([?X, :R, ?Y], [?Y, rdf:type, :B]), NOT EXISTS ?Z IN([?X, :S, ?Z], [?Z, rdf:type, :C]) .\n"
            "  0:                                    Derived tuple [ :b, rdf:type, :D ]    { normal, added }\n"
            "  0:        Extracted current tuple [ :b, :S, :b2 ]\n"
            "  0:            Matched atom NOT EXISTS ?Z IN([?X, :S, ?Z], [?Z, rdf:type, :C]) to tuple [ :b ]\n"
            "  0:                Matching atom [?X, rdf:type, :A]\n"
            "  0:                    Matched atom [?X, rdf:type, :A] to tuple [ :b, rdf:type, :A ]\n"
            "  0:                        Matching atom NOT EXISTS ?Y IN([?X, :R, ?Y], [?Y, rdf:type, :B])\n"
            "  0:                            Matched atom NOT EXISTS ?Y IN([?X, :R, ?Y], [?Y, rdf:type, :B]) to tuple [ :b ]\n"
            "  0:                                Matched rule [?X, rdf:type, :D] :- [?X, rdf:type, :A], NOT EXISTS ?Y IN([?X, :R, ?Y], [?Y, rdf:type, :B]), NOT EXISTS ?Z IN([?X, :S, ?Z], [?Z, rdf:type, :C]) .\n"
            "  0:                                    Derived tuple [ :b, rdf:type, :D ]    { normal, not added }\n"
            "  0:        Extracted current tuple [ :d, :R, :d1 ]\n"
            "  0:        Extracted current tuple [ :d, :S, :d3 ]\n"
            "  0:        Extracted current tuple [ :a, rdf:type, :D ]\n"
            "  0:    Applying recursive insertion rules\n"
            "  0:        Extracted current tuple [ :b, rdf:type, :D ]\n"
            "  0:            Tuple added [ :b, rdf:type, :D ]    { added }\n"
            "============================================================\n"
            "  0:    Propagating deleted and proved tuples into the store for level 0\n"
            "  0:        Tuple deleted [ :b, :R, :b1 ]    { deleted }\n"
            "  0:        Tuple deleted [ :b, :S, :b2 ]    { deleted }\n"
            "  0:        Tuple deleted [ :d, :R, :d1 ]    { deleted }\n"
            "  0:        Tuple deleted [ :d, :S, :d3 ]    { deleted }\n"
            "  0:        Tuple added [ :a, :R, :a1 ]    { added }\n"
            "  0:        Tuple added [ :a1, rdf:type, :B ]    { added }\n"
            "  0:        Tuple added [ :a, :S, :a2 ]    { added }\n"
            "  0:        Tuple added [ :a2, rdf:type, :C ]    { added }\n"
            "  0:        Tuple added [ :c, :R, :c3 ]    { added }\n"
            "  0:        Tuple added [ :c3, rdf:type, :B ]    { added }\n"
            "  0:        Tuple added [ :c, :S, :c4 ]    { added }\n"
            "  0:        Tuple added [ :c4, rdf:type, :C ]    { added }\n"
            "  0:    Propagating deleted and proved tuples into the store for level 1\n"
            "  0:        Tuple deleted [ :a, rdf:type, :D ]    { deleted }\n"
            "  0:        Tuple added [ :b, rdf:type, :D ]    { added }\n"

        );
    else
        ASSERT_APPLY_RULES_INCREMENTALLY(
            "== LEVEL   0 ===============================================\n"
            "  0:    Applying deletion rules to tuples from previous levels\n"
            "  0:    Applying recursive deletion rules\n"
            "  0:        Extracted possibly deleted tuple [ :b, :R, :b1 ]\n"
            "  0:            Checking provability of [ :b, :R, :b1 ]    { added to checked }\n"
            "  0:            Tuple disproved [ :b, :R, :b1 ]    { added }\n"
            "  0:            Applying deletion rules to [ :b, :R, :b1 ]\n"
            "  0:        Extracted possibly deleted tuple [ :b, :S, :b2 ]\n"
            "  0:            Checking provability of [ :b, :S, :b2 ]    { added to checked }\n"
            "  0:            Tuple disproved [ :b, :S, :b2 ]    { added }\n"
            "  0:            Applying deletion rules to [ :b, :S, :b2 ]\n"
            "  0:        Extracted possibly deleted tuple [ :d, :R, :d1 ]\n"
            "  0:            Checking provability of [ :d, :R, :d1 ]    { added to checked }\n"
            "  0:            Tuple disproved [ :d, :R, :d1 ]    { added }\n"
            "  0:            Applying deletion rules to [ :d, :R, :d1 ]\n"
            "  0:        Extracted possibly deleted tuple [ :d, :S, :d3 ]\n"
            "  0:            Checking provability of [ :d, :S, :d3 ]    { added to checked }\n"
            "  0:            Tuple disproved [ :d, :S, :d3 ]    { added }\n"
            "  0:            Applying deletion rules to [ :d, :S, :d3 ]\n"
            "  0:    Applying insertion rules to tuples from previous levels\n"
            "  0:    Applying recursive insertion rules\n"
            "  0:        Extracted current tuple [ :a, :R, :a1 ]\n"
            "  0:            Tuple added [ :a, :R, :a1 ]    { added }\n"
            "  0:        Extracted current tuple [ :a1, rdf:type, :B ]\n"
            "  0:            Tuple added [ :a1, rdf:type, :B ]    { added }\n"
            "  0:        Extracted current tuple [ :a, :S, :a2 ]\n"
            "  0:            Tuple added [ :a, :S, :a2 ]    { added }\n"
            "  0:        Extracted current tuple [ :a2, rdf:type, :C ]\n"
            "  0:            Tuple added [ :a2, rdf:type, :C ]    { added }\n"
            "  0:        Extracted current tuple [ :c, :R, :c3 ]\n"
            "  0:            Tuple added [ :c, :R, :c3 ]    { added }\n"
            "  0:        Extracted current tuple [ :c3, rdf:type, :B ]\n"
            "  0:            Tuple added [ :c3, rdf:type, :B ]    { added }\n"
            "  0:        Extracted current tuple [ :c, :S, :c4 ]\n"
            "  0:            Tuple added [ :c, :S, :c4 ]    { added }\n"
            "  0:        Extracted current tuple [ :c4, rdf:type, :C ]\n"
            "  0:            Tuple added [ :c4, rdf:type, :C ]    { added }\n"
            "============================================================\n"
            "== LEVEL   1 ===============================================\n"
            "  0:    Applying deletion rules to tuples from previous levels\n"
            "  0:        Applying deletion rules to [ :b, :R, :b1 ]\n"
            "  0:        Applying deletion rules to [ :b, :S, :b2 ]\n"
            "  0:        Applying deletion rules to [ :d, :R, :d1 ]\n"
            "  0:        Applying deletion rules to [ :d, :S, :d3 ]\n"
            "  0:        Applying deletion rules to [ :a, :R, :a1 ]\n"
            "  0:            Matched atom NOT EXISTS ?Y IN([?X, :R, ?Y], [?Y, rdf:type, :B]) to tuple [ :a ]\n"
            "  0:                Matching atom [?X, rdf:type, :A]\n"
            "  0:                    Matched atom [?X, rdf:type, :A] to tuple [ :a, rdf:type, :A ]\n"
            "  0:                        Matching atom NOT EXISTS ?Z IN([?X, :S, ?Z], [?Z, rdf:type, :C])\n"
            "  0:                            Matched atom NOT EXISTS ?Z IN([?X, :S, ?Z], [?Z, rdf:type, :C]) to tuple [ :a ]\n"
            "  0:                                Matched rule [?X, rdf:type, :D] :- [?X, rdf:type, :A], NOT EXISTS ?Y IN([?X, :R, ?Y], [?Y, rdf:type, :B]), NOT EXISTS ?Z IN([?X, :S, ?Z], [?Z, rdf:type, :C]) .\n"
            "  0:                                    Derived tuple [ :a, rdf:type, :D ]    { normal, added }\n"
            "  0:        Applying deletion rules to [ :a1, rdf:type, :B ]\n"
            "  0:        Applying deletion rules to [ :a, :S, :a2 ]\n"
            "  0:            Matched atom NOT EXISTS ?Z IN([?X, :S, ?Z], [?Z, rdf:type, :C]) to tuple [ :a ]\n"
            "  0:                Matching atom [?X, rdf:type, :A]\n"
            "  0:                    Matched atom [?X, rdf:type, :A] to tuple [ :a, rdf:type, :A ]\n"
            "  0:                        Matching atom NOT EXISTS ?Y IN([?X, :R, ?Y], [?Y, rdf:type, :B])\n"
            "  0:                            Matched atom NOT EXISTS ?Y IN([?X, :R, ?Y], [?Y, rdf:type, :B]) to tuple [ :a ]\n"
            "  0:                                Matched rule [?X, rdf:type, :D] :- [?X, rdf:type, :A], NOT EXISTS ?Y IN([?X, :R, ?Y], [?Y, rdf:type, :B]), NOT EXISTS ?Z IN([?X, :S, ?Z], [?Z, rdf:type, :C]) .\n"
            "  0:                                    Derived tuple [ :a, rdf:type, :D ]    { normal, not added }\n"
            "  0:        Applying deletion rules to [ :a2, rdf:type, :C ]\n"
            "  0:        Applying deletion rules to [ :c, :R, :c3 ]\n"
            "  0:        Applying deletion rules to [ :c3, rdf:type, :B ]\n"
            "  0:        Applying deletion rules to [ :c, :S, :c4 ]\n"
            "  0:        Applying deletion rules to [ :c4, rdf:type, :C ]\n"
            "  0:    Applying recursive deletion rules\n"
            "  0:        Extracted possibly deleted tuple [ :a, rdf:type, :D ]\n"
            "  0:            Checking provability of [ :a, rdf:type, :D ]    { added to checked }\n"
            "  0:                Checking nonrecursive rule [?X, rdf:type, :D] :- [?X, rdf:type, :A], NOT EXISTS ?Y IN([?X, :R, ?Y], [?Y, rdf:type, :B]), NOT EXISTS ?Z IN([?X, :S, ?Z], [?Z, rdf:type, :C]) .\n"
            "  0:            Tuple disproved [ :a, rdf:type, :D ]    { added }\n"
            "  0:            Applying deletion rules to [ :a, rdf:type, :D ]\n"
            "  0:    Applying insertion rules to tuples from previous levels\n"
            "  0:        Extracted current tuple [ :a, :R, :a1 ]\n"
            "  0:        Extracted current tuple [ :a1, rdf:type, :B ]\n"
            "  0:        Extracted current tuple [ :a, :S, :a2 ]\n"
            "  0:        Extracted current tuple [ :a2, rdf:type, :C ]\n"
            "  0:        Extracted current tuple [ :c, :R, :c3 ]\n"
            "  0:        Extracted current tuple [ :c3, rdf:type, :B ]\n"
            "  0:        Extracted current tuple [ :c, :S, :c4 ]\n"
            "  0:        Extracted current tuple [ :c4, rdf:type, :C ]\n"
            "  0:        Extracted current tuple [ :b, :R, :b1 ]\n"
            "  0:            Matched atom NOT EXISTS ?Y IN([?X, :R, ?Y], [?Y, rdf:type, :B]) to tuple [ :b ]\n"
            "  0:                Matching atom [?X, rdf:type, :A]\n"
            "  0:                    Matched atom [?X, rdf:type, :A] to tuple [ :b, rdf:type, :A ]\n"
            "  0:                        Matching atom NOT EXISTS ?Z IN([?X, :S, ?Z], [?Z, rdf:type, :C])\n"
            "  0:                            Matched atom NOT EXISTS ?Z IN([?X, :S, ?Z], [?Z, rdf:type, :C]) to tuple [ :b ]\n"
            "  0:                                Matched rule [?X, rdf:type, :D] :- [?X, rdf:type, :A], NOT EXISTS ?Y IN([?X, :R, ?Y], [?Y, rdf:type, :B]), NOT EXISTS ?Z IN([?X, :S, ?Z], [?Z, rdf:type, :C]) .\n"
            "  0:                                    Derived tuple [ :b, rdf:type, :D ]    { normal, added }\n"
            "  0:        Extracted current tuple [ :b, :S, :b2 ]\n"
            "  0:            Matched atom NOT EXISTS ?Z IN([?X, :S, ?Z], [?Z, rdf:type, :C]) to tuple [ :b ]\n"
            "  0:                Matching atom [?X, rdf:type, :A]\n"
            "  0:                    Matched atom [?X, rdf:type, :A] to tuple [ :b, rdf:type, :A ]\n"
            "  0:                        Matching atom NOT EXISTS ?Y IN([?X, :R, ?Y], [?Y, rdf:type, :B])\n"
            "  0:                            Matched atom NOT EXISTS ?Y IN([?X, :R, ?Y], [?Y, rdf:type, :B]) to tuple [ :b ]\n"
            "  0:                                Matched rule [?X, rdf:type, :D] :- [?X, rdf:type, :A], NOT EXISTS ?Y IN([?X, :R, ?Y], [?Y, rdf:type, :B]), NOT EXISTS ?Z IN([?X, :S, ?Z], [?Z, rdf:type, :C]) .\n"
            "  0:                                    Derived tuple [ :b, rdf:type, :D ]    { normal, not added }\n"
            "  0:        Extracted current tuple [ :d, :R, :d1 ]\n"
            "  0:        Extracted current tuple [ :d, :S, :d3 ]\n"
            "  0:        Extracted current tuple [ :a, rdf:type, :D ]\n"
            "  0:    Applying recursive insertion rules\n"
            "  0:        Extracted current tuple [ :b, rdf:type, :D ]\n"
            "  0:            Tuple added [ :b, rdf:type, :D ]    { added }\n"
            "============================================================\n"
            "  0:    Propagating deleted and proved tuples into the store for level 0\n"
            "  0:        Tuple deleted [ :b, :R, :b1 ]    { deleted }\n"
            "  0:        Tuple deleted [ :b, :S, :b2 ]    { deleted }\n"
            "  0:        Tuple deleted [ :d, :R, :d1 ]    { deleted }\n"
            "  0:        Tuple deleted [ :d, :S, :d3 ]    { deleted }\n"
            "  0:        Tuple added [ :a, :R, :a1 ]    { added }\n"
            "  0:        Tuple added [ :a1, rdf:type, :B ]    { added }\n"
            "  0:        Tuple added [ :a, :S, :a2 ]    { added }\n"
            "  0:        Tuple added [ :a2, rdf:type, :C ]    { added }\n"
            "  0:        Tuple added [ :c, :R, :c3 ]    { added }\n"
            "  0:        Tuple added [ :c3, rdf:type, :B ]    { added }\n"
            "  0:        Tuple added [ :c, :S, :c4 ]    { added }\n"
            "  0:        Tuple added [ :c4, rdf:type, :C ]    { added }\n"
            "  0:    Propagating deleted and proved tuples into the store for level 1\n"
            "  0:        Tuple deleted [ :a, rdf:type, :D ]    { deleted }\n"
            "  0:        Tuple added [ :b, rdf:type, :D ]    { added }\n"
        );
    // Check the result.
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":a rdf:type :A . "
        ":a :R :a1 . "
        ":a1 rdf:type :B . "
        ":a :S :a2 . "
        ":a2 rdf:type :C . "

        ":b rdf:type :A . "
        ":b rdf:type :D . "
        ":b1 rdf:type :B . "
        ":b2 rdf:type :C . "

        ":c rdf:type :A . "
        ":c :R :c1 . "
        ":c1 rdf:type :B . "
        ":c :S :c2 . "
        ":c2 rdf:type :C . "
        ":c :R :c3 . "
        ":c3 rdf:type :B . "
        ":c :S :c4 . "
        ":c4 rdf:type :C . "

        ":d rdf:type :A . "
        ":d1 rdf:type :B . "
        ":d :R :d2 . "
        ":d2 rdf:type :B . "
        ":d3 rdf:type :C . "
        ":d :S :d4 . "
        ":d4 rdf:type :C . "
    );
}

TEST(testPivotlessNegation) {
    addRules(
        "[:a,rdf:type,:C] :- NOT([:a,rdf:type,:A], [:a,rdf:type,:B]) . "
    );
    addTriples(
        ":a rdf:type :A . "
        ":a rdf:type :B . "
    );
    applyRules();
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":a rdf:type :A . "
        ":a rdf:type :B . "
    );
    // The following deletion should make the rule fire.
    forDeletion(
        ":a rdf:type :A . "
    );
    applyRulesIncrementally();
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":a rdf:type :B . "
        ":a rdf:type :C . "
    );
    // The rule should still fire after the following change.
    forDeletion(
        ":a rdf:type :B . "
    );
    forAddition(
        ":a rdf:type :A . "
    );
    applyRulesIncrementally();
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":a rdf:type :A . "
        ":a rdf:type :C . "
    );
    // The rule should still fire after the following change.
    forDeletion(
        ":a rdf:type :A . "
    );
    applyRulesIncrementally();
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":a rdf:type :C . "
    );
    // The rule should not fire after the following change.
    forAddition(
        ":a rdf:type :A . "
        ":a rdf:type :B . "
    );
    applyRulesIncrementally();
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":a rdf:type :A . "
        ":a rdf:type :B . "
    );
}


TEST(testAddRuleIncreasesLevels) {
    addRules(
        "[?X,rdf:type,:C] :- [?X,rdf:type,:A], NOT [?X,rdf:type,:B] . "
    );
    addTriples(
        ":a rdf:type :A . "

        ":b rdf:type :A . "
        ":b rdf:type :B . "
    );
    applyRules();
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":a rdf:type :A . "
        ":a rdf:type :C . "

        ":b rdf:type :A . "
        ":b rdf:type :B . "
    );
    // Now add a rule that increases the number of levels.
    addRules(
        "[?X,rdf:type,:D] :- [?X,rdf:type,:A], NOT [?X,rdf:type,:C] . "
    );
    applyRulesIncrementally();
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z }",
        ":a rdf:type :A . "
        ":a rdf:type :C . "

        ":b rdf:type :A . "
        ":b rdf:type :B . "
        ":b rdf:type :D . "
    );
}

#endif
