// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifdef WITH_TEST

#define AUTO_TEST    QueryTestWithPrimalGraph

#include <CppTest/AutoTest.h>

#include "DataStoreTest.h"
#include "../../src/querying/QueryDecomposition.h"

class QueryTestWithPrimalGraph : public DataStoreTest {

protected:

    virtual void initializeQueryParameters() {
        m_queryParameters.setBoolean("bushy", true);
        m_queryParameters.setBoolean("distinct", true);
        m_queryParameters.setBoolean("root-has-answers", true);
    }

public:

    QueryTestWithPrimalGraph() : DataStoreTest() {
    }

};

TEST(testLinearQuery) {
    addTriples(
        ":i1 :p :i2 ."
        ":i1 :p :i3 ."
        ":i2 :r :i4 ."
        ":i3 :r :i4 ."
        ":i4 :s :i5 ."
        ":i4 :s :i6 ."
        ":i4 :s :i7 ."
        ":i4 :s :i8 ."
    );

    ASSERT_QUERY_AND_PLAN("SELECT ?X WHERE { ?X :p ?Y . ?Y :r ?Z . ?Z :s ?W }",
        ":i1 * 8 .",

        "QueryIterator(?X*)\n"
        "    DistinctIterator(?X*)\n"
        "        NestedIndexLoopJoinIterator(?X*)\n"
        "            TripleTableIterator(?X*, :p, ?Y*)\n"
        "            DistinctIterator(?Y)\n"
        "                NestedIndexLoopJoinIterator(?Y)\n"
        "                    TripleTableIterator(?Y, :r, ?Z*)\n"
        "                    DistinctIterator(?Z)\n"
        "                        NestedIndexLoopJoinIterator(?Z)\n"
        "                            TripleTableIterator(?Z, :s, ?W*)\n"
    );

}

TEST(testLinearQueryWithOutput) {
    addTriples(
        ":i1 :p :i2 ."
        ":i1 :p :i3 ."
        ":i2 :r :i4 ."
        ":i3 :r :i4 ."
        ":i4 :s :i5 ."
        ":i4 :s :i6 ."
        ":i4 :s :i7 ."
        ":i4 :s :i8 ."
    );

    ASSERT_QUERY_AND_PLAN("SELECT ?X ?W WHERE { ?X :p ?Y . ?Y :r ?Z . ?Z :s ?W }",
        ":i1 :i5 * 2 ."
        ":i1 :i6 * 2 ."
        ":i1 :i7 * 2 ."
        ":i1 :i8 * 2 .",

        "QueryIterator(?X*, ?W*)\n"
        "    DistinctIterator(?X*, ?W*)\n"
        "        NestedIndexLoopJoinIterator(?X*, ?W*)\n"
        "            DistinctIterator(?X*, ?Z*)\n"
        "                NestedIndexLoopJoinIterator(?X*, ?Z*)\n"
        "                    TripleTableIterator(?X*, :p, ?Y*)\n"
        "                    TripleTableIterator(?Y, :r, ?Z*)\n"
        "            TripleTableIterator(?Z, :s, ?W*)\n"
    );

}

TEST(testTreeQueryWithParallelPaths) {
    addTriples(
        ":i1 :p :i4 ."
        ":i2 :p :i4 ."
        ":i3 :p :i4 ."
        ":i4 :r :i5 ."
        ":i4 :r :i6 ."
        ":i4 :r :i7 ."
        ":i4 :r :i8 ."
        ":i4 :s :i9 ."
        ":i4 :s :i10 ."
    );

    ASSERT_QUERY_AND_PLAN("SELECT ?X WHERE { ?X :p ?Y . ?Y :r ?Z . ?Y :s ?W }",
        ":i1 * 8 ."
        ":i2 * 8 ."
        ":i3 * 8 .",

        "QueryIterator(?X*)\n"
        "    DistinctIterator(?X*)\n"
        "        NestedIndexLoopJoinIterator(?X*)\n"
        "            DistinctIterator(?Y*)\n"
        "                NestedIndexLoopJoinIterator(?Y*)\n"
        "                    TripleTableIterator(?Y*, :s, ?W*)\n"
        "            TripleTableIterator(?X*, :p, ?Y)\n"
        "            DistinctIterator(?Y)\n"
        "                NestedIndexLoopJoinIterator(?Y)\n"
        "                    TripleTableIterator(?Y, :r, ?Z*)\n"
    );

}

TEST(testLinearQueryWithEndAnswers) {
    addTriples(
        ":l1 :p :a  ."
        ":l2 :p :a  ."
        ":a  :r :m1 ."
        ":a  :r :m2 ."
        ":a  :r :m3 ."
        ":a  :r :m4 ."
        ":a  :r :m5 ."
        ":a  :r :m6 ."
        ":m1 :s :b  ."
        ":m2 :s :b  ."
        ":m3 :s :b  ."
        ":m4 :s :b  ."
        ":b  :t :r1 ."
        ":b  :t :r2 ."
        ":b  :t :r3 ."
    );

    ASSERT_QUERY_AND_PLAN("SELECT ?X ?Y ?V ?W WHERE { ?X :p ?Y . ?Y :r ?Z . ?Z :s ?V . ?V :t ?W }",
        ":l1 :a :b :r1 * 4 ."
        ":l1 :a :b :r2 * 4 ."
        ":l1 :a :b :r3 * 4 ."
        ":l2 :a :b :r1 * 4 ."
        ":l2 :a :b :r2 * 4 ."
        ":l2 :a :b :r3 * 4 .",

        "QueryIterator(?X*, ?Y*, ?V*, ?W*)\n"
        "    DistinctIterator(?X*, ?Y*, ?V*, ?W*)\n"
        "        NestedIndexLoopJoinIterator(?X*, ?Y*, ?V*, ?W*)\n"
        "            TripleTableIterator(?X*, :p, ?Y*)\n"
        "            DistinctIterator(?Y, ?V*)\n"
        "                NestedIndexLoopJoinIterator(?Y, ?V*)\n"
        "                    TripleTableIterator(?Y, :r, ?Z*)\n"
        "                    TripleTableIterator(?Z, :s, ?V*)\n"
        "            TripleTableIterator(?V, :t, ?W*)\n"
    );

}

TEST(testLinearQueryWithMiddleAnswers) {
    addTriples(
        ":l1 :p :a  ."
        ":l2 :p :a  ."
        ":a  :r :m1 ."
        ":a  :r :m2 ."
        ":a  :r :m3 ."
        ":a  :r :m4 ."
        ":a  :r :m5 ."
        ":a  :r :m6 ."
        ":m1 :s :b  ."
        ":m2 :s :b  ."
        ":m3 :s :b  ."
        ":m4 :s :b  ."
        ":b  :t :r1 ."
        ":b  :t :r2 ."
        ":b  :t :r3 ."
    );

    ASSERT_QUERY_AND_PLAN("SELECT ?Y ?V WHERE { ?X :p ?Y . ?Y :r ?Z . ?Z :s ?V . ?V :t ?W }",
        ":a :b * 24 .",

        "QueryIterator(?Y*, ?V*)\n"
        "    DistinctIterator(?Y*, ?V*)\n"
        "        NestedIndexLoopJoinIterator(?Y*, ?V*)\n"
        "            DistinctIterator(?Y*)\n"
        "                NestedIndexLoopJoinIterator(?Y*)\n"
        "                    TripleTableIterator(?X*, :p, ?Y*)\n"
        "            TripleTableIterator(?Y, :r, ?Z*)\n"
        "            TripleTableIterator(?Z, :s, ?V*)\n"
        "            DistinctIterator(?V)\n"
        "                NestedIndexLoopJoinIterator(?V)\n"
        "                    TripleTableIterator(?V, :t, ?W*)\n"
    );

}

#endif
