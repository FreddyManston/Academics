// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifdef WITH_TEST

#define AUTO_TEST    QueryTestNoDecomposition

#include <CppTest/AutoTest.h>

#include "DataStoreTest.h"
#include "../../src/querying/QueryDecomposition.h"
#include "../../src/dictionary/Dictionary.h"

class QueryTestNoDecomposition : public DataStoreTest {

protected:

    virtual void initializeQueryParameters() {
        m_queryParameters.setBoolean("bushy", false);
        m_queryParameters.setBoolean("distinct", true);
        m_queryParameters.setBoolean("root-has-answers", true);
    }

public:

    QueryTestNoDecomposition() : DataStoreTest() {
    }

};

TEST(testBind) {
    addTriples(
        ":i1 :R \"a\" ."
        ":i2 :R \"bc\" ."
        ":i3 :R \"def\" ."

        ":i1 :S 1 ."
        ":i2 :S 2 ."
        ":i3 :S 4 ."
    );

    ASSERT_QUERY("SELECT ?X ?Z WHERE { ?X :R ?Y . BIND(STRLEN(?Y) AS ?Z) }",
        ":i1 1 ."
        ":i2 2 ."
        ":i3 3 ."
    );

    ASSERT_QUERY("SELECT ?X ?Z WHERE { ?X :R ?Y . BIND(STRLEN(?Y) AS ?Z) . ?X :S ?Z }",
        ":i1 1 ."
        ":i2 2 ."
    );
}

TEST(testBindWithOptional) {
    addTriples(
        ":i1 :R 1 ."
        ":i2 :R 2 ."
        ":i3 :R 3 ."

        ":i1 :S 1 ."
        ":i3 :S 4 ."
    );

    ASSERT_QUERY_AND_PLAN("SELECT ?X ?Y ?Z WHERE { { ?X :R ?Y . OPTIONAL { ?X :S ?Z } } . BIND(?Y AS ?Z) }",
        ":i1 1 1 ."
        ":i2 2 2 .",

        "QueryIterator(?X*, ?Y*, ?Z*)\n"
        "    DistinctIterator(?X*, ?Y*, ?Z*)\n"
        "        NestedIndexLoopJoinIterator(?X*, ?Y*, ?Z*)\n"
        "            LeftJoinIterator(?X*, ?Y*, ?Z#)\n"
        "                DistinctIterator(?X*, ?Y*)\n"
        "                    NestedIndexLoopJoinIterator(?X*, ?Y*)\n"
        "                        TripleTableIterator(?X*, :R, ?Y*)\n"
        "                DistinctIterator(?X, ?Z*)\n"
        "                    NestedIndexLoopJoinIterator(?X, ?Z*)\n"
        "                        TripleTableIterator(?X, :S, ?Z*)\n"
        "            BindTupleIterator(?Z+, ?Y)\n"
    );
}

TEST(testFilter) {
    addTriples(
        ":i1 :R :u1 ."
        ":i1 :R :u2 ."
        ":i1 :R :u3 ."

        ":i2 :R :u2 ."
        ":i2 :R :u3 ."
    );

    ASSERT_QUERY("SELECT ?X ?Y1 ?Y2 WHERE { ?X :R ?Y1 . FILTER (?Y1 != ?Y2) . ?X :R ?Y2 }",
        ":i1 :u1 :u2 ."
        ":i1 :u1 :u3 ."
        ":i1 :u2 :u1 ."
        ":i1 :u2 :u3 ."
        ":i1 :u3 :u2 ."
        ":i1 :u3 :u1 ."

        ":i2 :u2 :u3 ."
        ":i2 :u3 :u2 ."
    );
}

TEST(testSimpleRetrieval) {
    addTriples(
        ":i1 :i2 :i3 ."
        ":i1 :i4 :i3 ."
        ":i1 :i4 :i5 ."
        ":i1 :i6 :i7 ."
        ":i2 :i4 :i5 ."
        ":i3 :i6 :i7 ."
        ":i3 :i7 :i7 ."
        ":i4 :i8 :i8 ."
    );

    ASSERT_QUERY_AND_PLAN("SELECT ?S ?P ?O WHERE { ?S ?P ?O }",
        ":i1 :i2 :i3 ."
        ":i1 :i4 :i3 ."
        ":i1 :i4 :i5 ."
        ":i1 :i6 :i7 ."
        ":i2 :i4 :i5 ."
        ":i3 :i6 :i7 ."
        ":i3 :i7 :i7 ."
        ":i4 :i8 :i8 .",

        "QueryIterator(?S*, ?P*, ?O*)\n"
        "    DistinctIterator(?S*, ?P*, ?O*)\n"
        "        TripleTableIterator(?S*, ?P*, ?O*)\n"
    );

    ASSERT_QUERY_AND_PLAN("SELECT ?P ?O WHERE { :i1 ?P ?O }",
        ":i2 :i3 ."
        ":i4 :i3 ."
        ":i4 :i5 ."
        ":i6 :i7 .",

        "QueryIterator(?P*, ?O*)\n"
        "    DistinctIterator(?P*, ?O*)\n"
        "        NestedIndexLoopJoinIterator(?P*, ?O*)\n"
        "            TripleTableIterator(:i1, ?P*, ?O*)\n"
    );

    ASSERT_QUERY_AND_PLAN("SELECT ?S ?O WHERE { ?S :i6 ?O }",
        ":i1 :i7 ."
        ":i3 :i7 .",

        "QueryIterator(?S*, ?O*)\n"
        "    DistinctIterator(?S*, ?O*)\n"
        "        NestedIndexLoopJoinIterator(?S*, ?O*)\n"
        "            TripleTableIterator(?S*, :i6, ?O*)\n"
    );

    ASSERT_QUERY_AND_PLAN("SELECT ?S ?O WHERE { ?S ?P ?O }",
        ":i1 :i3 * 2 ."
        ":i1 :i5 ."
        ":i1 :i7 ."
        ":i2 :i5 ."
        ":i3 :i7 * 2 ."
        ":i4 :i8 .",

        "QueryIterator(?S*, ?O*)\n"
        "    DistinctIterator(?S*, ?O*)\n"
        "        NestedIndexLoopJoinIterator(?S*, ?O*)\n"
        "            TripleTableIterator(?S*, ?P*, ?O*)\n"
    );

    ASSERT_QUERY_AND_PLAN("SELECT ?S ?OP WHERE { ?S ?OP ?OP }",
        ":i3 :i7 ."
        ":i4 :i8 .",

        "QueryIterator(?S*, ?OP*)\n"
        "    DistinctIterator(?S*, ?OP*)\n"
        "        TripleTableIterator(?S*, ?OP*, ?OP*)\n"
    );

    ASSERT_QUERY_AND_PLAN("SELECT ?OP WHERE { :i3 ?OP ?OP }",
        ":i7 .",

        "QueryIterator(?OP*)\n"
        "    DistinctIterator(?OP*)\n"
        "        NestedIndexLoopJoinIterator(?OP*)\n"
        "            TripleTableIterator(:i3, ?OP*, ?OP*)\n"
    );
}

TEST(testSimpleJoins) {
    addTriples(
        ":i1 :p :i2 ."
        ":i2 :p :i3 ."
        ":i3 :p :i4 ."
        ":i4 :p :i5 ."
        ":i5 :p :i6 ."
        ":i1 a :A ."
        ":i2 a :B ."
        ":i3 a :C ."
        ":i4 a :A ."
        ":i5 a :B ."
        ":i6 a :C ."
    );

    ASSERT_QUERY_AND_PLAN("SELECT ?X ?Z WHERE { ?X :p ?Y . ?Y :p ?Z }",
        ":i1 :i3 ."
        ":i2 :i4 ."
        ":i3 :i5 ."
        ":i4 :i6 .",

        "QueryIterator(?X*, ?Z*)\n"
        "    DistinctIterator(?X*, ?Z*)\n"
        "        NestedIndexLoopJoinIterator(?X*, ?Z*)\n"
        "            TripleTableIterator(?X*, :p, ?Y*)\n"
        "            TripleTableIterator(?Y, :p, ?Z*)\n"
    );

    ASSERT_QUERY_AND_PLAN("SELECT ?X ?Z WHERE { ?X :p ?Y . ?Y :p ?Z . ?Y a :B }",
        ":i1 :i3 ."
        ":i4 :i6 .",

        "QueryIterator(?X*, ?Z*)\n"
        "    DistinctIterator(?X*, ?Z*)\n"
        "        NestedIndexLoopJoinIterator(?X*, ?Z*)\n"
        "            TripleTableIterator(?Y*, rdf:type, :B)\n"
        "            TripleTableIterator(?X*, :p, ?Y)\n"
        "            TripleTableIterator(?Y, :p, ?Z*)\n"
    );
}

TEST(testNestedJoin) {
    addTriples(
        ":a1 :r :b1 ."
        ":a2 :r :b2 ."
        ":a3 :r :b3 ."

        ":b1 :s :c1 ."
        ":b2 :s :c2 ."
        ":b3 :s :c3 ."

        ":c1 :t :d1 ."
        ":c2 :t :d2 ."
        ":c3 :t :d3 ."
    );

    ASSERT_QUERY_AND_PLAN("SELECT ?X ?W WHERE { ?X :r ?Y { ?Y :s ?Z . ?Z :t ?W } }",
        ":a1 :d1 ."
        ":a2 :d2 ."
        ":a3 :d3 .",

        "QueryIterator(?X*, ?W*)\n"
        "    DistinctIterator(?X*, ?W*)\n"
        "        NestedIndexLoopJoinIterator(?X*, ?W*)\n"
        "            TripleTableIterator(?X*, :r, ?Y*)\n"
        "            TripleTableIterator(?Y, :s, ?Z*)\n"
        "            TripleTableIterator(?Z, :t, ?W*)\n"
    );
}

TEST(testVaryingCardinality) {
    addTriples(
        ":i :p :i1 ."
            ":i1 a :A ."
            ":i1 a :B ."
            ":i1 a :C ."
            ":i1 a :D ."
            ":i1 :r :i11 ."
                ":i11 a :A ."
            ":i1 :r :i12 ."
                ":i12 a :A ."
        ":i :p :i2 ."
            ":i2 a :A ."
            ":i2 a :B ."
            ":i2 :r :i21 ."
                ":i21 a :A ."
            ":i2 :r :i22 ."
                ":i22 a :A ."
            ":i2 :r :i23 ."
            ":i2 :r :i24 ."
    );

    ASSERT_QUERY_AND_PLAN("SELECT ?X ?Z WHERE { ?X :p ?Y . ?Y a :A . ?Y :r ?Z }",
        ":i :i11 ."
        ":i :i12 ."
        ":i :i21 ."
        ":i :i22 ."
        ":i :i23 ."
        ":i :i24 .",

        "QueryIterator(?X*, ?Z*)\n"
        "    DistinctIterator(?X*, ?Z*)\n"
        "        NestedIndexLoopJoinIterator(?X*, ?Z*)\n"
        "            TripleTableIterator(?X*, :p, ?Y*)\n"
        "            TripleTableIterator(?Y, rdf:type, :A)\n"
        "            TripleTableIterator(?Y, :r, ?Z*)\n"
    );
}

TEST(testMiniClique) {
    addTriples(
        ":i1 :p :i2 ."
        ":i1 :p :i3 ."
        ":i1 :p :i4 ."
        ":i2 :p :i3 ."
        ":i2 :p :i4 ."
        ":i3 :p :i4 ."
    );

    ASSERT_QUERY_AND_PLAN("SELECT ?X ?Y ?Z WHERE { ?X :p ?Y . ?Y :p ?Z . ?X :p ?Z }",
        ":i1 :i2 :i3 ."
        ":i1 :i2 :i4 ."
        ":i1 :i3 :i4 ."
        ":i2 :i3 :i4 .",

        "QueryIterator(?X*, ?Y*, ?Z*)\n"
        "    DistinctIterator(?X*, ?Y*, ?Z*)\n"
        "        NestedIndexLoopJoinIterator(?X*, ?Y*, ?Z*)\n"
        "            TripleTableIterator(?X*, :p, ?Y*)\n"
        "            TripleTableIterator(?Y, :p, ?Z*)\n"
        "            TripleTableIterator(?X, :p, ?Z)\n"
    );
}

TEST(testClique) {
    ResourceID p = resolve(":p");
    const size_t cliqueSize = 30;
    ResourceID i[cliqueSize];
    for (size_t index = 0; index < cliqueSize; ++index) {
        std::ostringstream stream;
        stream << ":i" << index;
        i[index] = resolve(stream.str());
    }

    // Generate the clique
    for (size_t index1 = 0; index1 < cliqueSize; ++index1)
        for (size_t index2 = index1 + 1; index2 < cliqueSize; ++index2)
            addTriple(i[index1], p, i[index2]);

    // Query all triangles
    ResourceIDTuple resourceIDTuple;
    ResourceIDTupleMultiset expected;
    for (size_t index1 = 0; index1 < cliqueSize; ++index1) {
        ResourceID a = i[index1];
        for (size_t index2 = index1 + 1; index2 < cliqueSize; ++index2) {
            ResourceID b = i[index2];
            for (size_t index3 = index2 + 1; index3 < cliqueSize; ++index3) {
                ResourceID c = i[index3];
                resourceIDTuple.clear();
                resourceIDTuple.push_back(a);
                resourceIDTuple.push_back(b);
                resourceIDTuple.push_back(c);
                expected.add(resourceIDTuple);
            }
        }
    }
    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X :p ?Y . ?Y :p ?Z . ?X :p ?Z }", expected);
}

TEST(testNonVariableQueryTerms) {
    addTriples(
        ":i1 :i2 :i3 ."
        ":i1 :i4 :i3 ."
        ":i2 :i4 :i5 ."
        ":i3 :i6 :i7 ."
        ":i3 :i7 :i7 ."
        ":i4 :i8 :i8 ."
    );

    ASSERT_QUERY_AND_PLAN("SELECT :a1 ?S ?P ?P ?O WHERE { ?S ?P ?O }",
        ":a1 :i1 :i2 :i2 :i3 ."
        ":a1 :i1 :i4 :i4 :i3 ."
        ":a1 :i2 :i4 :i4 :i5 ."
        ":a1 :i3 :i6 :i6 :i7 ."
        ":a1 :i3 :i7 :i7 :i7 ."
        ":a1 :i4 :i8 :i8 :i8 .",

        "QueryIterator(:a1, ?S*, ?P*, ?P*, ?O*)\n"
        "    DistinctIterator(?S*, ?P*, ?O*)\n"
        "        TripleTableIterator(?S*, ?P*, ?O*)\n"
    );
}

TEST(testOptional1) {
    addTriples(
        ":a1 rdf:type :A ."
        ":a2 rdf:type :A ."
        ":a3 rdf:type :A ."
        ":a4 rdf:type :A ."

        ":a1 :R :b1a ."
        ":a1 :R :b1b ."
        ":a3 :R :b3a ."
        ":a5 :R :b5a ."
        ":a5 :R :b5b ."

        ":a1 :S :c1a ."
        ":a1 :S :c1b ."
        ":a2 :S :c2a ."
        ":a2 :S :c2b ."
        ":a5 :S :c5a ."
        ":a5 :S :c5b ."
    );

    ASSERT_QUERY_AND_PLAN("SELECT ?X ?Y WHERE { ?X rdf:type :A . OPTIONAL { ?X :R ?Y } }",
        ":a1 :b1a  ."
        ":a1 :b1b  ."
        ":a2 UNDEF ."
        ":a3 :b3a  ."
        ":a4 UNDEF .",

        "QueryIterator(?X*, ?Y#)\n"
        "    LeftJoinIterator(?X*, ?Y#)\n"
        "        DistinctIterator(?X*)\n"
        "            NestedIndexLoopJoinIterator(?X*)\n"
        "                TripleTableIterator(?X*, rdf:type, :A)\n"
        "        DistinctIterator(?X, ?Y*)\n"
        "            NestedIndexLoopJoinIterator(?X, ?Y*)\n"
        "                TripleTableIterator(?X, :R, ?Y*)\n"
    );

    ASSERT_QUERY_AND_PLAN("SELECT ?X ?Y WHERE { ?X rdf:type :A . OPTIONAL { ?X :R ?Y } . OPTIONAL { ?X :R ?Y } }",
        ":a1 :b1a  ."
        ":a1 :b1b  ."
        ":a2 UNDEF ."
        ":a3 :b3a  ."
        ":a4 UNDEF .",

        "QueryIterator(?X*, ?Y#)\n"
        "    LeftJoinIterator(?X*, ?Y#)\n"
        "        DistinctIterator(?X*)\n"
        "            NestedIndexLoopJoinIterator(?X*)\n"
        "                TripleTableIterator(?X*, rdf:type, :A)\n"
        "        DistinctIterator(?X, ?Y*)\n"
        "            NestedIndexLoopJoinIterator(?X, ?Y*)\n"
        "                TripleTableIterator(?X, :R, ?Y*)\n"
        "        DistinctIterator(?X, ?Y+)\n"
        "            NestedIndexLoopJoinIterator(?X, ?Y+)\n"
        "                TripleTableIterator(?X, :R, ?Y+)\n"
    );

    ASSERT_QUERY_AND_PLAN("SELECT ?X ?Y ?Z WHERE { ?X rdf:type :A . OPTIONAL { ?X :R ?Y } . OPTIONAL { ?X :S ?Z } }",
        ":a1 :b1a  :c1a ."
        ":a1 :b1a  :c1b ."
        ":a1 :b1b  :c1a ."
        ":a1 :b1b  :c1b ."
        ":a2 UNDEF :c2a ."
        ":a2 UNDEF :c2b ."
        ":a3 :b3a  UNDEF ."
        ":a4 UNDEF UNDEF .",

        "QueryIterator(?X*, ?Y#, ?Z#)\n"
        "    LeftJoinIterator(?X*, ?Y#, ?Z#)\n"
        "        DistinctIterator(?X*)\n"
        "            NestedIndexLoopJoinIterator(?X*)\n"
        "                TripleTableIterator(?X*, rdf:type, :A)\n"
        "        DistinctIterator(?X, ?Y*)\n"
        "            NestedIndexLoopJoinIterator(?X, ?Y*)\n"
        "                TripleTableIterator(?X, :R, ?Y*)\n"
        "        DistinctIterator(?X, ?Z*)\n"
        "            NestedIndexLoopJoinIterator(?X, ?Z*)\n"
        "                TripleTableIterator(?X, :S, ?Z*)\n"
    );
}

TEST(testOptional2) {
    addTriples(
        ":a1 :R :b1 ."
        ":a2 :R :b2 ."

        ":a1 :S :c1 ."
    );

    ASSERT_QUERY_AND_PLAN("SELECT ?Y ?Z WHERE { ?X :R ?Y . OPTIONAL { ?X :S ?Z } }",
        ":b1 :c1   ."
        ":b2 UNDEF .",

        "QueryIterator(?Y*, ?Z#)\n"
        "    LeftJoinIterator(?Y*, ?Z#)\n"
        "        DistinctIterator(?X*, ?Y*)\n"
        "            NestedIndexLoopJoinIterator(?X*, ?Y*)\n"
        "                TripleTableIterator(?X*, :R, ?Y*)\n"
        "        DistinctIterator(?X, ?Z*)\n"
        "            NestedIndexLoopJoinIterator(?X, ?Z*)\n"
        "                TripleTableIterator(?X, :S, ?Z*)\n"
    );
}

TEST(testOptional3) {
    addTriples(
        ":a1 rdf:type :A ."
        ":a2 rdf:type :A ."
        ":a3 rdf:type :A ."
        ":a4 rdf:type :A ."
        ":a5 rdf:type :A ."
        ":a6 rdf:type :A ."
        ":a7 rdf:type :A ."

        ":a1 :R :b1a ."
        ":a1 :R :b1b ."
        ":a2 :R :b2a ."
        ":a2 :R :b2b ."
        ":a3 :R :b3a ."
        ":a4 :R :b4a ."
        ":a4 :R :b4b ."
        ":a5 :R :b5a ."
        ":a5 :R :b5b ."

        ":a1 :S :b1a ."
        ":a1 :S :b1b ."
        ":a2 :S :b2a ."
        ":a3 :S :b3a ."
        ":a3 :S :b3b ."
        ":a6 :S :b6a ."
        ":a6 :S :b6b ."
    );

    // This is not a well-designed pattern, so the output depends on the order of OPTIONALs!

    ASSERT_QUERY_AND_PLAN("SELECT ?X ?Y WHERE { ?X rdf:type :A . OPTIONAL { ?X :R ?Y } . OPTIONAL { ?X :S ?Y } }",
        ":a1 :b1a  ."
        ":a1 :b1b  ."
        ":a2 :b2a  ."
        ":a2 :b2b  ."
        ":a3 :b3a  ."
        ":a4 :b4a  ."
        ":a4 :b4b  ."
        ":a5 :b5a  ."
        ":a5 :b5b  ."
        ":a6 :b6a  ."
        ":a6 :b6b  ."
        ":a7 UNDEF .",

        "QueryIterator(?X*, ?Y#)\n"
        "    LeftJoinIterator(?X*, ?Y#)\n"
        "        DistinctIterator(?X*)\n"
        "            NestedIndexLoopJoinIterator(?X*)\n"
        "                TripleTableIterator(?X*, rdf:type, :A)\n"
        "        DistinctIterator(?X, ?Y*)\n"
        "            NestedIndexLoopJoinIterator(?X, ?Y*)\n"
        "                TripleTableIterator(?X, :R, ?Y*)\n"
        "        DistinctIterator(?X, ?Y+)\n"
        "            NestedIndexLoopJoinIterator(?X, ?Y+)\n"
        "                TripleTableIterator(?X, :S, ?Y+)\n"
    );

    ASSERT_QUERY_AND_PLAN("SELECT ?X ?Y WHERE { ?X rdf:type :A . OPTIONAL { ?X :S ?Y } . OPTIONAL { ?X :R ?Y } }",
        ":a1 :b1a  ."
        ":a1 :b1b  ."
        ":a2 :b2a  ."
        ":a3 :b3a  ."
        ":a3 :b3b  ."
        ":a4 :b4a  ."
        ":a4 :b4b  ."
        ":a5 :b5a  ."
        ":a5 :b5b  ."
        ":a6 :b6a  ."
        ":a6 :b6b  ."
        ":a7 UNDEF .",

        "QueryIterator(?X*, ?Y#)\n"
        "    LeftJoinIterator(?X*, ?Y#)\n"
        "        DistinctIterator(?X*)\n"
        "            NestedIndexLoopJoinIterator(?X*)\n"
        "                TripleTableIterator(?X*, rdf:type, :A)\n"
        "        DistinctIterator(?X, ?Y*)\n"
        "            NestedIndexLoopJoinIterator(?X, ?Y*)\n"
        "                TripleTableIterator(?X, :S, ?Y*)\n"
        "        DistinctIterator(?X, ?Y+)\n"
        "            NestedIndexLoopJoinIterator(?X, ?Y+)\n"
        "                TripleTableIterator(?X, :R, ?Y+)\n"
    );
}

TEST(testOptional4) {
    addTriples(
        ":i1 rdf:type :A ."
        ":i1 :R :b1 ."
        ":i1 :S :b1 ."

        ":i2 rdf:type :A ."
        ":i2 :R :b2 ."
        ":i2 :S :c2 ."

        ":i3 rdf:type :A ."
        ":i3 :R :b3 ."
        ":i3 :R :c3 ."
        ":i3 :S :c3 ."

        ":i4 rdf:type :A ."
        ":i4 :R :b4 ."
        ":i4 :S :b4 ."
        ":i4 :S :c4 ."
    );

    // This is also not a well-designed pattern.

    ASSERT_QUERY_AND_PLAN("SELECT ?X ?Y { { ?X rdf:type :A . OPTIONAL { ?X :R ?Y } } OPTIONAL { ?X rdf:type :A . OPTIONAL { ?X :S ?Y } } }",
        ":i1 :b1 ."
        ":i2 :b2 ."
        ":i3 :b3 ."
        ":i3 :c3 ."
        ":i4 :b4 .",

        "QueryIterator(?X*, ?Y#)\n"
        "    LeftJoinIterator(?X*, ?Y#)\n"
        "        LeftJoinIterator(?X*, ?Y#)\n"
        "            DistinctIterator(?X*)\n"
        "                NestedIndexLoopJoinIterator(?X*)\n"
        "                    TripleTableIterator(?X*, rdf:type, :A)\n"
        "            DistinctIterator(?X, ?Y*)\n"
        "                NestedIndexLoopJoinIterator(?X, ?Y*)\n"
        "                    TripleTableIterator(?X, :R, ?Y*)\n"
        "        LeftJoinIterator(?X, ?Y+)\n"
        "            DistinctIterator(?X)\n"
        "                NestedIndexLoopJoinIterator(?X)\n"
        "                    TripleTableIterator(?X, rdf:type, :A)\n"
        "            DistinctIterator(?X, ?Y+)\n"
        "                NestedIndexLoopJoinIterator(?X, ?Y+)\n"
        "                    TripleTableIterator(?X, :S, ?Y+)\n"
    );
}

TEST(testOptional5) {
    addTriples(
        ":i1 rdf:type :A ."
        ":i1 :R :b1 ."
        ":i1 :R :b2 ."
    );

    // This is also not a well-designed pattern.

    ASSERT_QUERY_AND_PLAN("SELECT ?X ?Y ?Z { ?X rdf:type :A . OPTIONAL { { ?Y rdf:type :B } OPTIONAL { ?X :R ?Z } } }",
        ":i1 UNDEF UNDEF .",

        "QueryIterator(?X*, ?Y#, ?Z#)\n"
        "    LeftJoinIterator(?X*, ?Y#, ?Z#)\n"
        "        DistinctIterator(?X*)\n"
        "            NestedIndexLoopJoinIterator(?X*)\n"
        "                TripleTableIterator(?X*, rdf:type, :A)\n"
        "        LeftJoinIterator(?Y*, ?X, ?Z#)\n"
        "            DistinctIterator(?Y*)\n"
        "                NestedIndexLoopJoinIterator(?Y*)\n"
        "                    TripleTableIterator(?Y*, rdf:type, :B)\n"
        "            DistinctIterator(?X, ?Z*)\n"
        "                NestedIndexLoopJoinIterator(?X, ?Z*)\n"
        "                    TripleTableIterator(?X, :R, ?Z*)\n"
    );

    ASSERT_QUERY_AND_PLAN("SELECT ?X ?Y ?Z { ?X rdf:type :A . OPTIONAL { { ?X :R ?Z } OPTIONAL { ?Y rdf:type :B } } }",
        ":i1 UNDEF :b1 ."
        ":i1 UNDEF :b2 .",

        "QueryIterator(?X*, ?Y#, ?Z#)\n"
        "    LeftJoinIterator(?X*, ?Z#, ?Y#)\n"
        "        DistinctIterator(?X*)\n"
        "            NestedIndexLoopJoinIterator(?X*)\n"
        "                TripleTableIterator(?X*, rdf:type, :A)\n"
        "        LeftJoinIterator(?X, ?Z*, ?Y#)\n"
        "            DistinctIterator(?X, ?Z*)\n"
        "                NestedIndexLoopJoinIterator(?X, ?Z*)\n"
        "                    TripleTableIterator(?X, :R, ?Z*)\n"
        "            DistinctIterator(?Y*)\n"
        "                NestedIndexLoopJoinIterator(?Y*)\n"
        "                    TripleTableIterator(?Y*, rdf:type, :B)\n"
    );

    // We extend the data set and we get some answers.

    addTriples(
        ":i2 rdf:type :B ."
    );

    ASSERT_QUERY("SELECT ?X ?Y ?Z { ?X rdf:type :A . OPTIONAL { { ?Y rdf:type :B } OPTIONAL { ?X :R ?Z } } }",
        ":i1 :i2 :b1 ."
        ":i1 :i2 :b2 ."
    );
}

TEST(testOptional6) {
    addTriples(
        ":a  rdf:type :A ."
        ":a  :R :a1 ."
        ":a1 :S :a11 ."
        ":a1 :S :a12 ."
        ":a  :R :a2 ."
        ":a  :T :a3 ."

        ":b  rdf:type :A ."
        ":b  :T :b3 ."
        ":b  :T :b4 ."

        ":c  rdf:type :A ."
        ":c  :R :c1 ."
    );

    ASSERT_QUERY_AND_PLAN("SELECT ?X ?Y ?Z ?W { ?X rdf:type :A . OPTIONAL { ?X :R ?Y . OPTIONAL { ?Y :S ?Z } } . OPTIONAL { ?X :T ?W } }",
        ":a :a1   :a11  :a3   ."
        ":a :a1   :a12  :a3   ."
        ":a :a2   UNDEF :a3   ."
        ":b UNDEF UNDEF :b3   ."
        ":b UNDEF UNDEF :b4   ."
        ":c :c1   UNDEF UNDEF .",

        "QueryIterator(?X*, ?Y#, ?Z#, ?W#)\n"
        "    LeftJoinIterator(?X*, ?Y#, ?Z#, ?W#)\n"
        "        DistinctIterator(?X*)\n"
        "            NestedIndexLoopJoinIterator(?X*)\n"
        "                TripleTableIterator(?X*, rdf:type, :A)\n"
        "        LeftJoinIterator(?X, ?Y*, ?Z#)\n"
        "            DistinctIterator(?X, ?Y*)\n"
        "                NestedIndexLoopJoinIterator(?X, ?Y*)\n"
        "                    TripleTableIterator(?X, :R, ?Y*)\n"
        "            DistinctIterator(?Y, ?Z*)\n"
        "                NestedIndexLoopJoinIterator(?Y, ?Z*)\n"
        "                    TripleTableIterator(?Y, :S, ?Z*)\n"
        "        DistinctIterator(?X, ?W*)\n"
        "            NestedIndexLoopJoinIterator(?X, ?W*)\n"
        "                TripleTableIterator(?X, :T, ?W*)\n"
    );

}

TEST(testOptional7) {
    addTriples(
        ":d1 :issued \"1972\"^^xsd:integer . "
        ":d1 :creator :a1 . "

        ":d2 :issued \"1974\"^^xsd:integer . "
        ":d2 :creator :a1 . "

        ":d3 :issued \"1986\"^^xsd:integer . "
        ":d3 :creator :a3 . "
    );

    ASSERT_QUERY_AND_PLAN(
        "SELECT ?yr ?author ?document\n"
        "WHERE {\n"
        "?document :issued ?yr .\n"
        "?document :creator ?author .\n"
        "OPTIONAL {\n"
        "    ?document2 :issued ?yr2 .\n"
        "    ?document2 :creator ?author2 .\n"
        "    FILTER(?author = ?author2 && ?yr2 < ?yr)\n"
        "}\n"
        "FILTER(!BOUND(?author2))\n"
        "}",

        "\"1972\"^^xsd:integer :a1 :d1 ."
        "\"1986\"^^xsd:integer :a3 :d3 .",

        "QueryIterator(?yr*, ?author*, ?document*)\n"
        "    DistinctIterator(?document*, ?yr*, ?author*)\n"
        "        NestedIndexLoopJoinIterator(?document*, ?yr*, ?author*)\n"
        "            LeftJoinIterator(?document*, ?yr*, ?author*, ?document2#, ?yr2#, ?author2#)\n"
        "                DistinctIterator(?document*, ?yr*, ?author*)\n"
        "                    NestedIndexLoopJoinIterator(?document*, ?yr*, ?author*)\n"
        "                        TripleTableIterator(?document*, :issued, ?yr*)\n"
        "                        TripleTableIterator(?document, :creator, ?author*)\n"
        "                DistinctIterator(?document2*, ?yr2*, ?author2*, ?author, ?yr)\n"
        "                    NestedIndexLoopJoinIterator(?document2*, ?yr2*, ?author2*, ?author, ?yr)\n"
        "                        TripleTableIterator(?document2*, :issued, ?yr2*)\n"
        "                        TripleTableIterator(?document2, :creator, ?author2*)\n"
        "                        FilterTupleIterator(?author, ?author2, ?yr2, ?yr)\n"
        "            FilterTupleIterator(?author2+)\n"
    );
}

/* This is commented out because it doesn't work and it requires consderable change in the execution.
TEST(testOptional8) {
    addTriples(
       ":a :a :a . "
       ":b :b :b . "
       ":c :c :c . "
    );

    ASSERT_QUERY_AND_PLAN(
        "SELECT ?X ?Y \n"
        "WHERE {\n"
        "?X :a :a .\n"
        "OPTIONAL {\n"
        "    ?Y :b :b .\n"
        "    OPTIONAL {\n"
        "        ?X :c :c .\n"
        "    }\n"
        "}\n"
        "}",

        ":a UNDEF .",

        "QueryIterator(?X*, ?Y#)\n"
        "    LeftJoinIterator(?X*, ?Y#)\n"
        "        DistinctIterator(?X*)\n"
        "            NestedIndexLoopJoinIterator(?X*)\n"
        "                TripleTableIterator(?X*, :a, :a)\n"
        "        LeftJoinIterator(?Y*, ?X)\n"
        "            DistinctIterator(?Y*)\n"
        "                NestedIndexLoopJoinIterator(?Y*)\n"
        "                    TripleTableIterator(?Y*, :b, :b)\n"
        "            DistinctIterator(?X)\n"
        "                NestedIndexLoopJoinIterator(?X)\n"
        "                    TripleTableIterator(?X, :c, :c)\n"
    );
}*/

TEST(testUnion1) {
    addTriples(
        ":a1 :R :b1 . "
        ":a2 :R :b2 . "

        ":c1 :S :d1 . "
        ":c2 :S :d2 . "
    );

    ASSERT_QUERY_AND_PLAN("SELECT ?X ?Y ?Z WHERE { { ?X :R ?Y }  UNION { ?X :S ?Z } }",
        ":a1 :b1   UNDEF ."
        ":a2 :b2   UNDEF ."
        ":c1 UNDEF :d1   ."
        ":c2 UNDEF :d2   .",

        "QueryIterator(?X*, ?Y#, ?Z#)\n"
        "    UnionIterator(?X*, ?Y#, ?Z#)\n"
        "        DistinctIterator(?X*, ?Y*)\n"
        "            NestedIndexLoopJoinIterator(?X*, ?Y*)\n"
        "                TripleTableIterator(?X*, :R, ?Y*)\n"
        "        DistinctIterator(?X*, ?Z*)\n"
        "            NestedIndexLoopJoinIterator(?X*, ?Z*)\n"
        "                TripleTableIterator(?X*, :S, ?Z*)\n"
    );

    ASSERT_QUERY_AND_PLAN("SELECT ?Y ?Z WHERE { { ?X :R ?Y }  UNION { ?X :S ?Z } }",
        ":b1   UNDEF ."
        ":b2   UNDEF ."
        "UNDEF :d1   ."
        "UNDEF :d2   .",

        "QueryIterator(?Y#, ?Z#)\n"
        "    UnionIterator(?Y#, ?Z#)\n"
        "        DistinctIterator(?X*, ?Y*)\n"
        "            NestedIndexLoopJoinIterator(?X*, ?Y*)\n"
        "                TripleTableIterator(?X*, :R, ?Y*)\n"
        "        DistinctIterator(?X*, ?Z*)\n"
        "            NestedIndexLoopJoinIterator(?X*, ?Z*)\n"
        "                TripleTableIterator(?X*, :S, ?Z*)\n"
    );
}

TEST(testUnion2) {
    addTriples(
        ":a1 :R :b1 . "
        ":a2 :R :b2 . "

        ":b1 rdf:type :A . "

        ":c1 :S :d1 . "
        ":c2 :S :d2 . "
    );

    ASSERT_QUERY_AND_PLAN("SELECT ?X ?Y ?Z WHERE { ?Y rdf:type :A . { { ?X :R ?Y }  UNION { ?X :S ?Z } } }",
        ":a1 :b1 UNDEF ."
        ":c1 :b1 :d1   ."
        ":c2 :b1 :d2   .",

        "QueryIterator(?X*, ?Y*, ?Z#)\n"
        "    DistinctIterator(?Y*, ?X*, ?Z#)\n"
        "        NestedIndexLoopJoinIterator(?Y*, ?X*, ?Z#)\n"
        "            TripleTableIterator(?Y*, rdf:type, :A)\n"
        "            UnionIterator(?X*, ?Y, ?Z#)\n"
        "                DistinctIterator(?X*, ?Y)\n"
        "                    NestedIndexLoopJoinIterator(?X*, ?Y)\n"
        "                        TripleTableIterator(?X*, :R, ?Y)\n"
        "                DistinctIterator(?X*, ?Z*)\n"
        "                    NestedIndexLoopJoinIterator(?X*, ?Z*)\n"
        "                        TripleTableIterator(?X*, :S, ?Z*)\n"
    );

    ASSERT_QUERY_AND_PLAN("SELECT ?X ?Y ?Z WHERE { ?Y rdf:type :A . OPTIONAL { { ?X :R ?Y }  UNION { ?X :S ?Z } } }",
        ":a1 :b1 UNDEF ."
        ":c1 :b1 :d1   ."
        ":c2 :b1 :d2   .",

        "QueryIterator(?X#, ?Y*, ?Z#)\n"
        "    LeftJoinIterator(?Y*, ?X#, ?Z#)\n"
        "        DistinctIterator(?Y*)\n"
        "            NestedIndexLoopJoinIterator(?Y*)\n"
        "                TripleTableIterator(?Y*, rdf:type, :A)\n"
        "        UnionIterator(?X*, ?Y, ?Z#)\n"
        "            DistinctIterator(?X*, ?Y)\n"
        "                NestedIndexLoopJoinIterator(?X*, ?Y)\n"
        "                    TripleTableIterator(?X*, :R, ?Y)\n"
        "            DistinctIterator(?X*, ?Z*)\n"
        "                NestedIndexLoopJoinIterator(?X*, ?Z*)\n"
        "                    TripleTableIterator(?X*, :S, ?Z*)\n"
    );
}

TEST(testUnion3) {
    addTriples(
        ":b1 rdf:type :A . "
        ":b2 rdf:type :A . "

        ":b1 :S :c1 . "
        ":b2 :S :c2 . "
    );

    m_queryParameters.setBoolean("distinct", false);

    ASSERT_QUERY_AND_PLAN("SELECT ?X ?Y WHERE { { ?X :S ?Y } UNION { ?X rdf:type :A } }",
        ":b1 :c1   ."
        ":b2 :c2   ."
        ":b1 UNDEF ."
        ":b2 UNDEF .",

        "QueryIterator(?X*, ?Y#)\n"
        "    UnionIterator(?X*, ?Y#)\n"
        "        NestedIndexLoopJoinIterator(?X*, ?Y*)\n"
        "            TripleTableIterator(?X*, :S, ?Y*)\n"
        "        NestedIndexLoopJoinIterator(?X*)\n"
        "            TripleTableIterator(?X*, rdf:type, :A)\n"
    );

    ASSERT_QUERY_AND_PLAN("SELECT ?X ?Y WHERE { { ?X rdf:type :A } UNION { ?X :R ?Y } }",
        ":b1 UNDEF ."
        ":b2 UNDEF .",

        "QueryIterator(?X*, ?Y#)\n"
        "    UnionIterator(?X*, ?Y#)\n"
        "        NestedIndexLoopJoinIterator(?X*)\n"
        "            TripleTableIterator(?X*, rdf:type, :A)\n"
        "        NestedIndexLoopJoinIterator(?X*, ?Y*)\n"
        "            TripleTableIterator(?X*, :R, ?Y*)\n"
    );

    ASSERT_QUERY_AND_PLAN("SELECT ?X ?Y WHERE { { { ?X rdf:type :A } UNION { ?X :R ?Y } } . { { ?X :S ?Y } UNION { ?X rdf:type :A } } }",
        ":b1 :c1   ."
        ":b2 :c2   ."
        ":b1 UNDEF ."
        ":b2 UNDEF .",

        "QueryIterator(?X*, ?Y#)\n"
        "    NestedIndexLoopJoinIterator(?X*, ?Y#)\n"
        "        UnionIterator(?X*, ?Y#)\n"
        "            NestedIndexLoopJoinIterator(?X*)\n"
        "                TripleTableIterator(?X*, rdf:type, :A)\n"
        "            NestedIndexLoopJoinIterator(?X*, ?Y*)\n"
        "                TripleTableIterator(?X*, :R, ?Y*)\n"
        "        UnionIterator(?X, ?Y+)\n"
        "            NestedIndexLoopJoinIterator(?X, ?Y+)\n"
        "                TripleTableIterator(?X, :S, ?Y+)\n"
        "            NestedIndexLoopJoinIterator(?X)\n"
        "                TripleTableIterator(?X, rdf:type, :A)\n"
    );
}

TEST(testUnion4) {
    addTriples(
        ":b1 rdf:type :A . "
        ":b2 rdf:type :A . "

        ":b1 :S :c1 . "
        ":b2 :S :c2 . "

        ":c1 :T :d1 . "
        ":c2 :T :d2 . "
        ":c3 :T :d3 . "
        ":c4 :T :d4 . "

        ":e1 :U :f1 . "
        ":e2 :U :f2 . "
        ":e3 :U :f3 . "
        ":e4 :U :f4 . "
        ":e5 :U :f5 . "
        ":e6 :U :f6 . "
    );

    m_queryParameters.setBoolean("distinct", false);

    ASSERT_QUERY_AND_PLAN("SELECT ?X ?Y WHERE { { ?X rdf:type :A } UNION { ?X :R ?Y } }",
        ":b1 UNDEF ."
        ":b2 UNDEF .",

        "QueryIterator(?X*, ?Y#)\n"
        "    UnionIterator(?X*, ?Y#)\n"
        "        NestedIndexLoopJoinIterator(?X*)\n"
        "            TripleTableIterator(?X*, rdf:type, :A)\n"
        "        NestedIndexLoopJoinIterator(?X*, ?Y*)\n"
        "            TripleTableIterator(?X*, :R, ?Y*)\n"
    );

    ASSERT_QUERY_AND_PLAN("SELECT ?X ?Y ?Z ?W WHERE { { ?X :S ?Y . ?Y :T ?Z . ?Z :U ?W } UNION { ?X rdf:type :A } }",
        ":b1 UNDEF UNDEF UNDEF ."
        ":b2 UNDEF UNDEF UNDEF .",

        "QueryIterator(?X*, ?Y#, ?Z#, ?W#)\n"
        "    UnionIterator(?X*, ?Y#, ?Z#, ?W#)\n"
        "        NestedIndexLoopJoinIterator(?X*, ?Y*, ?Z*, ?W*)\n"
        "            TripleTableIterator(?X*, :S, ?Y*)\n"
        "            TripleTableIterator(?Y, :T, ?Z*)\n"
        "            TripleTableIterator(?Z, :U, ?W*)\n"
        "        NestedIndexLoopJoinIterator(?X*)\n"
        "            TripleTableIterator(?X*, rdf:type, :A)\n"
    );

    ASSERT_QUERY_AND_PLAN("SELECT ?X ?Y WHERE { { { ?X rdf:type :A } UNION { ?X :R ?Y } } . { { ?X :S ?Y . ?Y :T ?Z . ?Z :U ?W } UNION { ?X rdf:type :A } } }",
        ":b1 UNDEF ."
        ":b2 UNDEF .",

        "QueryIterator(?X*, ?Y#)\n"
        "    NestedIndexLoopJoinIterator(?X*, ?Y#)\n"
        "        UnionIterator(?X*, ?Y#)\n"
        "            NestedIndexLoopJoinIterator(?X*)\n"
        "                TripleTableIterator(?X*, rdf:type, :A)\n"
        "            NestedIndexLoopJoinIterator(?X*, ?Y*)\n"
        "                TripleTableIterator(?X*, :R, ?Y*)\n"
        "        UnionIterator(?X, ?Y+, ?Z#, ?W#)\n"
        "            NestedIndexLoopJoinIterator(?X, ?Y+, ?Z*, ?W*)\n"
        "                TripleTableIterator(?X, :S, ?Y+)\n"
        "                TripleTableIterator(?Y, :T, ?Z*)\n"
        "                TripleTableIterator(?Z, :U, ?W*)\n"
        "            NestedIndexLoopJoinIterator(?X)\n"
        "                TripleTableIterator(?X, rdf:type, :A)\n"
    );
}

TEST(testUnion5) {
    addTriples(
        ":a1 :R :b1 . "
        ":a2 :R :b2 . "
        ":a3 :R :b3 . "
        ":a4 :R :b4 . "

        ":c rdf:type :A . "
        ":d rdf:type :A . "

        ":b1 :S :e1 . "
        ":b4 :S :e4 . "
    );

    m_queryParameters.setBoolean("distinct", false);

    ASSERT_QUERY_AND_PLAN("SELECT ?X ?Y ?Z WHERE { ?X :R ?Y . { { ?Z rdf:type :A } UNION { ?Y :S ?Z } } }",
        ":a1 :b1 :c  ."
        ":a1 :b1 :d  ."
        ":a2 :b2 :c  ."
        ":a2 :b2 :d  ."
        ":a3 :b3 :c  ."
        ":a3 :b3 :d  ."
        ":a4 :b4 :c  ."
        ":a4 :b4 :d  ."
        ":a1 :b1 :e1 ."
        ":a4 :b4 :e4 .",

        "QueryIterator(?X*, ?Y*, ?Z*)\n"
        "    NestedIndexLoopJoinIterator(?X*, ?Y*, ?Z*)\n"
        "        TripleTableIterator(?X*, :R, ?Y*)\n"
        "        UnionIterator(?Y, ?Z*)\n"
        "            NestedIndexLoopJoinIterator(?Z*)\n"
        "                TripleTableIterator(?Z*, rdf:type, :A)\n"
        "            NestedIndexLoopJoinIterator(?Y, ?Z*)\n"
        "                TripleTableIterator(?Y, :S, ?Z*)\n"
    );
}

TEST(testSubquery1) {
    addTriples(
        ":a1 :R :b1 . "
        ":a1 :R :b2 . "
        ":a3 :R :b3 . "

        ":a1 :S :c1 . "
        ":a1 :S :c2 . "
        ":a1 :S :c3 . "

        ":c1 :T :d . "
        ":c2 :T :d . "
    );

    m_queryParameters.setBoolean("distinct", false);

    ASSERT_QUERY_AND_PLAN("SELECT ?X ?Y ?Z WHERE { ?X :R ?Y . { SELECT ?X ?Z WHERE { ?X :S ?Y . ?Y :T ?Z } } }",
        ":a1 :b1 :d * 2."
        ":a1 :b2 :d * 2.",

        "QueryIterator(?X*, ?Y*, ?Z*)\n"
        "    NestedIndexLoopJoinIterator(?X*, ?Y*, ?Z*)\n"
        "        TripleTableIterator(?X*, :R, ?Y*)\n"
        "        NestedIndexLoopJoinIterator(?X, ?Z*)\n"
        "            TripleTableIterator(?X, :S, ?__SQ1__Y*)\n"
        "            TripleTableIterator(?__SQ1__Y, :T, ?Z*)\n"
    );
}

TEST(testMinus1) {
    addTriples(
        ":a1 :R :b1 . "
        ":a2 :R :b2 . "
        ":a3 :R :b3 . "
        ":a4 :R :b4 . "

        ":a1 rdf:type :A . "
        ":a2 rdf:type :A . "

        ":a3 :S :c3 . "
    );

    ASSERT_QUERY_AND_PLAN("SELECT ?X ?Y WHERE { { ?X :R ?Y } MINUS { ?X rdf:type :A } }",
        ":a3 :b3 ."
        ":a4 :b4 .",

        "QueryIterator(?X*, ?Y*)\n"
        "    DifferenceIterator(?X*, ?Y*)\n"
        "        DistinctIterator(?X*, ?Y*)\n"
        "            NestedIndexLoopJoinIterator(?X*, ?Y*)\n"
        "                TripleTableIterator(?X*, :R, ?Y*)\n"
        "        DistinctIterator(?X)\n"
        "            NestedIndexLoopJoinIterator(?X)\n"
        "                TripleTableIterator(?X, rdf:type, :A)\n"
    );

    ASSERT_QUERY_AND_PLAN("SELECT ?X ?Y WHERE { { ?X :R ?Y } MINUS { ?X :S ?Z } }",
        ":a1 :b1 ."
        ":a2 :b2 ."
        ":a4 :b4 .",

        "QueryIterator(?X*, ?Y*)\n"
        "    DifferenceIterator(?X*, ?Y*)\n"
        "        DistinctIterator(?X*, ?Y*)\n"
        "            NestedIndexLoopJoinIterator(?X*, ?Y*)\n"
        "                TripleTableIterator(?X*, :R, ?Y*)\n"
        "        DistinctIterator(?X)\n"
        "            NestedIndexLoopJoinIterator(?X)\n"
        "                TripleTableIterator(?X, :S, ?__SM1__Z*)\n"
    );
}

TEST(testMinus2) {
    addTriples(
        ":a1 :R :b1 . "
        ":a2 :R :b2 . "
        ":a3 :R :b3 . "
        ":a4 :R :b4 . "

        ":b1 :S :c1 . "
        ":b2 :S :c2 . "
        ":b3 :S :c3 . "

        ":c3 :T :d3 . "
    );

    ASSERT_QUERY_AND_PLAN("SELECT ?X ?Y ?Z WHERE { ?X :R ?Y . { { ?Y :S ?Z } MINUS { ?Z :T ?X } } }",
        ":a1 :b1 :c1 ."
        ":a2 :b2 :c2 .",

        "QueryIterator(?X*, ?Y*, ?Z*)\n"
        "    DistinctIterator(?X*, ?Y*, ?Z*)\n"
        "        NestedIndexLoopJoinIterator(?X*, ?Y*, ?Z*)\n"
        "            TripleTableIterator(?X*, :R, ?Y*)\n"
        "            DifferenceIterator(?Y, ?Z*)\n"
        "                DistinctIterator(?Y, ?Z*)\n"
        "                    NestedIndexLoopJoinIterator(?Y, ?Z*)\n"
        "                        TripleTableIterator(?Y, :S, ?Z*)\n"
        "                DistinctIterator(?Z)\n"
        "                    NestedIndexLoopJoinIterator(?Z)\n"
        "                        TripleTableIterator(?Z, :T, ?__SM1__X*)\n"
    );

    m_queryParameters.setBoolean("distinct", false);

    ASSERT_QUERY_AND_PLAN("SELECT ?X ?Y ?Z WHERE { ?X :R ?Y . { { ?Y :S ?Z } MINUS { ?Z :T ?X } } }",
        ":a1 :b1 :c1 ."
        ":a2 :b2 :c2 .",

        "QueryIterator(?X*, ?Y*, ?Z*)\n"
        "    NestedIndexLoopJoinIterator(?X*, ?Y*, ?Z*)\n"
        "        TripleTableIterator(?X*, :R, ?Y*)\n"
        "        DifferenceIterator(?Y, ?Z*)\n"
        "            NestedIndexLoopJoinIterator(?Y, ?Z*)\n"
        "                TripleTableIterator(?Y, :S, ?Z*)\n"
        "            NestedIndexLoopJoinIterator(?Z)\n"
        "                TripleTableIterator(?Z, :T, ?__SM1__X*)\n"
    );
}

TEST(testMinus3) {
    addTriples(
        ":a1 rdf:type :A . "
        ":a2 rdf:type :A . "

        ":a3 :R :b3 . "
        ":a4 :R :b4 . "

        ":a1 :S :c1 . "

        ":a3 :T :d3 . "
    );

    ASSERT_QUERY_AND_PLAN("SELECT ?X ?Y WHERE { { { ?X rdf:type :A } UNION { ?X :R ?Y } } MINUS { ?X :S ?Z } MINUS { ?X :T ?Z } }",
        ":a2 UNDEF ."
        ":a4 :b4   .",

        "QueryIterator(?X*, ?Y#)\n"
        "    DifferenceIterator(?X*, ?Y#)\n"
        "        UnionIterator(?X*, ?Y#)\n"
        "            DistinctIterator(?X*)\n"
        "                NestedIndexLoopJoinIterator(?X*)\n"
        "                    TripleTableIterator(?X*, rdf:type, :A)\n"
        "            DistinctIterator(?X*, ?Y*)\n"
        "                NestedIndexLoopJoinIterator(?X*, ?Y*)\n"
        "                    TripleTableIterator(?X*, :R, ?Y*)\n"
        "        DistinctIterator(?X)\n"
        "            NestedIndexLoopJoinIterator(?X)\n"
        "                TripleTableIterator(?X, :S, ?__SM1__Z*)\n"
        "        DistinctIterator(?X)\n"
        "            NestedIndexLoopJoinIterator(?X)\n"
        "                TripleTableIterator(?X, :T, ?__SM2__Z*)\n"
    );

}

TEST(testValues1) {
    addTriples(
        ":a1 rdf:type :A . "

        ":a4 rdf:type :A . "
    );

    ASSERT_QUERY_AND_PLAN("SELECT ?X ?Y WHERE { ?X rdf:type :A . VALUES (?X ?Y) { (:a1 :b1) (:a1 :b2) (:a3 :b3) (UNDEF :b5) (UNDEF :b6) } }",
        ":a1 :b1 ."
        ":a1 :b2 ."
        ":a1 :b5 ."
        ":a1 :b6 ."
        ":a4 :b5 ."
        ":a4 :b6 .",

        "QueryIterator(?X*, ?Y*)\n"
        "    DistinctIterator(?X*, ?Y*)\n"
        "        NestedIndexLoopJoinIterator(?X*, ?Y*)\n"
        "            TripleTableIterator(?X*, rdf:type, :A)\n"
        "            ValuesIterator(?X, ?Y*)\n"
    );
}

TEST(testValues2) {
    addTriples(
        ":a1 rdf:type :A . "
        ":a3 :R :b3 . "
    );

    ASSERT_QUERY_AND_PLAN("SELECT ?X ?Y ?Z WHERE { { { ?X rdf:type :A } UNION { ?X :R ?Y } } . VALUES (?X ?Y ?Z) { (:a1 UNDEF :c0) (:a1 :b1 :c1) (:a1 :b2 :c2) (:a3 UNDEF :c3) (:a3 :b3 :c4) (:a3 :b3 UNDEF) (:a5 UNDEF :c5) } }",
        ":a1 UNDEF :c0   ."
        ":a1 :b1   :c1   ."
        ":a1 :b2   :c2   ."
        ":a3 :b3   :c3   ."
        ":a3 :b3   UNDEF ."
        ":a3 :b3   :c4   .",

        "QueryIterator(?X*, ?Y#, ?Z#)\n"
        "    DistinctIterator(?X*, ?Y#, ?Z#)\n"
        "        NestedIndexLoopJoinIterator(?X*, ?Y#, ?Z#)\n"
        "            UnionIterator(?X*, ?Y#)\n"
        "                DistinctIterator(?X*)\n"
        "                    NestedIndexLoopJoinIterator(?X*)\n"
        "                        TripleTableIterator(?X*, rdf:type, :A)\n"
        "                DistinctIterator(?X*, ?Y*)\n"
        "                    NestedIndexLoopJoinIterator(?X*, ?Y*)\n"
        "                        TripleTableIterator(?X*, :R, ?Y*)\n"
        "            ValuesIterator(?X, ?Y+, ?Z#)\n"
    );
}

TEST(testExistenceExpression1) {
    addTriples(
        ":a1 :R :b1 . "
        ":a2 :R :b2 . "
        ":a3 :R :b3 . "
        ":a4 :R :b4 . "

        ":a1 rdf:type :A . "
        ":a2 rdf:type :A . "
    );

    ASSERT_QUERY_AND_PLAN("SELECT ?X ?Y WHERE { ?X :R ?Y . FILTER EXISTS { ?X rdf:type :A } }",
        ":a1 :b1 ."
        ":a2 :b2 .",

        "QueryIterator(?X*, ?Y*)\n"
        "    DistinctIterator(?X*, ?Y*)\n"
        "        NestedIndexLoopJoinIterator(?X*, ?Y*)\n"
        "            TripleTableIterator(?X*, :R, ?Y*)\n"
        "            FilterTupleIterator(?X)\n"
    );

    ASSERT_QUERY_AND_PLAN("SELECT ?X ?Y WHERE { ?X :R ?Y . FILTER NOT EXISTS { ?X rdf:type :A } }",
        ":a3 :b3 ."
        ":a4 :b4 .",

        "QueryIterator(?X*, ?Y*)\n"
        "    DistinctIterator(?X*, ?Y*)\n"
        "        NestedIndexLoopJoinIterator(?X*, ?Y*)\n"
        "            TripleTableIterator(?X*, :R, ?Y*)\n"
        "            FilterTupleIterator(?X)\n"
    );
}

TEST(testExistenceExpression2) {
    addTriples(
        ":b1 rdf:type :B . "

        ":a2 :R :b2 . "
        ":a3 :R :b3 . "

        ":a1 rdf:type :A . "
        ":a2 rdf:type :A . "
    );

    ASSERT_QUERY_AND_PLAN("SELECT ?X ?Y WHERE { { { ?Y rdf:type :B } UNION { ?X :R ?Y } } . FILTER EXISTS { ?X rdf:type :A } }",
        "UNDEF :b1 ."
        ":a2   :b2 .",

        "QueryIterator(?X#, ?Y*)\n"
        "    DistinctIterator(?X#, ?Y*)\n"
        "        NestedIndexLoopJoinIterator(?X#, ?Y*)\n"
        "            UnionIterator(?X#, ?Y*)\n"
        "                DistinctIterator(?Y*)\n"
        "                    NestedIndexLoopJoinIterator(?Y*)\n"
        "                        TripleTableIterator(?Y*, rdf:type, :B)\n"
        "                DistinctIterator(?X*, ?Y*)\n"
        "                    NestedIndexLoopJoinIterator(?X*, ?Y*)\n"
        "                        TripleTableIterator(?X*, :R, ?Y*)\n"
        "            FilterTupleIterator(?X+)\n"
    );

    ASSERT_QUERY_AND_PLAN("SELECT ?X ?Y WHERE { { { ?Y rdf:type :B } UNION { ?X :R ?Y } } . FILTER NOT EXISTS { ?X rdf:type :A } }",
        ":a3   :b3 .",

        "QueryIterator(?X#, ?Y*)\n"
        "    DistinctIterator(?X#, ?Y*)\n"
        "        NestedIndexLoopJoinIterator(?X#, ?Y*)\n"
        "            UnionIterator(?X#, ?Y*)\n"
        "                DistinctIterator(?Y*)\n"
        "                    NestedIndexLoopJoinIterator(?Y*)\n"
        "                        TripleTableIterator(?Y*, rdf:type, :B)\n"
        "                DistinctIterator(?X*, ?Y*)\n"
        "                    NestedIndexLoopJoinIterator(?X*, ?Y*)\n"
        "                        TripleTableIterator(?X*, :R, ?Y*)\n"
        "            FilterTupleIterator(?X+)\n"
    );
}

TEST(testExistenceExpression3) {
    addTriples(
        ":a1 :R :b1 . "

        ":a2 :R :b2 . "
        ":b2 :S :c2 . "
    );

    ASSERT_QUERY_AND_PLAN("SELECT ?X ?Y ?Z WHERE { ?X ?Y ?Z . FILTER NOT EXISTS { SELECT ?Z WHERE { ?Z :S ?W } } }",
        ":a1 :R :b1 ."
        ":b2 :S :c2 .",

        "QueryIterator(?X*, ?Y*, ?Z*)\n"
        "    DistinctIterator(?X*, ?Y*, ?Z*)\n"
        "        NestedIndexLoopJoinIterator(?X*, ?Y*, ?Z*)\n"
        "            TripleTableIterator(?X*, ?Y*, ?Z*)\n"
        "            FilterTupleIterator(?Z)\n"
    );
}

TEST(testExistenceExpression4) {
    addTriples(
        ":R rdfs:subPropertyOf :T . "
        ":S rdfs:subPropertyOf :T . "

        ":a1 rdf:type :A . "

        ":a2 rdf:type :A . "
        ":a2 :R :b2 . "

        ":a3 rdf:type :A . "
        ":a3 :S :b3 . "

        ":a4 rdf:type :A . "
        ":a4 :R :b4 . "
        ":a4 :S :b4 . "
    );

    ASSERT_QUERY_AND_PLAN("SELECT ?X ?Y WHERE { ?X rdf:type :A . ?Y rdfs:subPropertyOf :T . FILTER NOT EXISTS { SELECT ?X ?Y WHERE { ?X ?Y ?Z } } }",
        ":a1 :R ."
        ":a1 :S ."
        ":a2 :S ."
        ":a3 :R .",

        "QueryIterator(?X*, ?Y*)\n"
        "    DistinctIterator(?Y*, ?X*)\n"
        "        NestedIndexLoopJoinIterator(?Y*, ?X*)\n"
        "            TripleTableIterator(?Y*, rdfs:subPropertyOf, :T)\n"
        "            TripleTableIterator(?X*, rdf:type, :A)\n"
        "            FilterTupleIterator(?X, ?Y)\n"
    );

    ASSERT_QUERY_AND_PLAN("SELECT ?X ?Y WHERE { ?X rdf:type :A . VALUES ?Y { :R :S } . FILTER NOT EXISTS { SELECT ?X ?Y WHERE { ?X ?Y ?Z } } }",
        ":a1 :R ."
        ":a1 :S ."
        ":a2 :S ."
        ":a3 :R .",

        "QueryIterator(?X*, ?Y*)\n"
        "    DistinctIterator(?X*, ?Y*)\n"
        "        NestedIndexLoopJoinIterator(?X*, ?Y*)\n"
        "            TripleTableIterator(?X*, rdf:type, :A)\n"
        "            ValuesIterator(?Y*)\n"
        "            FilterTupleIterator(?X, ?Y)\n"
    );
}

TEST(testExistenceExpression5) {
    addTriples(
        ":A1 rdfs:subClassOf :A2 . "
        ":A1 rdfs:subClassOf :A3 . "
        ":A2 rdfs:subClassOf :A3 . "

        ":B1 rdfs:subClassOf :B2 . "
        ":B1 rdfs:subClassOf :B3 . "
        ":B2 rdfs:subClassOf :B3 . "

        ":i1 rdf:type :A1 . "
        ":i1 rdf:type :A2 . "
        ":i1 rdf:type :A3 . "

        ":i1 rdf:type :B1 . "
        ":i1 rdf:type :B2 . "
        ":i1 rdf:type :B3 . "
    );

    ASSERT_QUERY_AND_PLAN("SELECT ?X ?Y WHERE { ?X rdf:type ?Y . FILTER NOT EXISTS { SELECT ?X ?Y WHERE { ?X rdf:type ?Z . ?Z rdfs:subClassOf ?Y } } }",
        ":i1 :A1 ."
        ":i1 :B1 .",

        "QueryIterator(?X*, ?Y*)\n"
        "    DistinctIterator(?X*, ?Y*)\n"
        "        NestedIndexLoopJoinIterator(?X*, ?Y*)\n"
        "            TripleTableIterator(?X*, rdf:type, ?Y*)\n"
        "            FilterTupleIterator(?X, ?Y)\n"
    );
}

TEST(testNegation) {
    addTriples(
        ":a1 rdf:type :A . "
        ":a2 rdf:type :A . "
        ":a3 rdf:type :A . "

        ":b1 rdf:type :B . "
        ":b2 rdf:type :B . "
        ":b3 rdf:type :B . "

        ":a2 :R :b2 . "
        ":a2 :R :b3 . "
    );

    ASSERT_QUERY_AND_PLAN("SELECT ?X ?Y WHERE { ?X rdf:type :A . NOT ?X :R ?Y . ?Y rdf:type :B }",
        ":a1 :b1 ."
        ":a1 :b2 ."
        ":a1 :b3 ."
        ":a2 :b1 ."
        ":a3 :b1 ."
        ":a3 :b2 ."
        ":a3 :b3 .",

        "QueryIterator(?X*, ?Y*)\n"
        "    DistinctIterator(?X*, ?Y*)\n"
        "        NestedIndexLoopJoinIterator(?X*, ?Y*)\n"
        "            TripleTableIterator(?X*, rdf:type, :A)\n"
        "            TripleTableIterator(?Y*, rdf:type, :B)\n"
        "            NegationIterator(?X, :R, ?Y)\n"
        "                TripleTableIterator(?X, :R, ?Y)\n"
    );
}

#endif
