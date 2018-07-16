// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifdef WITH_TEST

#define AUTO_TEST    QueryBuiltinsTest

#include <CppTest/AutoTest.h>

#include "DataStoreTest.h"
#include "../../src/querying/QueryDecomposition.h"

class QueryBuiltinsTest : public DataStoreTest {

protected:

    virtual void initializeQueryParameters() {
        m_queryParameters.setBoolean("bushy", false);
        m_queryParameters.setBoolean("distinct", true);
        m_queryParameters.setBoolean("root-has-answers", true);
    }

public:

    QueryBuiltinsTest() : DataStoreTest() {
    }

};

TEST(testSimpleComparison) {
    addTriples(
        ":a :R 1 . "
        ":a :R 2 . "
        ":a :R \"3.0\"^^xsd:float . "
        ":a :R \"4.1\"^^xsd:float . "
        ":a :R \"false\"^^xsd:boolean . "
        ":a :R \"true\"^^xsd:boolean . "
        ":a :R \"abc\" . "
        ":a :R \"def\" . "
        ":a :R \"ghi\"@de . "

        ":a :S 1 . "
        ":a :S \"2.2\"^^xsd:float . "
        ":a :S 3 . "
        ":a :S 4 . "
        ":a :S \"false\"^^xsd:boolean . "
        ":a :S \"true\"^^xsd:boolean . "
        ":a :S \"abc\" . "
        ":a :S \"def\"@en . "
        ":a :S \"ghi\"@es . "
    );

    ASSERT_QUERY("SELECT ?V1 ?V2 WHERE { ?X :R ?V1 . ?X :S ?V2 . FILTER(?V1 < ?V2) }",
        "1 \"2.2\"^^xsd:float . "
        "1 3 . "
        "1 4 . "
        "2 \"2.2\"^^xsd:float . "
        "2 3 . "
        "2 4 . "
        "\"3.0\"^^xsd:float 4 . "
        "\"false\"^^xsd:boolean \"true\"^^xsd:boolean . "
        "\"abc\" \"def\"@en . "
        "\"abc\" \"ghi\"@es . "
        "\"def\" \"def\"@en . "
        "\"def\" \"ghi\"@es . "
        "\"ghi\"@de \"ghi\"@es . "
    );

    ASSERT_QUERY("SELECT ?V1 ?V2 WHERE { ?X :R ?V1 . ?X :S ?V2 . FILTER(?V1 <= ?V2) }",
        "1 1 . "
        "1 \"2.2\"^^xsd:float . "
        "1 3 . "
        "1 4 . "
        "2 \"2.2\"^^xsd:float . "
        "2 3 . "
        "2 4 . "
        "\"3.0\"^^xsd:float 3 . "
        "\"3.0\"^^xsd:float 4 . "
        "\"false\"^^xsd:boolean \"false\"^^xsd:boolean . "
        "\"false\"^^xsd:boolean \"true\"^^xsd:boolean . "
        "\"true\"^^xsd:boolean \"true\"^^xsd:boolean . "
        "\"abc\" \"abc\" . "
        "\"abc\" \"def\"@en . "
        "\"abc\" \"ghi\"@es . "
        "\"def\" \"def\"@en . "
        "\"def\" \"ghi\"@es . "
        "\"ghi\"@de \"ghi\"@es . "
    );

    ASSERT_QUERY("SELECT ?V1 ?V2 WHERE { ?X :R ?V1 . ?X :S ?V2 . FILTER(?V1 > ?V2) }",
        "2 1 . "
        "\"3.0\"^^xsd:float 1 . "
        "\"3.0\"^^xsd:float \"2.2\"^^xsd:float . "
        "\"4.1\"^^xsd:float 1 . "
        "\"4.1\"^^xsd:float \"2.2\"^^xsd:float . "
        "\"4.1\"^^xsd:float 3 . "
        "\"4.1\"^^xsd:float 4 . "
        "\"true\"^^xsd:boolean \"false\"^^xsd:boolean . "
        "\"def\" \"abc\" . "
        "\"ghi\"@de \"abc\" . "
        "\"ghi\"@de \"def\"@en . "
    );

    ASSERT_QUERY("SELECT ?V1 ?V2 WHERE { ?X :R ?V1 . ?X :S ?V2 . FILTER(?V1 >= ?V2) }",
        "1 1 . "
        "2 1 . "
        "\"3.0\"^^xsd:float 1 . "
        "\"3.0\"^^xsd:float \"2.2\"^^xsd:float . "
        "\"3.0\"^^xsd:float 3 . "
        "\"4.1\"^^xsd:float 1 . "
        "\"4.1\"^^xsd:float \"2.2\"^^xsd:float . "
        "\"4.1\"^^xsd:float 3 . "
        "\"4.1\"^^xsd:float 4 . "
        "\"false\"^^xsd:boolean \"false\"^^xsd:boolean . "
        "\"true\"^^xsd:boolean \"false\"^^xsd:boolean . "
        "\"true\"^^xsd:boolean \"true\"^^xsd:boolean . "
        "\"abc\" \"abc\" . "
        "\"def\" \"abc\" . "
        "\"ghi\"@de \"abc\" . "
        "\"ghi\"@de \"def\"@en . "
    );
}

TEST(testNumericOperators) {
    addTriples(
        ":a :R 1 . "
        ":a :R \"7.7\"^^xsd:float . "
        ":a :R \"false\"^^xsd:boolean . "
        ":a :R \"abc\" . "

        ":a :S 0 . "
        ":a :S \"2.5\"^^xsd:float . "
        ":a :S \"true\"^^xsd:boolean . "
        ":a :S \"def\" . "
    );

    ASSERT_QUERY("SELECT ?V1 ?V2 ?Z WHERE { ?X :R ?V1 . ?X :S ?V2 . BIND (?V1 + ?V2 AS ?Z) }",
        "1 0 1 . "
        "1 \"2.5\"^^xsd:float \"3.5\"^^xsd:float . "
        "\"7.7\"^^xsd:float 0 \"7.7\"^^xsd:float . "
        "\"7.7\"^^xsd:float \"2.5\"^^xsd:float \"10.2\"^^xsd:float . "
    );

    ASSERT_QUERY("SELECT ?V1 ?V2 ?Z WHERE { ?X :R ?V1 . ?X :S ?V2 . BIND (?V1 - ?V2 AS ?Z) }",
        "1 0 1 . "
        "1 \"2.5\"^^xsd:float \"-1.5\"^^xsd:float . "
        "\"7.7\"^^xsd:float 0 \"7.7\"^^xsd:float . "
        "\"7.7\"^^xsd:float \"2.5\"^^xsd:float \"5.2\"^^xsd:float . "
    );

    ASSERT_QUERY("SELECT ?V1 ?V2 ?Z WHERE { ?X :R ?V1 . ?X :S ?V2 . BIND (?V1 * ?V2 AS ?Z) }",
        "1 0 0 . "
        "1 \"2.5\"^^xsd:float \"2.5\"^^xsd:float . "
        "\"7.7\"^^xsd:float 0 \"0\"^^xsd:float . "
        "\"7.7\"^^xsd:float \"2.5\"^^xsd:float \"19.25\"^^xsd:float . "
    );

    ASSERT_QUERY("SELECT ?V1 ?V2 ?Z WHERE { ?X :R ?V1 . ?X :S ?V2 . BIND (?V1 / ?V2 AS ?Z) }",
        "1 \"2.5\"^^xsd:float \"0.4\"^^xsd:float . "
        "\"7.7\"^^xsd:float 0 \"inf\"^^xsd:float . "
        "\"7.7\"^^xsd:float \"2.5\"^^xsd:float \"3.08\"^^xsd:float . "
    );
}

TEST(testBound) {
    addTriples(
        ":a :R 1 . "
    );

    ASSERT_QUERY("SELECT ?X ?Y WHERE { ?X :R ?Y . FILTER BOUND(?Y) }",
        ":a 1 ."
    );
}

TEST(testIf) {
    addTriples(
        ":a1 :R 1 . "
        ":a2 :R 2 . "
        ":a3 :R 3 . "

        ":b1 :S 1 . "
        ":b2 :S 2 . "
        ":b3 :S 3 . "
    );

    ASSERT_QUERY("SELECT ?X1 ?X2 ?Z WHERE { ?X1 :R ?Y1 . ?X2 :S ?Y2 . BIND(IF(?Y1<?Y2, ?X1,?X2) AS ?Z) }",
        ":a1 :b1 :b1 . "
        ":a1 :b2 :a1 . "
        ":a1 :b3 :a1 . "

        ":a2 :b1 :b1 . "
        ":a2 :b2 :b2 . "
        ":a2 :b3 :a2 . "

        ":a3 :b1 :b1 . "
        ":a3 :b2 :b2 . "
        ":a3 :b3 :b3 . "
    );
}

TEST(testCoalesce) {
    addTriples(
        ":a :R 1 . "

        ":a :S 1 . "
        ":a :S \"true\"^^xsd:boolean . "

        ":a :T 2 . "
        ":a :T \"true\"^^xsd:boolean . "

        ":a :U 3 . "
    );

    ASSERT_QUERY("SELECT ?R ?S ?T ?U ?C WHERE { ?X :R ?R . ?X :S ?S . ?X :T ?T . ?X :U ?U . BIND(COALESCE(?R + ?S, ?T * ?U, ?S || ?T) AS ?C) }",
        "1 1 2 3 2 . "
        "1 1 \"true\"^^xsd:boolean 3 2 . "
        "1 \"true\"^^xsd:boolean 2 3 6 . "
        "1 \"true\"^^xsd:boolean \"true\"^^xsd:boolean 3 \"true\"^^xsd:boolean . "
    );
}

TEST(testInAndNotIn) {
    addTriples(
        ":i1 :R 1 . "
        ":i2 :R 2 . "
        ":i3 :R 3 . "
        ":i4 :R 4 . "
    );

/*    ASSERT_QUERY("SELECT ?X ?Y WHERE { ?X :R ?Y . FILTER(?Y IN (2,4,5,6)) }",
        ":i2 2 . "
        ":i4 4 . "
    );*/

    ASSERT_QUERY("SELECT ?X ?Y WHERE { ?X :R ?Y . FILTER(?Y NOT IN (2,4,5,6)) }",
        ":i1 1 . "
        ":i3 3 . "
    );

    ASSERT_QUERY("SELECT ?X ?Y WHERE { ?X :R ?Y . FILTER(?Y IN (1,7,8) && ?Y NOT IN (2,4,5,6)) }",
        ":i1 1 . "
    );
}

TEST(testResourceType) {
    addTriples(
        ":i1 :R 1 . "
        ":i2 :R \"abc\" . "
        ":i3 :R _:b . "
        ":i4 :R <abc> . "
        ":i5 :R \"7.0\"^^xsd:float . "
    );

    ASSERT_QUERY("SELECT ?X WHERE { ?X :R ?Y . FILTER(isIRI(?Y)) }",
        ":i4 . "
    );

    ASSERT_QUERY("SELECT ?X WHERE { ?X :R ?Y . FILTER(isURI(?Y)) }",
        ":i4 . "
    );

    ASSERT_QUERY("SELECT ?X WHERE { ?X :R ?Y . FILTER(isBlank(?Y)) }",
        ":i3 . "
    );

    ASSERT_QUERY("SELECT ?X WHERE { ?X :R ?Y . FILTER(isLiteral(?Y)) }",
        ":i1 . "
        ":i2 . "
        ":i5 . "
    );

    ASSERT_QUERY("SELECT ?X WHERE { ?X :R ?Y . FILTER(isNumeric(?Y)) }",
        ":i1 . "
        ":i5 . "
    );
}

TEST(testSTR_LANG_DATATYPE) {
    addTriples(
        ":i1 :R 1 . "
        ":i2 :R \"abc\" . "
        ":i3 :R _:b . "
        ":i4 :R <someIRI> . "
        ":i5 :R \"7.0\"^^xsd:float . "
        ":i6 :R \"def\"@en . "
        ":i7 :R \"true\"^^xsd:boolean . "
        ":i8 :R \"1972-10-16T09:00:00+01:00\"^^xsd:dateTime . "
        ":i9 :R \"P2M\"^^xsd:duration . "
    );

    ASSERT_QUERY("SELECT ?X ?Z WHERE { ?X :R ?Y . BIND(STR(?Y) AS ?Z) }",
        ":i1 \"1\" . "
        ":i2 \"abc\" . "
        ":i3 \"b\" . "
        ":i4 \"someIRI\" . "
        ":i5 \"7\" . "
        ":i6 \"def@en\" . "
        ":i7 \"true\" . "
        ":i8 \"1972-10-16T09:00:00+01:00\" . "
        ":i9 \"P2M\" . "
    );

    ASSERT_QUERY("SELECT ?X ?Z WHERE { ?X :R ?Y . BIND(LANG(?Y) AS ?Z) }",
        ":i1 \"\" . "
        ":i2 \"\" . "
        ":i3 \"\" . "
        ":i4 \"\" . "
        ":i5 \"\" . "
        ":i6 \"en\" . "
        ":i7 \"\" . "
        ":i8 \"\" . "
        ":i9 \"\" . "
    );

    ASSERT_QUERY("SELECT ?X ?Z WHERE { ?X :R ?Y . BIND(DATATYPE(?Y) AS ?Z) }",
        ":i1 xsd:integer . "
        ":i2 xsd:string . "
        ":i5 xsd:float . "
        ":i6 rdf:PlainLiteral . "
        ":i7 xsd:boolean . "
        ":i8 xsd:dateTime . "
        ":i9 xsd:duration . "
    );
}

TEST(testSTRDT) {
    addTriples(
        ":i1 :LF 1 . "
        ":i1 :DT xsd:string . "

        ":i2 :LF \"abc@en\" . "
        ":i2 :DT xsd:string . "

        ":i3 :LF \"def@en\" . "
        ":i3 :DT rdf:PlainLiteral . "

        ":i4 :LF \"true\" . "
        ":i4 :DT xsd:boolean . "

        ":i5 :LF \"1\" . "
        ":i5 :DT xsd:integer . "

        ":i6 :LF \"ghi\" . "
        ":i6 :DT xsd:integer . "

        ":i7 :LF \"1.2\" . "
        ":i7 :DT xsd:float . "
    );

    ASSERT_QUERY("SELECT ?X ?Z WHERE { ?X :LF ?LF . ?X :DT ?DT . BIND(STRDT(?LF,?DT) AS ?Z) }",
        ":i2 \"abc@en\" . "
        ":i3 \"def\"@en . "
        ":i4 \"true\"^^xsd:boolean . "
        ":i5 1 . "
        ":i7 \"1.2\"^^xsd:float . "
    );
}

TEST(testSTRLANG) {
    addTriples(
        ":i1 :LF 1 . "
        ":i1 :LG \"en\" . "

        ":i2 :LF \"abc\" . "
        ":i2 :LG 1 . "

        ":i3 :LF \"def\" . "
        ":i3 :LG \"en\" . "

        ":i4 :LF \"ghi\" . "
        ":i4 :LG \"es\" . "
    );

    ASSERT_QUERY("SELECT ?X ?Z WHERE { ?X :LF ?LF . ?X :LG ?LG . BIND(STRLANG(?LF,?LG) AS ?Z) }",
        ":i3 \"def\"@en . "
        ":i4 \"ghi\"@es . "
    );
}

TEST(testSTRLEN) {
    addTriples(
        ":i1 :R \"abc\" . "
        ":i2 :R \"defg\"@en . "
        ":i3 :R 1 ."
    );

    ASSERT_QUERY("SELECT ?X ?Z WHERE { ?X :R ?Y . BIND(STRLEN(?Y) AS ?Z) }",
        ":i1 3 . "
        ":i2 4 . "
    );
}

TEST(testSUBSTR) {
    addTriples(
        ":i1 :R \"abc\" . "
        ":i2 :R \"defghi\"@en . "
        ":i3 :R 1 . "
    );

    ASSERT_QUERY("SELECT ?X ?Z WHERE { ?X :R ?Y . BIND(SUBSTR(?Y,2) AS ?Z) }",
        ":i1 \"bc\" . "
        ":i2 \"efghi\"@en . "
    );

    ASSERT_QUERY("SELECT ?X ?Z WHERE { ?X :R ?Y . BIND(SUBSTR(?Y,2,1) AS ?Z) }",
        ":i1 \"b\" . "
        ":i2 \"e\"@en . "
    );

    ASSERT_QUERY("SELECT ?X ?Z WHERE { ?X :R ?Y . BIND(SUBSTR(?Y,4) AS ?Z) }",
        ":i1 \"\" . "
        ":i2 \"ghi\"@en . "
    );

    ASSERT_QUERY("SELECT ?X ?Z WHERE { ?X :R ?Y . BIND(SUBSTR(?Y,5) AS ?Z) }",
        ":i2 \"hi\"@en . "
    );

    ASSERT_QUERY("SELECT ?X ?Z WHERE { ?X :R ?Y . BIND(SUBSTR(?Y,4,2) AS ?Z) }",
        ":i2 \"gh\"@en . "
    );
}

TEST(testUCASE_LCASE) {
    addTriples(
        ":i1 :R \"aBc\"@en . "
        ":i2 :R \"DeF\" . "
        ":i3 :R 3 . "
    );

    ASSERT_QUERY("SELECT ?X ?Z WHERE { ?X :R ?Y . BIND(UCASE(?Y) AS ?Z) }",
        ":i1 \"ABC\"@en . "
        ":i2 \"DEF\" . "
    );

    ASSERT_QUERY("SELECT ?X ?Z WHERE { ?X :R ?Y . BIND(LCASE(?Y) AS ?Z) }",
        ":i1 \"abc\"@en . "
        ":i2 \"def\" . "
    );
}

TEST(testSTRSTARTS) {
    addTriples(
        ":i1 :R \"abcdef\"@en . "
        ":i1 :S \"abc\"@en . "

        ":i2 :R \"abcdef\"@en . "
        ":i2 :S \"abc\" . "

        ":i3 :R \"abcdef\" . "
        ":i3 :S \"abc\" . "

        ":i4 :R \"abcdef\"@en . "
        ":i4 :S \"abcg\"@en . "

        ":i5 :R \"abcdef\"@en . "
        ":i5 :S \"abcg\" . "

        ":i6 :R \"abcdef\" . "
        ":i6 :S \"abcg\" . "

        ":i7 :R \"abcdef\" . "
        ":i7 :S \"abc\"@en . "

        ":i8 :R 1 . "
        ":i8 :S \"abc\"@en . "

        ":i9 :R \"abcdef\" . "
        ":i9 :S 1 . "
    );

    ASSERT_QUERY("SELECT ?X ?Z WHERE { ?X :R ?Y1 . ?X :S ?Y2 . BIND(STRSTARTS(?Y1,?Y2) AS ?Z) }",
        ":i1 \"true\"^^xsd:boolean . "
        ":i2 \"true\"^^xsd:boolean . "
        ":i3 \"true\"^^xsd:boolean . "
        ":i4 \"false\"^^xsd:boolean . "
        ":i5 \"false\"^^xsd:boolean . "
        ":i6 \"false\"^^xsd:boolean . "
    );
}

TEST(testSTRENDS) {
    addTriples(
        ":i1 :R \"abcdef\"@en . "
        ":i1 :S \"def\"@en . "

        ":i2 :R \"abcdef\"@en . "
        ":i2 :S \"def\" . "

        ":i3 :R \"abcdef\" . "
        ":i3 :S \"def\" . "

        ":i4 :R \"abcdef\"@en . "
        ":i4 :S \"gdef\"@en . "

        ":i5 :R \"abcdef\"@en . "
        ":i5 :S \"gdef\" . "

        ":i6 :R \"abcdef\" . "
        ":i6 :S \"gdef\" . "

        ":i7 :R \"abcdef\" . "
        ":i7 :S \"def\"@en . "

        ":i8 :R 1 . "
        ":i8 :S \"def\"@en . "

        ":i9 :R \"abcdef\" . "
        ":i9 :S 1 . "
    );

    ASSERT_QUERY("SELECT ?X ?Z WHERE { ?X :R ?Y1 . ?X :S ?Y2 . BIND(STRENDS(?Y1,?Y2) AS ?Z) }",
        ":i1 \"true\"^^xsd:boolean . "
        ":i2 \"true\"^^xsd:boolean . "
        ":i3 \"true\"^^xsd:boolean . "
        ":i4 \"false\"^^xsd:boolean . "
        ":i5 \"false\"^^xsd:boolean . "
        ":i6 \"false\"^^xsd:boolean . "
    );
}

TEST(testCONTAINS) {
    addTriples(
        ":i1 :R \"abcdef\"@en . "
        ":i1 :S \"def\"@en . "

        ":i2 :R \"abcdef\"@en . "
        ":i2 :S \"def\" . "

        ":i3 :R \"abcdef\" . "
        ":i3 :S \"def\" . "

        ":i4 :R \"abcdef\"@en . "
        ":i4 :S \"gdef\"@en . "

        ":i5 :R \"abcdef\"@en . "
        ":i5 :S \"gdef\" . "

        ":i6 :R \"abcdef\" . "
        ":i6 :S \"gdef\" . "

        ":i7 :R \"abcdef\" . "
        ":i7 :S \"def\"@en . "

        ":i8 :R 1 . "
        ":i8 :S \"def\"@en . "

        ":i9 :R \"abcdef\" . "
        ":i9 :S 1 . "
    );

    ASSERT_QUERY("SELECT ?X ?Z WHERE { ?X :R ?Y1 . ?X :S ?Y2 . BIND(CONTAINS(?Y1,?Y2) AS ?Z) }",
        ":i1 \"true\"^^xsd:boolean . "
        ":i2 \"true\"^^xsd:boolean . "
        ":i3 \"true\"^^xsd:boolean . "
        ":i4 \"false\"^^xsd:boolean . "
        ":i5 \"false\"^^xsd:boolean . "
        ":i6 \"false\"^^xsd:boolean . "
    );
}

TEST(testSTRBEFORE) {
    addTriples(
        ":i1 :R \"abcdef\"@en . "
        ":i1 :S \"def\"@en . "

        ":i2 :R \"abcdef\"@en . "
        ":i2 :S \"def\" . "

        ":i3 :R \"abcdef\" . "
        ":i3 :S \"def\" . "

        ":i4 :R \"abcdef\"@en . "
        ":i4 :S \"gdef\"@en . "

        ":i5 :R \"abcdef\"@en . "
        ":i5 :S \"gdef\" . "

        ":i6 :R \"abcdef\" . "
        ":i6 :S \"gdef\" . "

        ":i7 :R \"abcdef\" . "
        ":i7 :S \"def\"@en . "

        ":i8 :R 1 . "
        ":i8 :S \"def\"@en . "

        ":i9 :R \"abcdef\" . "
        ":i9 :S 1 . "
    );

    ASSERT_QUERY("SELECT ?X ?Z WHERE { ?X :R ?Y1 . ?X :S ?Y2 . BIND(STRBEFORE(?Y1,?Y2) AS ?Z) }",
        ":i1 \"abc\"@en . "
        ":i2 \"abc\"@en . "
        ":i3 \"abc\" . "
        ":i4 \"\"@en . "
        ":i5 \"\"@en . "
        ":i6 \"\" . "
    );
}

TEST(testSTRAFTER) {
    addTriples(
        ":i1 :R \"abcdef\"@en . "
        ":i1 :S \"bc\"@en . "

        ":i2 :R \"abcdef\"@en . "
        ":i2 :S \"bc\" . "

        ":i3 :R \"abcdef\" . "
        ":i3 :S \"bc\" . "

        ":i4 :R \"abcdef\"@en . "
        ":i4 :S \"cb\"@en . "

        ":i5 :R \"abcdef\"@en . "
        ":i5 :S \"cb\" . "

        ":i6 :R \"abcdef\" . "
        ":i6 :S \"cb\" . "

        ":i7 :R \"abcdef\" . "
        ":i7 :S \"bc\"@en . "

        ":i8 :R 1 . "
        ":i8 :S \"bc\"@en . "

        ":i9 :R \"abcdef\" . "
        ":i9 :S 1 . "
    );

    ASSERT_QUERY("SELECT ?X ?Z WHERE { ?X :R ?Y1 . ?X :S ?Y2 . BIND(STRAFTER(?Y1,?Y2) AS ?Z) }",
        ":i1 \"def\"@en . "
        ":i2 \"def\"@en . "
        ":i3 \"def\" . "
        ":i4 \"\"@en . "
        ":i5 \"\"@en . "
        ":i6 \"\" . "
    );
}

TEST(testENCODE_FOR_URI) {
    addTriples(
        ":i1 :R \"abc d+e/-f\"@en . "
        ":i2 :R \"abc d+e/-f\" . "
    );

    ASSERT_QUERY("SELECT ?X ?Z WHERE { ?X :R ?Y . BIND(ENCODE_FOR_URI(?Y) AS ?Z) }",
        ":i1 \"abc%20d%2Be%2F-f\"@en . "
        ":i2 \"abc%20d%2Be%2F-f\" . "
    );
}

TEST(testCONCAT) {
    addTriples(
        ":i1 :R \"abc\"@en . "
        ":i1 :S \"def\"@en . "
        ":i1 :T \"ghi\"@en . "

        ":i2 :R \"abc\" . "
        ":i2 :S \"def\"@en . "
        ":i2 :T \"ghi\"@en . "

        ":i3 :R \"abc\"@en . "
        ":i3 :S \"def\"@en . "
        ":i3 :T \"ghi\"@es . "

        ":i4 :R \"abc\"@en . "
        ":i4 :S \"def\"@en . "
        ":i4 :T 1 . "
    );

    ASSERT_QUERY("SELECT ?X ?Z WHERE { ?X :R ?Y1 . ?X :S ?Y2 . ?X :T ?Y3 . BIND(CONCAT(?Y1,?Y2,?Y3) AS ?Z) }",
        ":i1 \"abcdefghi\"@en . "
        ":i2 \"abcdefghi\" . "
        ":i3 \"abcdefghi\" . "
    );
}

TEST(testLangMatches) {
    addTriples(
        ":i1 :R \"en\" . "
        ":i1 :S \"EN\" . "

        ":i2 :R \"en-en\" . "
        ":i2 :S \"en\" . "

        ":i3 :R \"en-en\" . "
        ":i3 :S \"*\" . "

        ":i4 :R \"eng\" . "
        ":i4 :S \"EN\" . "

        ":i5 :R \"en\" . "
        ":i5 :S \"DE\" . "

        ":i6 :R \"en\" . "
        ":i6 :S \"\" . "

        ":i7 :R \"abcdef\"@en . "
        ":i7 :S \"en\" . "
    );

    ASSERT_QUERY("SELECT ?X ?Z WHERE { ?X :R ?Y1 . ?X :S ?Y2 . BIND(langMatches(?Y1,?Y2) AS ?Z) }",
        ":i1 \"true\"^^xsd:boolean . "
        ":i2 \"true\"^^xsd:boolean . "
        ":i3 \"true\"^^xsd:boolean . "
        ":i4 \"false\"^^xsd:boolean . "
        ":i5 \"false\"^^xsd:boolean . "
    );
}

TEST(testNumerics1) {
    addTriples(
        ":i1 :R 11 . "
        ":i2 :R -341 . "
        ":i3 :R \"4.5\"^^xsd:float . "
        ":i4 :R \"-6.5\"^^xsd:float . "
        ":i5 :R \"abc\" . "
    );

    ASSERT_QUERY("SELECT ?X ?Z WHERE { ?X :R ?Y . BIND(ABS(?Y) AS ?Z) }",
        ":i1 11 . "
        ":i2 341 . "
        ":i3 \"4.5\"^^xsd:float . "
        ":i4 \"6.5\"^^xsd:float . "
    );

    ASSERT_QUERY("SELECT ?X ?Z WHERE { ?X :R ?Y . BIND(ROUND(?Y) AS ?Z) }",
        ":i1 11 . "
        ":i2 -341 . "
        ":i3 \"5\"^^xsd:float . "
        ":i4 \"-7\"^^xsd:float . "
    );

    ASSERT_QUERY("SELECT ?X ?Z WHERE { ?X :R ?Y . BIND(CEIL(?Y) AS ?Z) }",
        ":i1 11 . "
        ":i2 -341 . "
        ":i3 \"5\"^^xsd:float . "
        ":i4 \"-6\"^^xsd:float . "
    );

    ASSERT_QUERY("SELECT ?X ?Z WHERE { ?X :R ?Y . BIND(FLOOR(?Y) AS ?Z) }",
        ":i1 11 . "
        ":i2 -341 . "
        ":i3 \"4\"^^xsd:float . "
        ":i4 \"-7\"^^xsd:float . "
    );
}

TEST(testSIN_COS_TAN) {
    addTriples(
        ":i1 :R \"0\"^^xsd:float . "
    );

    ASSERT_QUERY("SELECT ?X ?Z WHERE { ?X :R ?Y . BIND(SIN(?Y) AS ?Z) }",
        ":i1 \"0\"^^xsd:float . "
    );

    ASSERT_QUERY("SELECT ?X ?Z WHERE { ?X :R ?Y . BIND(COS(?Y) AS ?Z) }",
        ":i1 \"1\"^^xsd:float . "
    );

    ASSERT_QUERY("SELECT ?X ?Z WHERE { ?X :R ?Y . BIND(TAN(?Y) AS ?Z) }",
        ":i1 \"0\"^^xsd:float . "
    );

    ASSERT_QUERY("SELECT ?X ?Z WHERE { ?X :R ?Y . BIND(ASIN(?Y) AS ?Z) }",
        ":i1 \"0\"^^xsd:float . "
    );

    ASSERT_QUERY("SELECT ?X ?Z WHERE { ?X :R ?Y . BIND(ATAN(?Y) AS ?Z) }",
        ":i1 \"0\"^^xsd:float . "
    );
}

TEST(testPOW) {
    addTriples(
        ":i1 :R 2 . "
        ":i1 :S 3 . "

        ":i2 :R \"3\"^^xsd:float . "
        ":i2 :S 5 . "

        ":i3 :R \"4\"^^xsd:float . "
        ":i3 :S \"4\"^^xsd:float . "
    );

    ASSERT_QUERY("SELECT ?X ?Z WHERE { ?X :R ?Y1 . ?X :S ?Y2 . BIND(POW(?Y1,?Y2) AS ?Z) }",
        ":i1 \"8\"^^xsd:double . "
        ":i2 \"243\"^^xsd:double . "
        ":i3 \"256\"^^xsd:double . "
    );
}

TEST(testCasting) {
    addTriples(
        ":i1 :R 1 . "
        ":i2 :R \"2.2\"^^xsd:float . "
        ":i3 :R \"true\" . "
        ":i4 :R \"false\" . "
        ":i5 :R \"asd\" . "
        ":i6 :R \"asd\"@en . "
        ":i7 :R <someIRI> . "
        ":i8 :R \"true\"^^xsd:boolean . "
        ":i9 :R \"false\"^^xsd:boolean . "
    );

    ASSERT_QUERY("SELECT ?X ?Z WHERE { ?X :R ?Y . BIND(xsd:boolean(?Y) AS ?Z) }",
        ":i1 \"true\"^^xsd:boolean . "
        ":i2 \"true\"^^xsd:boolean . "
        ":i3 \"true\"^^xsd:boolean . "
        ":i4 \"false\"^^xsd:boolean . "
        ":i8 \"true\"^^xsd:boolean . "
        ":i9 \"false\"^^xsd:boolean . "
    );

    ASSERT_QUERY("SELECT ?X ?Z WHERE { ?X :R ?Y . BIND(xsd:float(?Y) AS ?Z) }",
        ":i1 \"1\"^^xsd:float . "
        ":i2 \"2.2\"^^xsd:float . "
        ":i8 \"1\"^^xsd:float . "
        ":i9 \"0\"^^xsd:float . "
    );

    ASSERT_QUERY("SELECT ?X ?Z WHERE { ?X :R ?Y . BIND(xsd:integer(?Y) AS ?Z) }",
        ":i1 1 . "
        ":i2 2 . "
        ":i8 1 . "
        ":i9 0 . "
    );

    ASSERT_QUERY("SELECT ?X ?Z WHERE { ?X :R ?Y . BIND(xsd:string(?Y) AS ?Z) }",
        ":i1 \"1\" . "
        ":i2 \"2.2\" . "
        ":i3 \"true\" . "
        ":i4 \"false\" . "
        ":i5 \"asd\" . "
        ":i6 \"asd@en\" . "
        ":i7 \"someIRI\" . "
        ":i8 \"true\" . "
        ":i9 \"false\" . "
    );
}

TEST(testDateTimeComparison) {
    addTriples(
        ":i1 :R \"0020-02-04T02:02:02\"^^xsd:dateTime . "
        ":i1 :S \"0022-02-04T02:02:02\"^^xsd:dateTime . "

        ":i2 :R \"0020-02-04T02:02:02\"^^xsd:dateTime . "
        ":i2 :S \"0020-02-04T02:02:02\"^^xsd:dateTime . "

        ":i3 :R \"0020-02-04T02:02:02\"^^xsd:dateTime . "
        ":i3 :S \"0018-02-04T02:02:02\"^^xsd:dateTime . "

        ":i4 :R \"0020-02-04T02:02:02Z\"^^xsd:dateTime . "
        ":i4 :S \"0020-02-04T02:02:02\"^^xsd:dateTime . "
    );

    ASSERT_QUERY("SELECT ?X WHERE { ?X :R ?Y1 . ?X :S ?Y2 . FILTER(?Y1 < ?Y2) }",
        ":i1 . "
    );

    ASSERT_QUERY("SELECT ?X WHERE { ?X :R ?Y1 . ?X :S ?Y2 . FILTER(?Y1 <= ?Y2) }",
        ":i1 . "
        ":i2 . "
    );

    ASSERT_QUERY("SELECT ?X WHERE { ?X :R ?Y1 . ?X :S ?Y2 . FILTER(?Y1 > ?Y2) }",
        ":i3 . "
    );

    ASSERT_QUERY("SELECT ?X WHERE { ?X :R ?Y1 . ?X :S ?Y2 . FILTER(?Y1 >= ?Y2) }",
        ":i2 . "
        ":i3 . "
    );

    ASSERT_QUERY("SELECT ?X WHERE { ?X :R ?Y1 . ?X :S ?Y2 . FILTER(?Y1 = ?Y2) }",
        ":i2 . "
    );

    ASSERT_QUERY("SELECT ?X WHERE { ?X :R ?Y1 . ?X :S ?Y2 . FILTER(?Y1 != ?Y2) }",
        ":i1 . "
        ":i3 . "
        ":i4 . "
    );
}

TEST(testDateTimeArithmetic) {
    addTriples(
        ":i1 :R \"0020-02-04T02:02:02\"^^xsd:dateTime . "
        ":i1 :S \"P2M\"^^xsd:duration . "

        ":i2 :R \"0020-04-04T02:02:02\"^^xsd:dateTime . "
        ":i2 :S \"P3Y\"^^xsd:duration . "
    );

    ASSERT_QUERY("SELECT ?X ?Z WHERE { ?X :R ?Y1 . ?X :S ?Y2 . BIND(?Y1 + ?Y2 AS ?Z) }",
        ":i1 \"0020-04-04T02:02:02\"^^xsd:dateTime . "
        ":i2 \"0023-04-04T02:02:02\"^^xsd:dateTime . "
    );
}

TEST(testDurationComparison) {
    addTriples(
        ":i1 :R \"P3M\"^^xsd:duration . "
        ":i1 :S \"P3MT0.001S\"^^xsd:duration . "

        ":i2 :R \"-P3M\"^^xsd:duration . "
        ":i2 :S \"-P3M\"^^xsd:duration . "

        ":i3 :R \"-P3M\"^^xsd:duration . "
        ":i3 :S \"-P3MT0.001S\"^^xsd:duration . "

        ":i4 :R \"P1M\"^^xsd:duration . "
        ":i4 :S \"P30D\"^^xsd:duration . "
    );
    
    ASSERT_QUERY("SELECT ?X WHERE { ?X :R ?Y1 . ?X :S ?Y2 . FILTER(?Y1 < ?Y2) }",
        ":i1 . "
    );

    ASSERT_QUERY("SELECT ?X WHERE { ?X :R ?Y1 . ?X :S ?Y2 . FILTER(?Y1 <= ?Y2) }",
        ":i1 . "
        ":i2 . "
    );

    ASSERT_QUERY("SELECT ?X WHERE { ?X :R ?Y1 . ?X :S ?Y2 . FILTER(?Y1 > ?Y2) }",
        ":i3 . "
    );

    ASSERT_QUERY("SELECT ?X WHERE { ?X :R ?Y1 . ?X :S ?Y2 . FILTER(?Y1 >= ?Y2) }",
        ":i2 . "
        ":i3 . "
    );

    ASSERT_QUERY("SELECT ?X WHERE { ?X :R ?Y1 . ?X :S ?Y2 . FILTER(?Y1 = ?Y2) }",
        ":i2 . "
    );

    ASSERT_QUERY("SELECT ?X WHERE { ?X :R ?Y1 . ?X :S ?Y2 . FILTER(?Y1 != ?Y2) }",
        ":i1 . "
        ":i3 . "
        ":i4 . "
    );
}

TEST(testDateTimeFunctions) {
    addTriples(
        ":i1 :R \"0020-01-02T03:04:05\"^^xsd:dateTime . "
        ":i2 :R \"1020-10-11T12:13:14-05:45\"^^xsd:dateTime . "
        ":i3 :R \"1020-10-11T12:13:14Z\"^^xsd:dateTime . "
    );

    ASSERT_QUERY(
        "SELECT ?X ?Y ?M ?D ?H ?I ?S ?T WHERE { "
        "?X :R ?DT . "
        "BIND(YEAR(?DT) AS ?Y) . "
        "BIND(MONTH(?DT) AS ?M) . "
        "BIND(DAY(?DT) AS ?D) . "
        "BIND(HOURS(?DT) AS ?H) . "
        "BIND(MINUTES(?DT) AS ?I) . "
        "BIND(SECONDS(?DT) AS ?S) . "
        "BIND(TZ(?DT) AS ?T) . "
        "}",

        ":i1   20  1  2  3  4  5 \"\" . "
        ":i2 1020 10 11 12 13 14 \"-05:45\" . "
        ":i3 1020 10 11 12 13 14 \"Z\" . "
    );

    ASSERT_QUERY(
        "SELECT ?X ?T WHERE { "
        "?X :R ?DT . "
        "BIND(TIMEZONE(?DT) AS ?T) . "
        "}",

        ":i2 \"-PT5H45M\"^^xsd:duration . "
        ":i3 \"P\"^^xsd:duration . "
    );
}

TEST(testFunctionInAnswer) {
    addTriples(
        ":a1 :R \"val-a1\" . "
        ":a2 :R \"val-a2\" . "
    );

    ASSERT_QUERY("SELECT \"const\" (CONCAT(?Y, \"-suff\") AS ?Z) WHERE { ?X :R ?Y }",
        "\"const\" \"val-a1-suff\" . "
        "\"const\" \"val-a2-suff\" . "
    );

    ASSERT_QUERY("SELECT \"const\" CONCAT(?Y, \"-suff\") WHERE { ?X :R ?Y }",
        "\"const\" \"val-a1-suff\" . "
        "\"const\" \"val-a2-suff\" . "
    );
}

TEST(testSkolem) {
    addTriples(
        ":i1 :R _:bn . "
        ":i2 :R \"abc\" . "
        ":i3 :R :ind . "
    );

    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X :R ?Y . BIND(SKOLEM(\"f\",?X,?Y) AS ?Z) }",
        ":i1 _:bn _:f_6_8 . "
        ":i2 \"abc\" _:f_9_10 . "
        ":i3 :ind _:f_11_12 . "
    );

    ASSERT_QUERY("SELECT ?X ?Y ?Z WHERE { ?X :R ?Y . BIND(SKOLEM(?Y,?X) AS ?Z) }",
        ":i2 \"abc\" _:abc_9 . "
    );
}

TEST(testDateTimeCreation) {
    addTriples(
        ":a1 :year 2016 . "
        ":a1 :month 4 . "
        ":a1 :day 1 . "
        ":a1 :hour 10 . "
        ":a1 :minute 30 . "
        ":a1 :second 15 . "
        ":a1 :tz 0 . "
    );

    ASSERT_QUERY("SELECT ?X (dateTime(?Y, ?M, ?D, ?H, ?I, ?S, ?TZ) AS ?DT) WHERE { ?X :year ?Y . ?X :month ?M . ?X :day ?D . ?X :hour ?H . ?X :minute ?I . ?X :second ?S . ?X :tz ?TZ }",
        ":a1 \"2016-04-01T10:30:15Z\"^^xsd:dateTime . "
    );
    ASSERT_QUERY("SELECT ?X (dateTime(?Y, ?M, ?D, ?H, ?I, ?S) AS ?DT) WHERE { ?X :year ?Y . ?X :month ?M . ?X :day ?D . ?X :hour ?H . ?X :minute ?I . ?X :second ?S }",
        ":a1 \"2016-04-01T10:30:15\"^^xsd:dateTime . "
    );
}

#endif
