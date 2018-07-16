// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifdef WITH_TEST

#define AUTO_TEST   SPARQLParserTest

#include <CppTest/AutoTest.h>

#include "../../src/RDFStoreException.h"
#include "../../src/formats/turtle/SPARQLParser.h"
#include "../../src/util/Vocabulary.h"
#include "../logic/AbstractLogicTest.h"

class SPARQLParserTest : public AbstractLogicTest {

protected:

    void assertQuery(const Query expectedQuery, const char* const queryText) {
        Prefixes prefixes;
        prefixes.declareStandardPrefixes();
        SPARQLParser parser(prefixes);
        Query actualQuery = parser.parse(expectedQuery->getFactory(), queryText, ::strlen(queryText));
        ASSERT_EQUAL(expectedQuery, actualQuery);
    }

    void assertQuery(const Query expectedQuery, const std::vector<Atom>& expectedConstructPattern, const SPARQLQueryType expectedSPARQLQueryType, const char* const queryText) {
        std::vector<Atom> actualConstructPattern;
        SPARQLQueryType actualSPARQLQueryType;
        Prefixes prefixes;
        prefixes.declareStandardPrefixes();
        SPARQLParser parser(prefixes);
        Query actualQuery = parser.parse(expectedQuery->getFactory(), queryText, ::strlen(queryText), actualConstructPattern, actualSPARQLQueryType);
        ASSERT_EQUAL(expectedQuery, actualQuery);
        ASSERT_COLLECTIONS_EQUAL(expectedConstructPattern, actualConstructPattern);
        ASSERT_EQUAL(expectedSPARQLQueryType, actualSPARQLQueryType);
    }

    void assertInvalidQuery(const char* const queryText) {
        Prefixes prefixes;
        prefixes.declareStandardPrefixes();
        SPARQLParser parser(prefixes);
        try {
            parser.parse(m_factory, queryText, ::strlen(queryText));
            FAIL2("Query should not be parsable");
        }
        catch (const RDFStoreException&) {
        }
    }

};

TEST(testIRIvsLessThan) {
    assertQuery(
        Q(
            false,
            TA() << V("X"),
            FA() << RA("?X", "abc", "?Y") << FLT(
                BBFC("internal$less-than", V("X"), I("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"))
            )
        ),
        "SELECT ?X WHERE { ?X <abc> ?Y . FILTER ?X<rdf:type }"
    );
}

TEST(testBind) {
    assertQuery(
        Q(
            false,
            TA() << V("X") << V("Y"),
            FA() << RA("?X", "abc", "?Y") << BND(
                BBFC("internal$equal", V("X"), V("Y")), V("Z")
            ) << RA("?Z", "def", "ghi")
        ),
        "SELECT ?X ?Y WHERE { ?X <abc> ?Y . BIND (?X = ?Y AS ?Z) ?Z <def> <ghi>}"
    );

    assertQuery(
        Q(
            false,
            TA() << V("X") << V("Y"),
            FA() << RA("?X", "abc", "?Y") << BND(
                BBFC("internal$equal",
                    V("X"),
                    BBFC("internal$logical-or",
                        V("Y"),
                        V("W")
                    )
                ),
                V("Z")
            ) << RA("?Z", "def", "ghi")
        ),
        "SELECT ?X ?Y WHERE { ?X <abc> ?Y . BIND (?X=(?Y||?W)AS?Z). ?Z <def> <ghi>}"
    );
}

TEST(testFilter) {
    assertQuery(
        Q(
            false,
            TA() << V("X") << V("Y"),
            FA() << RA("?X", "abc", "?Y") << FLT(
                BBFC("internal$logical-or",
                    BBFC("internal$equal", V("X"), V("Y")),
                    BBFC("internal$logical-and",
                        BBFC("internal$not-equal", V("Y"), V("X")),
                        UBFC("internal$logical-not", V("Z"))
                    )
                )
            )
        ),
        "SELECT ?X ?Y WHERE { ?X <abc> ?Y . FILTER ?X = ?Y || ?Y != ?X && !?Z }"
    );

    assertQuery(
        Q(
            false,
            TA() << V("X") << V("Y"),
            FA() << RA("?X", "abc", "?Y") << FLT(
                BBFC("internal$logical-and",
                    BBFC("internal$logical-or",
                        BBFC("internal$equal", V("X"), V("Y")),
                        BBFC("internal$not-equal", V("Y"), V("X"))
                    ),
                    UBFC("internal$logical-not", V("Z"))
                )
            )
        ),
        "SELECT ?X ?Y WHERE { ?X <abc> ?Y . FILTER (?X = ?Y || ?Y != ?X) && !?Z }"
    );

    assertQuery(
        Q(
            false,
            TA() << V("X") << V("Y"),
            FA() << RA("?X", "abc", "?Y") << FLT(
                BBFC("internal$logical-or",
                    BBFC("internal$equal",
                        UBFC("STRLEN", V("X")),
                        V("Y")
                    ),
                    UBFC("internal$logical-not", V("Z"))
                )
            )
        ),
        "SELECT ?X ?Y WHERE { ?X <abc> ?Y . FILTER (STRLEN(?X) = ?Y || !?Z) }"
    );
}

TEST(testSimpleQuery) {
    assertQuery(
        Q(
            false,
            TA() << V("X") << V("Y"),
            FA() << RA("?X", "abc", "?Y") << RA("?Y", "def", "?X")
        ),
        "SELECT ?X ?Y WHERE { ?X <abc> ?Y . ?Y <def> ?X }"
    );
}

TEST(testNestedJoin) {
    assertQuery(
        Q(
            false,
            TA() << V("X") << V("W"),
            AND(
                RA("?X", "r", "?Y"),
                AND(
                    RA("?Y", "s", "?Z"),
                    RA("?Z", "t", "?W")
                )
            )
        ),
        "SELECT ?X ?W WHERE { ?X <r> ?Y { ?Y <s> ?Z . ?Z <t> ?W } }"
    );
}

TEST(testAllVariables) {
    assertQuery(
        Q(
            true,
            TA() << V("X"),
            RA("?X", "abc", "def")
        ),
        "SELECT DISTINCT * WHERE { ?X <abc> <def> }"
    );
}

TEST(testBoolean) {
    // The SPARQL standard does not support Boolean queries, but we'll support them nonetheless.
    assertQuery(
        Q(
            false,
            TA(),
            FA() << RA("?X", "abc", "?Y") << RA("?Y", "def", "?X")
        ),
        "SELECT WHERE { ?X <abc> ?Y . ?Y <def> ?X }"
    );
}

TEST(testSingleAtom) {
    assertQuery(
        Q(
            true,
            TA() << V("X"),
            RA("?X", "abc", "?Y")
        ),
        "SELECT DISTINCT ?X WHERE { ?X <abc> ?Y . }"
    );
}

TEST(testPrefixes) {
    assertQuery(
        Q(
            false,
            TA() << V("X"),
            RA("default:abc", "a:abc", "prefixA:def")
        ),
        "PREFIX : <default:>"
        "PREFIX a: <prefixA:>"
        "SELECT ?X { :abc <a:abc> a:def . }"
    );
}

TEST(testList) {
    assertQuery(
        Q(
            false,
            TA() << V("X") << V("Y") << V("Z"),
            FA()
                << RA("?X", "abc", "?Y")
                << A(V("X"), I("abc"), L("qwe", std::string(XSD_NS) + "string"))
                << RA("?X", "def", "?Z")
        ),
        "SELECT ?X ?Y ?Z { ?X <abc> ?Y , \"qwe\" ; <def> ?Z . }"
    );
}

TEST(testMalformedTriple) {
    assertInvalidQuery("SELECT ?S ?O WHERE { ?S ?O . } ");
}

TEST(testOptional) {
    assertQuery(
        Q(
            false,
            TA() << V("X") << V("Y") << V("Z"),
            OPT(
                RA("?X", "abc", "?Y"),
                RA("?X", "def", "?Z")
            )
        ),
        "SELECT ?X ?Y ?Z { ?X <abc> ?Y . OPTIONAL { ?X <def> ?Z } }"
    );

    assertQuery(
        Q(
            false,
            TA() << V("X") << V("Y") << V("Z") << V("W"),
            OPT(
                AND(FA() << RA("?X", "abc", "?Y") << RA("?X", "ghi", "qwe")),
                AND(FA() << RA("?X", "def", "?Z") << RA("?X", "jkl", "?W"))
            )
        ),
        "SELECT ?X ?Y ?Z ?W { ?X <abc> ?Y . ?X <ghi> <qwe> . OPTIONAL { ?X <def> ?Z . ?X <jkl> ?W } }"
    );

    assertQuery(
        Q(
            false,
            TA() << V("X") << V("Y") << V("Z") << V("W"),
            OPT(
                AND(FA() << RA("?X", "abc", "?Y") << RA("?X", "ghi", "qwe")),
                FA()
                    << RA("?X", "def", "?Z")
                    << RA("?X", "jkl", "?W")
            )
        ),
        "SELECT ?X ?Y ?Z ?W { ?X <abc> ?Y . ?X <ghi> <qwe> . OPTIONAL { ?X <def> ?Z } . OPTIONAL { ?X <jkl> ?W } }"
    );

    assertQuery(
        Q(
            false,
            TA() << V("X") << V("Y") << V("Z") << V("W"),
            OPT(
                AND(FA() << RA("?X", "abc", "?Y") << RA("?X", "ghi", "qwe")),
                FA()
                    << OPT(RA("?X", "def", "?Z"), RA("?Z", "jkl", "?V"))
                    << RA("?X", "jkl", "?W")
            )
        ),
        "SELECT ?X ?Y ?Z ?W { ?X <abc> ?Y . ?X <ghi> <qwe> . OPTIONAL { ?X <def> ?Z . OPTIONAL { ?Z <jkl> ?V } } . OPTIONAL { ?X <jkl> ?W } }"
    );
}

TEST(testNestedOptional) {
    Query expectedQuery =
        Q(
            false,
            TA() << V("X") << V("Y"),
            OPT(
                OPT(
                    RA("?X", "type", "A"),
                    RA("?X", "R", "?Y")
                ),
                OPT(
                    RA("?X", "type", "A"),
                    RA("?X", "S", "?Y")
                )
            )
        );

    assertQuery(expectedQuery,
        "SELECT ?X ?Y { { ?X <type> <A> . OPTIONAL { ?X <R> ?Y } } OPTIONAL { ?X <type> <A> . OPTIONAL { ?X <S> ?Y } } }"
    );
    assertQuery(expectedQuery,
        "SELECT ?X ?Y { { ?X <type> <A> OPTIONAL { ?X <R> ?Y } } . OPTIONAL { ?X <type> <A> OPTIONAL { ?X <S> ?Y } } }"
    );
}

TEST(testTwoOptionals) {
    assertQuery(
        Q(
            false,
            TA() << V("X") << V("Y"),
            OPT(
                RA("?X", "type", "A"),
                FA()
                    << AND(FA()
                        << RA("?X", "R", "?Y")
                        << RA("?X", "S", "?Y")
                    )
                    << RA("?X", "T", "?Y")
            )
        ),
        "SELECT ?X ?Y { ?X <type> <A> . OPTIONAL { ?X <R> ?Y . ?X <S> ?Y . } OPTIONAL { ?X <T> ?Y }  }"
    );
}

TEST(testEmptyMainInOptional) {
    assertQuery(
        Q(
            false,
            TA() << V("X") << V("Y"),
            OPT(
                AND(FA()),
                FA()
                    << AND(FA()
                        << RA("?X", "R", "?Y")
                        << RA("?X", "S", "?Y")
                    )
                    << RA("?X", "T", "?Y")
            )
        ),
        "SELECT ?X ?Y { OPTIONAL { ?X <R> ?Y . ?X <S> ?Y . } OPTIONAL { ?X <T> ?Y }  }"
    );
}

TEST(testPostponedBuiltins) {
    assertQuery(
        Q(
            false,
            TA() << V("X") << V("Y"),
            AND(FA()
                << OPT(
                    RA("?X", "R", "V"),
                    RA("?X", "T", "?Y")
                )
                << FLT(
                    BBFC("internal$equal", V("Y"), L("2", "http://www.w3.org/2001/XMLSchema#integer"))
                )
            )
        ),
        "SELECT ?X ?Y { ?X <R> <V> FILTER(?Y = 2) OPTIONAL { ?X <T> ?Y }  }"
    );
}

TEST(testOptionalMinusFilterCombination) {
    assertQuery(
        Q(
            false,
            TA() << V("X") << V("Y"),
            AND(FA()
                << MINUS(
                    OPT(
                        MINUS(
                            OPT(
                                RA("?X", "R", "V"),
                                RA("?X", "T", "?Y")
                            ),
                            RA("?X", "Z", "A")
                        ),
                        FA()
                            << RA("?X", "V", "?Y")
                            << RA("?X", "W", "?Y")
                    ),
                    RA("?X", "Q", "B")
                )
                << FLT(
                    BBFC("internal$equal", V("X"), L("2", "http://www.w3.org/2001/XMLSchema#integer"))
                )
            )
        ),
        "SELECT ?X ?Y { ?X <R> <V> OPTIONAL { ?X <T> ?Y } MINUS { ?X <Z> <A> } FILTER(?X = 2) OPTIONAL { ?X <V> ?Y } OPTIONAL { ?X <W> ?Y } MINUS { ?X <Q> <B> } }"
    );
}

TEST(testNestedSelect) {
    assertQuery(
        Q(
            false,
            TA() << V("X") << V("Y"),
            Q(
                true,
                TA() << V("X") << V("Y"),
                RA("?X", "R", "?Y")
            )
        ),
        "SELECT ?X ?Y { SELECT DISTINCT ?X ?Y WHERE { ?X <R> ?Y } }"
    );
}

TEST(testNestedSelectUnion) {
    assertQuery(
        Q(
            false,
            TA() << V("X") << V("Y") << V("Z"),
            OR(
                Q(
                    true,
                    TA() << V("X") << V("Y"),
                    RA("?X", "R", "?Y")
                ),
                Q(
                    false,
                    TA() << V("X") << V("Z"),
                    RA("?X", "S", "?Z")
                )
            )
        ),
        "SELECT ?X ?Y ?Z { { SELECT DISTINCT ?X ?Y WHERE { ?X <R> ?Y } } UNION { SELECT ?X ?Z WHERE { ?X <S> ?Z } } }"
    );
}

TEST(testBaiscUnion) {
    assertQuery(
        Q(
            false,
            TA() << V("X") << V("Y"),
            AND(FA()
                << RA("a", "?X", "C")
                << OR(FA()
                    << RA("?X", "type", "A")
                    << AND(
                        RA("?X", "R", "?Y"),
                        RA("?Y", "?X", "P")
                    )
                    << RA("?Y", "S", "?Z")
                )
                << RA("b", "?Y", "E")
            )
        ),
        "SELECT ?X ?Y WHERE { <a> ?X <C> . { ?X <type> <A> } UNION { ?X <R> ?Y . ?Y ?X <P> } UNION { ?Y <S> ?Z } . <b> ?Y <E> }"
    );
}

TEST(testValuesSingleVariable) {
    std::vector<std::vector<GroundTerm> > data;
    data.push_back(
        GTA() << UNDEF()
    );
    data.push_back(
        GTA() << L("2", "http://www.w3.org/2001/XMLSchema#integer")
    );
    data.push_back(
        GTA() << L("abc", "http://www.w3.org/2001/XMLSchema#string")
    );
    assertQuery(
        Q(
            false,
            TA() << V("X"),
            VALUES(
                VA() << V("X"),
                data
            )
        ),
        "SELECT ?X WHERE { VALUES ?X { UNDEF 2 \"abc\" } }"
    );
}

TEST(testValuesMultipleVariables) {
    std::vector<std::vector<GroundTerm> > data;
    data.push_back(
        GTA() << UNDEF() << L("2", "http://www.w3.org/2001/XMLSchema#integer")
    );
    data.push_back(
        GTA() << L("3", "http://www.w3.org/2001/XMLSchema#integer") << UNDEF()
    );
    data.push_back(
        GTA() << L("abc", "http://www.w3.org/2001/XMLSchema#string") << L("def", "http://www.w3.org/2001/XMLSchema#string")
    );
    assertQuery(
        Q(
            false,
            TA() << V("X") << V("Y"),
            VALUES(
                VA() << V("X") << V("Y"),
                data
            )
        ),
        "SELECT ?X ?Y WHERE { VALUES (?X ?Y) { (UNDEF 2) (3 UNDEF) (\"abc\" \"def\") } }"
    );
}

TEST(testExistenceExpression) {
    assertQuery(
        Q(
            false,
            TA() << V("X") << V("Y"),
            FA() << RA("?X", "abc", "?Y") << FLT(
                BBFC("internal$logical-or",
                    BBFC("internal$equal", V("X"), V("Y")),
                    EXISTS(
                        AND(FA()
                            << RA("?X", "type", "A")
                            << RA("?Y", "type", "B")
                        )
                    )
                )
            )
        ),
        "SELECT ?X ?Y WHERE { ?X <abc> ?Y . FILTER ?X = ?Y || EXISTS { ?X <type> <A> . ?Y <type> <B> } }"
    );

    assertQuery(
        Q(
            false,
            TA() << V("X") << V("Y"),
            FA() << RA("?X", "abc", "?Y") << FLT(
                BBFC("internal$logical-or",
                    BBFC("internal$equal", V("X"), V("Y")),
                    NOT_EXISTS(
                        AND(FA()
                            << RA("?X", "type", "A")
                            << RA("?Y", "type", "B")
                        )
                    )
                )
            )
        ),
        "SELECT ?X ?Y WHERE { ?X <abc> ?Y . FILTER ?X = ?Y || NOT EXISTS { ?X <type> <A> . ?Y <type> <B> } }"
    );
}

TEST(testFunctionInAnswer) {
    assertQuery(
        Q(
            false,
            TA() << V("anonymous_var$0") << V("anonymous_var$1"),
            FA()
                << RA("?X", "abc", "?Y")
                << BND(
                    BBFC("internal$add", V("X"), V("Y")),
                    V("anonymous_var$0")
                )
                << BND(
                    BBFC("internal$multiply", V("X"), V("Y")),
                    V("anonymous_var$1")
                )
        ),
        "SELECT ?X + ?Y ?X * ?Y WHERE { ?X <abc> ?Y }"
    );

    assertQuery(
        Q(
            false,
            TA() << V("Z") << V("W"),
            FA()
                << RA("?X", "abc", "?Y")
                << BND(
                    BBFC("internal$add", V("X"), V("Y")),
                    V("Z")
                )
                << BND(
                    BBFC("internal$multiply", V("X"), V("Y")),
                    V("W")
                )
        ),
        "SELECT (?X + ?Y AS ?Z) (?X * ?Y AS ?W) WHERE { ?X <abc> ?Y }"
    );

    assertInvalidQuery("SELECT (?X + 2 AS ?Y) WHERE { ?X <abc> ?Y }");
}

TEST(testAtomNegation) {
    assertQuery(
        Q(
            false,
            TA() << V("X") << V("Y") << V("Z") << V("W"),
            FA()
                << RA("?X", "abc", "?Y")
                << NOT(RA("?X", "def", "?Z"))
                << RA("?Z", "ghi", "?W")
            ),
        "SELECT ?X ?Y ?Z ?W { ?X <abc> ?Y . not ?X <def> ?Z . ?Z <ghi> ?W }"
    );
}

TEST(testAskQuery) {
    assertQuery(
        Q(
            true,
            TA(),
            RA("?X", "abc", "?Y")
        ),
        AA(),
        SPARQL_ASK_QUERY,
        "ASK WHERE { ?X <abc> ?Y }"
    );
}

TEST(testConstructQuery) {
    assertQuery(
        Q(
            false,
            TA() << V("X") << V("Y"),
            RA("?X", "abc", "?Y")
        ),
        AA() << RA("?X", "R1", "A") << RA("?Y", "R2", "B"),
        SPARQL_CONSTRUCT_QUERY,
        "CONSTRUCT { ?X <R1> <A> . ?Y <R2> <B> } WHERE { ?X <abc> ?Y }"
    );
}

TEST(testConstructWhereQuery) {
    assertQuery(
        Q(
            false,
            TA() << V("X") << V("Y"),
            FA() << RA("?X", "abc", "?Y") << FLT(
                BBFC("internal$less-than", V("X"), I("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"))
            )
        ),
        AA() << RA("?X", "abc", "?Y"),
        SPARQL_CONSTRUCT_QUERY,
        "CONSTRUCT WHERE { ?X <abc> ?Y . FILTER ?X<rdf:type }"
    );
}

#endif
