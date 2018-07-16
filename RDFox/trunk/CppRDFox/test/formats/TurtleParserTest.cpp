// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifdef WITH_TEST

#define AUTO_TEST   TurtleParserTest

#include <CppTest/AutoTest.h>

#include "../../src/formats/sources/MemorySource.h"
#include "../../src/formats/turtle/TurtleParser.h"
#include "AbstractParserTest.h"

class TurtleParserTest : public AbstractParserTest {

protected:

    void assertTriples(const char* const string, bool expectedHasTriG = false, bool expectedHasQuads = false) {
        startTest();
        Prefixes prefixes;
        MemorySource memorySource(string, ::strlen(string));
        TurtleParser turtleParser(prefixes);
        bool hasTurtle;
        bool hasTriG;
        bool hasQuads;
        turtleParser.parse(memorySource, *this, hasTurtle, hasTriG, hasQuads);
        ASSERT_EQUAL(expectedHasTriG, hasTriG);
        ASSERT_EQUAL(expectedHasQuads, hasQuads);
        endTest();
    }

};

TEST(testBasic) {
    RT("ob0", "ob0", "ob0");

    RT("ob1", "ob2", "ob3");
    RT("ob1", "ob2", "ob4");
    RT("ob1", "ob2", "ob5");
    RT("ob1", "ob3", "ob4");
    RT("ob1", "ob4", "ob4");
    RT("ob1", "ob4", "ob5");

    RT("ob2", "ob3", "ob4");

    add("http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", MY + "Class");

    LT("ob1", "p", "ghe", XSD + "string");
    LT("ob1", "p", "ghe@en-US", RDF + "PlainLiteral");
    LT("ob1", "p", "123", XSD + "integer");

    assertTriples(
        "@prefix   : <http://my.com/local#>.\n"
        "@prefix my: <http://my.com/local#>.\n"

        ":ob0 :ob0 :ob0 .\n"

        ":ob1 :ob2 :ob3,\n"
        "          :ob4,\n"
        "          :ob5;\n"
        "     :ob3 :ob4;\n"
        "     :ob4 :ob4,\n"
        "          :ob5 .\n"

        ":ob2 :ob3 :ob4;.\n"

        "a a my:Class .\n"

        "@prefix xsd: <http://www.w3.org/2001/XMLSchema#>.\n"

        "my:ob1 my:p \"ghe\"."
        "my:ob1 my:p \"ghe\"@en-US ;"
        "         :p 123 ."
    );
}

TEST(testListSimple) {
    add(MY + "ob1", MY + "p", RDF + "nil");

    add("_:__bnode_1", RDF + "first", MY + "val1");
    add("_:__bnode_1", RDF + "rest", "_:__bnode_2");
    add("_:__bnode_2", RDF + "first", "val2", XSD + "string");
    add("_:__bnode_2", RDF + "rest", "_:__bnode_3");
    add("_:__bnode_3", RDF + "first", MY + "val3");
    add("_:__bnode_3", RDF + "rest", RDF + "nil");
    add(MY + "ob2", MY + "p", "_:__bnode_1");

    assertTriples(
        "@prefix   : <http://my.com/local#>.\n"

        ":ob1 :p ().\n"

        ":ob2 :p (:val1 \"val2\" :val3).\n"
    );
}

TEST(testListDouble) {
    // outer LHS list
    add("_:__bnode_1", RDF + "first", MY + "val1");
    // inner LHS list: the head is _:__bnode_2
    add("_:__bnode_2", RDF + "first", MY + "val2");
    add("_:__bnode_2", RDF + "rest", "_:__bnode_3");
    add("_:__bnode_3", RDF + "first", MY + "val3");
    add("_:__bnode_3", RDF + "rest", RDF + "nil");
    // continue the outer LHS list
    add("_:__bnode_1", RDF + "rest", "_:__bnode_4"); // _:__bnode_4 is the second node in the outer LHS list
    add("_:__bnode_4", RDF + "first", "_:__bnode_2"); // the cargo of _:__bnode_4 is _:__bnode_2
    add("_:__bnode_4", RDF + "rest", "_:__bnode_5"); // the remainder of the outer LHS list follows
    add("_:__bnode_5", RDF + "first", MY + "val4");
    add("_:__bnode_5", RDF + "rest", RDF + "nil");

    // the RHS list
    add("_:__bnode_6", RDF + "first", MY + "val5");
    add("_:__bnode_6", RDF + "rest", "_:__bnode_7");
    add("_:__bnode_7", RDF + "first", MY + "val6");
    add("_:__bnode_7", RDF + "rest", RDF + "nil");

    // the main triple
    add("_:__bnode_1", MY + "p", "_:__bnode_6");

    assertTriples(
        "@prefix   : <http://my.com/local#>.\n"

        "(:val1 (:val2 :val3) :val4) :p (:val5 :val6).\n"
    );
}

TEST(testNestedObject) {
    add("_:__bnode_1", MY + "p1", MY + "val11");
    add("_:__bnode_1", MY + "p1", MY + "val12");
    add("_:__bnode_1", MY + "p2", MY + "val2");

    add("_:__bnode_2", MY + "p4", MY + "val4");

    add("_:__bnode_1", MY + "p3", "_:__bnode_2");

    add(MY + "ob", MY + "p", "_:__bnode_1");

    assertTriples(
        "@prefix   : <http://my.com/local#>.\n"

        ":ob :p [:p1 :val11, :val12; :p2 :val2; :p3 [:p4 :val4];].\n"
    );
}

TEST(testEscapedString) {
    add(MY + "ob", MY + "p", "\"Still_Life\"_(American_Concert_1981)", XSD + "string");
    add(MY + "ob", MY + "p", "Rewind_(1971-1984)", XSD + "string");

    assertTriples(
        "@prefix   : <http://my.com/local#>.\n"

        ":ob :p \"\\\"Still_Life\\\"_(American_Concert_1981)\" , \"Rewind_(1971-1984)\".\n"
    );
}

TEST(testBlankNodePropertyList) {
    add("_:__bnode_1", MY + "p1", MY + "val11");
    add("_:__bnode_1", MY + "p1", MY + "val12");
    add("_:__bnode_1", MY + "p2", MY + "val2");

    assertTriples(
        "@prefix   : <http://my.com/local#>.\n"

        "[:p1 :val11, :val12 ; :p2 :val2].\n"
    );
}

TEST(testQuads) {
    RT("s0", "p0", "o0");
    RT("s0", "p1", "o1");
    RT("s2", "p2", "o2");
    RT("s2", "p2", "o3");
    RT("s4", "p4", "o4");
    RT("s4", "p4", "o5");

    assertTriples(
        "@prefix   : <http://my.com/local#>.\n"
        "@prefix my: <http://my.com/local#>.\n"

        ":s0 :p0 :o0 ;\n"
        "    :p1 :o1 :ctx1 .\n"
        ":s2 :p2 :o2 ,\n"
        "        :o3 :ctx2 .\n"
        ":s4 :p4 :o4 ,\n"
        "        :o5 .\n",
        false, true
    );
}

TEST(testTriG) {
    RT("s0", "p0", "o0");
    RT("s0", "p1", "o1");
    RT("s2", "p2", "o2");
    RT("s2", "p2", "o3");
    RT("s4", "p4", "o4");
    RT("s4", "p4", "o5");
    RT("s6", "p6", "o6");
    add("_:__bnode_1", MY + "p7", MY + "o7");

    assertTriples(
        "@prefix   : <http://my.com/local#>.\n"
        "@prefix my: <http://my.com/local#>.\n"

        "GRAPH :g1 {\n"
        "    :s0 :p0 :o0 ;\n"
        "        :p1 :o1 .\n"
        "}\n"
        ":s2 :p2 :o2 ,\n"
        "        :o3 .\n"
        "{\n"
        "    :s4 :p4 :o4 , \n"
        "            :o5 .\n"
        "}\n"
        ":g2 {\n"
        "    :s6 :p6 :o6 .\n"
        "}\n"
        ":g3 {\n"
        "    [:p7 :o7] .\n"
        "}\n",
        true, false
    );
}

#endif
