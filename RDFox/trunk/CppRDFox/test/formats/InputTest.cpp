// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifdef WITH_TEST

#define AUTO_TEST   InputTest

#include <CppTest/AutoTest.h>

#include "../../src/logic/Logic.h"
#include "../../src/formats/InputOutput.h"
#include "../../src/formats/sources/MemorySource.h"
#include "../../src/util/Prefixes.h"
#include "AbstractParserTest.h"

class InputTest : public AbstractParserTest {

protected:

    void assertInput(const char* const string, const std::string& expectedFormatName) {
        startTest();
        Prefixes prefixes;
        MemorySource memorySource(string, ::strlen(string));
        std::string formatName;
        LogicFactory logicFactory(::newLogicFactory());
        ::load(memorySource, prefixes, logicFactory, *this, formatName);
        ASSERT_EQUAL(expectedFormatName, formatName);
        endTest();
    }


};

TEST(testTurtle) {
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

    assertInput(
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
        "         :p 123 .",
        "Turtle"
    );

}

TEST(testTriG) {
    RT("ob0", "ob0", "ob0");
    RT("ob1", "ob1", "ob1");
    RT("ob2", "ob2", "ob2");

    assertInput(
        "@prefix : <http://my.com/local#>.\n"
        ":ctx1 {\n"
        "    :ob0 :ob0 :ob0 .\n"
        "}\n"
        "GRAPH :ctx2 {\n"
        "    :ob1 :ob1 :ob1 .\n"
        "}\n"
        ":ob2 :ob2 :ob2 .\n",
        "TriG"
    );

}

TEST(testNTriples) {
    RT("ob0", "ob0", "ob0");
    RT("ob1", "ob1", "ob1");

    assertInput(
        "<http://my.com/local#ob0> <http://my.com/local#ob0> <http://my.com/local#ob0> .\n"
        "<http://my.com/local#ob1> <http://my.com/local#ob1> <http://my.com/local#ob1> .\n",
        "N-Triples"
    );

}

TEST(testNQuads) {
    RT("ob0", "ob0", "ob0");
    RT("ob1", "ob1", "ob1");

    assertInput(
        "<http://my.com/local#ob0> <http://my.com/local#ob0> <http://my.com/local#ob0> <http://my.com/local#ctx1> .\n"
        "<http://my.com/local#ob1> <http://my.com/local#ob1> <http://my.com/local#ob1> .\n",
        "N-Quads"
    );

}

#endif
