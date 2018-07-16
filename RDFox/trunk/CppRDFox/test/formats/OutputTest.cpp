// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifdef WITH_TEST

#define AUTO_TEST   OutputTest

#include <CppTest/AutoTest.h>

#include "../../src/formats/InputOutput.h"
#include "../../src/util/MemoryOutputStream.h"
#include "../querying/DataStoreTest.h"

class OutputTest : public DataStoreTest {

protected:

    void assertOutput(const std::string& formatName, const std::string& expectedOutput) {
        std::string buffer;
        MemoryOutputStream memoryOutputStream(buffer);
        ::save(*m_dataStore, m_prefixes, memoryOutputStream, formatName);
        ASSERT_EQUAL(expectedOutput, buffer);
    }

public:

    OutputTest() : DataStoreTest("seq", EQUALITY_AXIOMATIZATION_NO_UNA) {
    }

};

TEST(testBasic) {
    addTriples(
        ":i1 :p :i2 . "
        ":i1 :p :i3 . "
        ":i1 :r :i4 . "
        ":i1 :r :i5 . "
        ":i1 owl:sameAs :i11 . "
        ":i11 owl:sameAs :i12 . "

        ":i8 :p :i2 . "
        ":i8 :p :i3 . "
        ":i8 :r :i4 . "
        ":i8 :r :i5 . "
        ":i8 owl:sameAs :i81 . "
        ":i81 owl:sameAs :i82 . "
    );
    assertOutput("Turtle",
        "@prefix : <http://krr.cs.ox.ac.uk/RDF-store/test#> .\n"
        "@prefix owl: <http://www.w3.org/2002/07/owl#> .\n"
        "@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n"
        "@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n"
        "@prefix ruleml: <http://www.w3.org/2003/11/ruleml#> .\n"
        "@prefix swrl: <http://www.w3.org/2003/11/swrl#> .\n"
        "@prefix swrlb: <http://www.w3.org/2003/11/swrlb#> .\n"
        "@prefix swrlx: <http://www.w3.org/2003/11/swrlx#> .\n"
        "@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n"
        "\n"
        ":i1 owl:sameAs :i11 ;\n"
        "    :r :i4 , :i5 ;\n"
        "    :p :i2 , :i3 .\n"
        "\n"
        ":i11 owl:sameAs :i12 .\n"
        "\n"
        ":i8 owl:sameAs :i81 ;\n"
        "    :r :i4 , :i5 ;\n"
        "    :p :i2 , :i3 .\n"
        "\n"
        ":i81 owl:sameAs :i82 .\n"
    );
    assertOutput("N-Triples",
        "<http://krr.cs.ox.ac.uk/RDF-store/test#i1> <http://www.w3.org/2002/07/owl#sameAs> <http://krr.cs.ox.ac.uk/RDF-store/test#i11> .\n"
        "<http://krr.cs.ox.ac.uk/RDF-store/test#i1> <http://krr.cs.ox.ac.uk/RDF-store/test#r> <http://krr.cs.ox.ac.uk/RDF-store/test#i4> .\n"
        "<http://krr.cs.ox.ac.uk/RDF-store/test#i1> <http://krr.cs.ox.ac.uk/RDF-store/test#r> <http://krr.cs.ox.ac.uk/RDF-store/test#i5> .\n"
        "<http://krr.cs.ox.ac.uk/RDF-store/test#i1> <http://krr.cs.ox.ac.uk/RDF-store/test#p> <http://krr.cs.ox.ac.uk/RDF-store/test#i2> .\n"
        "<http://krr.cs.ox.ac.uk/RDF-store/test#i1> <http://krr.cs.ox.ac.uk/RDF-store/test#p> <http://krr.cs.ox.ac.uk/RDF-store/test#i3> .\n"
        "<http://krr.cs.ox.ac.uk/RDF-store/test#i11> <http://www.w3.org/2002/07/owl#sameAs> <http://krr.cs.ox.ac.uk/RDF-store/test#i12> .\n"
        "<http://krr.cs.ox.ac.uk/RDF-store/test#i8> <http://www.w3.org/2002/07/owl#sameAs> <http://krr.cs.ox.ac.uk/RDF-store/test#i81> .\n"
        "<http://krr.cs.ox.ac.uk/RDF-store/test#i8> <http://krr.cs.ox.ac.uk/RDF-store/test#r> <http://krr.cs.ox.ac.uk/RDF-store/test#i4> .\n"
        "<http://krr.cs.ox.ac.uk/RDF-store/test#i8> <http://krr.cs.ox.ac.uk/RDF-store/test#r> <http://krr.cs.ox.ac.uk/RDF-store/test#i5> .\n"
        "<http://krr.cs.ox.ac.uk/RDF-store/test#i8> <http://krr.cs.ox.ac.uk/RDF-store/test#p> <http://krr.cs.ox.ac.uk/RDF-store/test#i2> .\n"
        "<http://krr.cs.ox.ac.uk/RDF-store/test#i8> <http://krr.cs.ox.ac.uk/RDF-store/test#p> <http://krr.cs.ox.ac.uk/RDF-store/test#i3> .\n"
        "<http://krr.cs.ox.ac.uk/RDF-store/test#i81> <http://www.w3.org/2002/07/owl#sameAs> <http://krr.cs.ox.ac.uk/RDF-store/test#i82> .\n"
    );

    applyRules();
    assertOutput("Turtle",
        "@prefix : <http://krr.cs.ox.ac.uk/RDF-store/test#> .\n"
        "@prefix owl: <http://www.w3.org/2002/07/owl#> .\n"
        "@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n"
        "@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n"
        "@prefix ruleml: <http://www.w3.org/2003/11/ruleml#> .\n"
        "@prefix swrl: <http://www.w3.org/2003/11/swrl#> .\n"
        "@prefix swrlb: <http://www.w3.org/2003/11/swrlb#> .\n"
        "@prefix swrlx: <http://www.w3.org/2003/11/swrlx#> .\n"
        "@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n"
        "\n"
        "owl:sameAs owl:sameAs owl:sameAs .\n"
        "\n"
        ":i1 owl:sameAs :i1 , :i11 , :i12 ;\n"
        "    :r :i4 , :i5 ;\n"
        "    :p :i2 , :i3 .\n"
        "\n"
        ":p owl:sameAs :p .\n"
        "\n"
        ":i2 owl:sameAs :i2 .\n"
        "\n"
        ":i3 owl:sameAs :i3 .\n"
        "\n"
        ":r owl:sameAs :r .\n"
        "\n"
        ":i4 owl:sameAs :i4 .\n"
        "\n"
        ":i5 owl:sameAs :i5 .\n"
        "\n"
        ":i8 owl:sameAs :i8 , :i81 , :i82 ;\n"
        "    :r :i4 , :i5 ;\n"
        "    :p :i2 , :i3 .\n"
    );
    assertOutput("N-Triples",
        "<http://www.w3.org/2002/07/owl#sameAs> <http://www.w3.org/2002/07/owl#sameAs> <http://www.w3.org/2002/07/owl#sameAs> .\n"
        "<http://krr.cs.ox.ac.uk/RDF-store/test#i1> <http://www.w3.org/2002/07/owl#sameAs> <http://krr.cs.ox.ac.uk/RDF-store/test#i1> .\n"
        "<http://krr.cs.ox.ac.uk/RDF-store/test#i1> <http://www.w3.org/2002/07/owl#sameAs> <http://krr.cs.ox.ac.uk/RDF-store/test#i11> .\n"
        "<http://krr.cs.ox.ac.uk/RDF-store/test#i1> <http://www.w3.org/2002/07/owl#sameAs> <http://krr.cs.ox.ac.uk/RDF-store/test#i12> .\n"
        "<http://krr.cs.ox.ac.uk/RDF-store/test#i1> <http://krr.cs.ox.ac.uk/RDF-store/test#r> <http://krr.cs.ox.ac.uk/RDF-store/test#i4> .\n"
        "<http://krr.cs.ox.ac.uk/RDF-store/test#i1> <http://krr.cs.ox.ac.uk/RDF-store/test#r> <http://krr.cs.ox.ac.uk/RDF-store/test#i5> .\n"
        "<http://krr.cs.ox.ac.uk/RDF-store/test#i1> <http://krr.cs.ox.ac.uk/RDF-store/test#p> <http://krr.cs.ox.ac.uk/RDF-store/test#i2> .\n"
        "<http://krr.cs.ox.ac.uk/RDF-store/test#i1> <http://krr.cs.ox.ac.uk/RDF-store/test#p> <http://krr.cs.ox.ac.uk/RDF-store/test#i3> .\n"
        "<http://krr.cs.ox.ac.uk/RDF-store/test#p> <http://www.w3.org/2002/07/owl#sameAs> <http://krr.cs.ox.ac.uk/RDF-store/test#p> .\n"
        "<http://krr.cs.ox.ac.uk/RDF-store/test#i2> <http://www.w3.org/2002/07/owl#sameAs> <http://krr.cs.ox.ac.uk/RDF-store/test#i2> .\n"
        "<http://krr.cs.ox.ac.uk/RDF-store/test#i3> <http://www.w3.org/2002/07/owl#sameAs> <http://krr.cs.ox.ac.uk/RDF-store/test#i3> .\n"
        "<http://krr.cs.ox.ac.uk/RDF-store/test#r> <http://www.w3.org/2002/07/owl#sameAs> <http://krr.cs.ox.ac.uk/RDF-store/test#r> .\n"
        "<http://krr.cs.ox.ac.uk/RDF-store/test#i4> <http://www.w3.org/2002/07/owl#sameAs> <http://krr.cs.ox.ac.uk/RDF-store/test#i4> .\n"
        "<http://krr.cs.ox.ac.uk/RDF-store/test#i5> <http://www.w3.org/2002/07/owl#sameAs> <http://krr.cs.ox.ac.uk/RDF-store/test#i5> .\n"
        "<http://krr.cs.ox.ac.uk/RDF-store/test#i8> <http://www.w3.org/2002/07/owl#sameAs> <http://krr.cs.ox.ac.uk/RDF-store/test#i8> .\n"
        "<http://krr.cs.ox.ac.uk/RDF-store/test#i8> <http://www.w3.org/2002/07/owl#sameAs> <http://krr.cs.ox.ac.uk/RDF-store/test#i81> .\n"
        "<http://krr.cs.ox.ac.uk/RDF-store/test#i8> <http://www.w3.org/2002/07/owl#sameAs> <http://krr.cs.ox.ac.uk/RDF-store/test#i82> .\n"
        "<http://krr.cs.ox.ac.uk/RDF-store/test#i8> <http://krr.cs.ox.ac.uk/RDF-store/test#r> <http://krr.cs.ox.ac.uk/RDF-store/test#i4> .\n"
        "<http://krr.cs.ox.ac.uk/RDF-store/test#i8> <http://krr.cs.ox.ac.uk/RDF-store/test#r> <http://krr.cs.ox.ac.uk/RDF-store/test#i5> .\n"
        "<http://krr.cs.ox.ac.uk/RDF-store/test#i8> <http://krr.cs.ox.ac.uk/RDF-store/test#p> <http://krr.cs.ox.ac.uk/RDF-store/test#i2> .\n"
        "<http://krr.cs.ox.ac.uk/RDF-store/test#i8> <http://krr.cs.ox.ac.uk/RDF-store/test#p> <http://krr.cs.ox.ac.uk/RDF-store/test#i3> .\n"
    );
}

TEST(testIRICoding) {
    addTriples(
        ":a1 :r :b\\-\\%%FE . "
        ":a2 :r :\\- . "
        ":a3 :r <\\u003c\\u003D\\u003e> . "
        ":a4 :r <http://krr.cs.ox.ac.uk/RDF-store/test#94> . "
        ":a5 :r <http://krr.cs.ox.ac.uk/RDF-store/test#_abc> . "
        ":a6 :r <http://krr.cs.ox.ac.uk/RDF-store/test#-abc> . "
    );
    assertOutput("Turtle",
        "@prefix : <http://krr.cs.ox.ac.uk/RDF-store/test#> .\n"
        "@prefix owl: <http://www.w3.org/2002/07/owl#> .\n"
        "@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n"
        "@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n"
        "@prefix ruleml: <http://www.w3.org/2003/11/ruleml#> .\n"
        "@prefix swrl: <http://www.w3.org/2003/11/swrl#> .\n"
        "@prefix swrlb: <http://www.w3.org/2003/11/swrlb#> .\n"
        "@prefix swrlx: <http://www.w3.org/2003/11/swrlx#> .\n"
        "@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n"
        "\n"
        ":a1 :r :b-\\%%FE .\n"
        "\n"
        ":a2 :r :\\- .\n"
        "\n"
        ":a3 :r <\\u003C=\\u003E> .\n"
        "\n"
        ":a4 :r :94 .\n"
        "\n"
        ":a5 :r :_abc .\n"
        "\n"
        ":a6 :r :\\-abc .\n"
    );
}

TEST(testUnicode) {
    addTriples(
        ":a1 :r :tr""\xC3\xA4""nen\xC3\xBC""berstr""\xC3\xB6""mt . "
        ":a2 :r \"tr\\u00E4nen\\u00fCberstr\\U000000f6mt\" . "
    );
    assertOutput("Turtle",
        "@prefix : <http://krr.cs.ox.ac.uk/RDF-store/test#> .\n"
        "@prefix owl: <http://www.w3.org/2002/07/owl#> .\n"
        "@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n"
        "@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n"
        "@prefix ruleml: <http://www.w3.org/2003/11/ruleml#> .\n"
        "@prefix swrl: <http://www.w3.org/2003/11/swrl#> .\n"
        "@prefix swrlb: <http://www.w3.org/2003/11/swrlb#> .\n"
        "@prefix swrlx: <http://www.w3.org/2003/11/swrlx#> .\n"
        "@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n"
        "\n"
        ":a1 :r :tr""\xC3\xA4""nen\xC3\xBC""berstr""\xC3\xB6""mt .\n"
        "\n"
        ":a2 :r \"tr""\xC3\xA4""nen\xC3\xBC""berstr""\xC3\xB6""mt\" .\n"
    );
}

TEST(testString) {
    addTriples(
        ":a1 :r \"'in quotes'\" . "
        ":a2 :r '\"in quotes\"' . "
        ":a3 :r '''some\nline''' . "
    );
    assertOutput("Turtle",
        "@prefix : <http://krr.cs.ox.ac.uk/RDF-store/test#> .\n"
        "@prefix owl: <http://www.w3.org/2002/07/owl#> .\n"
        "@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n"
        "@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n"
        "@prefix ruleml: <http://www.w3.org/2003/11/ruleml#> .\n"
        "@prefix swrl: <http://www.w3.org/2003/11/swrl#> .\n"
        "@prefix swrlb: <http://www.w3.org/2003/11/swrlb#> .\n"
        "@prefix swrlx: <http://www.w3.org/2003/11/swrlx#> .\n"
        "@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n"
        "\n"
        ":a1 :r \"'in quotes'\" .\n"
        "\n"
        ":a2 :r '\"in quotes\"' .\n"
        "\n"
        ":a3 :r \"\"\"some\nline\"\"\" .\n"
    );
}

#endif
