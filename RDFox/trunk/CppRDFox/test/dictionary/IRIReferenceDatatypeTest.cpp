// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifdef WITH_TEST

#define AUTO_TEST    IRIReferenceDatatypeTest

#include <CppTest/AutoTest.h>

#include "AbstractDatatypeTest.h"

class IRIReferenceDatatypeTest : public AbstractDatatypeTest {

public:

    virtual DatatypeID getDefaultDatatypeID() {
        return D_IRI_REFERENCE;
    }

};

TEST(testBasic) {
    const ResourceID abcID = resolve("abc");
    ASSERT_EQUAL(6, abcID);
    ASSERT_LITERAL("abc", abcID);
    ASSERT_TURTLE("<abc>", abcID);

    ASSERT_EQUAL(abcID, resolve("abc"));

    ASSERT_EQUAL(abcID, getResourceID("abc"));

    const ResourceID abcdefgID = resolve("abcdefg");
    ASSERT_EQUAL(7, abcdefgID);
    ASSERT_LITERAL("abcdefg", abcdefgID);
    ASSERT_TURTLE("<abcdefg>", abcdefgID);

    const ResourceID abcdefghID = resolve("abcdefgh");
    ASSERT_EQUAL(8, abcdefghID);
    ASSERT_LITERAL("abcdefgh", abcdefghID);
    ASSERT_TURTLE("<abcdefgh>", abcdefghID);

    const ResourceID prefAbcID = resolve("http://www.xyz/abc");
    ASSERT_EQUAL(9, prefAbcID);
    ASSERT_LITERAL("http://www.xyz/abc", prefAbcID);
    ASSERT_TURTLE("<http://www.xyz/abc>", prefAbcID);

    const ResourceID prefDefID = resolve("http://www.xyz/def");
    ASSERT_EQUAL(10, prefDefID);
    ASSERT_LITERAL("http://www.xyz/def", prefDefID);
    ASSERT_TURTLE("<http://www.xyz/def>", prefDefID);
}

TEST(testBulk) {
    const size_t TEST_SIZE = 40000;
    for (size_t index = 0; index < TEST_SIZE; index++) {
        std::ostringstream stream;
        stream << "http://www.xyz." << index << ".com/abcd" << index;
        std::string value = stream.str();
        const ResourceID resourceID = resolve(value);
        ASSERT_EQUAL(resourceID, index + 6);
        const ResourceID controlID = resolve(value);
        ASSERT_EQUAL(resourceID, controlID);
        ASSERT_LITERAL(value.c_str(), resourceID);
        ASSERT_EQUAL(index + 6, getResourceID(value));
    }
    for (size_t index = 0; index < TEST_SIZE; index++) {
        std::ostringstream stream;
        stream << "http://www.xyz." << index << ".com/abcd" << index;
        std::string value = stream.str();
        const ResourceID resourceID = resolve(value);
        ASSERT_EQUAL(resourceID, index + 6);
        ASSERT_LITERAL(value.c_str(), resourceID);
        ASSERT_EQUAL(index + 6, getResourceID(value));
    }
}

struct IRIReferenceValueMaker {
    always_inline void makeValue(const size_t valueIndex, std::string& lexicalForm, DatatypeID& datatypeID) const {
        std::ostringstream stream;
        stream << "http:://www.mydomain" << valueIndex % 1000 << ".com/local#value" << valueIndex;
        lexicalForm = stream.str();
        datatypeID = D_IRI_REFERENCE;
    }
};

TEST(testParallel) {
    runParallelTest(100000, 4, IRIReferenceValueMaker());
}

#endif
