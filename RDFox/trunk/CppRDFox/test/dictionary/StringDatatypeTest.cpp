// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifdef WITH_TEST

#define AUTO_TEST    StringDatatypeTest

#include <CppTest/AutoTest.h>

#include "../../src/RDFStoreException.h"
#include "AbstractDatatypeTest.h"

class StringDatatypeTest : public AbstractDatatypeTest {

public:

    virtual DatatypeID getDefaultDatatypeID() {
        return D_XSD_STRING;
    }

};

TEST(testBasic) {
    const ResourceID abcID = resolve("abc");
    ASSERT_EQUAL(6, abcID);
    ASSERT_LITERAL("abc", abcID);
    ASSERT_TURTLE("\"abc\"", abcID);
    ASSERT_EQUAL(abcID, resolve("abc"));
    ASSERT_EQUAL(abcID, getResourceID("abc"));

    const ResourceID abcdefgID = resolve("abcdefg");
    ASSERT_EQUAL(7, abcdefgID);
    ASSERT_LITERAL("abcdefg", abcdefgID);
    ASSERT_TURTLE("\"abcdefg\"", abcdefgID);

    const ResourceID abcdefghID = resolve("abcdefgh");
    ASSERT_EQUAL(8, abcdefghID);
    ASSERT_LITERAL("abcdefgh", abcdefghID);
    ASSERT_TURTLE("\"abcdefgh\"", abcdefghID);
}

TEST(testMalformed) {
    try {
        resolveEx("abc", D_RDF_PLAIN_LITERAL);
        FAIL();
    }
    catch (const RDFStoreException&) {
    }
}

TEST(testBulk) {
    const size_t TEST_SIZE = 40000;
    for (size_t index = 0; index < TEST_SIZE; index++) {
        std::ostringstream stream;
        stream << "abcd" << index;
        std::string value = stream.str();
        const ResourceID resourceID = resolve(value);
        ASSERT_EQUAL(resourceID, static_cast<ResourceID>(index + 6));
        const ResourceID controlID = resolve(value);
        ASSERT_EQUAL(resourceID, controlID);
        ASSERT_LITERAL(value.c_str(), resourceID);
        ASSERT_EQUAL(static_cast<ResourceID>(index + 6), getResourceID(value));
    }
    for (size_t index = 0; index < TEST_SIZE; index++) {
        std::ostringstream stream;
        stream << "abcd" << index;
        std::string value = stream.str();
        const ResourceID resourceID = resolve(value);
        ASSERT_EQUAL(resourceID, static_cast<ResourceID>(index + 6));
        ASSERT_LITERAL(value.c_str(), resourceID);
        ASSERT_EQUAL(static_cast<ResourceID>(index + 6), getResourceID(value));
    }
}

struct StringValueMaker {
    always_inline void makeValue(const size_t valueIndex, std::string& lexicalForm, DatatypeID& datatypeID) const {
        std::ostringstream stream;
        stream << "abcd" << valueIndex;
        lexicalForm = stream.str();
        datatypeID = D_XSD_STRING;
    }
};

TEST(testParallel) {
    runParallelTest(100000, 4, StringValueMaker());
}

#endif
