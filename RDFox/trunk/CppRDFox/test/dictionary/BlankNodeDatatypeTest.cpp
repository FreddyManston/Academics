// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifdef WITH_TEST

#define AUTO_TEST    BlankNodeDatatypeTest

#include <CppTest/AutoTest.h>

#include "AbstractDatatypeTest.h"

class BlankNodeDatatypeTest : public AbstractDatatypeTest {

public:

    virtual DatatypeID getDefaultDatatypeID() {
        return D_BLANK_NODE;
    }

};

TEST(testBasic) {
    const ResourceID abcID = resolve("abc");
    ASSERT_EQUAL(6, abcID);
    ASSERT_LITERAL("abc", abcID);
    ASSERT_EQUAL(abcID, resolve("abc"));
    ASSERT_EQUAL(abcID, getResourceID("abc"));
    ASSERT_TURTLE("_:abc", abcID);

    const ResourceID abcdefgID = resolve("abcdefg");
    ASSERT_EQUAL(7, abcdefgID);
    ASSERT_LITERAL("abcdefg", abcdefgID);
    ASSERT_TURTLE("_:abcdefg", abcdefgID);

    const ResourceID abcdefghID = resolve("abcdefgh");
    ASSERT_EQUAL(8, abcdefghID);
    ASSERT_LITERAL("abcdefgh", abcdefghID);
    ASSERT_TURTLE("_:abcdefgh", abcdefghID);
}

TEST(testBulk) {
    const size_t TEST_SIZE = 40000;
    for (size_t index = 0; index < TEST_SIZE; index++) {
        std::ostringstream stream;
        stream << "bn" << index;
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
        stream << "bn" << index;
        std::string value = stream.str();
        const ResourceID resourceID = resolve(value);
        ASSERT_EQUAL(resourceID, index + 6);
        ASSERT_LITERAL(value.c_str(), resourceID);
        ASSERT_EQUAL(index +6, getResourceID(value));
    }
}

struct BlankNodeValueMaker {
    always_inline void makeValue(const size_t valueIndex, std::string& lexicalForm, DatatypeID& datatypeID) const {
        std::ostringstream stream;
        stream << "bn" << valueIndex;
        lexicalForm = stream.str();
        datatypeID = D_BLANK_NODE;
    }
};

TEST(testParallel) {
    runParallelTest(100000, 4, BlankNodeValueMaker());
}

#endif
