// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifdef WITH_TEST

#define AUTO_TEST    NumberDatatypeTest

#include <CppTest/AutoTest.h>

#include "AbstractDatatypeTest.h"

class NumberDatatypeTest : public AbstractDatatypeTest {

public:

    virtual DatatypeID getDefaultDatatypeID() {
        return D_XSD_INTEGER;
    }

    using AbstractDatatypeTest::assertLiteral;

    using AbstractDatatypeTest::getResourceID;

    void assertLiteral(const int64_t expectedValue, const ResourceID resourceID, const char* const fileName, const long lineNumber) {
        ResourceValue resourceValue;
        m_dictionary.getResource(resourceID, resourceValue);
        CppTest::assertEqual(expectedValue, resourceValue.getInteger(), fileName, lineNumber);
        CppTest::assertEqual(D_XSD_INTEGER, resourceValue.getDatatypeID(), fileName, lineNumber);
    }

    ResourceID getResourceID(const int64_t value) {
        ResourceValue resourceValue;
        resourceValue.setInteger(value);
        return m_dictionary.tryResolveResource(resourceValue);
    }

};

TEST(testBasic) {
    const ResourceID res123 = resolve("123");
    ASSERT_EQUAL(6, res123);
    ASSERT_LITERAL(123, res123);
    ASSERT_LITERAL("123", res123);
    ASSERT_TURTLE("123", res123);
    ASSERT_EQUAL(res123, getResourceID(123));
    ASSERT_EQUAL(res123, getResourceID("123"));

    ASSERT_EQUAL(res123, resolve("123"));
    ASSERT_LITERAL(123, res123);
    ASSERT_EQUAL(res123, getResourceID(123));

    const ResourceID res1neg = resolve("-1");
    ASSERT_EQUAL(7, res1neg);
    ASSERT_LITERAL(-1, res1neg);
    ASSERT_LITERAL("-1", res1neg);
    ASSERT_TURTLE("-1", res1neg);
    ASSERT_EQUAL(res1neg, getResourceID(-1));
}

TEST(testBulk) {
    const size_t TEST_SIZE = 40000;
    for (size_t index = 0; index < TEST_SIZE; index++) {
        std::ostringstream stream;
        stream << index;
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
        stream << index;
        std::string value = stream.str();
        const ResourceID resourceID = resolve(value);
        ASSERT_EQUAL(resourceID, index + 6);
        ASSERT_LITERAL(value.c_str(), resourceID);
        ASSERT_EQUAL(index + 6, getResourceID(value));
    }
}

struct NumberValueMaker {
    always_inline void makeValue(const size_t valueIndex, std::string& lexicalForm, DatatypeID& datatypeID) const {
        std::ostringstream stream;
        stream << valueIndex;
        lexicalForm = stream.str();
        datatypeID = D_XSD_INTEGER;
    }
};

TEST(testParallel) {
    runParallelTest(100000, 4, NumberValueMaker());
}

#endif
