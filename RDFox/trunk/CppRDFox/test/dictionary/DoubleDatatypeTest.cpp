// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifdef WITH_TEST

#define AUTO_TEST    DoubleDatatypeTest

#include <CppTest/AutoTest.h>

#include "AbstractDatatypeTest.h"

class DoubleDatatypeTest : public AbstractDatatypeTest {

public:

    virtual DatatypeID getDefaultDatatypeID() {
        return D_XSD_DOUBLE;
    }

    using AbstractDatatypeTest::assertLiteral;

    using AbstractDatatypeTest::getResourceID;

    void assertLiteral(const double expectedValue, const ResourceID resourceID, const char* const fileName, const long lineNumber) {
        ResourceValue resourceValue;
        m_dictionary.getResource(resourceID, resourceValue);
        CppTest::assertEqual(expectedValue, resourceValue.getDouble(), fileName, lineNumber);
        CppTest::assertEqual(D_XSD_DOUBLE, resourceValue.getDatatypeID(), fileName, lineNumber);
    }

    ResourceID getResourceID(const double value) {
        ResourceValue resourceValue;
        resourceValue.setDouble(value);
        return m_dictionary.tryResolveResource(resourceValue);
    }

};

TEST(testBasic) {
    const ResourceID res123 = resolve("123");
    ASSERT_EQUAL(6, res123);
    ASSERT_LITERAL(123.0, res123);
    ASSERT_LITERAL("123", res123);
    ASSERT_EQUAL(res123, getResourceID(123.0));
    ASSERT_EQUAL(res123, getResourceID("123"));
    ASSERT_TURTLE("\"123\"^^xsd:double", res123);

    ASSERT_EQUAL(res123, resolve("123"));
    ASSERT_LITERAL(123.0, res123);
    ASSERT_EQUAL(res123, getResourceID(123.0));

    const ResourceID res1neg = resolve("-1");
    ASSERT_EQUAL(7, res1neg);
    ASSERT_LITERAL(-1.0, res1neg);
    ASSERT_LITERAL("-1", res1neg);
    ASSERT_EQUAL(res1neg, getResourceID(-1));
    ASSERT_TURTLE("\"-1\"^^xsd:double", res1neg);
}

TEST(testBulk) {
    const size_t TEST_SIZE = 4000;
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

struct DoubleValueMaker {
    always_inline void makeValue(const size_t valueIndex, std::string& lexicalForm, DatatypeID& datatypeID) const {
        std::ostringstream stream;
        stream << valueIndex;
        lexicalForm = stream.str();
        datatypeID = D_XSD_DOUBLE;
    }
};

TEST(testParallel) {
    runParallelTest(10000, 4, DoubleValueMaker());
}

#endif
