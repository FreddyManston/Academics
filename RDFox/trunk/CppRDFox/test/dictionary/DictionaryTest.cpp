// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifdef WITH_TEST

#define AUTO_TEST    DictionaryTest

#include <CppTest/AutoTest.h>

#include "../../src/RDFStoreException.h"
#include "../../src/util/Vocabulary.h"
#include "../../src/util/Prefixes.h"
#include "../../src/util/MemoryManager.h"
#include "../../src/dictionary/Dictionary.h"

class DictionaryTest {

protected:

    MemoryManager m_memoryManager;
    Dictionary m_dictionary;

public:

    DictionaryTest() : m_memoryManager(10000000), m_dictionary(m_memoryManager, false) {
    }

    void initialize() {
        m_dictionary.initialize();
    }

    ResourceID resolveResource(const ResourceType resourceType, const std::string& literal, const char* namespaceIRI, const char* localName) {
        std::string datatypeIRI;
        datatypeIRI.append(namespaceIRI);
        datatypeIRI.append(localName);
        return m_dictionary.resolveResource(resourceType, literal, datatypeIRI);
    }

    ResourceID resolveResource(const std::string& literal, const DatatypeID datatypeID) {
        return m_dictionary.resolveResource(literal, datatypeID);
    }

    void assertLiteral(const int64_t expectedValue, ResourceID resourceID, const char* const fileName, const long lineNumber) {
        ResourceValue resourceValue;
        m_dictionary.getResource(resourceID, resourceValue);
        CppTest::assertEqual(D_XSD_INTEGER, resourceValue.getDatatypeID(), fileName, lineNumber);
        CppTest::assertEqual(expectedValue, resourceValue.getInteger(), fileName, lineNumber);
    }

    void assertLiteral(const char* expectedValue, const DatatypeID expectedDatatypeID, ResourceID resourceID, const char* const fileName, const long lineNumber) {
        std::string lexicalValue;
        DatatypeID datatypeID;
        m_dictionary.getResource(resourceID, lexicalValue, datatypeID);
        std::string expectedValueString = expectedValue;
        CppTest::assertEqual(expectedValueString, lexicalValue, fileName, lineNumber);
        CppTest::assertEqual(expectedDatatypeID, datatypeID, fileName, lineNumber);
    }

};

#define ASSERT_LITERAL(expectedValue, expectedDatatypeID, resourceID)    \
    assertLiteral(expectedValue, expectedDatatypeID, resourceID, __FILE__, __LINE__)

#define ASSERT_INTEGER(expectedValue, resourceID)    \
    assertLiteral(expectedValue, resourceID, __FILE__, __LINE__)

TEST(testBasic) {
    ResourceID abc = resolveResource(LITERAL, "abc", XSD_NS, "string");
    ASSERT_LITERAL("abc", D_XSD_STRING, abc);
    ResourceID abcAgain = resolveResource(LITERAL, "abc", XSD_NS, "string");
    ASSERT_EQUAL(abc, abcAgain);

    ResourceID def = resolveResource(LITERAL, "def", XSD_NS, "string");
    ASSERT_LITERAL("def", D_XSD_STRING, def);
    ASSERT_NOT_EQUAL(abc, def);

    ResourceID abcPlainLiteral = resolveResource(LITERAL, "abc@", RDF_NS, "PlainLiteral");
    ASSERT_LITERAL("abc", D_XSD_STRING, abcPlainLiteral);
    ASSERT_EQUAL(abc, abcPlainLiteral);

    ResourceID abcEn = resolveResource(LITERAL, "abc@en", RDF_NS, "PlainLiteral");
    ASSERT_LITERAL("abc@en", D_RDF_PLAIN_LITERAL, abcEn);

    ResourceID pos123 = resolveResource(LITERAL, "123", XSD_NS, "integer");
    ASSERT_LITERAL("123", D_XSD_INTEGER, pos123);
    ASSERT_INTEGER(123, pos123);
}

#endif
