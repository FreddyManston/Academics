// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifdef WITH_TEST

#define AUTO_TEST    ResourceValueCacheTest

#include <CppTest/AutoTest.h>

#include "../../src/util/Prefixes.h"
#include "../../src/util/MemoryManager.h"
#include "../../src/util/ThreadContext.h"
#include "../../src/dictionary/Dictionary.h"
#include "../../src/dictionary/ResourceValueCache.h"

class ResourceValueCacheTest {

protected:

    MemoryManager m_memoryManager;
    Dictionary m_dictionary;
    ResourceValueCache m_resourceValueCache;

public:

    ResourceValueCacheTest() : m_memoryManager(10000000), m_dictionary(m_memoryManager, false), m_resourceValueCache(m_dictionary, m_memoryManager) {
    }

    void initialize() {
        m_dictionary.initialize();
    }

    ResourceID resolveInDictionary(const ResourceValue& resourceValue) {
        return m_dictionary.resolveResource(resourceValue);
    }

    ResourceID resolveInDictionary(const std::string& value) {
        ResourceValue resourceValue;
        resourceValue.setString(D_XSD_STRING, value);
        return resolveInDictionary(resourceValue);
    }

    ResourceID resolveInDictionary(const int64_t value) {
        ResourceValue resourceValue;
        resourceValue.setInteger(value);
        return resolveInDictionary(resourceValue);
    }

    ResourceID resolve(const ResourceValue& resourceValue) {
        return m_resourceValueCache.resolveResource(ThreadContext::getCurrentThreadContext(), resourceValue);
    }

    ResourceID resolve(const std::string& value) {
        ResourceValue resourceValue;
        resourceValue.setString(D_XSD_STRING, value);
        return resolve(resourceValue);
    }

    ResourceID resolve(const int64_t value) {
        ResourceValue resourceValue;
        resourceValue.setInteger(value);
        return resolve(resourceValue);
    }

    void assertLiteral(const ResourceValue& expectedResourceValue, ResourceID resourceID, const char* const fileName, const long lineNumber) {
        ResourceValue actualResourceValue;
        if (m_resourceValueCache.getResource(resourceID, actualResourceValue)) {
            if (expectedResourceValue != actualResourceValue) {
                std::string value;
                std::ostringstream message;
                message << "Expected value '";
                Dictionary::toTurtleLiteral(expectedResourceValue, Prefixes::s_defaultPrefixes, value);
                message << value;
                message << "' but got value '";
                Dictionary::toTurtleLiteral(actualResourceValue, Prefixes::s_defaultPrefixes, value);
                message << value;
                message << "'.";
                FAIL2(message.str());
            }
        }
        else {
            std::ostringstream message;
            message << "Cannot retrieve resource for ID " << resourceID << ".";
            FAIL2(message.str());
        }
    }

    void assertLiteral(const std::string& expectedValue, ResourceID resourceID, const char* const fileName, const long lineNumber) {
        ResourceValue expectedResourceValue;
        expectedResourceValue.setString(D_XSD_STRING, expectedValue);
        return assertLiteral(expectedResourceValue, resourceID, fileName, lineNumber);
    }

    void assertLiteral(const int64_t expectedValue, ResourceID resourceID, const char* const fileName, const long lineNumber) {
        ResourceValue expectedResourceValue;
        expectedResourceValue.setInteger(expectedValue);
        return assertLiteral(expectedResourceValue, resourceID, fileName, lineNumber);
    }

};

#define ASSERT_LITERAL(expectedValue, resourceID)    \
    assertLiteral(expectedValue, resourceID, __FILE__, __LINE__)

TEST(testBasic) {
    const ResourceID abcID = resolveInDictionary("abc");
    const ResourceID twelveID = resolveInDictionary(12);
    ASSERT_LITERAL("abc", abcID);
    ASSERT_LITERAL(12, twelveID);
    ASSERT_EQUAL(abcID, resolve("abc"));
    ASSERT_EQUAL(twelveID, resolve(12));

    const ResourceID fourteenID = resolve(14);
    ASSERT_LITERAL(14, fourteenID);
    ASSERT_TRUE(fourteenID >= 0x8000000000000000ULL);
    ASSERT_EQUAL(INVALID_RESOURCE_ID, m_dictionary.tryResolveResource("14", D_XSD_INTEGER));

    const ResourceID defID = resolve("def");
    ASSERT_LITERAL("def", defID);
    ASSERT_TRUE(defID >= 0x8000000000000000ULL);
    ASSERT_EQUAL(INVALID_RESOURCE_ID, m_dictionary.tryResolveResource("def", D_XSD_STRING));

    ASSERT_EQUAL(fourteenID, resolve(14));
    ASSERT_EQUAL(defID, resolve("def"));
}

TEST(testResize) {
    for (size_t index = 0; index < 50000; index++)
        resolve(index);
}

#endif
