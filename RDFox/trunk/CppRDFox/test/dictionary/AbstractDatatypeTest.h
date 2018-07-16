// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#if defined(WITH_TEST) && !defined(ABSTRACTDATATYPETEST_H_)
#define ABSTRACTDATATYPETEST_H_

#include "../../src/util/Prefixes.h"
#include "../../src/util/MemoryManager.h"
#include "../../src/util/Thread.h"
#include "../../src/dictionary/Dictionary.h"

class AbstractDatatypeTest {

protected:

    MemoryManager m_memoryManager;
    Dictionary m_dictionary;

public:

    AbstractDatatypeTest() : m_memoryManager(10000000), m_dictionary(m_memoryManager, true) {
    }

    void initialize() {
        m_dictionary.initialize();
    }

    ResourceID resolveValue(ResourceValue& resourceValue) {
        return m_dictionary.resolveResource(resourceValue);
    }

    ResourceID resolveEx(const std::string& lexicalForm, const DatatypeID datatypeID) {
        return m_dictionary.resolveResource(lexicalForm, datatypeID);
    }

    ResourceID getResourceValueID(ResourceValue& resourceValue) {
        return m_dictionary.tryResolveResource(resourceValue);
    }

    ResourceID getResourceIDEx(const std::string& lexicalForm, const DatatypeID datatypeID) {
        return m_dictionary.tryResolveResource(lexicalForm, datatypeID);
    }

    void assertLiteralEx(const char* expectedLexicalForm, const DatatypeID expectedDatatypeID, const ResourceID resourceID, const char* const fileName, const long lineNumber) {
        std::string lexicalForm;
        DatatypeID datatypeID;
        m_dictionary.getResource(resourceID, lexicalForm, datatypeID);
        CppTest::assertEqual(expectedLexicalForm, lexicalForm, fileName, lineNumber);
        CppTest::assertEqual(expectedDatatypeID, datatypeID, fileName, lineNumber);
    }

    void assertTurtle(const char* expectedLiteralText, const ResourceID resourceID, const char* const fileName, const long lineNumber) {
        std::string literalText;
        m_dictionary.toTurtleLiteral(resourceID, Prefixes::s_defaultPrefixes, literalText);
        CppTest::assertEqual(expectedLiteralText, literalText, fileName, lineNumber);
    }

    virtual DatatypeID getDefaultDatatypeID() = 0;

    ResourceID resolve(const std::string& lexicalForm) {
        return resolveEx(lexicalForm, getDefaultDatatypeID());
    }

    ResourceID getResourceID(const std::string& lexicalForm) {
        return getResourceIDEx(lexicalForm, getDefaultDatatypeID());
    }

    void assertLiteral(const char* expectedLexicalForm, const ResourceID resourceID, const char* const fileName, const long lineNumber) {
        assertLiteralEx(expectedLexicalForm, getDefaultDatatypeID(), resourceID, fileName, lineNumber);
    }

    void checkValue(const char* lexicalForm, const DatatypeID datatypeID, const ResourceID expectedResourceID) {
        const ResourceID valueID = resolveEx(lexicalForm, datatypeID);
        ASSERT_EQUAL(expectedResourceID, valueID);
        ASSERT_EQUAL(expectedResourceID, getResourceIDEx(lexicalForm, datatypeID));
        assertLiteralEx(lexicalForm, datatypeID, valueID, __FILE__, __LINE__);
        std::string literalText;
        literalText.push_back('"');
        literalText.append(lexicalForm);
        literalText.append("\"^^");
        literalText.append(Prefixes::s_defaultPrefixes.encodeIRI(Dictionary::getDatatypeIRI(datatypeID)));
        assertTurtle(literalText.c_str(), valueID, __FILE__, __LINE__);
    }

    template<class VM>
    class Writer : public Thread {

    protected:

        Dictionary& m_dictionary;
        const size_t m_numberOfValues;
        const size_t m_startIndex;
        const size_t m_stride;
        ResourceID* const m_resourceIDs;
        const VM m_valueMaker;

    public:

        Writer(Dictionary& dictionary, const size_t numberOfValues, const size_t startIndex, const size_t stride, ResourceID* const resourceIDs, VM valueMaker) :
            m_dictionary(dictionary),
            m_numberOfValues(numberOfValues),
            m_startIndex(startIndex),
            m_stride(stride),
            m_resourceIDs(resourceIDs),
            m_valueMaker(valueMaker)
        {
        }

        virtual void run() {
            std::string lexicalForm;
            DatatypeID datatypeID;
            for (size_t valueIndex = m_startIndex; valueIndex < m_numberOfValues; valueIndex += m_stride) {
                m_valueMaker.makeValue(valueIndex, lexicalForm, datatypeID);
                ResourceID resolvedResourceID = m_dictionary.resolveResource(lexicalForm, datatypeID);
                ResourceID existingResourceID = ::atomicRead(m_resourceIDs[valueIndex]);
                if (existingResourceID != INVALID_RESOURCE_ID)
                    ASSERT_EQUAL(existingResourceID, resolvedResourceID);
                else
                    ::atomicWrite(m_resourceIDs[valueIndex], resolvedResourceID);
            }
        }

    };

    template<class VM>
    void runParallelTest(const size_t numberOfValues, const size_t numberOfThreads, const VM valueMaker) {
        std::unique_ptr<ResourceID[]> resourceIDs(new ResourceID[numberOfValues]);
        for (size_t valueIndex = 0; valueIndex < numberOfValues; ++valueIndex)
            resourceIDs[valueIndex] = INVALID_RESOURCE_ID;

        unique_ptr_vector<Thread> threads;
        for (size_t threadIndex = 0; threadIndex < numberOfThreads; ++threadIndex) {
            std::unique_ptr<Thread> thread(new Writer<VM>(m_dictionary, numberOfValues, threadIndex, numberOfThreads/2, resourceIDs.get(), valueMaker));
            threads.push_back(std::move(thread));
        }
        for (size_t threadIndex = 0; threadIndex < numberOfThreads; ++threadIndex)
            threads[threadIndex]->start();
        for (size_t threadIndex = 0; threadIndex < numberOfThreads; ++threadIndex)
            threads[threadIndex]->join();

        // Check the values
        std::string lexicalForm;
        DatatypeID datatypeID;
        std::string dictionaryLexicalForm;
        DatatypeID dictionaryDatatypeID;
        for (size_t valueIndex = 0; valueIndex < numberOfValues; ++valueIndex) {
            valueMaker.makeValue(valueIndex, lexicalForm, datatypeID);
            ResourceID resourceID = resourceIDs[valueIndex];
            m_dictionary.getResource(resourceID, dictionaryLexicalForm, dictionaryDatatypeID);
            ASSERT_EQUAL(lexicalForm, dictionaryLexicalForm);
            ASSERT_EQUAL(datatypeID, dictionaryDatatypeID);
            ResourceID tryResourceID = m_dictionary.tryResolveResource(lexicalForm, datatypeID);
            ASSERT_EQUAL(resourceID, tryResourceID);
        }
    }

};

#define ASSERT_LITERAL_EX(expectedLexicalForm, expectedDatatypeID, resourceID) \
    assertLiteralEx(expectedLexicalForm, expectedDatatypeID, resourceID, __FILE__, __LINE__)

#define ASSERT_LITERAL(expectedLexicalForm, resourceID) \
    assertLiteral(expectedLexicalForm, resourceID, __FILE__, __LINE__)

#define ASSERT_TURTLE(expectedLiteralValue, resourceID) \
    assertTurtle(expectedLiteralValue, resourceID, __FILE__, __LINE__)

#endif /* ABSTRACTDATATYPETEST_H_ */
