// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifdef WITH_TEST

#define AUTO_TEST    ValuationCacheTest

#include <CppTest/AutoTest.h>

#include "../../src/querying/ValuationCacheImpl.h"

class ValuationCacheTest {

protected:

    static ArgumentIndex s_valuesIndexes[20];

    MemoryManager m_memoryManager;
    std::unique_ptr<ValuationCache> m_valuationCache;

public:

    ValuationCacheTest() : m_memoryManager(100000000) {
    }

    void initializeCache(const uint16_t inputValuationSize, const uint16_t outputValuationSize) {
        m_valuationCache.reset(new ValuationCache(m_memoryManager, inputValuationSize, outputValuationSize));
        m_valuationCache->initialize();
    }

    static void loadValuation(ResourceID values[], uint16_t& size, va_list arguments) {
        size = 0;
        ResourceID value = va_arg(arguments, ResourceID);
        while (value != INVALID_RESOURCE_ID) {
            values[size++] = value;
            value = va_arg(arguments, ResourceID);
        }
    }

    static void assertValuation(const char* const fileName, const long lineNumber, const ResourceID expectedValues[], const uint16_t expectedSize, const ResourceID actualValues[], const uint16_t actualSize) {
        bool equal = (expectedSize == actualSize);
        for (uint16_t index = 0; equal && index < expectedSize; ++index)
            equal = (expectedValues[index] == actualValues[index]);
        if (!equal) {
            std::ostringstream message;
            message << "Expected valuation: [ ";
            for (uint16_t index = 0; index < expectedSize; ++index) {
                if (index != 0)
                    message << ", ";
                message << expectedValues[index];
            }
            message << " ]" << std::endl << "Actual valuation:   [ ";
            for (uint16_t index = 0; index < actualSize; ++index) {
                if (index != 0)
                    message << ", ";
                message << actualValues[index];
            }
            message << " ]";
            throw CppTest::AssertionError(fileName, lineNumber, message.str());
        }
    }

    InputValuation* resolveInputValuation(const char* const fileName, const long lineNumber, ...) {
        ResourceID values[20];
        uint16_t size;
        va_list arguments;
        va_start(arguments, lineNumber);
        loadValuation(values, size, arguments);
        va_end(arguments);
        CppTest::assertEqual(m_valuationCache->m_inputValuationManager.getInputValuationSize(), size, fileName, lineNumber);
        bool alreadyExists;
        return m_valuationCache->m_inputValuationManager.resolveInputValuation(values, s_valuesIndexes, alreadyExists);
    }

    void assertInputValuation(const char* const fileName, const long lineNumber, const InputValuation* inputValuation, const OutputValuation* expectedFirstOutputValuation, ...) {
        ResourceID expectedValues[20];
        uint16_t expectedSize;
        va_list arguments;
        va_start(arguments, expectedFirstOutputValuation);
        loadValuation(expectedValues, expectedSize, arguments);
        va_end(arguments);
        assertValuation(fileName, lineNumber, expectedValues, expectedSize, inputValuation->getValues(), m_valuationCache->m_inputValuationManager.getInputValuationSize());
        CppTest::assertEqual(expectedFirstOutputValuation, inputValuation->getFirstOutputValuation(), fileName, lineNumber);
    }

    OutputValuation* resolveOutputValuation(const char* const fileName, const long lineNumber, InputValuation* inputValuation, const size_t multiplicity, ...) {
        ResourceID values[20];
        uint16_t size;
        va_list arguments;
        va_start(arguments, multiplicity);
        loadValuation(values, size, arguments);
        va_end(arguments);
        CppTest::assertEqual(m_valuationCache->m_outputValuationManager.getOutputValuationSize(), size, fileName, lineNumber);
        bool alreadyExists;
        return m_valuationCache->m_outputValuationManager.resolveOutputValuation(inputValuation, values, s_valuesIndexes, multiplicity, alreadyExists);
    }

    void assertOutputValuation(const char* const fileName, const long lineNumber, const OutputValuation* outputValuation, const InputValuation* expectedInputValuation, const OutputValuation* expectedNextOutputValuation, const size_t expectedMultiplicity, ...) {
        ResourceID expectedValues[20];
        uint16_t expectedSize;
        va_list arguments;
        va_start(arguments, expectedMultiplicity);
        loadValuation(expectedValues, expectedSize, arguments);
        va_end(arguments);
        assertValuation(fileName, lineNumber, expectedValues, expectedSize, outputValuation->getValues(), m_valuationCache->m_outputValuationManager.getOutputValuationSize());
        CppTest::assertEqual(expectedInputValuation, outputValuation->getInputValuation(), fileName, lineNumber);
        CppTest::assertEqual(expectedNextOutputValuation, outputValuation->getNextOutputValuation(), fileName, lineNumber);
        CppTest::assertEqual(expectedMultiplicity, outputValuation->getMultiplicity(), fileName, lineNumber);
    }

};

ArgumentIndex ValuationCacheTest::s_valuesIndexes[20] = {
     0,  1,  2,  3,  4,  5,  6,  7,  8,  9,
    10, 11, 12, 13, 14, 15, 16, 17, 18, 19
};

#define R(r)    static_cast<ResourceID>(r)

#define RESOLVE_INPUT(...) \
    resolveInputValuation(__FILE__, __LINE__, ##__VA_ARGS__, R(INVALID_RESOURCE_ID))

#define ASSERT_INPUT(inputValuation, expectedFirstOutputValuation,...) \
    assertInputValuation(__FILE__, __LINE__, inputValuation, expectedFirstOutputValuation, ##__VA_ARGS__, R(INVALID_RESOURCE_ID))

#define RESOLVE_OUTPUT(inputValuation, multiplicity, ...) \
    resolveOutputValuation(__FILE__, __LINE__, inputValuation, multiplicity, ##__VA_ARGS__, R(INVALID_RESOURCE_ID))

#define ASSERT_OUTPUT(outputValuation, expectedInputValuation, expectedNextOutputValuation, expectedMultiplicity,...) \
    assertOutputValuation(__FILE__, __LINE__, outputValuation, expectedInputValuation, expectedNextOutputValuation, expectedMultiplicity, ##__VA_ARGS__, R(INVALID_RESOURCE_ID))

always_inline static uint16_t low(const uint8_t depth, const uint8_t maxDepth) {
    return (depth <= maxDepth ? 1 : INVALID_RESOURCE_ID);
}

always_inline static uint16_t high(const uint8_t depth, const uint8_t maxDepth) {
    return (depth <= maxDepth ? 20 : INVALID_RESOURCE_ID);
}

TEST(testInput) {
    for (uint8_t maxDepth = 0; maxDepth <= 4; ++maxDepth) {
        initializeCache(maxDepth, 0);
        for (ResourceID v1 = low(1, maxDepth); v1 <= high(1, maxDepth); ++v1)
            for (ResourceID v2 = low(2, maxDepth); v2 <= high(2, maxDepth); ++v2)
                for (ResourceID v3 = low(3, maxDepth); v3 <= high(3, maxDepth); ++v3)
                    for (ResourceID v4 = low(4, maxDepth); v4 <= high(4, maxDepth); ++v4) {
                        InputValuation* inputValuation1 = RESOLVE_INPUT(v1, v2, v3, v4);
                        ASSERT_EQUAL(inputValuation1, RESOLVE_INPUT(v1, v2, v3, v4));
                        ASSERT_INPUT(inputValuation1, 0, v1, v2, v3, v4);
                    }
    }
}

TEST(testOutput) {
    for (uint8_t maxDepth = 0; maxDepth <= 4; ++maxDepth) {
        initializeCache(4, maxDepth);
        InputValuation* inputValuation1 = RESOLVE_INPUT(R(1), R(2), R(3), R(4));
        InputValuation* inputValuation2 = RESOLVE_INPUT(R(4), R(3), R(2), R(1));
        OutputValuation* lastOutputValuation1 = 0;
        OutputValuation* lastOutputValuation2 = 0;
        for (ResourceID v1 = low(1, maxDepth); v1 <= high(1, maxDepth); ++v1)
            for (ResourceID v2 = low(2, maxDepth); v2 <= high(2, maxDepth); ++v2)
                for (ResourceID v3 = low(3, maxDepth); v3 <= high(3, maxDepth); ++v3)
                    for (ResourceID v4 = low(4, maxDepth); v4 <= high(4, maxDepth); ++v4) {
                        // Testing inputValuation1
                        OutputValuation* outputValuation1 = RESOLVE_OUTPUT(inputValuation1, 5, v1, v2, v3, v4);
                        ASSERT_EQUAL(outputValuation1, inputValuation1->getFirstOutputValuation());

                        ASSERT_EQUAL(outputValuation1, RESOLVE_OUTPUT(inputValuation1, 11, v1, v2, v3, v4));
                        ASSERT_EQUAL(outputValuation1, inputValuation1->getFirstOutputValuation());

                        ASSERT_OUTPUT(outputValuation1, inputValuation1, lastOutputValuation1, 16, v1, v2, v3, v4);
                        lastOutputValuation1 = outputValuation1;

                        // Testing inputValuation2
                        OutputValuation* outputValuation2 = RESOLVE_OUTPUT(inputValuation2, 15, v1, v2, v3, v4);
                        ASSERT_EQUAL(outputValuation2, inputValuation2->getFirstOutputValuation());

                        ASSERT_EQUAL(outputValuation2, RESOLVE_OUTPUT(inputValuation2, 17, v1, v2, v3, v4));
                        ASSERT_EQUAL(outputValuation2, inputValuation2->getFirstOutputValuation());

                        ASSERT_OUTPUT(outputValuation2, inputValuation2, lastOutputValuation2, 32, v1, v2, v3, v4);
                        lastOutputValuation2 = outputValuation2;

                        // Output valuations should not be equal
                        ASSERT_NOT_EQUAL(outputValuation1, outputValuation2);
                    }
    }
}

#endif
