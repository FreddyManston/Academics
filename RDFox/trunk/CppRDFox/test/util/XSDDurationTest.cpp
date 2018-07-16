// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifdef WITH_TEST

#define AUTO_TEST XSDDurationTest

#include <CppTest/AutoTest.h>

#include "../../src/util/XSDDuration.h"
#include "../../src/RDFStoreException.h"

class XSDDurationTest {

public:

    static XSDDuration D(const std::string& durationValue) {
        return XSDDuration::parseDuration(durationValue);
    }

    static void assertDuration(const char* const fileName, const long lineNumber, const std::string& durationValue, const int32_t expectedMonth, const int32_t expectedSecond, const int32_t expectedMillisecond, const std::string& expectedLexicalValue) {
        const XSDDuration duration = D(durationValue);
        assertDuration(fileName, lineNumber, duration, expectedMonth, expectedSecond, expectedMillisecond, expectedLexicalValue);
    }

    static void assertDuration(const char* const fileName, const long lineNumber, const XSDDuration& duration, const int32_t expectedMonth, const int32_t expectedSecond, const int32_t expectedMillisecond, const std::string& expectedLexicalValue) {
        CppTest::assertEqual(expectedMonth, duration.getMonths(), fileName, lineNumber);
        CppTest::assertEqual(expectedSecond, duration.getSeconds(), fileName, lineNumber);
        CppTest::assertEqual(expectedMillisecond, duration.getMilliseconds(), fileName, lineNumber);
        CppTest::assertEqual(expectedLexicalValue, duration.toString(), fileName, lineNumber);
    }

    static void assertDurationError(const char* const fileName, const long lineNumber, const std::string& durationValue) {
        try {
            D(durationValue);
            std::ostringstream buffer;
            buffer << "Parsing of the value '" << durationValue << "' should have failed.";
            throw CppTest::AssertionError(fileName, lineNumber, buffer.str());
        }
        catch (const RDFStoreException&) {
        }
    }

    static void assertDurationOrdering(const char* const fileName, const long lineNumber, const std::string& durationValue1, const std::string& durationValue2, const XSDDurationComparison expectedDurationComparison) {
        const XSDDuration duration1 = D(durationValue1);
        const XSDDuration duration2 = D(durationValue2);
        const XSDDurationComparison durationComparison = duration1.compare(duration2);
        CppTest::assertEqual(expectedDurationComparison, durationComparison, fileName, lineNumber);
    }

};

#define ASSERT_DURATION(durationValue, expectedMonth, expectedSecond, expectedMillisecond) \
    assertDuration(__FILE__, __LINE__, durationValue, expectedMonth, expectedSecond, expectedMillisecond, durationValue)

#define ASSERT_DURATION2(durationValue, expectedMonth, expectedSecond, expectedMillisecond, expectedLexicalValue) \
    assertDuration(__FILE__, __LINE__, durationValue, expectedMonth, expectedSecond, expectedMillisecond, expectedLexicalValue)

#define ASSERT_DURATION_ERROR(durationValue) \
    assertDurationError(__FILE__, __LINE__, durationValue)

#define ASSERT_LESS_THAN(durationValue1, durationValue2) \
    assertDurationOrdering(__FILE__, __LINE__, durationValue1, durationValue2, XSD_DURATION_LESS_THAN)

#define ASSERT_SAME(durationValue1, durationValue2) \
    assertDurationOrdering(__FILE__, __LINE__, durationValue1, durationValue2, XSD_DURATION_EQUAL)

#define ASSERT_GREATER_THAN(durationValue1, durationValue2) \
    assertDurationOrdering(__FILE__, __LINE__, durationValue1, durationValue2, XSD_DURATION_GREATER_THAN)

#define ASSERT_INCOMPARABLE(durationValue1, durationValue2) \
    assertDurationOrdering(__FILE__, __LINE__, durationValue1, durationValue2, XSD_DURATION_INCOMPARABLE)

TEST(testDuration) {
    ASSERT_DURATION("P", 0, 0, 0);
    ASSERT_DURATION2("PT", 0, 0, 0, "P");
    ASSERT_DURATION2(" P1Y2M3DT4H22M10.3S  ", 1 * 12 + 2, 3 * 86400 + 4 * 3600 + 22 * 60 + 10, 300, "P1Y2M3DT4H22M10.300S");
    ASSERT_DURATION2("  -P1Y2M3DT4H22M10.3S  ", -1 * 12 - 2, -3 * 86400 - 4 * 3600 - 22 * 60 - 10, -300, "-P1Y2M3DT4H22M10.300S");
    ASSERT_DURATION2("P1Y3MT", 1 * 12 + 3, 0, 0, "P1Y3M");
    ASSERT_DURATION2("P1Y3MT", 1 * 12 + 3, 0, 0, "P1Y3M");
    ASSERT_DURATION2("P1Y3M", 1 * 12 + 3, 0, 0, "P1Y3M");
    ASSERT_DURATION("PT3H10M5S", 0, 3 * 3600 + 10 * 60 + 5, 0);
    ASSERT_DURATION("-PT3H10M5.563S", 0, -3 * 3600 - 10 * 60 - 5, -563);

    ASSERT_DURATION_ERROR("1Y2M3DT4H22M10.3S");
    ASSERT_DURATION_ERROR("P1.4Y2M3DT4H22M10.3S");
    ASSERT_DURATION_ERROR("PY2M3D4H22M10.3S");
    ASSERT_DURATION_ERROR("-PTY2D4H22M10.3S");
}

TEST(testOrdering) {
    ASSERT_LESS_THAN("P3M", "P3MT0.001S");
    ASSERT_GREATER_THAN("-P3M", "-P3MT0.001S");
    ASSERT_SAME("-P3M", "-P3M");
    ASSERT_INCOMPARABLE("P1M", "P30D");
    ASSERT_INCOMPARABLE("P1Y1M", "P1Y30D");
}

#endif
