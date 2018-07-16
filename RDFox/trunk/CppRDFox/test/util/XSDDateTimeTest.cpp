// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifdef WITH_TEST

#define AUTO_TEST XSDDateTimeTest

#include <CppTest/AutoTest.h>

#include "../../src/util/XSDDateTime.h"
#include "../../src/util/XSDDuration.h"
#include "../../src/RDFStoreException.h"

class XSDDateTimeTest {

public:

    static XSDDuration D(const std::string& durationValue) {
        return XSDDuration::parseDuration(durationValue);
    }

    static XSDDateTime DT(const std::string& dateTimeValue, const DatatypeID datatypeID) {
        switch (datatypeID) {
        case D_XSD_DATE_TIME:
            return XSDDateTime::parseDateTime(dateTimeValue);
        case D_XSD_TIME:
            return XSDDateTime::parseTime(dateTimeValue);
        case D_XSD_DATE:
            return XSDDateTime::parseDate(dateTimeValue);
        case D_XSD_G_YEAR_MONTH:
            return XSDDateTime::parseGYearMonth(dateTimeValue);
        case D_XSD_G_YEAR:
            return XSDDateTime::parseGYear(dateTimeValue);
        case D_XSD_G_MONTH_DAY:
            return XSDDateTime::parseGMonthDay(dateTimeValue);
        case D_XSD_G_DAY:
            return XSDDateTime::parseGDay(dateTimeValue);
        case D_XSD_G_MONTH:
            return XSDDateTime::parseGMonth(dateTimeValue);
        default:
            UNREACHABLE;
        }
    }

    static void assertDateTime(const char* const fileName, const long lineNumber, const std::string& dateTimeValue, const DatatypeID expectedDatatypeID, const int32_t expectedYear, const uint8_t expectedMonth, const uint8_t expectedDay, const uint8_t expectedHour, const uint8_t expectedMinute, const uint8_t expectedSecond, const uint16_t expectedMillisecond, const int16_t expectedTimeZoneOffset, const std::string& expectedLexicalValue) {
        const XSDDateTime dateTime = DT(dateTimeValue, expectedDatatypeID);
        assertDateTime(fileName, lineNumber, dateTime, expectedDatatypeID, expectedYear, expectedMonth, expectedDay, expectedHour, expectedMinute, expectedSecond, expectedMillisecond, expectedTimeZoneOffset, dateTime);
        CppTest::assertEqual(expectedLexicalValue, dateTime.toString(), fileName, lineNumber);
    }

    static void assertDateTime(const char* const fileName, const long lineNumber, const XSDDateTime& dateTime, const DatatypeID expectedDatatypeID, const int32_t expectedYear, const uint8_t expectedMonth, const uint8_t expectedDay, const uint8_t expectedHour, const uint8_t expectedMinute, const uint8_t expectedSecond, const uint16_t expectedMillisecond, const int16_t expectedTimeZoneOffset, const XSDDateTime& dummy) {
        CppTest::assertEqual(static_cast<int32_t>(expectedDatatypeID), static_cast<int32_t>(dateTime.getDatatypeID()), fileName, lineNumber);
        CppTest::assertEqual(expectedYear, dateTime.getYear(), fileName, lineNumber);
        CppTest::assertEqual(static_cast<int32_t>(expectedMonth), static_cast<int32_t>(dateTime.getMonth()), fileName, lineNumber);
        CppTest::assertEqual(static_cast<int32_t>(expectedDay), static_cast<int32_t>(dateTime.getDay()), fileName, lineNumber);
        CppTest::assertEqual(static_cast<int32_t>(expectedHour), static_cast<int32_t>(dateTime.getHour()), fileName, lineNumber);
        CppTest::assertEqual(static_cast<int32_t>(expectedMinute), static_cast<int32_t>(dateTime.getMinute()), fileName, lineNumber);
        CppTest::assertEqual(static_cast<int32_t>(expectedSecond), static_cast<int32_t>(dateTime.getSecond()), fileName, lineNumber);
        CppTest::assertEqual(static_cast<int32_t>(expectedMillisecond), static_cast<int32_t>(dateTime.getMillisecond()), fileName, lineNumber);
        CppTest::assertEqual(static_cast<int32_t>(expectedTimeZoneOffset), static_cast<int32_t>(dateTime.getTimeZoneOffset()), fileName, lineNumber);
    }


    static void assertDateTimeError(const char* const fileName, const long lineNumber, const std::string& dateTimeValue, const DatatypeID datatypeID) {
        try {
            DT(dateTimeValue, datatypeID);
            std::ostringstream buffer;
            buffer << "Parsing of the value '" << dateTimeValue << "' should have failed.";
            throw CppTest::AssertionError(fileName, lineNumber, buffer.str());
        }
        catch (const RDFStoreException&) {
        }
    }

    static void assertDateTimeOrdering(const char* const fileName, const long lineNumber, const std::string& dateTimeValue1, const DatatypeID datatypeID1, const std::string& dateTimeValue2, const DatatypeID datatypeID2, const XSDDateTimeComparison expectedDateTimeComparison) {
        const XSDDateTime dateTime1 = DT(dateTimeValue1, datatypeID1);
        const XSDDateTime dateTime2 = DT(dateTimeValue2, datatypeID2);
        const XSDDateTimeComparison dateTimeComparison = dateTime1.compare(dateTime2);
        CppTest::assertEqual(expectedDateTimeComparison, dateTimeComparison, fileName, lineNumber);
    }

};

#define ASSERT_DATE_TIME(dateTime, expectedDatatypeID, expectedYear, expectedMonth, expectedDay, expectedHour, expectedMinute, expectedSecond, expectedMillisecond, expectedTimeZoneOffset) \
    assertDateTime(__FILE__, __LINE__, dateTime, expectedDatatypeID, expectedYear, expectedMonth, expectedDay, expectedHour, expectedMinute, expectedSecond, expectedMillisecond, expectedTimeZoneOffset, dateTime)

#define ASSERT_DATE_TIME2(dateTime, expectedDatatypeID, expectedYear, expectedMonth, expectedDay, expectedHour, expectedMinute, expectedSecond, expectedMillisecond, expectedTimeZoneOffset, expectedLexicalValue) \
    assertDateTime(__FILE__, __LINE__, dateTime, expectedDatatypeID, expectedYear, expectedMonth, expectedDay, expectedHour, expectedMinute, expectedSecond, expectedMillisecond, expectedTimeZoneOffset, expectedLexicalValue)

#define ASSERT_DATE_TIME_ERROR(dateTimeValue, datatypeID) \
    assertDateTimeError(__FILE__, __LINE__, dateTimeValue, datatypeID)

#define ASSERT_LESS_THAN(dateTimeValue1, datatypeID1, dateTimeValue2, datatypeID2) \
    assertDateTimeOrdering(__FILE__, __LINE__, dateTimeValue1, datatypeID1, dateTimeValue2, datatypeID2, XSD_DATE_TIME_LESS_THAN)

#define ASSERT_SAME(dateTimeValue1, datatypeID1, dateTimeValue2, datatypeID2) \
    assertDateTimeOrdering(__FILE__, __LINE__, dateTimeValue1, datatypeID1, dateTimeValue2, datatypeID2, XSD_DATE_TIME_EQUAL)

#define ASSERT_GREATER_THAN(dateTimeValue1, datatypeID1, dateTimeValue2, datatypeID2) \
    assertDateTimeOrdering(__FILE__, __LINE__, dateTimeValue1, datatypeID1, dateTimeValue2, datatypeID2, XSD_DATE_TIME_GREATER_THAN)

#define ASSERT_INCOMPARABLE(dateTimeValue1, datatypeID1, dateTimeValue2, datatypeID2) \
    assertDateTimeOrdering(__FILE__, __LINE__, dateTimeValue1, datatypeID1, dateTimeValue2, datatypeID2, XSD_DATE_TIME_INCOMPARABLE)

TEST(testDateTime) {
    ASSERT_DATE_TIME("1965-12-04T13:23:45", D_XSD_DATE_TIME, 1965, 12, 4, 13, 23, 45, 0, TIME_ZONE_OFFSET_ABSENT);
    ASSERT_DATE_TIME2("-1324-09-05T12:20:09.12", D_XSD_DATE_TIME, -1324, 9, 5, 12, 20, 9, 120, TIME_ZONE_OFFSET_ABSENT, "-1324-09-05T12:20:09.120");
    ASSERT_DATE_TIME("-1324-09-05T12:20:09.256Z", D_XSD_DATE_TIME, -1324, 9, 5, 12, 20, 9, 256, 0);
    ASSERT_DATE_TIME2("1324-09-05T24:00:00-12:45", D_XSD_DATE_TIME, 1324, 9, 6, 0, 0, 0, 0, -765, "1324-09-06T00:00:00-12:45");
    ASSERT_DATE_TIME2("-1324-09-05T24:00:00-12:45", D_XSD_DATE_TIME, -1324, 9, 6, 0, 0, 0, 0, -765, "-1324-09-06T00:00:00-12:45");
    ASSERT_DATE_TIME("1324-09-05T14:23:26.145+12:45", D_XSD_DATE_TIME, 1324, 9, 5, 14, 23, 26, 145, 765);
    ASSERT_DATE_TIME2("-0024-09-05T24:00:00-12:45", D_XSD_DATE_TIME, -24, 9, 6, 0, 0, 0, 0, -765, "-0024-09-06T00:00:00-12:45");
    ASSERT_DATE_TIME2("0000-01-01T00:00:00Z", D_XSD_DATE_TIME, 0, 1, 1, 0, 0, 0, 0, 0, "0000-01-01T00:00:00Z");

    ASSERT_DATE_TIME_ERROR("-0000-01-01T00:00:00Z", D_XSD_DATE_TIME);
    ASSERT_DATE_TIME_ERROR("0000-00-00T00:00:00Z", D_XSD_DATE_TIME);
    ASSERT_DATE_TIME_ERROR("001965-10-10T13:23:45", D_XSD_DATE_TIME);
    ASSERT_DATE_TIME_ERROR("965-10-10T13:23:45", D_XSD_DATE_TIME);
    ASSERT_DATE_TIME_ERROR("-001965-10-10T13:23:45", D_XSD_DATE_TIME);
    ASSERT_DATE_TIME_ERROR("-965-10-10T13:23:45", D_XSD_DATE_TIME);
    ASSERT_DATE_TIME_ERROR("1965-2-04T13:23:45", D_XSD_DATE_TIME);
    ASSERT_DATE_TIME_ERROR("1965-002-04T13:23:45", D_XSD_DATE_TIME);
    ASSERT_DATE_TIME_ERROR("1965-02-4T13:23:45", D_XSD_DATE_TIME);
    ASSERT_DATE_TIME_ERROR("1965-2-T13:23:45", D_XSD_DATE_TIME);
    ASSERT_DATE_TIME_ERROR("1965-2-444T13:23:45", D_XSD_DATE_TIME);
    ASSERT_DATE_TIME_ERROR("1965-2T13-23:45", D_XSD_DATE_TIME);
    ASSERT_DATE_TIME_ERROR("1965-12-12T13-23:45D", D_XSD_DATE_TIME);
    ASSERT_DATE_TIME_ERROR("1965-12-12T13-23:45+1:01", D_XSD_DATE_TIME);
    ASSERT_DATE_TIME_ERROR("1965-12-12T13-23:45-01:1", D_XSD_DATE_TIME);
    ASSERT_DATE_TIME_ERROR("1965-02-13Z", D_XSD_DATE_TIME);
    ASSERT_DATE_TIME_ERROR("1965-10-10T24:00:01", D_XSD_DATE_TIME);
}

TEST(testTime) {
    ASSERT_DATE_TIME("12:03:56", D_XSD_TIME, YEAR_ABSENT, MONTH_ABSENT, DAY_ABSENT, 12, 3, 56, 0, TIME_ZONE_OFFSET_ABSENT);
    ASSERT_DATE_TIME("12:03:56Z", D_XSD_TIME, YEAR_ABSENT, MONTH_ABSENT, DAY_ABSENT, 12, 3, 56, 0, 0);
    ASSERT_DATE_TIME("12:03:56+14:00", D_XSD_TIME, YEAR_ABSENT, MONTH_ABSENT, DAY_ABSENT, 12, 3, 56, 0, 840);
    ASSERT_DATE_TIME("12:03:56-14:00", D_XSD_TIME, YEAR_ABSENT, MONTH_ABSENT, DAY_ABSENT, 12, 3, 56, 0, -840);
    ASSERT_DATE_TIME2("24:00:00-14:00", D_XSD_TIME, YEAR_ABSENT, MONTH_ABSENT, DAY_ABSENT, 0, 0, 0, 0, -840, "00:00:00-14:00");

    ASSERT_DATE_TIME_ERROR("24:00:01", D_XSD_TIME);
    ASSERT_DATE_TIME_ERROR("-24:00:01", D_XSD_TIME);
    ASSERT_DATE_TIME_ERROR("T24:00:01", D_XSD_TIME);
}

TEST(testDate) {
    ASSERT_DATE_TIME("1965-12-04", D_XSD_DATE, 1965, 12, 4, HOUR_ABSENT, MINUTE_ABSENT, SECOND_ABSENT, 0, TIME_ZONE_OFFSET_ABSENT);
    ASSERT_DATE_TIME("-1324-09-05", D_XSD_DATE, -1324, 9, 5, HOUR_ABSENT, MINUTE_ABSENT, SECOND_ABSENT, 0, TIME_ZONE_OFFSET_ABSENT);
    ASSERT_DATE_TIME("-1324-09-05Z", D_XSD_DATE, -1324, 9, 5, HOUR_ABSENT, MINUTE_ABSENT, SECOND_ABSENT, 0, 0);
    ASSERT_DATE_TIME("1324-09-05-12:45", D_XSD_DATE, 1324, 9, 5, HOUR_ABSENT, MINUTE_ABSENT, SECOND_ABSENT, 0, -765);
    ASSERT_DATE_TIME("-1324-09-15+06:45", D_XSD_DATE, -1324, 9, 15, HOUR_ABSENT, MINUTE_ABSENT, SECOND_ABSENT, 0, 405);

    ASSERT_DATE_TIME_ERROR("1965-10-10T13:23:45", D_XSD_DATE);
    ASSERT_DATE_TIME_ERROR("1965-10-", D_XSD_DATE);
    ASSERT_DATE_TIME_ERROR("1965-10", D_XSD_DATE);
}

TEST(testGYearMonth) {
    ASSERT_DATE_TIME("0045-12", D_XSD_G_YEAR_MONTH, 45, 12, DAY_ABSENT, HOUR_ABSENT, MINUTE_ABSENT, SECOND_ABSENT, 0, TIME_ZONE_OFFSET_ABSENT);
    ASSERT_DATE_TIME("0045-12Z", D_XSD_G_YEAR_MONTH, 45, 12, DAY_ABSENT, HOUR_ABSENT, MINUTE_ABSENT, SECOND_ABSENT, 0, 0);
    ASSERT_DATE_TIME("0045-07+03:00", D_XSD_G_YEAR_MONTH, 45, 7, DAY_ABSENT, HOUR_ABSENT, MINUTE_ABSENT, SECOND_ABSENT, 0, 180);

    ASSERT_DATE_TIME_ERROR("0045-07+03:", D_XSD_G_YEAR_MONTH);
}

TEST(testGYear) {
    ASSERT_DATE_TIME("0045", D_XSD_G_YEAR, 45, MONTH_ABSENT, DAY_ABSENT, HOUR_ABSENT, MINUTE_ABSENT, SECOND_ABSENT, 0, TIME_ZONE_OFFSET_ABSENT);
    ASSERT_DATE_TIME("0045Z", D_XSD_G_YEAR, 45, MONTH_ABSENT, DAY_ABSENT, HOUR_ABSENT, MINUTE_ABSENT, SECOND_ABSENT, 0, 0);
    ASSERT_DATE_TIME("0045+03:00", D_XSD_G_YEAR, 45, MONTH_ABSENT, DAY_ABSENT, HOUR_ABSENT, MINUTE_ABSENT, SECOND_ABSENT, 0, 180);
    ASSERT_DATE_TIME2("-0045", D_XSD_G_YEAR, -45, MONTH_ABSENT, DAY_ABSENT, HOUR_ABSENT, MINUTE_ABSENT, SECOND_ABSENT, 0, TIME_ZONE_OFFSET_ABSENT, "-0045");

    ASSERT_DATE_TIME_ERROR("0045-03:", D_XSD_G_YEAR);
}

TEST(testGMonthDay) {
    ASSERT_DATE_TIME("--12-31", D_XSD_G_MONTH_DAY, YEAR_ABSENT, 12, 31, HOUR_ABSENT, MINUTE_ABSENT, SECOND_ABSENT, 0, TIME_ZONE_OFFSET_ABSENT);
    ASSERT_DATE_TIME("--12-31Z", D_XSD_G_MONTH_DAY, YEAR_ABSENT, 12, 31, HOUR_ABSENT, MINUTE_ABSENT, SECOND_ABSENT, 0, 0);
    ASSERT_DATE_TIME("--12-31+02:00", D_XSD_G_MONTH_DAY, YEAR_ABSENT, 12, 31, HOUR_ABSENT, MINUTE_ABSENT, SECOND_ABSENT, 0, 120);
    ASSERT_DATE_TIME("--02-29+02:00", D_XSD_G_MONTH_DAY, YEAR_ABSENT, 2, 29, HOUR_ABSENT, MINUTE_ABSENT, SECOND_ABSENT, 0, 120);

    ASSERT_DATE_TIME_ERROR("--02-03+", D_XSD_G_MONTH_DAY);
    ASSERT_DATE_TIME_ERROR("--02-30+02:00", D_XSD_G_MONTH_DAY);
}

TEST(testGDay) {
    ASSERT_DATE_TIME("---31", D_XSD_G_DAY, YEAR_ABSENT, MONTH_ABSENT, 31, HOUR_ABSENT, MINUTE_ABSENT, SECOND_ABSENT, 0, TIME_ZONE_OFFSET_ABSENT);
    ASSERT_DATE_TIME("---31Z", D_XSD_G_DAY, YEAR_ABSENT, MONTH_ABSENT, 31, HOUR_ABSENT, MINUTE_ABSENT, SECOND_ABSENT, 0, 0);
    ASSERT_DATE_TIME("---31+02:20", D_XSD_G_DAY, YEAR_ABSENT, MONTH_ABSENT, 31, HOUR_ABSENT, MINUTE_ABSENT, SECOND_ABSENT, 0, 140);

    ASSERT_DATE_TIME_ERROR("---32", D_XSD_G_DAY);
}

TEST(testGMonth) {
    ASSERT_DATE_TIME("--12", D_XSD_G_MONTH, YEAR_ABSENT, 12, DAY_ABSENT, HOUR_ABSENT, MINUTE_ABSENT, SECOND_ABSENT, 0, TIME_ZONE_OFFSET_ABSENT);
    ASSERT_DATE_TIME("--12Z", D_XSD_G_MONTH, YEAR_ABSENT, 12, DAY_ABSENT, HOUR_ABSENT, MINUTE_ABSENT, SECOND_ABSENT, 0, 0);
    ASSERT_DATE_TIME("--12+04:00", D_XSD_G_MONTH, YEAR_ABSENT, 12, DAY_ABSENT, HOUR_ABSENT, MINUTE_ABSENT, SECOND_ABSENT, 0, 240);

    ASSERT_DATE_TIME_ERROR("--13", D_XSD_G_MONTH);
    ASSERT_DATE_TIME_ERROR("---1", D_XSD_G_MONTH);
    ASSERT_DATE_TIME_ERROR("---01", D_XSD_G_MONTH);
}

TEST(testDateTimeOrdering) {
    ASSERT_LESS_THAN("-1324-09-05T12:20:09.12", D_XSD_DATE_TIME, "-1324-09-05T12:20:09.121", D_XSD_DATE_TIME);
    ASSERT_GREATER_THAN("-1324-09-05T12:20:09.121", D_XSD_DATE_TIME, "-1324-09-05T12:20:09.120", D_XSD_DATE_TIME);
    ASSERT_LESS_THAN("-1324-09-05T12:20:09.12Z", D_XSD_DATE_TIME, "-1324-09-05T12:20:09.121Z", D_XSD_DATE_TIME);
    ASSERT_GREATER_THAN("-1324-09-05T12:20:09.121Z", D_XSD_DATE_TIME, "-1324-09-05T12:20:09.120Z", D_XSD_DATE_TIME);
    ASSERT_GREATER_THAN("1324-09-05T12:20:09.12Z", D_XSD_DATE_TIME, "1324-09-05T12:20:09.121+00:01", D_XSD_DATE_TIME);
    ASSERT_INCOMPARABLE("-1324-09-05T12:20:09.12Z", D_XSD_DATE_TIME, "-1324-09-05T12:20:09.121", D_XSD_DATE_TIME);
    ASSERT_INCOMPARABLE("-1324-09-05T12:20:09.12", D_XSD_DATE_TIME, "-1324-09-05T12:20:09.121Z", D_XSD_DATE_TIME);
}

TEST(testDateTimeDurationArithmetic) {
    ASSERT_DATE_TIME(DT("1965-12-04T13:23:45", D_XSD_DATE_TIME).addDuration(D("P3M6DT0.6S")), D_XSD_DATE_TIME, 1966, 3, 10, 13, 23, 45, 600, TIME_ZONE_OFFSET_ABSENT);
    ASSERT_DATE_TIME(DT("1965-12-04T13:23:45Z", D_XSD_DATE_TIME).addDuration(D("-P3M6DT0.6S")), D_XSD_DATE_TIME, 1965, 8, 29, 13, 23, 44, 400, 0);
    ASSERT_DATE_TIME(DT("1965-00:04", D_XSD_G_YEAR).addDuration(D("P15M6DT5H0.6S")), D_XSD_G_YEAR, 1966, MONTH_ABSENT, DAY_ABSENT, HOUR_ABSENT, MINUTE_ABSENT, SECOND_ABSENT, 0, -4);
}

#endif
