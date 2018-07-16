// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../RDFStoreException.h"
#include "XSDDateTime.h"
#include "XSDDuration.h"

uint8_t XSDDateTime::daysInMonth(const int32_t year, const uint8_t month) {
    switch (month) {
    case 4:
    case 6:
    case 9:
    case 11:
        return 30;
    case 1:
    case 3:
    case 5:
    case 7:
    case 8:
    case 10:
    case 12:
        return 31;
    case 2:
        if ((year == YEAR_ABSENT) || (year % 4 != 0) || (year % 100 == 0 && year % 400 != 0))
            return 28;
        else
            return 29;
    default:
        UNREACHABLE;
    }
}

int64_t XSDDateTime::getTimeOnTimeLine(const int32_t year, const uint8_t month, const uint8_t day, const uint8_t hour, const uint8_t minute, const uint8_t second, const uint16_t millisecond, const int16_t timeZoneOffset) {
    const int32_t yrPlusOne = (year == YEAR_ABSENT ? 1972 : year);
    const int64_t yr = yrPlusOne - 1;
    const uint8_t mo = (month == MONTH_ABSENT ? 12 : month);
    const uint8_t da = (day == DAY_ABSENT ? daysInMonth(yrPlusOne, mo) : day) - 1;
    const uint8_t hr = (hour == HOUR_ABSENT ? 0 : hour);
    const uint8_t mi = (minute == MINUTE_ABSENT ? 0 : minute) - (timeZoneOffset == TIME_ZONE_OFFSET_ABSENT ? 0 : timeZoneOffset);
    const uint8_t se = (second == SECOND_ABSENT ? 0 : second);
    // First sum the days, so that multiplication with 86400 can be done only once
    int64_t result = (yr / 400 - yr / 100 + yr / 4) + da;
    for (uint8_t m = 1; m < mo; ++m)
        result += daysInMonth(yrPlusOne, m);
    result = 31536000LL * yr + 86400LL * result;
    result += 3600LL * hr + 60LL * mi + se;
    result = result * 1000 + (second == SECOND_ABSENT ? 0 : millisecond);
    return result;
}

static always_inline void normalizeToRange(int32_t& value, int32_t& carry, const uint16_t range) {
    carry += value / range;
    value = value % range;
    if (value < 0) {
        value += range;
        --carry;
    }
}

void XSDDateTime::normalizeMonth(int32_t& year, int32_t& month) {
    --month;
    normalizeToRange(month, year, 12);
    ++month;
}

void XSDDateTime::normalizeDay(int32_t& year, int32_t& month, int32_t& day) {
    normalizeMonth(year, month);
    if (day <= 0) {
        while (day <= 0) {
            --month;
            normalizeMonth(year, month);
            day += daysInMonth(year, month);
        }
    }
    else {
        uint8_t daysInMonthYear;
        while (day > (daysInMonthYear = daysInMonth(year, month))) {
            day -= daysInMonthYear;
            ++month;
            normalizeMonth(year, month);
        }
    }
}

void XSDDateTime::normalizeSecond(int32_t& year, int32_t& month, int32_t& day, int32_t& hour, int32_t& minute, int32_t& second, int32_t& millisecond) {
    normalizeToRange(millisecond, second, 1000);
    normalizeToRange(second, minute, 60);
    normalizeToRange(minute, hour, 60);
    normalizeToRange(hour, day, 24);
    normalizeDay(year, month, day);
}

static always_inline bool isWhitespace(const char c) {
    return c == ' ' || c == '\t' || c == '\r' || c == '\n';
}

static always_inline std::string getErrorMessage(const std::string& value, const char* const errorMessage) {
    std::ostringstream buffer;
    buffer << "Error parsing value '" << value << "': " << errorMessage << ".";
    return buffer.str();
}

static always_inline int32_t parseInteger(const std::string& value, const size_t startPosition, const size_t endPosition, const char* const errorMessage) {
    std::string subValue(value, startPosition, endPosition - startPosition);
    char* end;
    errno = 0;
    const int32_t result = static_cast<int32_t>(::strtol(subValue.c_str(), &end, 10));
    if (errno == ERANGE || subValue.length() == 0 || *end != '\0')
        throw RDF_STORE_EXCEPTION(::getErrorMessage(value, errorMessage));
    return result;
}

static always_inline int32_t parseTwoDigitInteger(const std::string& value, const size_t startPosition, const size_t endPosition, const char* const errorMessage) {
    if (endPosition != startPosition + 2 || !::isdigit(value[startPosition]) || !::isdigit(value[startPosition + 1]))
        throw RDF_STORE_EXCEPTION(::getErrorMessage(value, errorMessage));
    return ::parseInteger(value, startPosition, endPosition, errorMessage);
}

static always_inline void moveToNextNonDigit(const std::string& value, size_t& position) {
    const size_t length = value.length();
    while (position < length && ::isdigit(value[position]))
        ++position;
}

static always_inline void ensureCharacter(const std::string& value, size_t& position, char expectedCharacter, const char* const errorMessage) {
    if (position >= value.length() || value[position] != expectedCharacter)
        throw RDF_STORE_EXCEPTION(::getErrorMessage(value, errorMessage));
    ++position;
}

static int32_t parseYear(const std::string& value, size_t& position) {
    int32_t sign;
    if (value[position] == '-') {
        sign = -1;
        ++position;
    }
    else
        sign = 1;
    const size_t yearStart = position;
    ::moveToNextNonDigit(value, position);
    const size_t numberOfDigits = position - yearStart;
    if (numberOfDigits < 4 || (numberOfDigits > 4 && value[yearStart] == '0'))
        throw RDF_STORE_EXCEPTION(::getErrorMessage(value, "the year part of the value is not formatted properly"));
    for (size_t index = yearStart; index < position; ++index)
        if (!::isdigit(value[index]))
            throw RDF_STORE_EXCEPTION(::getErrorMessage(value, "the year part of the value is not formatted properly"));
    const int32_t yearPositive = ::parseInteger(value, yearStart, position, "the year part of the value is not formatted properly");
    if (yearPositive == 0 && sign == -1)
        throw RDF_STORE_EXCEPTION(::getErrorMessage(value, "the year part of the value is not formatted properly"));
    return sign * yearPositive;
}

static int32_t parseMonth(const std::string& value, size_t& position) {
    const size_t monthStart = position;
    ::moveToNextNonDigit(value, position);
    const int32_t month = parseTwoDigitInteger(value, monthStart, position, "the month part of the value is not formatted properly");
    if (month < 1 || month > 12)
        throw RDF_STORE_EXCEPTION(::getErrorMessage(value, "the month part of the value is invalid"));
    return month;
}

static int32_t parseDay(const std::string& value, size_t& position) {
    const size_t dayStart = position;
    ::moveToNextNonDigit(value, position);
    const int32_t day = parseTwoDigitInteger(value, dayStart, position, "the day part of the value is not formatted properly");
    if (day < 1 || day > 31)
        throw RDF_STORE_EXCEPTION(::getErrorMessage(value, "the day part of the value is invalid"));
    return day;
}

static int32_t parseHour(const std::string& value, size_t& position) {
    const size_t hourStart = position;
    ::moveToNextNonDigit(value, position);
    const int32_t hour = parseTwoDigitInteger(value, hourStart, position, "the hour part of the value is not formatted properly");
    if (hour < 0 || hour > 24)
        throw RDF_STORE_EXCEPTION(::getErrorMessage(value, "the hour part of the value is invalid"));
    return hour;
}

static int32_t parseMinute(const std::string& value, size_t& position) {
    const size_t minuteStart = position;
    ::moveToNextNonDigit(value, position);
    const int32_t minute = parseTwoDigitInteger(value, minuteStart, position, "the minute part of the value is not formatted properly");
    if (minute < 0 || minute > 59)
        throw RDF_STORE_EXCEPTION(::getErrorMessage(value, "the minute part of the value is invalid"));
    return minute;
}

static void parseSecond(const std::string& value, size_t& position, int32_t& second, int32_t& millisecond) {
    const size_t secondStart = position;
    ::moveToNextNonDigit(value, position);
    second = ::parseTwoDigitInteger(value, secondStart, position, "the second part of the value is not formatted properly");
    if (second < 0 || second > 59)
        throw RDF_STORE_EXCEPTION(::getErrorMessage(value, "the second part of the value is invalid"));
    if (position < value.length() && value[position] == '.') {
        ++position;
        const size_t millisecondStart = position;
        ::moveToNextNonDigit(value, position);
        millisecond = ::parseInteger(value, millisecondStart, position, "the millisecond part of the value is not formatted properly");
        if (millisecond < 0)
            throw RDF_STORE_EXCEPTION(::getErrorMessage(value, "the millisecond part of the value is invalid"));
        const size_t numberOfDigits = position - millisecondStart;
        for (size_t index = numberOfDigits; index < 3; ++index)
            millisecond *= 10;
        for (size_t index = 4; index <= numberOfDigits; ++index)
            millisecond /= 10;
        if (millisecond >= 1000)
            throw RDF_STORE_EXCEPTION(::getErrorMessage(value, "the millisecond part of the value is invalid"));
    }
    else
        millisecond = 0;
}

static int16_t parseTimeZoneOffset(const std::string& value, size_t& position) {
    int16_t timezoneOffset;
    if (position >= value.length() || ::isWhitespace(value[position]))
        timezoneOffset = TIME_ZONE_OFFSET_ABSENT;
    else if (value[position] == 'Z') {
        timezoneOffset = 0;
        ++position;
    }
    else {
        int32_t sign;
        if (value[position] == '-')
            sign = -1;
        else if (value[position] == '+')
            sign = 1;
        else
            throw RDF_STORE_EXCEPTION(::getErrorMessage(value, "the time zone offset has an invalid value"));
        ++position;
        const size_t hoursStart = position;
        ::moveToNextNonDigit(value, position);
        const int32_t hours = ::parseTwoDigitInteger(value, hoursStart, position, "the hours part of the time zone offset is not formatted properly");
        ::ensureCharacter(value, position, ':', "the time zone offset does not contain ':' in the right place");
        const size_t minutesStart = position;
        ::moveToNextNonDigit(value, position);
        const int32_t minutes = ::parseTwoDigitInteger(value, minutesStart, position, "the minutes part of the time zone offset is not formatted properly");
        ::moveToNextNonDigit(value, position);
        if (hours < 0 || hours > 14 || minutes < 0 || minutes > 59 || (hours == 14 && minutes !=0))
            throw RDF_STORE_EXCEPTION(::getErrorMessage(value, "the time zone offset has an invalid value"));
        timezoneOffset = sign * (hours * 60 + minutes);
    }
    return timezoneOffset;
}

static always_inline void skipWhiteSpace(const std::string& value, size_t& position) {
    while (position < value.length() && ::isWhitespace(value[position]))
        ++position;
}

static always_inline void findValueStart(const std::string& value, size_t& position) {
    ::skipWhiteSpace(value, position);
    if (position >= value.length())
        throw RDF_STORE_EXCEPTION(::getErrorMessage(value, "there value is empty"));
}

static always_inline void ensureAtEnd(const std::string& value, size_t& position) {
    ::skipWhiteSpace(value, position);
    if (position != value.length())
        throw RDF_STORE_EXCEPTION(::getErrorMessage(value, "there are excess characters at the end of the value"));
}

void XSDDateTime::initialize(XSDDateTime* dateTime, const int32_t year, const uint8_t month, const uint8_t day, const uint8_t hour, const uint8_t minute, const uint8_t second, const uint16_t millisecond, const int16_t timeZoneOffset) {
    dateTime->m_timeOnTimeLine = getTimeOnTimeLine(year, month, day, hour, minute, second, millisecond, timeZoneOffset);
    dateTime->m_year = year;
    dateTime->m_timeZoneOffset = timeZoneOffset;
    dateTime->m_secondMillisecond = (second == SECOND_ABSENT ? 0xffff : static_cast<uint16_t>(second) * static_cast<uint16_t>(1000) + millisecond);
    dateTime->m_month = month;
    dateTime->m_day = day;
    dateTime->m_hour = hour;
    dateTime->m_minute = minute;
    dateTime->m_filler = 0;
}

XSDDateTime XSDDateTime::parseDateTime(const std::string& value) {
    size_t position = 0;
    ::findValueStart(value, position);
    int32_t year = ::parseYear(value, position);
    ::ensureCharacter(value, position, '-', "the date part does not contain '-' between the year and the month");
    int32_t month = ::parseMonth(value, position);
    ::ensureCharacter(value, position, '-', "the date part does not contain '-' between the month and the day");
    int32_t day = ::parseDay(value, position);
    if (day > daysInMonth(year, static_cast<uint8_t>(month)))
        throw RDF_STORE_EXCEPTION(::getErrorMessage(value, "invalid day of the month for the given year"));
    ::ensureCharacter(value, position, 'T', "separator 'T' is not in the right place");
    int32_t hour = ::parseHour(value, position);
    ::ensureCharacter(value, position, ':', "the date part does not contain ':' between the hour and the minute");
    int32_t minute = ::parseMinute(value, position);
    ::ensureCharacter(value, position, ':', "the date part does not contain ':' between the minute and the second");
    int32_t second;
    int32_t millisecond;
    ::parseSecond(value, position, second, millisecond);
    if (hour == 24 && (minute != 0 || second != 0 || millisecond != 0))
        throw RDF_STORE_EXCEPTION(::getErrorMessage(value, "invalid time value"));
    int32_t timeZoneOffset = ::parseTimeZoneOffset(value, position);
    ::ensureAtEnd(value, position);
    normalizeSecond(year, month, day, hour, minute, second, millisecond);
    return XSDDateTime(year, static_cast<uint8_t>(month), static_cast<uint8_t>(day), static_cast<uint8_t>(hour), static_cast<uint8_t>(minute), static_cast<uint8_t>(second), static_cast<uint16_t>(millisecond), static_cast<int16_t>(timeZoneOffset));
}

XSDDateTime XSDDateTime::parseTime(const std::string& value) {
    size_t position = 0;
    ::findValueStart(value, position);
    int32_t year = 1;
    int32_t month = 1;
    int32_t day = 1;
    int32_t hour = ::parseHour(value, position);
    ::ensureCharacter(value, position, ':', "the date part does not contain ':' between the hour and the minute");
    int32_t minute = ::parseMinute(value, position);
    ::ensureCharacter(value, position, ':', "the date part does not contain ':' between the minute and the second");
    int32_t second;
    int32_t millisecond;
    ::parseSecond(value, position, second, millisecond);
    if (hour == 24 && (minute != 0 || second != 0 || millisecond != 0))
        throw RDF_STORE_EXCEPTION(::getErrorMessage(value, "invalid time value"));
    int32_t timeZoneOffset = ::parseTimeZoneOffset(value, position);
    ::ensureAtEnd(value, position);
    normalizeSecond(year, month, day, hour, minute, second, millisecond);
    return XSDDateTime(YEAR_ABSENT, MONTH_ABSENT, DAY_ABSENT, static_cast<uint8_t>(hour), static_cast<uint8_t>(minute), static_cast<uint8_t>(second), static_cast<uint16_t>(millisecond), static_cast<int16_t>(timeZoneOffset));
}

XSDDateTime XSDDateTime::parseDate(const std::string& value) {
    size_t position = 0;
    ::findValueStart(value, position);
    int32_t year = ::parseYear(value, position);
    ::ensureCharacter(value, position, '-', "the date part does not contain '-' between the year and the month");
    int32_t month = ::parseMonth(value, position);
    ::ensureCharacter(value, position, '-', "the date part does not contain '-' between the month and the day");
    int32_t day = ::parseDay(value, position);
    if (day > daysInMonth(year, static_cast<uint8_t>(month)))
        throw RDF_STORE_EXCEPTION(::getErrorMessage(value, "invalid day of the month for the given year"));
    int32_t hour = 0;
    int32_t minute = 0;
    int32_t second = 0;
    int32_t millisecond = 0;
    int32_t timeZoneOffset = ::parseTimeZoneOffset(value, position);
    ::ensureAtEnd(value, position);
    normalizeSecond(year, month, day, hour, minute, second, millisecond);
    return XSDDateTime(year, static_cast<uint8_t>(month), static_cast<uint8_t>(day), HOUR_ABSENT, MINUTE_ABSENT, SECOND_ABSENT, 0, static_cast<int16_t>(timeZoneOffset));
}

XSDDateTime XSDDateTime::parseGYearMonth(const std::string& value) {
    size_t position = 0;
    ::findValueStart(value, position);
    int32_t year = ::parseYear(value, position);
    ::ensureCharacter(value, position, '-', "the date part does not contain '-' between the year and the month");
    int32_t month = ::parseMonth(value, position);
    int32_t day = 1;
    int32_t hour = 0;
    int32_t minute = 0;
    int32_t second = 0;
    int32_t millisecond = 0;
    int32_t timeZoneOffset = ::parseTimeZoneOffset(value, position);
    ::ensureAtEnd(value, position);
    normalizeSecond(year, month, day, hour, minute, second, millisecond);
    return XSDDateTime(year, static_cast<uint8_t>(month), DAY_ABSENT, HOUR_ABSENT, MINUTE_ABSENT, SECOND_ABSENT, 0, static_cast<int16_t>(timeZoneOffset));
}

XSDDateTime XSDDateTime::parseGYear(const std::string& value) {
    size_t position = 0;
    ::findValueStart(value, position);
    int32_t year = ::parseYear(value, position);
    int32_t month = 1;
    int32_t day = 1;
    int32_t hour = 0;
    int32_t minute = 0;
    int32_t second = 0;
    int32_t millisecond = 0;
    int32_t timeZoneOffset = ::parseTimeZoneOffset(value, position);
    ::ensureAtEnd(value, position);
    normalizeSecond(year, month, day, hour, minute, second, millisecond);
    return XSDDateTime(year, MONTH_ABSENT, DAY_ABSENT, HOUR_ABSENT, MINUTE_ABSENT, SECOND_ABSENT, 0, static_cast<int16_t>(timeZoneOffset));
}

XSDDateTime XSDDateTime::parseGMonthDay(const std::string& value) {
    size_t position = 0;
    ::findValueStart(value, position);
    ::ensureCharacter(value, position, '-', "the value does not start with '--'");
    ::ensureCharacter(value, position, '-', "the value does not start with '--'");
    int32_t year = 4; // This is different from the spec and is so to make the 'current' year leap year and thus allow for the '--02-29' value.
    int32_t month = ::parseMonth(value, position);
    ::ensureCharacter(value, position, '-', "the date part does not contain '-' between the month and the day");
    int32_t day = ::parseDay(value, position);
    if (day > daysInMonth(4, static_cast<uint8_t>(month)))
        throw RDF_STORE_EXCEPTION(::getErrorMessage(value, "invalid day of the month"));
    int32_t hour = 0;
    int32_t minute = 0;
    int32_t second = 0;
    int32_t millisecond = 0;
    int32_t timeZoneOffset = ::parseTimeZoneOffset(value, position);
    ::ensureAtEnd(value, position);
    normalizeSecond(year, month, day, hour, minute, second, millisecond);
    return XSDDateTime(YEAR_ABSENT, static_cast<uint8_t>(month), static_cast<uint8_t>(day), HOUR_ABSENT, MINUTE_ABSENT, SECOND_ABSENT, 0, static_cast<int16_t>(timeZoneOffset));
}

XSDDateTime XSDDateTime::parseGDay(const std::string& value) {
    size_t position = 0;
    ::findValueStart(value, position);
    ::ensureCharacter(value, position, '-', "the value does not start with '---'");
    ::ensureCharacter(value, position, '-', "the value does not start with '---'");
    ::ensureCharacter(value, position, '-', "the value does not start with '---'");
    int32_t year = 1;
    int32_t month = 1;
    int32_t day = ::parseDay(value, position);
    int32_t hour = 0;
    int32_t minute = 0;
    int32_t second = 0;
    int32_t millisecond = 0;
    int32_t timeZoneOffset = ::parseTimeZoneOffset(value, position);
    ::ensureAtEnd(value, position);
    normalizeSecond(year, month, day, hour, minute, second, millisecond);
    return XSDDateTime(YEAR_ABSENT, MONTH_ABSENT, static_cast<uint8_t>(day), HOUR_ABSENT, MINUTE_ABSENT, SECOND_ABSENT, 0, static_cast<int16_t>(timeZoneOffset));
}

XSDDateTime XSDDateTime::parseGMonth(const std::string& value) {
    size_t position = 0;
    ::findValueStart(value, position);
    ::ensureCharacter(value, position, '-', "the value does not start with '--'");
    ::ensureCharacter(value, position, '-', "the value does not start with '--'");
    int32_t year = 1;
    int32_t month = ::parseMonth(value, position);
    int32_t day = 1;
    int32_t hour = 0;
    int32_t minute = 0;
    int32_t second = 0;
    int32_t millisecond = 0;
    int32_t timeZoneOffset = ::parseTimeZoneOffset(value, position);
    ::ensureAtEnd(value, position);
    normalizeSecond(year, month, day, hour, minute, second, millisecond);
    return XSDDateTime(YEAR_ABSENT, static_cast<uint8_t>(month),DAY_ABSENT, HOUR_ABSENT, MINUTE_ABSENT, SECOND_ABSENT, 0, static_cast<int16_t>(timeZoneOffset));
}

XSDDateTime::XSDDateTime(const int32_t year, const uint8_t month, const uint8_t day, const uint8_t hour, const uint8_t minute, const uint8_t second, const uint16_t millisecond, const int16_t timeZoneOffset) :
    m_timeOnTimeLine(getTimeOnTimeLine(year, month, day, hour, minute, second, millisecond, timeZoneOffset)),
    m_year(year),
    m_timeZoneOffset(timeZoneOffset),
    m_secondMillisecond(second == SECOND_ABSENT ? 0xffff : static_cast<uint16_t>(second) * static_cast<uint16_t>(1000) + millisecond),
    m_month(month),
    m_day(day),
    m_hour(hour),
    m_minute(minute),
    m_filler(0)
{
}

XSDDateTime XSDDateTime::addDuration(const XSDDuration& duration) const {
    int32_t yr = (isYearAbsent() ? 1 : getYear());
    int32_t mo = (isMonthAbsent() ? 1 : getMonth());
    int32_t da = (isDayAbsent() ? 1 : getDay());
    int32_t hr = (isHourAbsent() ? 0 : getHour());
    int32_t mi (isMinuteAbsent() ? 0 : getMinute());
    int32_t se = (isSecondAbsent() ? 0 : getSecond());
    int32_t ms = (isSecondAbsent() ? 0 : getMillisecond());
    mo += duration.getMonths();
    normalizeMonth(yr, mo);
    const uint8_t dim = daysInMonth(yr, mo);
    da = (da < dim ? da : dim);
    se += duration.getSeconds();
    ms += duration.getMilliseconds();
    normalizeSecond(yr, mo, da, hr, mi, se, ms);
    return XSDDateTime(isYearAbsent() ? YEAR_ABSENT : yr, isMonthAbsent() ? MONTH_ABSENT : mo, isDayAbsent() ? DAY_ABSENT : da, isHourAbsent() ? HOUR_ABSENT : hr, isMinuteAbsent() ? MINUTE_ABSENT : mi, isSecondAbsent() ? SECOND_ABSENT : se, isSecondAbsent() ? 0 : ms, m_timeZoneOffset);
}

XSDDateTimeComparison XSDDateTime::compare(const XSDDateTime& that) const {
    XSDDateTimeComparison result;
    if (m_timeOnTimeLine == that.m_timeOnTimeLine) {
        result = XSD_DATE_TIME_EQUAL;
        if (isTimeZoneOffsetAbsent() != that.isTimeZoneOffsetAbsent())
            result = XSD_DATE_TIME_INCOMPARABLE;
    }
    else if (m_timeOnTimeLine < that.m_timeOnTimeLine) {
        result = XSD_DATE_TIME_LESS_THAN;
        if (isTimeZoneOffsetAbsent()) {
            if (!that.isTimeZoneOffsetAbsent() && m_timeOnTimeLine + 840 * 60 * 1000 >= that.m_timeOnTimeLine)
                result = XSD_DATE_TIME_INCOMPARABLE;
        }
        else {
            if (that.isTimeZoneOffsetAbsent() && m_timeOnTimeLine >= that.m_timeOnTimeLine - 840 * 60 * 1000)
                result = XSD_DATE_TIME_INCOMPARABLE;
        }
    }
    else {
        result = XSD_DATE_TIME_GREATER_THAN;
        if (isTimeZoneOffsetAbsent()) {
            if (!that.isTimeZoneOffsetAbsent() && m_timeOnTimeLine - 840 * 60 * 1000 <= that.m_timeOnTimeLine)
                result = XSD_DATE_TIME_INCOMPARABLE;
        }
        else {
            if (that.isTimeZoneOffsetAbsent() && m_timeOnTimeLine <= that.m_timeOnTimeLine + 840 * 60 * 1000)
                result = XSD_DATE_TIME_INCOMPARABLE;
        }
    }
    return result;
}

DatatypeID XSDDateTime::getDatatypeID() const {
    static const DatatypeID s_datatypeIDsByComponents[64] = {
                                    // Y M D H M S
        D_INVALID_DATATYPE_ID,      // - - - - - -
        D_INVALID_DATATYPE_ID,      // - - - - - +
        D_INVALID_DATATYPE_ID,      // - - - - + -
        D_INVALID_DATATYPE_ID,      // - - - - + +
        D_INVALID_DATATYPE_ID,      // - - - + - -
        D_INVALID_DATATYPE_ID,      // - - - + - +
        D_INVALID_DATATYPE_ID,      // - - - + + -
        D_XSD_TIME,                 // - - - + + +
        D_XSD_G_DAY,                // - - + - - -
        D_INVALID_DATATYPE_ID,      // - - + - - +
        D_INVALID_DATATYPE_ID,      // - - + - + -
        D_INVALID_DATATYPE_ID,      // - - + - + +
        D_INVALID_DATATYPE_ID,      // - - + + - -
        D_INVALID_DATATYPE_ID,      // - - + + - +
        D_INVALID_DATATYPE_ID,      // - - + + + -
        D_INVALID_DATATYPE_ID,      // - - + + + +
        D_XSD_G_MONTH,              // - + - - - -
        D_INVALID_DATATYPE_ID,      // - + - - - +
        D_INVALID_DATATYPE_ID,      // - + - - + -
        D_INVALID_DATATYPE_ID,      // - + - - + +
        D_INVALID_DATATYPE_ID,      // - + - + - -
        D_INVALID_DATATYPE_ID,      // - + - + - +
        D_INVALID_DATATYPE_ID,      // - + - + + -
        D_INVALID_DATATYPE_ID,      // - + - + + +
        D_XSD_G_MONTH_DAY,          // - + + - - -
        D_INVALID_DATATYPE_ID,      // - + + - - +
        D_INVALID_DATATYPE_ID,      // - + + - + -
        D_INVALID_DATATYPE_ID,      // - + + - + +
        D_INVALID_DATATYPE_ID,      // - + + + - -
        D_INVALID_DATATYPE_ID,      // - + + + - +
        D_INVALID_DATATYPE_ID,      // - + + + + -
        D_INVALID_DATATYPE_ID,      // - + + + + +
        D_XSD_G_YEAR,               // + - - - - -
        D_INVALID_DATATYPE_ID,      // + - - - - +
        D_INVALID_DATATYPE_ID,      // + - - - + -
        D_INVALID_DATATYPE_ID,      // + - - - + +
        D_INVALID_DATATYPE_ID,      // + - - + - -
        D_INVALID_DATATYPE_ID,      // + - - + - +
        D_INVALID_DATATYPE_ID,      // + - - + + -
        D_INVALID_DATATYPE_ID,      // + - - + + +
        D_INVALID_DATATYPE_ID,      // + - + - - -
        D_INVALID_DATATYPE_ID,      // + - + - - +
        D_INVALID_DATATYPE_ID,      // + - + - + -
        D_INVALID_DATATYPE_ID,      // + - + - + +
        D_INVALID_DATATYPE_ID,      // + - + + - -
        D_INVALID_DATATYPE_ID,      // + - + + - +
        D_INVALID_DATATYPE_ID,      // + - + + + -
        D_INVALID_DATATYPE_ID,      // + - + + + +
        D_XSD_G_YEAR_MONTH,         // + + - - - -
        D_INVALID_DATATYPE_ID,      // + + - - - +
        D_INVALID_DATATYPE_ID,      // + + - - + -
        D_INVALID_DATATYPE_ID,      // + + - - + +
        D_INVALID_DATATYPE_ID,      // + + - + - -
        D_INVALID_DATATYPE_ID,      // + + - + - +
        D_INVALID_DATATYPE_ID,      // + + - + + -
        D_INVALID_DATATYPE_ID,      // + + - + + +
        D_XSD_DATE,                 // + + + - - -
        D_INVALID_DATATYPE_ID,      // + + + - - +
        D_INVALID_DATATYPE_ID,      // + + + - + -
        D_INVALID_DATATYPE_ID,      // + + + - + +
        D_INVALID_DATATYPE_ID,      // + + + + - -
        D_INVALID_DATATYPE_ID,      // + + + + - +
        D_INVALID_DATATYPE_ID,      // + + + + + -
        D_XSD_DATE_TIME             // + + + + + +
    };

    return s_datatypeIDsByComponents[
        (isYearAbsent()   ? 0 : 32) +
        (isMonthAbsent()  ? 0 : 16) +
        (isDayAbsent()    ? 0 : 8) +
        (isHourAbsent()   ? 0 : 4) +
        (isMinuteAbsent() ? 0 : 2) +
        (isSecondAbsent() ? 0 : 1)
    ];
}

std::string XSDDateTime::toString() const {
    std::ostringstream buffer;
    buffer.fill('0');
    const bool hasTime = !isHourAbsent() && !isMinuteAbsent() && !isSecondAbsent();
    if (!isYearAbsent() || !isMonthAbsent() || !isDayAbsent()) {
        if (!isYearAbsent()) {
            int32_t year = getYear();
            if (year < 0) {
                buffer << '-';
                year = -year;
            }
            buffer.width(4);
            buffer << year;
        }
        else if (!isMonthAbsent() || !isDayAbsent())
            buffer << '-';
        if (!isMonthAbsent()) {
            buffer << '-';
            buffer.width(2);
            buffer << static_cast<int32_t>(getMonth());
        }
        else if (!isDayAbsent())
            buffer << '-';
        if (!isDayAbsent()) {
            buffer << '-';
            buffer.width(2);
            buffer << static_cast<int32_t>(getDay());
        }
        if (hasTime)
            buffer << 'T';
    }
    if (hasTime) {
        buffer.width(2);
        buffer << static_cast<int32_t>(getHour());
        buffer << ':';
        buffer.width(2);
        buffer << static_cast<int32_t>(getMinute());
        buffer << ':';
        buffer.width(2);
        buffer << static_cast<int32_t>(getSecond());
        const uint16_t millisecond = getMillisecond();
        if (millisecond != 0) {
            buffer << '.';
            buffer.width(3);
            buffer << millisecond;
        }
    }
    if (!isTimeZoneOffsetAbsent()) {
        int16_t timeZoneOffset = getTimeZoneOffset();
        if (timeZoneOffset == 0)
            buffer << 'Z';
        else {
            if (timeZoneOffset < 0) {
                buffer << '-';
                timeZoneOffset = -timeZoneOffset;
            }
            else
                buffer << '+';
            buffer.width(2);
            buffer << (timeZoneOffset / 60);
            buffer << ':';
            buffer.width(2);
            buffer << (timeZoneOffset % 60);
        }
    }
    return buffer.str();
}
