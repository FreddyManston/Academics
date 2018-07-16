// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef XSDDATETIME_H_
#define XSDDATETIME_H_

#include "../Common.h"

const int32_t YEAR_ABSENT = 0x7fffffff;
const uint8_t MONTH_ABSENT = 0xff;
const uint8_t DAY_ABSENT = 0xff;
const uint8_t HOUR_ABSENT = 0xff;
const uint8_t MINUTE_ABSENT = 0xff;
const uint8_t SECOND_ABSENT = 0xff;
const int16_t TIME_ZONE_OFFSET_ABSENT = 0x7fff;

class XSDDuration;

enum XSDDateTimeComparison { XSD_DATE_TIME_LESS_THAN, XSD_DATE_TIME_EQUAL, XSD_DATE_TIME_GREATER_THAN, XSD_DATE_TIME_INCOMPARABLE };

class XSDDateTime {

protected:

    // The order of elements obeys data packing constraints!
    int64_t m_timeOnTimeLine;
    int32_t m_year;
    int16_t m_timeZoneOffset;
    uint16_t m_secondMillisecond;
    uint8_t m_month;
    uint8_t m_day;
    uint8_t m_hour;
    uint8_t m_minute;
    uint32_t m_filler;

public:

    static const size_t BUFFER_SIZE = sizeof(int64_t) + sizeof(int32_t) + sizeof(int16_t) + sizeof(uint16_t) + sizeof(uint8_t) + sizeof(uint8_t) + sizeof(uint8_t) + sizeof(uint8_t);

    static uint8_t daysInMonth(const int32_t year, const uint8_t month);

    static int64_t getTimeOnTimeLine(const int32_t year, const uint8_t month, const uint8_t day, const uint8_t hour, const uint8_t minute, const uint8_t second, const uint16_t millisecond, const int16_t timeZoneOffset);

    static void normalizeMonth(int32_t& year, int32_t& month);

    static void normalizeDay(int32_t& year, int32_t& month, int32_t& day);

    static void normalizeSecond(int32_t& year, int32_t& month, int32_t& day, int32_t& hour, int32_t& minute, int32_t& second, int32_t& millisecond);

    static void initialize(XSDDateTime* dateTime, const int32_t year, const uint8_t month, const uint8_t day, const uint8_t hour, const uint8_t minute, const uint8_t second, const uint16_t millisecond, const int16_t timeZoneOffset);

    static XSDDateTime parseDateTime(const std::string& value);

    static XSDDateTime parseTime(const std::string& value);

    static XSDDateTime parseDate(const std::string& value);

    static XSDDateTime parseGYearMonth(const std::string& value);

    static XSDDateTime parseGYear(const std::string& value);

    static XSDDateTime parseGMonthDay(const std::string& value);

    static XSDDateTime parseGDay(const std::string& value);

    static XSDDateTime parseGMonth(const std::string& value);

    XSDDateTime(const int32_t year, const uint8_t month, const uint8_t day, const uint8_t hour, const uint8_t minute, const uint8_t second, const uint16_t millisecond, const int16_t timeZoneOffset);

    always_inline int32_t getYear() const {
        return m_year;
    }

    always_inline uint8_t getMonth() const {
        return m_month;
    }

    always_inline uint8_t getDay() const {
        return m_day;
    }

    always_inline uint8_t getHour() const {
        return m_hour;
    }

    always_inline uint8_t getMinute() const {
        return m_minute;
    }

    always_inline uint8_t getSecond() const {
        return isSecondAbsent() ? SECOND_ABSENT : m_secondMillisecond / 1000;
    }

    always_inline uint16_t getMillisecond() const {
        return isSecondAbsent() ? 0 : m_secondMillisecond % 1000;
    }

    always_inline int16_t getTimeZoneOffset() const {
        return m_timeZoneOffset;
    }

    always_inline bool isYearAbsent() const {
        return m_year == YEAR_ABSENT;
    }

    always_inline bool isMonthAbsent() const {
        return m_month == MONTH_ABSENT;
    }

    always_inline bool isDayAbsent() const {
        return m_day == DAY_ABSENT;
    }

    always_inline bool isHourAbsent() const {
        return m_hour == HOUR_ABSENT;
    }

    always_inline bool isMinuteAbsent() const {
        return m_minute == MINUTE_ABSENT;
    }

    always_inline bool isSecondAbsent() const {
        return m_secondMillisecond == 0xffff;
    }

    always_inline bool isTimeZoneOffsetAbsent() const {
        return m_timeZoneOffset == TIME_ZONE_OFFSET_ABSENT;
    }

    always_inline int64_t getTimeOnTimeLine() const {
        return m_timeOnTimeLine;
    }

    XSDDateTime addDuration(const XSDDuration& duration) const;

    XSDDateTimeComparison compare(const XSDDateTime& other) const;

    DatatypeID getDatatypeID() const;

    std::string toString() const;

    always_inline void load(const uint8_t* const buffer) {
        m_timeOnTimeLine = *reinterpret_cast<const int64_t*>(buffer);
        m_year = *reinterpret_cast<const int32_t*>(buffer + sizeof(int64_t));
        m_timeZoneOffset = *reinterpret_cast<const int32_t*>(buffer + sizeof(int64_t) + sizeof(int32_t));
        m_secondMillisecond = *reinterpret_cast<const int32_t*>(buffer + sizeof(int64_t) + sizeof(int32_t) + sizeof(int16_t));
        m_month = *reinterpret_cast<const int32_t*>(buffer + sizeof(int64_t) + sizeof(int32_t) + sizeof(int16_t) + sizeof(uint16_t));
        m_day = *reinterpret_cast<const int32_t*>(buffer + sizeof(int64_t) + sizeof(int32_t) + sizeof(int16_t) + sizeof(uint16_t) + sizeof(uint8_t));
        m_hour = *reinterpret_cast<const int32_t*>(buffer + sizeof(int64_t) + sizeof(int32_t) + sizeof(int16_t) + sizeof(uint16_t) + sizeof(uint8_t) + sizeof(uint8_t));
        m_minute = *reinterpret_cast<const int32_t*>(buffer + sizeof(int64_t) + sizeof(int32_t) + sizeof(int16_t) + sizeof(uint16_t) + sizeof(uint8_t) + sizeof(uint8_t) + sizeof(uint8_t));
    }

    always_inline void save(uint8_t* const buffer) {
        *reinterpret_cast<int64_t*>(buffer) = m_timeOnTimeLine;
        *reinterpret_cast<int32_t*>(buffer + sizeof(int64_t)) = m_year;
        *reinterpret_cast<int32_t*>(buffer + sizeof(int64_t) + sizeof(int32_t)) = m_timeZoneOffset;
        *reinterpret_cast<int32_t*>(buffer + sizeof(int64_t) + sizeof(int32_t) + sizeof(int16_t)) = m_secondMillisecond;
        *reinterpret_cast<int32_t*>(buffer + sizeof(int64_t) + sizeof(int32_t) + sizeof(int16_t) + sizeof(uint16_t)) = m_month;
        *reinterpret_cast<int32_t*>(buffer + sizeof(int64_t) + sizeof(int32_t) + sizeof(int16_t) + sizeof(uint16_t) + sizeof(uint8_t)) = m_day;
        *reinterpret_cast<int32_t*>(buffer + sizeof(int64_t) + sizeof(int32_t) + sizeof(int16_t) + sizeof(uint16_t) + sizeof(uint8_t) + sizeof(uint8_t)) = m_hour;
        *reinterpret_cast<int32_t*>(buffer + sizeof(int64_t) + sizeof(int32_t) + sizeof(int16_t) + sizeof(uint16_t) + sizeof(uint8_t) + sizeof(uint8_t) + sizeof(uint8_t)) = m_minute;
    }
    
    always_inline bool operator==(const XSDDateTime& other) const {
        return m_year == other.m_year &&
               m_month == other.m_month &&
               m_day == other.m_day &&
               m_hour == other.m_hour &&
               m_minute == other.m_minute &&
               m_secondMillisecond == other.m_secondMillisecond &&
               m_timeZoneOffset == other.m_timeZoneOffset;
    }

    always_inline bool operator!=(const XSDDateTime& other) const {
        return !(*this == other);
    }

};

namespace std {

template<>
struct hash<XSDDateTime> {
    always_inline size_t operator()(const XSDDateTime& key) const {
        return static_cast<size_t>(key.getTimeOnTimeLine());
    }
};

}

#endif /* XSDDATETIME_H_ */
