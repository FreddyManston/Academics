// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../RDFStoreException.h"
#include "XSDDateTime.h"
#include "XSDDuration.h"

static always_inline std::string getErrorMessage(const std::string& value, const char* const errorMessage) {
    std::ostringstream buffer;
    buffer << "Error parsing value '" << value << "': " << errorMessage << ".";
    return buffer.str();
}

static always_inline bool isWhitespace(const char c) {
    return c == ' ' || c == '\t' || c == '\r' || c == '\n';
}

static always_inline void skipWhiteSpace(const std::string& value, size_t& position) {
    while (position < value.length() && ::isWhitespace(value[position]))
        ++position;
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

static always_inline void findValueStart(const std::string& value, size_t& position) {
    ::skipWhiteSpace(value, position);
    if (position >= value.length())
        throw RDF_STORE_EXCEPTION(::getErrorMessage(value, "there value is empty"));
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

static always_inline void ensureAtEnd(const std::string& value, size_t& position) {
    ::skipWhiteSpace(value, position);
    if (position != value.length())
        throw RDF_STORE_EXCEPTION(::getErrorMessage(value, "there are excess characters at the end of the value"));
}

void XSDDuration::initialize(XSDDuration* duration, const int32_t months, const int32_t seconds, const int16_t milliseconds) {
    duration->m_secondMilliseconds = static_cast<int64_t>(seconds) * 1000LL + milliseconds;
    duration->m_months = months;
    duration->m_filler = 0;
    if ((duration->m_months < 0 && duration->m_secondMilliseconds > 0) || (duration->m_months > 0 && duration->m_secondMilliseconds < 0))
        throw RDF_STORE_EXCEPTION("Months and seconds have a different sign.");

}

XSDDuration XSDDuration::parseDuration(const std::string& value) {
    size_t position = 0;
    ::findValueStart(value, position);
    if (position >= value.length())
        throw RDF_STORE_EXCEPTION(::getErrorMessage(value, "there value is empty"));
    int32_t sign;
    if (value[position] == '-') {
        sign = -1;
        ++position;
    }
    else
        sign = 1;
    ::ensureCharacter(value, position, 'P', "character 'P' is missing");
    int32_t years = 0;
    int32_t months = 0;
    int32_t days = 0;
    int32_t hours = 0;
    int32_t minutes = 0;
    int32_t seconds = 0;
    int32_t milliseconds = 0;
    int lastPart = 0;
    bool timeSeparatorSeen = false;
    while (position < value.length() && !::isWhitespace(value[position])) {
        if (value[position] == 'T') {
            ++position;
            timeSeparatorSeen = true;
        }
        else {
            const size_t startPosition = position;
            moveToNextNonDigit(value, position);
            const int32_t amount = parseInteger(value, startPosition, position, "the amount is formatted incorrectly");
            if (position >= value.length())
                throw RDF_STORE_EXCEPTION(::getErrorMessage(value, "the amount type is missing"));
            const bool decimalPresent = (value[position] == '.');
            int32_t decimalAmount = 0;
            if (decimalPresent) {
                ++position;
                const size_t decimalStart = position;
                moveToNextNonDigit(value, position);
                decimalAmount = parseInteger(value, decimalStart, position, "the amount is formatted incorrectly");
                const size_t numberOfDigits = position - decimalStart;
                for (size_t index = numberOfDigits; index < 3; ++index)
                    decimalAmount *= 10;
                for (size_t index = 4; index <= numberOfDigits; ++index)
                    decimalAmount /= 10;
                if (decimalAmount >= 1000)
                    throw RDF_STORE_EXCEPTION(::getErrorMessage(value, "the decimal part of the value is invalid"));

            }
            int currentPart;
            switch (value[position]) {
            case 'Y':
                currentPart = 1;
                years = amount;
                break;
            case 'M':
                if (timeSeparatorSeen) {
                    currentPart = 5;
                    minutes = amount;
                }
                else {
                    currentPart = 2;
                    months = amount;
                }
                break;
            case 'D':
                currentPart = 3;
                days = amount;
                break;
            case 'H':
                currentPart = 4;
                hours = amount;
                break;
            case 'S':
                currentPart = 6;
                seconds = amount;
                milliseconds = decimalAmount;
                break;
            default:
                throw RDF_STORE_EXCEPTION(::getErrorMessage(value, "the amount type is invalid"));
            }
            ++position;
            if (decimalPresent && currentPart != 6)
                throw RDF_STORE_EXCEPTION(::getErrorMessage(value, "only the seconds amount can be decimal"));
            if (currentPart <= lastPart || (currentPart > 3 && !timeSeparatorSeen))
                throw RDF_STORE_EXCEPTION(::getErrorMessage(value, "amounts are listed in an incorrect order"));
            lastPart = currentPart;
        }
    }
    ::ensureAtEnd(value, position);
    return XSDDuration(sign * (years * 12 + months), sign * (86400 * days + hours * 3600 + minutes * 60 + seconds), static_cast<int16_t>(milliseconds * sign));
}

XSDDuration::XSDDuration(const int32_t months, const int32_t seconds, const int16_t milliseconds) :
    m_secondMilliseconds(static_cast<int64_t>(seconds) * 1000LL + milliseconds), m_months(months), m_filler(0)
{
    if ((m_months < 0 && m_secondMilliseconds > 0) || (m_months > 0 && m_secondMilliseconds < 0))
        throw RDF_STORE_EXCEPTION("Months and seconds have a different sign.");
}

XSDDurationComparison XSDDuration::compare(const XSDDuration& other) const {
    static const XSDDateTime s_dateTime1(1696, 9, 1, 0, 0, 0, 0, 0);
    static const XSDDateTime s_dateTime2(1697, 2, 1, 0, 0, 0, 0, 0);
    static const XSDDateTime s_dateTime3(1903, 3, 1, 0, 0, 0, 0, 0);
    static const XSDDateTime s_dateTime4(1903, 7, 1, 0, 0, 0, 0, 0);

    if (m_months == other.m_months && m_secondMilliseconds == other.m_secondMilliseconds)
        return XSD_DURATION_EQUAL;
    XSDDateTimeComparison result = s_dateTime1.addDuration(*this).compare(s_dateTime1.addDuration(other));
    if (result == s_dateTime2.addDuration(*this).compare(s_dateTime2.addDuration(other)) && result == s_dateTime3.addDuration(*this).compare(s_dateTime3.addDuration(other)) && result == s_dateTime4.addDuration(*this).compare(s_dateTime4.addDuration(other)))
        return static_cast<XSDDurationComparison>(result);
    else
        return XSD_DURATION_INCOMPARABLE;
}

std::string XSDDuration::toString() const {
    std::ostringstream buffer;
    int32_t sign;
    if (m_months < 0 || m_secondMilliseconds < 0) {
        buffer << "-";
        sign = -1;
    }
    else
        sign = 1;
    buffer << "P";
    const int32_t years = (sign * m_months) / 12;
    const int32_t months = (sign * m_months) % 12;
    uint64_t remainder = static_cast<uint64_t>(sign * m_secondMilliseconds);
    const int16_t milliseconds = static_cast<int16_t>(remainder % 1000LL);
    remainder /= 1000LL;
    const int32_t seconds = static_cast<int32_t>(remainder % 60LL);
    remainder /= 60LL;
    const int32_t minutes = static_cast<int32_t>(remainder % 60LL);
    remainder /= 60LL;
    const int32_t hours = static_cast<int32_t>(remainder % 24LL);
    remainder /= 24LL;
    const int32_t days = static_cast<int32_t>(remainder);
    if (years != 0)
        buffer << years << "Y";
    if (months != 0)
        buffer << months << "M";
    if (days != 0)
        buffer << days << "D";
    if (hours != 0 || minutes != 0 || seconds != 0 || milliseconds != 0) {
        buffer << "T";
        if (hours != 0)
            buffer << hours << "H";
        if (minutes != 0)
            buffer << minutes << "M";
        if (seconds != 0) {
            buffer << seconds;
            if (milliseconds !=0)
                buffer << "." << milliseconds;
            buffer << "S";
        }
    }
    return buffer.str();
}
