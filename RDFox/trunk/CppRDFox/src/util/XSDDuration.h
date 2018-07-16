// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef XSDDURATION_H_
#define XSDDURATION_H_

#include "../Common.h"

enum XSDDurationComparison { XSD_DURATION_LESS_THAN, XSD_DURATION_EQUAL, XSD_DURATION_GREATER_THAN, XSD_DURATION_INCOMPARABLE };

class XSDDuration {

protected:

    int64_t m_secondMilliseconds;
    int32_t m_months;
    uint32_t m_filler;

    friend struct std::hash<XSDDuration>;

public:

    static const size_t BUFFER_SIZE = sizeof(int64_t) + sizeof(int32_t);

    static void initialize(XSDDuration* duration, const int32_t months, const int32_t seconds, const int16_t milliseconds);

    static XSDDuration parseDuration(const std::string& value);

    XSDDuration(const int32_t months, const int32_t seconds, const int16_t milliseconds);

    always_inline int32_t getMonths() const {
        return m_months;
    }

    always_inline int32_t getSeconds() const {
        return static_cast<int32_t>(m_secondMilliseconds / 1000);
    }

    always_inline int16_t getMilliseconds() const {
        return static_cast<int16_t>(m_secondMilliseconds % 1000);
    }

    XSDDurationComparison compare(const XSDDuration& other) const;

    std::string toString() const;

    always_inline void load(const uint8_t* const buffer) {
        m_secondMilliseconds = *reinterpret_cast<const int64_t*>(buffer);
        m_months = *reinterpret_cast<const int32_t*>(buffer + sizeof(int64_t));
    }

    always_inline void save(uint8_t* const buffer) {
        *reinterpret_cast<int64_t*>(buffer) = m_secondMilliseconds;
        *reinterpret_cast<int32_t*>(buffer + sizeof(int64_t)) = m_months;
    }
    
    always_inline bool operator==(const XSDDuration& other) const {
        return m_months == other.m_months && m_secondMilliseconds == other.m_secondMilliseconds;
    }

    always_inline bool operator!=(const XSDDuration& other) const {
        return !(*this == other);
    }

};

namespace std {

template<>
struct hash<XSDDuration> {
    always_inline size_t operator()(const XSDDuration& key) const {
        return ::combineHashCodes(key.m_months, key.m_secondMilliseconds);
    }
};

}

#endif /* XSDDURATION_H_ */
