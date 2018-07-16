// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef STATISTICSCOUNTERS_H_
#define STATISTICSCOUNTERS_H_

#include "../Common.h"

class StatisticsCounters {

protected:

    const size_t m_numberOfCountersPerLevel;
    const char* const* m_counterDescriptions;
    size_t m_numberOfLevels;
    std::unique_ptr<size_t[]> m_counters;

public:

    StatisticsCounters(const size_t numberOfCountersPerLevel, const char* const* counterDescriptions, const size_t initialNumberOfLevels);

    StatisticsCounters(const StatisticsCounters& other);

    StatisticsCounters(StatisticsCounters&& other);

    StatisticsCounters& operator+=(const StatisticsCounters& other);

    void reset();

    void ensureLevelExists(const size_t levelIndex);

    always_inline size_t getNumberOfCountersPerLevel() const {
        return m_numberOfCountersPerLevel;
    }

    always_inline const char* getCounterDescription(const size_t counterIndex) const {
        return m_counterDescriptions[counterIndex];
    }

    always_inline size_t getNumberOfLevels() const {
        return m_numberOfLevels;
    }

    always_inline size_t* operator[](const size_t levelIndex) {
        return m_counters.get() + levelIndex * m_numberOfCountersPerLevel;
    }

    always_inline const size_t* operator[](const size_t levelIndex) const {
        return m_counters.get() + levelIndex * m_numberOfCountersPerLevel;
    }

};

extern std::ostream& operator<<(std::ostream& output, const StatisticsCounters& statisticsCounters);

#endif /* STATISTICSCOUNTERS_H_ */
