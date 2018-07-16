// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "StatisticsCounters.h"

StatisticsCounters::StatisticsCounters(const size_t numberOfCountersPerLevel, const char* const* counterDescriptions, const size_t initialNumberOfLevels) :
    m_numberOfCountersPerLevel(numberOfCountersPerLevel),
    m_counterDescriptions(counterDescriptions),
    m_numberOfLevels(initialNumberOfLevels),
    m_counters(new size_t[m_numberOfLevels * m_numberOfCountersPerLevel])
{
    reset();
}

StatisticsCounters::StatisticsCounters(const StatisticsCounters& other) :
    m_numberOfCountersPerLevel(other.m_numberOfCountersPerLevel),
    m_counterDescriptions(other.m_counterDescriptions),
    m_numberOfLevels(other.m_numberOfLevels),
    m_counters(new size_t[m_numberOfLevels * m_numberOfCountersPerLevel])
{
    const size_t totalNumberOfCounters = m_numberOfLevels * m_numberOfCountersPerLevel;
    for (size_t counterIndex = 0; counterIndex < totalNumberOfCounters; ++counterIndex)
        m_counters[counterIndex] = other.m_counters[counterIndex];
}

StatisticsCounters::StatisticsCounters(StatisticsCounters&& other) :
    m_numberOfCountersPerLevel(other.m_numberOfCountersPerLevel),
    m_counterDescriptions(other.m_counterDescriptions),
    m_numberOfLevels(other.m_numberOfLevels),
    m_counters(std::move(other.m_counters))
{
}

StatisticsCounters& StatisticsCounters::operator+=(const StatisticsCounters& other) {
    const size_t totalNumberOfCounters = m_numberOfLevels * m_numberOfCountersPerLevel;
    for (size_t counterIndex = 0; counterIndex < totalNumberOfCounters; ++counterIndex)
        m_counters[counterIndex] += other.m_counters[counterIndex];
    return *this;
}

void StatisticsCounters::reset() {
    ::memset(m_counters.get(), 0, sizeof(size_t) * m_numberOfLevels * m_numberOfCountersPerLevel);
}

void StatisticsCounters::ensureLevelExists(const size_t levelIndex) {
    if (levelIndex >= m_numberOfLevels) {
        const size_t oldNumberOfCounters = m_numberOfLevels * m_numberOfCountersPerLevel;
        const size_t newNumberOfCounters = (levelIndex + 1) * m_numberOfCountersPerLevel;
        std::unique_ptr<size_t[]> newCounters(new size_t[newNumberOfCounters]);
        ::memcpy(newCounters.get(), m_counters.get(), sizeof(size_t) * oldNumberOfCounters);
        ::memset(newCounters.get() + oldNumberOfCounters, 0, sizeof(size_t) * (newNumberOfCounters - oldNumberOfCounters));
        std::swap(m_counters, newCounters);
        m_numberOfLevels = levelIndex + 1;
    }
}

always_inline static void centerDelimited(std::ostream& output, const char* const text, const size_t textLength, const size_t width) {
    const size_t before = (width - 2 - textLength - 2) / 2;
    const size_t after = width - before - 2 - textLength - 2;
    for (size_t index = 0; index < before; ++index)
        output << '-';
    output << "- " << text << " -";
    for (size_t index = 0; index < after; ++index)
        output << '-';
}

std::ostream& operator<<(std::ostream& output, const StatisticsCounters& statisticsCounters) {
    std::unique_ptr<size_t[]> largestCounterValuePerLevel(new size_t[statisticsCounters.getNumberOfLevels()]);
    ::memset(largestCounterValuePerLevel.get(), 0, sizeof(size_t) * statisticsCounters.getNumberOfLevels());
    std::unique_ptr<size_t[]> counterSum(new size_t[statisticsCounters.getNumberOfCountersPerLevel()]);
    ::memset(counterSum.get(), 0, sizeof(size_t) * statisticsCounters.getNumberOfCountersPerLevel());
    size_t maxCounterSum = 0;
    size_t numberOfPrintedLines = 0;
    size_t maxCounterDescriptionLength = 0;
    for (size_t counterIndex = 0; counterIndex < statisticsCounters.getNumberOfCountersPerLevel(); ++counterIndex) {
        for (size_t levelIndex = 0; levelIndex < statisticsCounters.getNumberOfLevels(); ++levelIndex) {
            const size_t counterValue = statisticsCounters[levelIndex][counterIndex];
            counterSum[counterIndex] += counterValue;
            if (largestCounterValuePerLevel[levelIndex] < counterValue)
                largestCounterValuePerLevel[levelIndex] = counterValue;
        }
        if (counterSum[counterIndex] > maxCounterSum)
            maxCounterSum = counterSum[counterIndex];
        const char* const conterDescription = statisticsCounters.getCounterDescription(counterIndex);
        if (conterDescription[0] != '$') {
            ++numberOfPrintedLines;
            const size_t counterDescriptionLength = ::strlen(conterDescription) - (conterDescription[0] == '-' || conterDescription[0] == '+' ? 1 : 0);
            if (counterDescriptionLength > maxCounterDescriptionLength)
                maxCounterDescriptionLength = counterDescriptionLength;
        }
    }
    const size_t counterSumWidth = ::getNumberOfDigitsFormated(static_cast<uint64_t>(maxCounterSum));
    std::unique_ptr<size_t[]> counterWidthPerLevel(new size_t[statisticsCounters.getNumberOfLevels()]);
    for (size_t levelIndex = 0; levelIndex < statisticsCounters.getNumberOfLevels(); ++levelIndex) {
        // The second argument to std::max encodes the length of 'LEVEL nnn'
        counterWidthPerLevel[levelIndex] = std::max(::getNumberOfDigitsFormated(static_cast<uint64_t>(largestCounterValuePerLevel[levelIndex])), 5 + 1 + ::getNumberOfDigits(static_cast<uint64_t>(levelIndex)));
    }
    const size_t numberOfPrintedLinesDigits = ::getNumberOfDigits(static_cast<uint64_t>(numberOfPrintedLines));
    //                                      ss  line                         s   |   ss  <sum>            sss  (  abbr )   ss  |
    size_t beforeContentDescriptionLength = 2 + numberOfPrintedLinesDigits + 1 + 1 + 2 + counterSumWidth + 3 + 1 + 6 + 1 + 2 + 1;
    //                                      ss  description                   ss
    const size_t counterDescriptionLength = 2 + maxCounterDescriptionLength + 2;
    size_t totalLineLength = beforeContentDescriptionLength + counterDescriptionLength;
    if (statisticsCounters.getNumberOfLevels() > 1) {
        for (size_t levelIndex = 0; levelIndex < statisticsCounters.getNumberOfLevels(); ++levelIndex) {
            //                 |   ss  value                      sss  (  abbr )   ss
            totalLineLength += 1 + 2 + counterWidthPerLevel[levelIndex] + 3 + 1 + 6 + 1 + 2;
        }
    }
    for (size_t columnIndex = 0; columnIndex < totalLineLength; ++ columnIndex)
        output << '=';
    output << std::endl;
    size_t printedLineCounter = 0;
    for (size_t counterIndex = 0; counterIndex < statisticsCounters.getNumberOfCountersPerLevel(); ++counterIndex) {
        const char* const counterDescription = statisticsCounters.getCounterDescription(counterIndex);;
        if (counterDescription[0] == '-') {
            for (size_t index = 0; index < beforeContentDescriptionLength; ++index)
                output << '-';
            if (counterDescription[1] == 0) {
                for (size_t index = 0; index < counterDescriptionLength; ++index)
                    output << '-';
            }
            else
                centerDelimited(output, counterDescription + 1, ::strlen(counterDescription + 1), counterDescriptionLength);
            if (statisticsCounters.getNumberOfLevels() > 1) {
                for (size_t levelIndex = 0; levelIndex < statisticsCounters.getNumberOfLevels(); ++levelIndex) {
                    output << '-';
                    std::ostringstream levelNameBuffer;
                    levelNameBuffer << "LEVEL " << levelIndex;
                    std::string levelName = levelNameBuffer.str();
                    centerDelimited(output, levelName.c_str(), levelName.length(), 2 + counterWidthPerLevel[levelIndex] + 3 + 1 + 6 + 1 + 2);
                }
            }
            output << std::endl;
        }
        else if (counterDescription[0] != '$') {
            output << "  ";
            const std::ostream::fmtflags oldFlags = output.flags();
            output.setf(std::ios::right);
            const std::streamsize oldWidth = output.width();
            output.width(numberOfPrintedLinesDigits);
            output << ++printedLineCounter;
            output.width(oldWidth);
            output.flags(oldFlags);
            output << " |  ";
            ::printNumberFormatted(output, static_cast<uint64_t>(counterSum[counterIndex]), counterSumWidth);
            output << "   (";
            ::printNumberAbbreviated(output, static_cast<uint64_t>(counterSum[counterIndex]));
            output << ")  |  ";
            if (counterDescription[0] == '+')
                output << (counterDescription + 1);
            else {
                output << counterDescription;
                if (statisticsCounters.getNumberOfLevels() > 1) {
                    for (size_t index = ::strlen(counterDescription); index < maxCounterDescriptionLength; ++index)
                        output << ' ';
                    output << "  ";
                    for (size_t levelIndex = 0; levelIndex < statisticsCounters.getNumberOfLevels(); ++levelIndex) {
                        output << "|  ";
                        ::printNumberFormatted(output, static_cast<uint64_t>(statisticsCounters[levelIndex][counterIndex]), counterWidthPerLevel[levelIndex]);
                        output << "   (";
                        ::printNumberAbbreviated(output, static_cast<uint64_t>(statisticsCounters[levelIndex][counterIndex]));
                        output << ")  ";
                    }
                }
            }
            output << std::endl;
        }
    }
    for (size_t columnIndex = 0; columnIndex < totalLineLength; ++ columnIndex)
        output << '=';
    output << std::endl;
    return output;
}
