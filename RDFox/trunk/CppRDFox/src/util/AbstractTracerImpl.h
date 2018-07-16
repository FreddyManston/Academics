// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef ABSTRACTTRACERIMPL_H_
#define ABSTRACTTRACERIMPL_H_

#include "../dictionary/Dictionary.h"
#include "../reasoning/RuleIndex.h"
#include "../storage/DataStore.h"
#include "AbstractTracer.h"

template<class Interface>
always_inline void AbstractTracer<Interface>::increaseIndent(const size_t workerIndex) {
    m_currentIndents[workerIndex] += 4;
}

template<class Interface>
always_inline void AbstractTracer<Interface>::decreaseIndent(const size_t workerIndex) {
    m_currentIndents[workerIndex] -= 4;
}

template<class Interface>
void AbstractTracer<Interface>::printIndent(const size_t workerIndex) {
    std::streamsize oldWidth = m_output.width(3);
    std::ios_base::fmtflags oldFlags = m_output.flags(m_output.flags() | std::ios_base::right);
    m_output << workerIndex;
    m_output.width(oldWidth);
    m_output.flags(oldFlags);
    m_output << ":    ";
    for (size_t index = 0; index < m_currentIndents[workerIndex]; ++index)
        m_output << ' ';
}

template<class Interface>
void AbstractTracer<Interface>::print(const ResourceID resourceID) {
    ResourceText resourceText;
    if (m_dictionary.getResource(resourceID, resourceText))
        m_output << resourceText.toString(m_prefixes);
    else
        m_output << "UNDEF";

}

template<class Interface>
void AbstractTracer<Interface>::print(const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) {
    m_output << "[ ";
    for (std::vector<ArgumentIndex>::const_iterator iterator = argumentIndexes.begin(); iterator != argumentIndexes.end(); ++iterator) {
        if (iterator != argumentIndexes.begin())
            m_output << ", ";
        print(argumentsBuffer[*iterator]);
    }
    m_output << " ]";
}

template<class Interface>
AbstractTracer<Interface>::AbstractTracer(Prefixes& prefixes, Dictionary& dictionary, std::ostream& output) :
    m_prefixes(prefixes),
    m_dictionary(dictionary),
    m_output(output),
    m_mutex(),
    m_currentIndents()
{
}

template<class Interface>
void AbstractTracer<Interface>::taskStarted(const DataStore& dataStore, const size_t maxComponenetLevel) {
    m_currentIndents.clear();
    m_currentIndents.insert(m_currentIndents.begin(), dataStore.getNumberOfThreads(), 0);
}

template<class Interface>
void AbstractTracer<Interface>::taskFinished(const DataStore& dataStore) {
}

template<class Interface>
void AbstractTracer<Interface>::componentLevelStarted(const size_t componentLevel) {
    MutexHolder mutexHolder(m_mutex);
    if (componentLevel == static_cast<size_t>(-1))
        m_output << "============================================================" << std::endl;
    else {
        m_output << "== LEVEL ";
        const std::ostream::fmtflags oldFlags = m_output.flags();
        m_output.setf(std::ios::right);
        const std::streamsize oldWidth = m_output.width(3);
        m_output << componentLevel;
        m_output.width(oldWidth);
        m_output.flags(oldFlags);
        m_output << " ===============================================" << std::endl;
    }
}

template<class Interface>
void AbstractTracer<Interface>::componentLevelFinished(const size_t componentLevel) {
    MutexHolder mutexHolder(m_mutex);
    m_output << "============================================================" << std::endl;
}

template<class Interface>
void AbstractTracer<Interface>::materializationStarted(const size_t workerIndex) {
}

template<class Interface>
void AbstractTracer<Interface>::materializationFinished(const size_t workerIndex) {
}

template<class Interface>
void AbstractTracer<Interface>::currentTupleExtracted(const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) {
    MutexHolder mutexHolder(m_mutex);
    printIndent(workerIndex);
    m_output << "Extracted current tuple ";
    print(argumentsBuffer, argumentIndexes);
    m_output << std::endl;
    increaseIndent(workerIndex);
}

template<class Interface>
void AbstractTracer<Interface>::currentTupleNormalized(const size_t workerIndex, std::vector<ResourceID>& normalizedArgumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const bool wasAdded) {
    MutexHolder mutexHolder(m_mutex);
    printIndent(workerIndex);
    m_output << "Current tuple normalized to ";
    print(normalizedArgumentsBuffer, argumentIndexes);
    m_output << "    { " << (wasAdded ? "" : "not ") << "added }" << std::endl;
}

template<class Interface>
void AbstractTracer<Interface>::currentTupleProcessed(const size_t workerIndex) {
    MutexHolder mutexHolder(m_mutex);
    decreaseIndent(workerIndex);
}

template<class Interface>
void AbstractTracer<Interface>::bodyLiteralMatchedStarted(const size_t workerIndex, const BodyLiteralInfo& bodyLiteralInfo) {
    MutexHolder mutexHolder(m_mutex);
    printIndent(workerIndex);
    m_output << "Matched atom " << bodyLiteralInfo.getLiteral()->toString(m_prefixes) << " to tuple ";
    print(bodyLiteralInfo.getArgumentsBuffer<0>(workerIndex), bodyLiteralInfo.getArgumentIndexes());
    m_output << std::endl;
    increaseIndent(workerIndex);
}

template<class Interface>
void AbstractTracer<Interface>::bodyLiteralMatchedFinish(const size_t workerIndex) {
    decreaseIndent(workerIndex);
    MutexHolder mutexHolder(m_mutex);
}

template<class Interface>
void AbstractTracer<Interface>::bodyLiteralMatchingStarted(const size_t workerIndex, const BodyLiteralInfo& bodyLiteralInfo) {
    MutexHolder mutexHolder(m_mutex);
    printIndent(workerIndex);
    m_output << "Matching atom " << bodyLiteralInfo.getLiteral()->toString(m_prefixes) << std::endl;
    increaseIndent(workerIndex);
}

template<class Interface>
void AbstractTracer<Interface>::bodyLiteralMatchingFinished(const size_t  workerIndex) {
    MutexHolder mutexHolder(m_mutex);
    decreaseIndent(workerIndex);
}

template<class Interface>
void AbstractTracer<Interface>::ruleReevaluationStarted(const size_t workerIndex, const RuleInfo& ruleInfo) {
    MutexHolder mutexHolder(m_mutex);
    printIndent(workerIndex);
    m_output << "Reevaluating rule " << ruleInfo.getRule()->toString(m_prefixes) << std::endl;
    increaseIndent(workerIndex);
}

template<class Interface>
void AbstractTracer<Interface>::ruleReevaluationFinished(const size_t workerIndex) {
    MutexHolder mutexHolder(m_mutex);
    decreaseIndent(workerIndex);
}

template<class Interface>
void AbstractTracer<Interface>::pivotlessRuleEvaluationStarted(const size_t workerIndex, const RuleInfo& ruleInfo) {
    MutexHolder mutexHolder(m_mutex);
    printIndent(workerIndex);
    m_output << "Evaluating pivotless rule " << ruleInfo.getRule()->toString(m_prefixes) << std::endl;
    increaseIndent(workerIndex);
}

template<class Interface>
void AbstractTracer<Interface>::pivotlessRuleEvaluationFinished(const size_t workerIndex) {
    MutexHolder mutexHolder(m_mutex);
    decreaseIndent(workerIndex);
}

template<class Interface>
void AbstractTracer<Interface>::ruleMatchedStarted(const size_t workerIndex, const RuleInfo& ruleInfo, const BodyLiteralInfo* const lastBodyLiteralInfo, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) {
    MutexHolder mutexHolder(m_mutex);
    printIndent(workerIndex);
    m_output << "Matched rule " << ruleInfo.getRule()->toString(m_prefixes) << std::endl;
    increaseIndent(workerIndex);
}

template<class Interface>
void AbstractTracer<Interface>::ruleMatchedFinished(const size_t workerIndex) {
    MutexHolder mutexHolder(m_mutex);
    decreaseIndent(workerIndex);
}

template<class Interface>
void AbstractTracer<Interface>::tupleDerived(const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const bool isNormal, const bool wasAdded) {
    MutexHolder mutexHolder(m_mutex);
    printIndent(workerIndex);
    m_output << "Derived tuple ";
    print(argumentsBuffer, argumentIndexes);
    m_output << "    { " << (isNormal ? "" : "not ") << "normal, " << (wasAdded ? "" : "not ") << "added }" << std::endl;
}

template<class Interface>
void AbstractTracer<Interface>::constantMerged(const size_t workerIndex, const ResourceID sourceID, const ResourceID targetID, const bool isSuccessful) {
    MutexHolder mutexHolder(m_mutex);
    printIndent(workerIndex);
    m_output << "Merged ";
    print(sourceID);
    m_output << " -> ";
    print(targetID);
    m_output << "    { " << (isSuccessful ? "" : "not ") << "successful }" << std::endl;
}

template<class Interface>
void AbstractTracer<Interface>::reflexiveSameAsTupleDerived(const size_t workerIndex, const ResourceID resourceID, const bool wasAdded) {
    MutexHolder mutexHolder(m_mutex);
    printIndent(workerIndex);
    m_output << "Derived reflexive tuple [ ";
    print(resourceID);
    m_output << " , owl:sameAs, ";
    print(resourceID);
    m_output << " ]    { " << (wasAdded ? "" : "not ") << "added }" << std::endl;
}

template<class Interface>
void AbstractTracer<Interface>::normalizeConstantStarted(const size_t workerIndex, const ResourceID mergedID) {
    MutexHolder mutexHolder(m_mutex);
    printIndent(workerIndex);
    m_output << "Normalizing constant ";
    print(mergedID);
    m_output << std::endl;
    increaseIndent(workerIndex);
}

template<class Interface>
void AbstractTracer<Interface>::tupleNormalized(const size_t workerIndex, std::vector<ResourceID>& originalArgumentsBuffer, const std::vector<ArgumentIndex>& originalArgumentIndexes, std::vector<ResourceID>& normalizedArgumentsBuffer, const std::vector<ArgumentIndex>& normalizedArgumentIndexes, const bool wasAdded) {
    MutexHolder mutexHolder(m_mutex);
    printIndent(workerIndex);
    m_output << "Tuple ";
    print(originalArgumentsBuffer, originalArgumentIndexes);
    m_output << " was normalized to ";
    print(normalizedArgumentsBuffer, normalizedArgumentIndexes);
    m_output << "    { " << (wasAdded ? "" : "not ") << "added }" << std::endl;
}

template<class Interface>
void AbstractTracer<Interface>::normalizeConstantFinished(const size_t workerIndex) {
    MutexHolder mutexHolder(m_mutex);
    decreaseIndent(workerIndex);
}

#endif /* ABSTRACTTRACERIMPL_H_ */
