// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef QUERYEVALUATOR_H_
#define QUERYEVALUATOR_H_

#include "../Common.h"
#include "../logic/Logic.h"
#include "OccurrenceManager.h"
#include "QueryInfo.h"

class IncomingMessage;
class NodeServer;
class ResourceIDMapper;
class TupleIterator;
class Dictionary;
class DataStore;
class TermArray;
class ArgumentIndexSet;
class QueryListener;
class ThreadContext;
class ResourceValueCache;

class QueryEvaluator : private Unmovable {

    friend class QueryInfo;

protected:

    QueryInfo& m_queryInfo;
    OccurrenceManager& m_occurrenceManager;
    ResourceIDMapper& m_resourceIDMapper;
    ResourceValueCache& m_resourceValueCache;
    const size_t m_threadIndex;
    std::vector<ResourceID> m_argumentsBuffer;
    std::vector<uint8_t> m_occurrenceSets;
    std::unique_ptr<uint8_t[]> m_temporaryOccurrenceSet;
    QueryInfo::ConjunctInfo *m_processingPartialBindingsFor;
    uint8_t m_afterLastQueryConjunctIndex;
    uint8_t m_lastQueryConjunctIndex;
    unique_ptr_vector<TupleIterator> m_tupleIteratorsForConjuncts;

    ImmutableOccurrenceSet getOccurrenceSetFor(const ArgumentIndex argumentIndex, const uint8_t position);

    OccurrenceSet getTargetOccurrenceSet(const ArgumentIndex argumentIndex, const uint8_t position);

    void compileLiteral(const TermArray& termArray, const Literal& literal, const ArgumentIndexSet& answerVariables, ArgumentIndexSet& variablesBoundSoFar, const ArgumentIndexSet& variablesAfterConjunct);

    template<bool callMonitor>
    void sendPartialBindingsMessage(const uint8_t conjunctIndex, const NodeID destinationNodeID, const size_t multiplicity);

    template<bool callMonitor>
    void sendStageFinishedMessage(const uint8_t conjunctIndex, const NodeID destinationNodeID);

public:

    QueryEvaluator(QueryInfo& queryInfo, const TermArray& termArray, const std::vector<Literal>& conjuncts, const ArgumentIndexSet& answerVariables, std::vector<ArgumentIndexSet>& variablesAfterConjunct, const size_t threadIndex);

    ~QueryEvaluator();

    void readPartialBindingsMessage(ThreadContext& threadContext, IncomingMessage& message, const uint8_t conjunctIndex, size_t& multiplicity);

    template<bool callMonitor>
    void processPartialBindingsMessage(const uint8_t conjunctIndex, const size_t multiplicity);

    template<bool callMonitor>
    void checkStageFinished(const uint8_t conjunctIndex);

    template<bool callMonitor>
    void matchNext(const uint8_t conjunctIndex, const size_t multiplicitySoFar);

};

#endif // QUERYEVALUATOR_H_
