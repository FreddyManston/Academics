// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../RDFStoreException.h"
#include "../querying/TermArray.h"
#include "../querying/GroupIterator.h"
#include "../dictionary/Dictionary.h"
#include "../storage/DataStore.h"
#include "../storage/TupleTable.h"
#include "../storage/TupleIterator.h"
#include "../builtins/FilterTupleIterator.h"
#include "../builtins/BindTupleIterator.h"
#include "../builtins/BuiltinExpressionEvaluator.h"
#include "../util/ThreadContext.h"
#include "QueryEvaluatorImpl.h"
#include "QueryInfo.h"
#include "NodeServer.h"
#include "NodeServerQueryListener.h"
#include "OccurrenceManagerImpl.h"
#include "ResourceIDMapperImpl.h"

// QueryEvaluator

void QueryEvaluator::compileLiteral(const TermArray& termArray, const Literal& literal, const ArgumentIndexSet& answerVariables, ArgumentIndexSet& variablesBoundSoFar, const ArgumentIndexSet& variablesAfterConjunct) {
    std::vector<ArgumentIndex> groupedVariableIndexes;
    bool hasNonGroupedVariables = false;
    ArgumentIndexSet inputArguments;
    ArgumentIndexSet outputVariables;
    std::vector<ArgumentIndex> argumentIndexes;
    const std::vector<Term>& arguments = literal->getArguments();
    for (auto iterator = arguments.begin(); iterator != arguments.end(); ++iterator) {
        const Term& term = *iterator;
        const ArgumentIndex argumentIndex = termArray.getPosition(term);
        argumentIndexes.push_back(argumentIndex);
        if (term->getType() != VARIABLE || variablesBoundSoFar.contains(argumentIndex))
            inputArguments.add(argumentIndex);
        else
            outputVariables.add(argumentIndex);
        if (term->getType() == VARIABLE) {
            if (!variablesAfterConjunct.contains(argumentIndex))
                hasNonGroupedVariables = true;
            else
                groupedVariableIndexes.push_back(argumentIndex);
        }
    }
    // Create the iterator
    std::unique_ptr<TupleIterator> tupleIterator;
    switch (literal->getType()) {
    case ATOM_FORMULA:
        tupleIterator = m_queryInfo.getNodeServer().getDataStore().getTupleTable(to_pointer_cast<Atom>(literal)->getPredicate()->getName()).createTupleIterator(m_argumentsBuffer, argumentIndexes, inputArguments, inputArguments);
        if (hasNonGroupedVariables)
            tupleIterator = ::newGroupIterator(nullptr, groupedVariableIndexes, std::move(tupleIterator));
        break;
    case FILTER_FORMULA:
        {
            for (auto iterator = argumentIndexes.begin(); iterator != argumentIndexes.end(); ++iterator)
                if (!inputArguments.contains(*iterator))
                    throw RDF_STORE_EXCEPTION("All arguments must been bound in the FILTER operator.");
            std::unique_ptr<BuiltinExpressionEvaluator> builtinExpressionEvaluator(BuiltinExpressionEvaluator::compile(nullptr, m_queryInfo.getNodeServer().getDataStore(), m_resourceValueCache, termArray, inputArguments, inputArguments, m_argumentsBuffer, to_pointer_cast<Filter>(literal)->getBuiltinExpression()));
            tupleIterator = ::newFilterTupleIterator(std::move(builtinExpressionEvaluator), nullptr, m_argumentsBuffer, argumentIndexes, inputArguments, inputArguments);
        }
        break;
    case BIND_FORMULA:
        {
            for (auto iterator = argumentIndexes.begin() + 1; iterator != argumentIndexes.end(); ++iterator)
                if (!inputArguments.contains(*iterator))
                    throw RDF_STORE_EXCEPTION("All but possibly the first argument must be boind in the BIND operator.");
            std::unique_ptr<BuiltinExpressionEvaluator> builtinExpressionEvaluator(BuiltinExpressionEvaluator::compile(nullptr, m_queryInfo.getNodeServer().getDataStore(), m_resourceValueCache, termArray, inputArguments, inputArguments, m_argumentsBuffer, to_pointer_cast<Bind>(literal)->getBuiltinExpression()));
            tupleIterator = ::newBindTupleIterator<ResourceValueCache>(m_resourceValueCache, std::move(builtinExpressionEvaluator), nullptr, m_argumentsBuffer, argumentIndexes, inputArguments, inputArguments);
        }
        break;
    default:
        throw RDF_STORE_EXCEPTION("Internal error: unsupported formula type.");
    }
    // Create the atom
    m_tupleIteratorsForConjuncts.push_back(std::move(tupleIterator));
    variablesBoundSoFar.unionWith(outputVariables);
}

QueryEvaluator::QueryEvaluator(QueryInfo& queryInfo, const TermArray& termArray, const std::vector<Literal>& conjuncts, const ArgumentIndexSet& answerVariables, std::vector<ArgumentIndexSet>& variablesAfterConjunct, const size_t threadIndex) :
    m_queryInfo(queryInfo),
    m_occurrenceManager(m_queryInfo.getNodeServer().m_occurrenceManager),
    m_resourceIDMapper(m_queryInfo.getNodeServer().m_resourceIDMapper),
    m_resourceValueCache(*m_queryInfo.m_resourceValueCaches[threadIndex]),
    m_threadIndex(threadIndex),
    m_argumentsBuffer(termArray.getNumberOfTerms()),
    m_occurrenceSets(termArray.getNumberOfTerms() * m_occurrenceManager.getResourceWidth(), 0),
    m_temporaryOccurrenceSet(new uint8_t[m_occurrenceManager.getSetWidth()]),
    m_processingPartialBindingsFor(nullptr),
    m_afterLastQueryConjunctIndex(0),
    m_lastQueryConjunctIndex(0),
    m_tupleIteratorsForConjuncts()
{
    ArgumentIndexSet variablesBoundSoFar;
    for (auto iterator = conjuncts.begin(); iterator != conjuncts.end(); ++iterator)
        compileLiteral(termArray, *iterator, answerVariables, variablesBoundSoFar, variablesAfterConjunct[iterator - conjuncts.begin()]);
    const size_t numberOfTerms = termArray.getNumberOfTerms();
    for (ArgumentIndex argumentIndex = 0; argumentIndex < numberOfTerms; ++argumentIndex) {
        const Term& term = termArray.getTerm(argumentIndex);
        if (term->getType() == RESOURCE_BY_NAME) {
            ResourceValue resourceValue;
            Dictionary::parseResourceValue(resourceValue, to_pointer_cast<ResourceByName>(term)->getResourceText());
            m_argumentsBuffer[argumentIndex] = m_resourceValueCache.resolveResource(ThreadContext::getCurrentThreadContext(), resourceValue);
        }
    }
    m_afterLastQueryConjunctIndex = static_cast<uint8_t>(m_tupleIteratorsForConjuncts.size());
    m_lastQueryConjunctIndex = m_afterLastQueryConjunctIndex - 1;
}

QueryEvaluator::~QueryEvaluator() {
}

template<bool callMonitor>
void QueryEvaluator::matchNext(const uint8_t conjunctIndex, const size_t multiplicitySoFar) {
    if (conjunctIndex == m_afterLastQueryConjunctIndex) {
        m_queryInfo.m_nodeServerQueryListener->queryAnswer(m_resourceValueCache, m_resourceIDMapper, m_argumentsBuffer, m_queryInfo.m_answerArgumentIndexes, multiplicitySoFar);
        if (callMonitor)
            m_queryInfo.getNodeServer().m_nodeServerMonitor->queryAnswerProduced(m_queryInfo.getQueryID(), m_threadIndex, m_resourceValueCache, m_argumentsBuffer, m_queryInfo.m_answerArgumentIndexes, multiplicitySoFar);
    }
    else {
        const uint8_t nextConjunctIndex = conjunctIndex + 1;
        TupleIterator& tupleIterator = *m_tupleIteratorsForConjuncts[conjunctIndex];
        size_t multiplicity = tupleIterator.open();
        const QueryInfo::ConjunctInfo& nextConjunctInfo = *m_queryInfo.m_conjunctInfos[nextConjunctIndex];
        if (nextConjunctInfo.m_isBuiltin) {
            while (multiplicity != 0) {
                if (callMonitor)
                    m_queryInfo.getNodeServer().m_nodeServerMonitor->queryAtomMatched(m_queryInfo.getQueryID(), m_threadIndex);
                matchNext<callMonitor>(nextConjunctIndex, multiplicitySoFar * multiplicity);
                multiplicity = tupleIterator.advance();
            }
        }
        else {
            while (multiplicity != 0) {
                if (callMonitor)
                    m_queryInfo.getNodeServer().m_nodeServerMonitor->queryAtomMatched(m_queryInfo.getQueryID(), m_threadIndex);
                const size_t newMultiplicity = multiplicitySoFar * multiplicity;
                if (nextConjunctIndex == m_afterLastQueryConjunctIndex) {
                    if (m_queryInfo.getOriginatorNodeID() == m_queryInfo.getMyNodeID())
                        matchNext<callMonitor>(nextConjunctIndex, newMultiplicity);
                    else
                        sendPartialBindingsMessage<callMonitor>(nextConjunctIndex, m_queryInfo.getOriginatorNodeID(), newMultiplicity);
                }
                else {
                    // Compute the intersection of ownership sets of all constants
                    m_occurrenceManager.makeFullSet(m_temporaryOccurrenceSet.get());
                    for (auto iterator = nextConjunctInfo.m_conjunctArgumentsBoundBeforeConjunctIsMatched.begin(); iterator != nextConjunctInfo.m_conjunctArgumentsBoundBeforeConjunctIsMatched.end(); ++iterator)
                        m_occurrenceManager.intersectWith(m_temporaryOccurrenceSet.get(), getOccurrenceSetFor(iterator->first, iterator->second));
                    // Send the bindings message to everyone else other than self
                    bool sendToSelf = false;
                    for (OccurrenceManager::iterator iterator = m_occurrenceManager.begin(m_temporaryOccurrenceSet.get()); !iterator.isAtEnd(); ++iterator) {
                        if (*iterator) {
                            if (m_queryInfo.getMyNodeID() == iterator.getCurrentNodeID())
                                sendToSelf = true;
                            else
                                sendPartialBindingsMessage<callMonitor>(nextConjunctIndex, iterator.getCurrentNodeID(), newMultiplicity);
                        }
                    }
                    if (sendToSelf)
                        matchNext<callMonitor>(nextConjunctIndex, newMultiplicity);
                }
                multiplicity = tupleIterator.advance();
            }
        }
    }
}

template void QueryEvaluator::matchNext<false>(const uint8_t conjunctIndex, const size_t multiplicitySoFar);
template void QueryEvaluator::matchNext<true>(const uint8_t conjunctIndex, const size_t multiplicitySoFar);
