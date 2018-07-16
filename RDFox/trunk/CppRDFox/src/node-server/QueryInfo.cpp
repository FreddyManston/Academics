// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../dictionary/ResourceValueCache.h"
#include "../querying/TermArray.h"
#include "../storage/DataStore.h"
#include "../util/Socket.h"
#include "QueryInfoImpl.h"
#include "QueryEvaluator.h"

// QueryInfo::ConjunctInfo

QueryInfo::ConjunctInfo::ConjunctInfo(const size_t numberOfNodes, const size_t numberOfTerms) :
    m_partialAnswersIProcessed(0),
    m_partialAnswersOthersSentMe(0),
    m_nodesSentTermination(0),
    m_finished(0),
    m_partialAnswersISentToOthers(new uint32_t[numberOfNodes]),
    m_isBuiltin(false),
    m_conjunctArgumentsBoundBeforeConjunctIsMatched(),
    m_resourceValuesToSend(),
    m_occurrenceSetsToSend(),
    m_occrrenceSetsToSendByArgumentIndex(numberOfTerms, false)
{
    for (size_t index = 0; index < numberOfNodes; ++index)
        m_partialAnswersISentToOthers[index] = 0;
}

QueryInfo::ConjunctInfo::~ConjunctInfo() {
}

// QueryInfo

QueryInfo::QueryInfo(NodeServer& nodeServer, const QueryID queryID) :
    m_nodeServer(nodeServer),
    m_numberOfQueryPlanAcknowledgements(0),
    m_numberOfNodes(m_nodeServer.getNumberOfNodes()),
    m_queryID(queryID),
    m_originatorNodeID(::getNodeID(m_queryID)),
    m_myNodeID(m_nodeServer.getMyNodeID()),
    m_numberOfConjunctsPlusOne(0),
    m_numberOfUsers(0),
    m_nodeServerQueryListener(nullptr),
    m_answerArgumentIndexes(),
    m_conjunctInfos(),
    m_resourceValueCaches(),
    m_queryEvauatorsByThreadAndConjunct(),
    m_queryFinished(false)
{
    for (size_t threadIndex = 0; threadIndex < m_nodeServer.m_numberOfThreads; ++threadIndex)
        m_resourceValueCaches.push_back(std::unique_ptr<ResourceValueCache>(new ResourceValueCache(m_nodeServer.getDataStore().getDictionary(), m_nodeServer.getDataStore().getMemoryManager())));
}

QueryInfo::~QueryInfo() {
}

void QueryInfo::loadQueryPlan(const Query& query, NodeServerQueryListener* const nodeServerQueryListener) {
    if (hasQueryPlan())
        throw RDF_STORE_EXCEPTION("The plan for this query has already been loaded.");
    std::vector<Literal> conjuncts;
    if (::isLiteralType(query->getQueryFormula()->getType()))
        conjuncts.push_back(static_pointer_cast<Literal>(query->getQueryFormula()));
    else if (query->getQueryFormula()->getType() == CONJUNCTION_FORMULA) {
        const std::vector<Formula>& queryConjuncts = to_pointer_cast<Conjunction>(query->getQueryFormula())->getConjuncts();
        for (auto iterator = queryConjuncts.begin(); iterator != queryConjuncts.end(); ++iterator)
            if (::isLiteralType((*iterator)->getType()))
                conjuncts.push_back(static_pointer_cast<Literal>(*iterator));
            else
                throw RDF_STORE_EXCEPTION("The query form is unsupported.");
    }
    else
        throw RDF_STORE_EXCEPTION("The query form is unsupported.");
    const size_t numberOfConjuncts = conjuncts.size();
    if (numberOfConjuncts > NODE_SERVER_MAX_NUMBER_OF_LITERALS)
        throw RDF_STORE_EXCEPTION("Node server supports queries with at most " TO_STRING(NODE_SERVER_MAX_NUMBER_OF_LITERALS) " literals.");
    m_nodeServerQueryListener = nodeServerQueryListener;
    TermArray termArray(query);
    ArgumentIndexSet answerVariables;
    const std::vector<Term>& answerTerms = query->getAnswerTerms();
    for (auto iterator = answerTerms.begin(); iterator != answerTerms.end(); ++iterator) {
        const ArgumentIndex argumentIndex = termArray.getPosition(*iterator);
        m_answerArgumentIndexes.push_back(argumentIndex);
        if ((*iterator)->getType() == VARIABLE)
            answerVariables.add(argumentIndex);
    }
    std::vector<ArgumentIndexSet> variablesInConjunct(numberOfConjuncts);
    std::vector<ArgumentIndexSet> termsInConjunct(numberOfConjuncts);
    for (size_t conjunctIndex = 0; conjunctIndex < numberOfConjuncts; ++conjunctIndex) {
        ArgumentIndexSet& variablesIn = variablesInConjunct[conjunctIndex];
        ArgumentIndexSet& termsIn = termsInConjunct[conjunctIndex];
        const std::vector<Term>& arguments = conjuncts[conjunctIndex]->getArguments();
        for (auto iterator = arguments.begin(); iterator != arguments.end(); ++iterator) {
            const ArgumentIndex argumentIndex = termArray.getPosition(*iterator);
            if ((*iterator)->getType() == VARIABLE)
                variablesIn.add(argumentIndex);
            termsIn.add(argumentIndex);
        }
    }
    std::vector<ArgumentIndexSet> variablesAfterConjunct(numberOfConjuncts);
    std::vector<ArgumentIndexSet> termsAfterConjunct(numberOfConjuncts);
    const int64_t lastConjunctIndex = static_cast<int64_t>(numberOfConjuncts) - 1;
    if (lastConjunctIndex >= 0) {
        variablesAfterConjunct[lastConjunctIndex].unionWith(answerVariables);
        for (int64_t conjunctIndex = lastConjunctIndex - 1; conjunctIndex >= 0; --conjunctIndex) {
            variablesAfterConjunct[conjunctIndex].unionWith(variablesAfterConjunct[conjunctIndex + 1]);
            variablesAfterConjunct[conjunctIndex].unionWith(variablesInConjunct[conjunctIndex + 1]);
            termsAfterConjunct[conjunctIndex].unionWith(termsAfterConjunct[conjunctIndex + 1]);
            termsAfterConjunct[conjunctIndex].unionWith(termsInConjunct[conjunctIndex + 1]);
        }
    }
    ArgumentIndexSet termsSeenSoFar;
    for (size_t conjunctIndex = 0; conjunctIndex < numberOfConjuncts; ++conjunctIndex) {
        m_conjunctInfos.push_back(std::unique_ptr<ConjunctInfo>(new ConjunctInfo(m_numberOfNodes, termArray.getNumberOfTerms())));
        ConjunctInfo& conjunctInfo = *m_conjunctInfos.back();
        ArgumentIndexSet& variablesIn = variablesInConjunct[conjunctIndex];
        ArgumentIndexSet& variablesAfter = variablesAfterConjunct[conjunctIndex];
        ArgumentIndexSet& termsIn = termsInConjunct[conjunctIndex];
        ArgumentIndexSet& termsAfter = termsAfterConjunct[conjunctIndex];
        const Literal& conjunct = conjuncts[conjunctIndex];
        if (conjunct->getType() != ATOM_FORMULA)
            conjunctInfo.m_isBuiltin = true;
        for (auto iterator = termsSeenSoFar.begin(); iterator != termsSeenSoFar.end(); ++iterator) {
            const ArgumentIndex argumentIndex = *iterator;
            if (termArray.getTerm(*iterator)->getType() == VARIABLE && (variablesIn.contains(argumentIndex) || variablesAfter.contains(argumentIndex))) {
                bool sendAsValue = false;
                for (size_t scanConjunctIndex = conjunctIndex; !sendAsValue && scanConjunctIndex < numberOfConjuncts; ++scanConjunctIndex)
                    if (conjuncts[scanConjunctIndex]->getType() != ATOM_FORMULA && variablesInConjunct[scanConjunctIndex].contains(argumentIndex))
                        sendAsValue = true;
                conjunctInfo.m_resourceValuesToSend.emplace_back(argumentIndex, sendAsValue);
            }
            if (termsAfter.contains(argumentIndex) && !termsIn.contains(argumentIndex)) {
                conjunctInfo.m_occurrenceSetsToSend.push_back(argumentIndex);
                conjunctInfo.m_occrrenceSetsToSendByArgumentIndex[argumentIndex] = true;
            }
        }
        const std::vector<Term>& arguments = conjunct->getArguments();
        for (uint8_t position = 0; position < arguments.size(); ++position) {
            const ArgumentIndex argumentIndex = termArray.getPosition(arguments[position]);
            if (arguments[position]->getType() != VARIABLE || termsSeenSoFar.contains(argumentIndex))
                conjunctInfo.m_conjunctArgumentsBoundBeforeConjunctIsMatched.emplace_back(argumentIndex, position);
        }
        termsSeenSoFar.unionWith(termsInConjunct[conjunctIndex]);
    }
    m_conjunctInfos.push_back(std::unique_ptr<ConjunctInfo>(new ConjunctInfo(m_numberOfNodes, termArray.getNumberOfTerms())));
    for (auto iterator = answerVariables.begin(); iterator != answerVariables.end(); ++iterator)
        m_conjunctInfos.back()->m_resourceValuesToSend.emplace_back(*iterator, false);
    assert(m_conjunctInfos.back()->m_occurrenceSetsToSend.empty());
    m_numberOfConjunctsPlusOne = numberOfConjuncts + 1;
    const size_t numberOfThreads = m_nodeServer.m_numberOfThreads;
    for (size_t threadIndex = 0; threadIndex < numberOfThreads; ++threadIndex)
        for (size_t conjunctIndex = 0; conjunctIndex < m_numberOfConjunctsPlusOne; ++conjunctIndex)
            m_queryEvauatorsByThreadAndConjunct.push_back(std::unique_ptr<QueryEvaluator>(new QueryEvaluator(*this, termArray, conjuncts, answerVariables, variablesAfterConjunct, threadIndex)));
    ::atomicWrite(m_conjunctInfos[0]->m_partialAnswersOthersSentMe, 1);
    ::atomicWrite(m_conjunctInfos[0]->m_nodesSentTermination, m_nodeServer.getNumberOfNodes());
}
