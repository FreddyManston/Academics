// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef ABSTRACTSTATISTICS_H_
#define ABSTRACTSTATISTICS_H_

#include "../Common.h"
#include "StatisticsCounters.h"

class AbstractStatistics {

public:

    static const size_t EXTRACTED_TUPLES                            = 0;
    static const size_t EXTRACTED_TUPLES_NOT_NORMAL                 = 1;
    static const size_t EXTRACTED_TUPLES_MATCHING_A_BODY_LITERAL    = 2;
    static const size_t EXTRACTED_TUPLES_MATCHING_NO_DERIVATION     = 3;
    static const size_t MATCHED_BODY_LITERALS                       = 4;
    static const size_t MATCHED_FIRST_BODY_LITERALS                 = 5;
    static const size_t MATCHED_FIRST_BODY_LITERALS_NO_DERIVATION   = 6;
    static const size_t MATCHED_RULE_INSTANCES                      = 7;
    static const size_t MATCHED_RULE_INSTANCES_TUPLE_EXTRACTION     = 8;
    static const size_t MATCHED_RULE_INSTANCES_RULE_REEVALUATION    = 9;
    static const size_t MATCHED_RULE_INSTANCES_PIVOTLESS_EVALUATION = 10;
    static const size_t ATTEMPTS_TO_REEVALUATE_RULE                 = 11;
    static const size_t ATTEMPTS_TO_REEVALUATE_RULE_NO_DERIVATION   = 12;
    static const size_t EQUALITIES                                  = 13;
    static const size_t EQUALITIES_SUCCESSFUL                       = 14;
    static const size_t DERIVATIONS                                 = 15;
    static const size_t DERIVATIONS_REPLACEMENT                     = 16;
    static const size_t DERIVATIONS_REPLACEMENT_SUCCESFUL           = 17;
    static const size_t DERIVATIONS_REFLEXIVITY                     = 18;
    static const size_t DERIVATIONS_REFLEXIVITY_SUCCESFUL           = 19;
    static const size_t DERIVATIONS_RULES                           = 20;
    static const size_t DERIVATIONS_RULES_NOT_NORMAL                = 21;
    static const size_t DERIVATIONS_RULES_SUCCESSFUL                = 22;
    static const size_t NUMBER_OF_DERIVATION_COUNTERS               = 23;

protected:

    struct ThreadState : private ::Unmovable {
        StatisticsCounters m_counters;
        size_t m_currentComponentLevel;
        size_t m_matchedFirstBodyLiterals;
        size_t m_matchedRulesInstancesSinceFirstBodyLiteral;
        size_t m_matchedFirstBodyLiteralsWithNoDerivation;
        size_t m_matchedInstancesForRule;
        size_t m_currentBodyLiteralDepth;
        const size_t* m_currentRedirectionTable;
        bool m_inRuleReevaluation;

        ThreadState(const size_t numberOfCountersPerLevel, const char* const* counterDescriptions, const size_t initialNumberOfLevels);

        void setCurrentComponentLevel(const size_t componentLevel);

        void increment(const size_t counterIndex);

        void incrementNoRedirect(const size_t counterIndex);

        void set(const size_t counterIndex, const size_t value);

    };

    unique_ptr_vector<ThreadState> m_statesByThread;

    void setRedirectionTable(const size_t workerIndex, const size_t* redirectionTable);

    virtual const char* const* describeStatistics(size_t& numberOfCountersPerLevel) = 0;

    virtual std::unique_ptr<ThreadState> newThreadState(const size_t numberOfCountersPerLevel, const char* const* counterDescriptions, const size_t initialNumberOfLevels);

public:

    AbstractStatistics();

    virtual ~AbstractStatistics();

    virtual StatisticsCounters getStatisticsCounters();

};

template<class Interface>
class AbstractStatisticsImpl : public AbstractStatistics, public Interface {

public:

    AbstractStatisticsImpl();

    virtual ~AbstractStatisticsImpl();

    virtual void taskStarted(const DataStore& dataStore, const size_t maxComponentLevel);

    virtual void taskFinished(const DataStore& dataStore);

    virtual void componentLevelStarted(const size_t componentLevel);
    
    virtual void componentLevelFinished(const size_t componentLevel);

    virtual void materializationStarted(const size_t workerIndex);

    virtual void materializationFinished(const size_t workerIndex);

    virtual void currentTupleExtracted(const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes);

    virtual void currentTupleNormalized(const size_t workerIndex, std::vector<ResourceID>& normalizedArgumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const bool wasAdded);

    virtual void currentTupleProcessed(const size_t workerIndex);

    virtual void bodyLiteralMatchedStarted(const size_t workerIndex, const BodyLiteralInfo& bodyLiteralInfo);

    virtual void bodyLiteralMatchedFinish(const size_t workerIndex);

    virtual void bodyLiteralMatchingStarted(const size_t workerIndex, const BodyLiteralInfo& bodyLiteralInfo);

    virtual void bodyLiteralMatchingFinished(const size_t  workerIndex);

    virtual void ruleReevaluationStarted(const size_t workerIndex, const RuleInfo& ruleInfo);

    virtual void ruleReevaluationFinished(const size_t workerIndex);

    virtual void pivotlessRuleEvaluationStarted(const size_t workerIndex, const RuleInfo& ruleInfo);

    virtual void pivotlessRuleEvaluationFinished(const size_t workerIndex);

    virtual void ruleMatchedStarted(const size_t workerIndex, const RuleInfo& ruleInfo, const BodyLiteralInfo* const lastBodyLiteralInfo, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes);

    virtual void ruleMatchedFinished(const size_t workerIndex);

    virtual void tupleDerived(const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const bool isNormal, const bool wasAdded);

    virtual void constantMerged(const size_t workerIndex, const ResourceID sourceID, const ResourceID targetID, const bool isSuccessful);

    virtual void reflexiveSameAsTupleDerived(const size_t workerIndex, const ResourceID resourceID, const bool wasAdded);

    virtual void normalizeConstantStarted(const size_t workerIndex, const ResourceID mergedID);

    virtual void tupleNormalized(const size_t workerIndex, std::vector<ResourceID>& originalArgumentsBuffer, const std::vector<ArgumentIndex>& originalArgumentIndexes, std::vector<ResourceID>& normalizedArgumentsBuffer, const std::vector<ArgumentIndex>& normalizedArgumentIndexes, const bool wasAdded);

    virtual void normalizeConstantFinished(const size_t workerIndex);

};

#endif /* ABSTRACTSTATISTICS_H_ */
