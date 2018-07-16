// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../RDFStoreException.h"
#include "../dictionary/Dictionary.h"
#include "../logic/Logic.h"
#include "../storage/DataStore.h"
#include "../storage/TupleTable.h"
#include "../util/Vocabulary.h"
#include "../util/Prefixes.h"
#include "../reasoning/RuleIndex.h"
#include "../reasoning/IncrementalMonitor.h"
#include "ShellCommand.h"

// SummaryTracer

class SummaryTracer : public IncrementalMonitor {

protected:

    struct BodyLiteralStatistics : private ::Unmovable {
        const size_t m_level;
        BodyLiteralStatistics* const m_parent;
        const BodyLiteralInfo& m_bodyLiteralInfo;
        size_t m_matches;
        size_t m_derivations;
        size_t m_successfulDerivations;

        always_inline BodyLiteralStatistics(BodyLiteralStatistics* const parent, const BodyLiteralInfo& bodyLiteralInfo) : m_level(parent == nullptr ? 0 : parent->m_level + 1), m_parent(parent), m_bodyLiteralInfo(bodyLiteralInfo), m_matches(0), m_derivations(0), m_successfulDerivations(0) {
        }

        void print(const Prefixes& prefixes, std::ostream& output) {
            if (m_parent !=  nullptr) {
                m_parent->print(prefixes, output);
                output << ", ";
            }
            output << m_bodyLiteralInfo.getLiteral()->toString(prefixes);
        }
    };

    always_inline static bool compareBodyLiteralStatistics(const BodyLiteralStatistics* object1, const BodyLiteralStatistics* object2) {
        return object1->m_matches > object2->m_matches;
    }

    struct RuleStatistics : private ::Unmovable {
        const RuleInfo& m_ruleInfo;
        size_t m_derivations;
        size_t m_successfulDerivations;

        always_inline RuleStatistics(const RuleInfo& ruleInfo) : m_ruleInfo(ruleInfo), m_derivations(0), m_successfulDerivations(0) {
        }
    };

    always_inline static bool compareRuleStatistics(const RuleStatistics* object1, const RuleStatistics* object2) {
        return object1->m_derivations > object2->m_derivations;
    }

    struct PredicateStatistics : private ::Unmovable {
        ResourceID m_resourceID;
        ResourceText m_resourceText;
        size_t m_derivations;
        size_t m_successfulDerivations;

        PredicateStatistics(const ResourceID resourceID) : m_resourceID(resourceID), m_resourceText(), m_derivations(0), m_successfulDerivations(0) {
        }
    };

    always_inline static bool comparePredicateStatistics(const PredicateStatistics* object1, const PredicateStatistics* object2) {
        return object1->m_derivations > object2->m_derivations;
    }

    struct StatisticsBlock {
        size_t m_currentBodyLiteralLevel;
        size_t m_tuplesExtracted;
        size_t m_matchedFirstLiterals;
        size_t m_matchedFirstLiteralsLeadingToDerivation;
        size_t m_matchedFirstLiteralsLeadingToSuccessfulDerivation;
        size_t m_derivations;
        size_t m_successfulDerivations;
        std::vector<BodyLiteralStatistics*> m_bodyLiteralStatisticsSorted;
        std::unordered_map<const BodyLiteralInfo*, std::unique_ptr<BodyLiteralStatistics> > m_bodyLiteralStatistics;
        BodyLiteralStatistics* m_currentBodyLiteralStatistics;
        bool m_noDerivationSeenForCurrentFirstBodyLiteral;
        bool m_noSuccessfulDerivationSeenForCurrentFirstBodyLiteral;
        std::vector<RuleStatistics*> m_ruleStatisticsSorted;
        std::unordered_map<const RuleInfo*, std::unique_ptr<RuleStatistics> > m_ruleStatistics;
        RuleStatistics* m_currentRuleStatistics;
        std::vector<PredicateStatistics*> m_predicateStatisticsSorted;
        std::unordered_map<ResourceID, std::unique_ptr<PredicateStatistics> > m_predicateStatistics;

        always_inline StatisticsBlock() :
            m_currentBodyLiteralLevel(0),
            m_tuplesExtracted(0),
            m_matchedFirstLiterals(0),
            m_matchedFirstLiteralsLeadingToDerivation(0),
            m_matchedFirstLiteralsLeadingToSuccessfulDerivation(0),
            m_derivations(0),
            m_successfulDerivations(0),
            m_bodyLiteralStatisticsSorted(),
            m_bodyLiteralStatistics(),
            m_currentBodyLiteralStatistics(nullptr),
            m_noDerivationSeenForCurrentFirstBodyLiteral(false),
            m_noSuccessfulDerivationSeenForCurrentFirstBodyLiteral(false),
            m_ruleStatisticsSorted(),
            m_ruleStatistics(),
            m_currentRuleStatistics(nullptr),
            m_predicateStatisticsSorted(),
            m_predicateStatistics()
        {
        }

        always_inline void initialize() {
            m_currentBodyLiteralLevel = m_tuplesExtracted = m_matchedFirstLiterals = m_matchedFirstLiteralsLeadingToDerivation = m_matchedFirstLiteralsLeadingToSuccessfulDerivation = m_derivations = m_successfulDerivations = 0;
            m_noDerivationSeenForCurrentFirstBodyLiteral = false;
            m_noSuccessfulDerivationSeenForCurrentFirstBodyLiteral = false;
            m_currentRuleStatistics = nullptr;
            m_bodyLiteralStatisticsSorted.clear();
            m_bodyLiteralStatistics.clear();
            m_currentBodyLiteralStatistics = nullptr;
            m_currentRuleStatistics = nullptr;
            m_ruleStatisticsSorted.clear();
            m_ruleStatistics.clear();
            m_predicateStatisticsSorted.clear();
            m_predicateStatistics.clear();
        }

        always_inline void print(std::ostream& output, const Prefixes& prefixes, const bool trackBodyLiterals, const bool trackRules, const bool trackPredicates) {
            output
                << "  Extracted tuples:                                   " << m_tuplesExtracted << std::endl
                << "  Matched first literals:                             " << m_matchedFirstLiterals << std::endl
                << "  Matched first literals with derivations:            " << m_matchedFirstLiteralsLeadingToDerivation << std::endl
                << "  Matched first literals with successful derivations: " << m_matchedFirstLiteralsLeadingToSuccessfulDerivation << std::endl
                << "  Derivations:                                        " << m_derivations << std::endl
                << "  Successful derivations:                             " << m_successfulDerivations << std::endl;
            if (trackBodyLiterals) {
                output << "---- 100 most applied body literals ----" << std::endl;
                output << "matches / derivations / successful derivations" << std::endl;
                std::sort(m_bodyLiteralStatisticsSorted.begin(), m_bodyLiteralStatisticsSorted.end(), compareBodyLiteralStatistics);
                size_t printedBodyLiterals = 0;
                for (auto iterator = m_bodyLiteralStatisticsSorted.begin(); printedBodyLiterals < 100 && iterator != m_bodyLiteralStatisticsSorted.end(); ++iterator) {
                    BodyLiteralStatistics* bodyLiteralStatistics = *iterator;
                    output << bodyLiteralStatistics->m_matches << " / " << bodyLiteralStatistics->m_derivations << " / " << bodyLiteralStatistics->m_successfulDerivations << " : ";
                    bodyLiteralStatistics->print(prefixes, output);
                    output << std::endl;
                    ++printedBodyLiterals;
                }
                output << "----------------------------------------------------------------------------------" << std::endl;
            }
            if (trackRules) {
                output << "---- 100 most applied rules ----" << std::endl;
                output << "derivations / successful derivations" << std::endl;
                std::sort(m_ruleStatisticsSorted.begin(), m_ruleStatisticsSorted.end(), compareRuleStatistics);
                size_t printedRules = 0;
                for (auto iterator = m_ruleStatisticsSorted.begin(); printedRules < 100 && iterator != m_ruleStatisticsSorted.end(); ++iterator) {
                    RuleStatistics* ruleStatistics = *iterator;
                    output << ruleStatistics->m_derivations << " / " << ruleStatistics->m_successfulDerivations << " : " << ruleStatistics->m_ruleInfo.getRule()->toString(prefixes) << std::endl;
                    ++printedRules;
                }
                output << "----------------------------------------------------------------------------------" << std::endl;
            }
            if (trackPredicates) {
                output << "---- 100 most derived predicates ----" << std::endl;
                output << "derivations / successful derivations" << std::endl;
                std::sort(m_predicateStatisticsSorted.begin(), m_predicateStatisticsSorted.end(), comparePredicateStatistics);
                size_t printedPredicates = 0;
                for (auto iterator = m_predicateStatisticsSorted.begin(); printedPredicates < 100 && iterator != m_predicateStatisticsSorted.end(); ++iterator) {
                    PredicateStatistics* predicateInfo = *iterator;
                    output << predicateInfo->m_derivations << " / " << predicateInfo->m_successfulDerivations << " : " << predicateInfo->m_resourceText.toString(prefixes) << std::endl;
                    ++printedPredicates;
                }
                output << "----------------------------------------------------------------------------------" << std::endl;
            }
            output << std::endl;
        }

        always_inline void currentTupleExtracted() {
            ++m_tuplesExtracted;
        }

        always_inline BodyLiteralStatistics& getBodyLiteralStatistics(const BodyLiteralInfo& bodyLiteral) {
            std::unique_ptr<BodyLiteralStatistics>& bodyLiteralStatistics = m_bodyLiteralStatistics[&bodyLiteral];
            if (bodyLiteralStatistics.get() == nullptr) {
                bodyLiteralStatistics.reset(new BodyLiteralStatistics(m_currentBodyLiteralStatistics, bodyLiteral));
                m_bodyLiteralStatisticsSorted.push_back(bodyLiteralStatistics.get());
            }
            return *bodyLiteralStatistics;
        }

        always_inline RuleStatistics& getRuleStatistics(const RuleInfo& ruleInfo) {
            std::unique_ptr<RuleStatistics>& ruleStatistics = m_ruleStatistics[&ruleInfo];
            if (ruleStatistics.get() == nullptr) {
                ruleStatistics.reset(new RuleStatistics(ruleInfo));
                m_ruleStatisticsSorted.push_back(ruleStatistics.get());
            }
            return *ruleStatistics;
        }

        always_inline PredicateStatistics& getPredicateStatistics(const ResourceID resourceID, const Dictionary& dictionary) {
            std::unique_ptr<PredicateStatistics>& predicateStatistics = m_predicateStatistics[resourceID];
            if (predicateStatistics.get() == nullptr) {
                predicateStatistics.reset(new PredicateStatistics(resourceID));
                dictionary.getResource(resourceID, predicateStatistics->m_resourceText);
                m_predicateStatisticsSorted.push_back(predicateStatistics.get());
            }
            return *predicateStatistics;
        }

        always_inline void bodyLiteralMatchedStarted(const bool trackBodyLiterals, const BodyLiteralInfo& bodyLiteral) {
            if (++m_currentBodyLiteralLevel == 1) {
                ++m_matchedFirstLiterals;
                m_noDerivationSeenForCurrentFirstBodyLiteral = true;
                m_noSuccessfulDerivationSeenForCurrentFirstBodyLiteral = true;
            }
            if (trackBodyLiterals) {
                m_currentBodyLiteralStatistics = &getBodyLiteralStatistics(bodyLiteral);
                ++m_currentBodyLiteralStatistics->m_matches;
            }
        }

        always_inline void bodyLiteralMatchedFinish() {
            if (--m_currentBodyLiteralLevel == 0) {
                m_noDerivationSeenForCurrentFirstBodyLiteral = false;
                m_noSuccessfulDerivationSeenForCurrentFirstBodyLiteral = false;
            }
            if (m_currentBodyLiteralStatistics != nullptr)
                m_currentBodyLiteralStatistics = m_currentBodyLiteralStatistics->m_parent;
        }

        always_inline void ruleMatchedStarted(const bool trackRules, const RuleInfo& ruleInfo) {
            if (trackRules) {
                m_currentRuleStatistics = &getRuleStatistics(ruleInfo);
                ++m_currentRuleStatistics->m_derivations;
            }
        }

        always_inline void ruleMatchedFinished() {
            m_currentRuleStatistics = nullptr;
        }

        always_inline void tupleDerived(const bool trackPredicates, const Dictionary& dictionary, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const bool isNormal, const bool wasAdded) {
            if (m_currentBodyLiteralStatistics != nullptr) {
                BodyLiteralStatistics* bodyLiteralStatistics = m_currentBodyLiteralStatistics;
                while (bodyLiteralStatistics != nullptr) {
                    ++bodyLiteralStatistics->m_derivations;
                    if (wasAdded)
                        ++bodyLiteralStatistics->m_successfulDerivations;
                    bodyLiteralStatistics = bodyLiteralStatistics->m_parent;
                }
            }
            if (m_currentRuleStatistics != nullptr) {
                ++m_currentRuleStatistics->m_derivations;
                if (wasAdded)
                    ++m_currentRuleStatistics->m_successfulDerivations;
            }
            if (trackPredicates) {
                const ResourceID predicateID = (argumentsBuffer[argumentIndexes[1]] == RDF_TYPE_ID ? argumentsBuffer[argumentIndexes[2]] : argumentsBuffer[argumentIndexes[1]]);
                PredicateStatistics& predicateStatistics = getPredicateStatistics(predicateID, dictionary);
                ++predicateStatistics.m_derivations;
                if (wasAdded)
                    ++predicateStatistics.m_successfulDerivations;
            }
            ++m_derivations;
            if (m_noDerivationSeenForCurrentFirstBodyLiteral) {
                ++m_matchedFirstLiteralsLeadingToDerivation;
                m_noDerivationSeenForCurrentFirstBodyLiteral = false;
            }
            if (wasAdded) {
                ++m_successfulDerivations;
                if (m_noSuccessfulDerivationSeenForCurrentFirstBodyLiteral) {
                    ++m_matchedFirstLiteralsLeadingToSuccessfulDerivation;
                    m_noSuccessfulDerivationSeenForCurrentFirstBodyLiteral = false;
                }
            }
        }

    };

    std::ostream& m_output;
    Dictionary& m_dictionary;
    const Prefixes& m_prefixes;
    const bool m_incrementalReasoning;
    const bool m_trackBodyLiterals;
    const bool m_trackRules;
    const bool m_trackPredicates;
    const bool m_trackDeletionPropagation;
    const bool m_trackBackwardChaining;
    const bool m_trackForwardChaining;
    const ResourceID m_rdfTypeID;
    const Duration m_reportingInterval;
    TimePoint m_materialisationStartTime;
    TimePoint m_nextReportTime;
    StatisticsBlock m_statisticsBlocks[3];
    StatisticsBlock* m_currentStatisticsBlock;

    void printReport() {
        const TimePoint currentTime = ::getTimePoint();
        if (currentTime > m_nextReportTime) {
            m_output << "PROGRESS REPORT: +" << (currentTime - m_materialisationStartTime)/1000 << " s" << std::endl;
            if (m_incrementalReasoning) {
                m_output << "**** DELETION PROPAGATION ****" << std::endl;
                m_statisticsBlocks[0].print(m_output, m_prefixes, m_trackBodyLiterals, m_trackRules, m_trackPredicates);
                m_output << "**** BACKWARD CHAINING ****" << std::endl;
                m_statisticsBlocks[1].print(m_output, m_prefixes, false, m_trackRules, false);
                m_output << "**** FORWARD CHAINING ****" << std::endl;
                m_statisticsBlocks[2].print(m_output, m_prefixes, m_trackBodyLiterals, m_trackRules, m_trackPredicates);
            }
            else
                m_statisticsBlocks[0].print(m_output, m_prefixes, m_trackBodyLiterals, m_trackRules, m_trackPredicates);
            m_nextReportTime = currentTime + m_reportingInterval;
        }
    }

public:

    SummaryTracer(std::ostream& output, Dictionary& dictionary, const Prefixes& prefixes, const bool incrementalReasoning, const bool trackBodyLiterals, const bool trackRules, const bool trackPredicates, const bool trackDeletionPropagation, const bool trackBackwardChaining, const bool trackForwardChaining, const Duration reportingInterval) :
        m_output(output),
        m_dictionary(dictionary),
        m_prefixes(prefixes),
        m_incrementalReasoning(incrementalReasoning),
        m_trackBodyLiterals(trackBodyLiterals),
        m_trackRules(trackRules),
        m_trackPredicates(trackPredicates),
        m_trackDeletionPropagation(trackDeletionPropagation),
        m_trackBackwardChaining(trackBackwardChaining),
        m_trackForwardChaining(trackForwardChaining),
        m_rdfTypeID(m_dictionary.resolveResource(RDF_TYPE, D_IRI_REFERENCE)),
        m_reportingInterval(reportingInterval),
        m_currentStatisticsBlock(0)
    {
    }

    // MaterialisationMonitor

    virtual void taskStarted(const DataStore& dataStore, const size_t maxComponentLevel) {
        for (size_t index = 0; index < 3; ++index)
            m_statisticsBlocks[index].initialize();
        m_currentStatisticsBlock = &m_statisticsBlocks[0];
        m_materialisationStartTime = ::getTimePoint();
        m_nextReportTime = m_materialisationStartTime + m_reportingInterval;
    }

    virtual void taskFinished(const DataStore& dataStore) {
        m_currentStatisticsBlock = 0;
        m_nextReportTime = 0;
        printReport();
    }

    virtual void componentLevelStarted(const size_t componentLevel) {
    }
    
    virtual void componentLevelFinished(const size_t componentLevel) {
    }

    virtual void materializationStarted(const size_t workerIndex) {
    }

    virtual void materializationFinished(const size_t workerIndex) {
    }

    virtual void currentTupleExtracted(const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) {
        if (m_currentStatisticsBlock != 0)
            m_currentStatisticsBlock->currentTupleExtracted();
    }

    virtual void currentTupleNormalized(const size_t workerIndex, std::vector<ResourceID>& normalizedArgumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const bool wasAdded) {
    }

    virtual void currentTupleProcessed(const size_t workerIndex) {
        printReport();
    }

    virtual void bodyLiteralMatchedStarted(const size_t workerIndex, const BodyLiteralInfo& bodyLiteral) {
        if (m_currentStatisticsBlock != 0)
            m_currentStatisticsBlock->bodyLiteralMatchedStarted(m_trackBodyLiterals, bodyLiteral);
    }

    virtual void bodyLiteralMatchedFinish(const size_t workerIndex) {
        if (m_currentStatisticsBlock != 0)
            m_currentStatisticsBlock->bodyLiteralMatchedFinish();
    }

    virtual void bodyLiteralMatchingStarted(const size_t workerIndex, const BodyLiteralInfo& bodyLiteral) {
    }

    virtual void bodyLiteralMatchingFinished(const size_t workerIndex) {
    }

    virtual void ruleMatchedStarted(const size_t workerIndex, const RuleInfo& ruleInfo, const BodyLiteralInfo* const lastBodyLiteral, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) {
        if (m_currentStatisticsBlock != 0)
            m_currentStatisticsBlock->ruleMatchedStarted(m_trackRules, ruleInfo);
    }

    virtual void ruleMatchedFinished(const size_t workerIndex) {
        if (m_currentStatisticsBlock != 0)
            m_currentStatisticsBlock->ruleMatchedFinished();
    }

    virtual void ruleReevaluationStarted(const size_t workerIndex, const RuleInfo& ruleInfo) {
    }

    virtual void ruleReevaluationFinished(const size_t workerIndex) {
    }

    virtual void pivotlessRuleEvaluationStarted(const size_t workerIndex, const RuleInfo& ruleInfo) {
    }

    virtual void pivotlessRuleEvaluationFinished(const size_t workerIndex) {
    }
    
    virtual void tupleDerived(const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const bool isNormal, const bool wasAdded) {
        if (m_currentStatisticsBlock != 0)
            m_currentStatisticsBlock->tupleDerived(m_trackPredicates, m_dictionary, argumentsBuffer, argumentIndexes, isNormal, wasAdded);
    }

    virtual void constantMerged(const size_t workerIndex, const ResourceID sourceID, const ResourceID targetID, const bool isSuccessful) {
    }

    virtual void reflexiveSameAsTupleDerived(const size_t workerIndex, const ResourceID resourceID, const bool wasAdded) {
    }

    virtual void normalizeConstantStarted(const size_t workerIndex, const ResourceID mergedID) {
    }

    virtual void tupleNormalized(const size_t workerIndex, std::vector<ResourceID>& originalArgumentsBuffer, const std::vector<ArgumentIndex>& originalArgumentIndexes, std::vector<ResourceID>& normalizedArgumentsBuffer, const std::vector<ArgumentIndex>& normalizedArgumentIndexes, const bool wasAdded) {
    }

    virtual void normalizeConstantFinished(const size_t workerIndex) {
    }

    // IncrementalMonitor

    virtual void deletedRuleEvaluationStarted(const size_t workerIndex, const RuleInfo& ruleInfo) {
    }
    
    virtual void deletedRuleEvaluationFinished(const size_t workerIndex) {
    }
    
    virtual void addedRuleEvaluationStarted(const size_t workerIndex, const RuleInfo& ruleInfo) {
    }
    
    virtual void addedRuleEvaluationFinished(const size_t workerIndex) {
    }

    virtual void tupleDeletionPreviousLevelsStarted(const size_t workerIndex) {
    }

    virtual void tupleDeletionRecursiveStarted(const size_t workerIndex) {
    }

    virtual void tupleDeletionFinished(const size_t workerIndex) {
    }

    virtual void possiblyDeletedTupleExtracted(const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) {
    }

    virtual void possiblyDeletedTupleProcessed(const size_t workerIndex, const bool proved) {
    }

    virtual void deletionPropagationStarted(const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const bool fromPreviousLevel) {
        if (m_trackDeletionPropagation)
            m_currentStatisticsBlock = &m_statisticsBlocks[0];
        else
            m_currentStatisticsBlock = 0;
    }

    virtual void propagatedDeletionViaReplacement(const size_t workerIndex, const ResourceID resourceID, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const bool wasAdded) {
    }

    virtual void deletionPropagationFinished(const size_t workerIndex) {
        m_currentStatisticsBlock = 0;
    }

    virtual void checkingProvabilityStarted(const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const bool addedToChecked) {
    }

    virtual void tupleOptimized(const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) {
    }

    virtual void reflexiveSameAsRuleInstancesOptimized(const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) {
    }
    
    virtual void backwardReflexiveSameAsRuleInstanceStarted(const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const std::vector<ResourceID>& supportingArgumentsBuffer, const std::vector<ArgumentIndex>& supportingArgumentIndexes) {
    }

    virtual void backwardReflexiveSameAsRuleInstanceFinished(const size_t workerIndex) {
        printReport();
    }

    virtual void backwardReplacementRuleInstanceStarted(const size_t workerIndex, const size_t positionIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) {
    }

    virtual void backwardReplacementRuleInstanceFinished(const size_t workerIndex) {
        printReport();
    }

    virtual void backwardRecursiveRuleStarted(const size_t workerIndex, const HeadAtomInfo& headAtomInfo) {
        m_statisticsBlocks[1].getRuleStatistics(headAtomInfo.getRuleInfo()).m_derivations++;
    }

    virtual void backwardRecursiveRuleInstanceStarted(const size_t workerIndex, const HeadAtomInfo& headAtomInfo, const SupportingFactsEvaluator& currentBodyMatches) {
        m_statisticsBlocks[1].getRuleStatistics(headAtomInfo.getRuleInfo()).m_successfulDerivations++;
    }

    virtual void backwardRecursiveRuleInstanceAtomStarted(const size_t workerIndex, const HeadAtomInfo& headAtomInfo, const SupportingFactsEvaluator& currentBodyMatches, const size_t currentBodyIndex) {
    }

    virtual void backwardRecursiveRuleInstanceAtomFinished(const size_t workerIndex) {
    }

    virtual void backwardRecursiveRuleInstanceFinished(const size_t workerIndex) {
        printReport();
    }

    virtual void backwardRecursiveRuleFinished(const size_t workerIndex) {
        printReport();
    }

    virtual void checkingProvabilityFinished(const size_t workerIndex) {
        printReport();
    }

    virtual void backwardNonrecursiveRuleStarted(const size_t workerIndex, const HeadAtomInfo& headAtomInfo) {
    }

    virtual void backwardNonrecursiveInstanceMatched(const size_t workerIndex, const HeadAtomInfo& headAtomInfo, const SupportingFactsEvaluator& currentBodyMatches) {
    }

    virtual void backwardNonrecursiveRuleFinished(const size_t workerIndex) {
        printReport();
    }

    virtual void checkedReflexiveSameAsTupleProvedFromEDB(const size_t workerIndex, const ResourceID originalResourceID, const ResourceID denormalizedResourceID, const ResourceID normalizedProvedResourceID, const bool wasAdded) {
    }

    virtual void checkedTupleChecked(const size_t workerIndex, const std::vector<ResourceID>& originalArgumentsBuffer, const std::vector<ResourceID>& denormalizedArgumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) {
    }

    virtual void checkedTupleProved(const size_t workerIndex, const std::vector<ResourceID>& originalArgumentsBuffer, const std::vector<ResourceID>& denormalizedArgumentsBuffer, const std::vector<ResourceID>& normalizedProvedArgumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const bool fromEDB, const bool fromDelayed, const bool fromPrviousLevel, const bool wasAdded) {
    }

    virtual void processProvedStarted(const size_t workerIndex) {
        if (m_trackForwardChaining)
            m_currentStatisticsBlock = &m_statisticsBlocks[2];
        else
            m_currentStatisticsBlock = 0;
    }

    virtual void tupleProvedDelayed(const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const bool wasAdded) {
    }

    virtual void reflexiveSameAsTupleDerivedDelayed(const size_t workerIndex, const ResourceID resourceID, const bool wasAdded) {
    }

    virtual void tupleNormalizedDelayed(const size_t workerIndex, std::vector<ResourceID>& originalArgumentsBuffer, const std::vector<ArgumentIndex>& originalArgumentIndexes, std::vector<ResourceID>& normalizedArgumentsBuffer, const std::vector<ArgumentIndex>& normalizedArgumentIndexes, const bool wasAdded) {
    }

    virtual void checkedTupleDisproved(const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const bool wasAdded) {
    }

    virtual void processProvedFinished(const size_t workerIndex) {
        m_currentStatisticsBlock = 0;
    }

    virtual void updateEqualityManagerStarted(const size_t workerIndex) {
    }

    virtual void equivalenceClassCopied(const size_t workerIndex, const ResourceID resourceID, const EqualityManager& sourceEqualityManager, const EqualityManager& targetEqualityManager) {
    }

    virtual void updateEqualityManagerFinished(const size_t workerIndex) {
    }

    virtual void deleteReflexiveSameAsStarted(const size_t workerIndex) {
    }

    virtual void deleteReflexiveSameAsFinished(const size_t workerIndex) {
    }

    virtual void insertionPreviousLevelsStarted(const size_t workerIndex) {
    }
    
    virtual void insertionRecursiveStarted(const size_t workerIndex) {
    }

    virtual void insertedTupleAddedToIDB(const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const bool wasAdded) {
    }
    
    virtual void insertionFinished(const size_t workerIndex) {
    }

    virtual void propagateDeletedProvedStarted(const size_t workerIndex, const size_t componentLevel) {
    }

    virtual void tupleDeleted(const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const bool wasDeleted) {
    }

    virtual void tupleAdded(const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const bool wasAdded) {
    }

    virtual void propagateDeletedProvedFinished(const size_t workerIndex) {
    }
    
};

// TraceMaterialize

class TraceMaterialize : public ShellCommand {

public:

    TraceMaterialize() : ShellCommand("tramat") {
    }

    virtual std::string getOneLineHelp() const {
        return "traces the (incremental) materialization of the data in the store with respect to the current rule set";
    }

    virtual void printHelpPage(std::ostream& output) const {
        output
            << "tramat ([inc] | [body-literals] | [rules] | [predicates] | [deletion] | [backward] | [forward] | [<reporting interval in seconds>] )*" << std::endl
            << "    Evaluates (incrementailly) the current datalog program on the current data store with one thread while tracing the evaluation." << std::endl;
    }

    always_inline void printStartMessage(Shell& shell, const Duration reportingInterval, const bool trackBodyLiterals, const bool trackRules, const bool trackPredicates, const bool trackDeletionPropagation, const bool trackBackwardChaining, const bool trackForwardChaining) const {
        OutputProtector protector(shell);
        shell.getOutput() << "Materializing rules on one thread";
        if (shell.getDataStore().getEqualityAxiomatizationType() == EQUALITY_AXIOMATIZATION_NO_UNA)
            shell.getOutput() << " with equality but no UNA";
        else if (shell.getDataStore().getEqualityAxiomatizationType() == EQUALITY_AXIOMATIZATION_UNA)
            shell.getOutput() << " with equality and UNA";
        shell.getOutput() << "." << std::endl;
        shell.getOutput() << "Progress report will be printed every " << reportingInterval / 1000 << " s.";
        if (trackBodyLiterals)
            shell.getOutput() << " Tracking body literal matches.";
        if (trackRules)
            shell.getOutput() << " Tracking rule applications.";
        if (trackPredicates)
            shell.getOutput() << " Tracking predicate derivations.";
        if (trackDeletionPropagation)
            shell.getOutput() << " Tracking deletion propagation.";
        if (trackBackwardChaining)
            shell.getOutput() << " Tracking backward chaining.";
        if (trackForwardChaining)
            shell.getOutput() << " Tracking forward chaining.";
        shell.getOutput() << std::endl;
    }

    virtual void execute(Shell& shell, Shell::ArgumentsTokenizer& arguments) const {
        if (isDataStoreActive(shell)) {
            bool incremental = false;
            bool trackBodyLiterals = false;
            bool trackRules = false;
            bool trackPredicates = false;
            bool trackDeletionPropagation = false;
            bool trackBackwardChaining = false;
            bool trackForwardChaining = false;
            Duration reportingInterval = 30000;
            while (!arguments.isEOF()) {
                if (arguments.symbolLowerCaseTokenEquals("inc")) {
                    incremental = true;
                    arguments.nextToken();
                }
                else if (arguments.symbolLowerCaseTokenEquals("body-literals")) {
                    trackBodyLiterals = true;
                    arguments.nextToken();
                }
                else if (arguments.symbolLowerCaseTokenEquals("rules")) {
                    trackRules = true;
                    arguments.nextToken();
                }
                else if (arguments.symbolLowerCaseTokenEquals("predicates")) {
                    trackPredicates = true;
                    arguments.nextToken();
                }
                else if (arguments.symbolLowerCaseTokenEquals("deletion")) {
                    trackDeletionPropagation = true;
                    arguments.nextToken();
                }
                else if (arguments.symbolLowerCaseTokenEquals("backward")) {
                    trackBackwardChaining = true;
                    arguments.nextToken();
                }
                else if (arguments.symbolLowerCaseTokenEquals("forward")) {
                    trackForwardChaining = true;
                    arguments.nextToken();
                }
                else if (arguments.isNumber()) {
                    reportingInterval = 1000 * static_cast<size_t>(atoi(arguments.getToken(0).c_str()));
                    arguments.nextToken();
                }
                else {
                    shell.getOutput() << "Invalid argument '" << arguments.getToken() << "'." << std::endl;
                    return;
                }
            }
            const bool processComponentsByLevels = shell.getBooleanVariable("reason.by-levels");
            const bool useDRedAlgorithm = shell.getBooleanVariable("reason.use-DRed");
            shell.getDataStore().setNumberOfThreads(1);
            printStartMessage(shell, reportingInterval, trackBodyLiterals, trackRules, trackPredicates, trackDeletionPropagation, trackBackwardChaining, trackForwardChaining);
            TupleTable& tupleTable = shell.getDataStore().getTupleTable("internal$rdf");
            const size_t numberOfTriplesBeforeReasoning = tupleTable.getTupleCount(TUPLE_STATUS_COMPLETE, TUPLE_STATUS_COMPLETE);
            const size_t numberOfIDBTriplesBeforeReasoning = tupleTable.getTupleCount(TUPLE_STATUS_IDB | TUPLE_STATUS_IDB_MERGED, TUPLE_STATUS_IDB);
            SummaryTracer summaryTracer(shell.getOutput(), shell.getDataStore().getDictionary(), shell.getPrefixes(), incremental, trackBodyLiterals, trackRules, trackPredicates, trackDeletionPropagation, trackBackwardChaining, trackForwardChaining, reportingInterval);
            const TimePoint materializationStartTime = ::getTimePoint();
            try {
                if (incremental)
                    shell.getDataStore().applyRulesIncrementally(processComponentsByLevels, useDRedAlgorithm, &summaryTracer);
                else
                    shell.getDataStore().applyRules(processComponentsByLevels, &summaryTracer);
                const Duration duration = ::getTimePoint() - materializationStartTime;
                const size_t numberOfTriplesAfterReasoning = tupleTable.getTupleCount(TUPLE_STATUS_COMPLETE, TUPLE_STATUS_COMPLETE);
                const size_t numberOfIDBTriplesAfterReasoning = tupleTable.getTupleCount(TUPLE_STATUS_IDB | TUPLE_STATUS_IDB_MERGED, TUPLE_STATUS_IDB);
                OutputProtector protector(shell);
                shell.getOutput() << "Materialization time:      " << duration / 1000.0 << " s." << std::endl;
                shell.getOutput() <<
                    "Total number of stored triples: " << numberOfTriplesBeforeReasoning << " -> " << numberOfTriplesAfterReasoning << "." << std::endl <<
                    "The number of IDB triples:      " << numberOfIDBTriplesBeforeReasoning << " -> " << numberOfIDBTriplesAfterReasoning << "." << std::endl;
            }
            catch (const RDFStoreException& e) {
                shell.printError(e, "Materialization has been aborted due to errors.");
            }
        }
    }
};

static TraceMaterialize s_traceMaterialize;
