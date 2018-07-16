// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef MATERIALIZATIONMONITOR_H_
#define MATERIALIZATIONMONITOR_H_

#include "../Common.h"

class DataStore;
class BodyLiteralInfo;
class RuleInfo;

class MaterializationMonitor {

public:

    virtual ~MaterializationMonitor() {
    }

    virtual void taskStarted(const DataStore& dataStore, const size_t maxComponentLevel) = 0;

    virtual void taskFinished(const DataStore& dataStore) = 0;

    virtual void componentLevelStarted(const size_t componentLevel) = 0;
    
    virtual void componentLevelFinished(const size_t componentLevel) = 0;
    
    virtual void materializationStarted(const size_t workerIndex) = 0;

    virtual void materializationFinished(const size_t workerIndex) = 0;

    virtual void currentTupleExtracted(const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) = 0;

    virtual void currentTupleNormalized(const size_t workerIndex, std::vector<ResourceID>& normalizedArgumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const bool wasAdded) = 0;

    virtual void currentTupleProcessed(const size_t workerIndex) = 0;

    virtual void bodyLiteralMatchedStarted(const size_t workerIndex, const BodyLiteralInfo& bodyLiteralInfo) = 0;

    virtual void bodyLiteralMatchedFinish(const size_t  workerIndex) = 0;

    virtual void bodyLiteralMatchingStarted(const size_t workerIndex, const BodyLiteralInfo& bodyLiteralInfo) = 0;

    virtual void bodyLiteralMatchingFinished(const size_t  workerIndex) = 0;

    virtual void ruleReevaluationStarted(const size_t workerIndex, const RuleInfo& ruleInfo) = 0;

    virtual void ruleReevaluationFinished(const size_t workerIndex) = 0;

    virtual void pivotlessRuleEvaluationStarted(const size_t workerIndex, const RuleInfo& ruleInfo) = 0;

    virtual void pivotlessRuleEvaluationFinished(const size_t workerIndex) = 0;
    
    virtual void ruleMatchedStarted(const size_t workerIndex, const RuleInfo& ruleInfo, const BodyLiteralInfo* const lastBodyLiteralInfo, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes) = 0;

    virtual void ruleMatchedFinished(const size_t workerIndex) = 0;

    virtual void tupleDerived(const size_t workerIndex, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const bool isNormal, const bool wasAdded) = 0;

    virtual void constantMerged(const size_t workerIndex, const ResourceID sourceID, const ResourceID targetID, const bool isSuccessful) = 0;

    virtual void reflexiveSameAsTupleDerived(const size_t workerIndex, const ResourceID resourceID, const bool wasAdded) = 0;

    virtual void normalizeConstantStarted(const size_t workerIndex, const ResourceID mergedID) = 0;

    virtual void tupleNormalized(const size_t workerIndex, std::vector<ResourceID>& originalArgumentsBuffer, const std::vector<ArgumentIndex>& originalArgumentIndexes, std::vector<ResourceID>& normalizedArgumentsBuffer, const std::vector<ArgumentIndex>& normalizedArgumentIndexes, const bool wasAdded) = 0;

    virtual void normalizeConstantFinished(const size_t workerIndex) = 0;

};

#endif /* MATERIALIZATIONMONITOR_H_ */
