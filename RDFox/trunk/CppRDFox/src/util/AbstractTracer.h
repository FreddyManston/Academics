// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef ABSTRACTTRACER_H_
#define ABSTRACTTRACER_H_

#include "../Common.h"
#include "../util/Mutex.h"

class Prefixes;
class DataStore;
class Dictionary;
class RuleInfo;
class BodyLiteralInfo;

template<class Interface>
class AbstractTracer : public Interface {

protected:

    Prefixes& m_prefixes;
    Dictionary& m_dictionary;
    std::ostream& m_output;
    Mutex m_mutex;
    std::vector<size_t> m_currentIndents;

    void increaseIndent(const size_t workerIndex);

    void decreaseIndent(const size_t workerIndex);

    void printIndent(const size_t workerIndex);

    void print(const ResourceID resourceID);

    void print(const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes);

public:

    AbstractTracer(Prefixes& prefixes, Dictionary& dictionary, std::ostream& output);

    virtual void taskStarted(const DataStore& dataStore, const size_t maxComponenetLevel);

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

#endif /* ABSTRACTTRACER_H_ */
