// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef DATASTORE_H_
#define DATASTORE_H_

#include "../Common.h"
#include "../logic/Logic.h"

class DataStoreType;
class MemoryManager;
class Dictionary;
class TupleTable;
class RuleIterator;
class ComponentStatistics;
class EqualityManager;
class MaterializationMonitor;
class IncrementalMonitor;
class TupleIteratorMonitor;
class InputStream;
class OutputStream;
class Parameters;
class ExplanationProvider;
class QueryIterator;
class TermArray;

class DataStore : private Unmovable {

public:

    virtual ~DataStore();

    virtual bool isConcurrent() const = 0;

    virtual const DataStoreType& getType() const = 0;

    virtual const Parameters& getDataStoreParameters() const = 0;

    virtual EqualityAxiomatizationType getEqualityAxiomatizationType() const = 0;

    virtual void initialize(const size_t initialTripleCapacity = 0, const size_t initialResourceCapacity = 0) = 0;

    virtual void reindex(const bool dropIDB, const size_t initialTripleCapacity = 0, const size_t initialResourceCapacity = 0) = 0;

    virtual size_t getNumberOfThreads() const = 0;

    virtual void setNumberOfThreads(const size_t numberOfThreads) = 0;

    virtual MemoryManager& getMemoryManager() const = 0;

    virtual Dictionary& getDictionary() const = 0;

    virtual const EqualityManager& getEqualityManager() const = 0;

    virtual void makeFactsExplicit() = 0;

    virtual void clearRulesAndMakeFactsExplicit() = 0;

    virtual bool addRule(const Rule& rule) = 0;

    virtual bool addRules(const DatalogProgram& datalogProgram) = 0;

    virtual bool removeRule(const Rule& rule) = 0;

    virtual bool removeRules(const DatalogProgram& datalogProgram) = 0;

    virtual void recompileRules() = 0;

    virtual void applyRules(const bool processComponentsByLevels, MaterializationMonitor* const materializationMonitor = nullptr) = 0;

    virtual void applyRulesIncrementally(const bool processComponentsByLevels, const bool useDRedAlgorithm = false, IncrementalMonitor* const incrementalMonitor = nullptr) = 0;

    virtual std::unique_ptr<RuleIterator> createRuleIterator() const = 0;

    virtual const std::map<std::string, TupleTable*>& getTupleTablesByName() const = 0;

    virtual bool containsTupleTable(const std::string& tupleTablePredicateName) const = 0;

    virtual TupleTable& getTupleTable(const std::string& tupleTablePredicateName) const = 0;

    virtual std::unique_ptr<ExplanationProvider> createExplanationProvider() const = 0;

    virtual std::unique_ptr<QueryIterator> compileQuery(const Query& query, const Parameters& parameters, TupleIteratorMonitor* const tupleIteratorMonitor = nullptr) const = 0;

    virtual std::unique_ptr<QueryIterator> compileQuery(const Query& query, TermArray& termArray, const Parameters& parameters, TupleIteratorMonitor* const tupleIteratorMonitor = nullptr) const = 0;

    virtual std::unique_ptr<QueryIterator> compileQuery(Prefixes& prefixes, const char* queryText, const Parameters& parameters, TupleIteratorMonitor* const tupleIteratorMonitor = nullptr) const = 0;

    virtual std::unique_ptr<QueryIterator> compileQuery(Prefixes& prefixes, const char* queryText, TermArray& termArray, const Parameters& parameters, TupleIteratorMonitor* const tupleIteratorMonitor = nullptr) const = 0;

    virtual void updateStatistics() = 0;

    virtual void saveFormatted(OutputStream& outputStream) const = 0;

    virtual void loadFormatted(InputStream& inputStream, const bool formatKindAlreadyCheched, const bool dataStoreParametersAlreadyCheched) = 0;

    virtual void saveUnformatted(OutputStream& outputStream) const = 0;

    virtual void loadUnformatted(InputStream& inputStream, const bool formatKindAlreadyCheched, const size_t numberOfThreads) = 0;

    virtual std::unique_ptr<ComponentStatistics> getComponentStatistics() const = 0;

};

extern std::unique_ptr<DataStore> newDataStore(const char* const dataStoreTypeName, const Parameters& dataStoreParameters);

extern std::unique_ptr<DataStore> newDataStoreFromFormattedFile(InputStream& intputStream, const bool formatKindAlreadyCheched);

#endif /* DATASTORE_H_ */
