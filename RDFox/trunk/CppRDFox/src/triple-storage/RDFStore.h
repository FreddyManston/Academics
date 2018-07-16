// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef RDFSTORE_H_
#define RDFSTORE_H_

#include "../util/MemoryManager.h"
#include "../util/Mutex.h"
#include "../dictionary/Dictionary.h"
#include "../equality/EqualityManager.h"
#include "../storage/DataStore.h"
#include "../storage/DataStoreType.h"
#include "../reasoning/DatalogEngine.h"

template<class TT> class RDFStoreRuleIterator;

class ComponentStatistics;

template<class TT>
class RDFStore : public DataStore {

    friend class RDFStoreRuleIterator<TT>;

protected:

    typedef typename MutexSelector<TT::TripleListType::IS_CONCURRENT>::MutexType MutexType;
    typedef typename MutexSelector<TT::TripleListType::IS_CONCURRENT>::MutexHolderType MutexHolderType;

    class RDFStoreType : public DataStoreType {

    public:

        RDFStoreType();

        virtual std::unique_ptr<DataStore> newDataStore(const Parameters& dataStoreParameters) const;

    };

    static RDFStoreType s_rdfStoreType;

    struct TupleTableMap : public std::map<std::string, TupleTable*> {

        TupleTableMap(TupleTable* tripleTable) : std::map<std::string, TupleTable*>() {
            (*this)["internal$rdf"] = tripleTable;
        }
    };

    const Parameters m_dataStoreParameters;
    const EqualityAxiomatizationType m_equalityAxiomatizationType;
    MemoryManager m_memoryManager;
    mutable MutexType m_mutex;
    size_t m_numberOfThreads;
    Dictionary m_dictionary;
    EqualityManager m_equalityManager;
    TT m_tripleTable;
    TupleTableMap m_tupleTablesByName;
    DatalogEngine m_datalogEngine;

    static EqualityAxiomatizationType getEqualityAxiomatizationType(const Parameters& dataStoreParameters);

    void initializeInternal(const size_t initialTripleCapacity = 0, const size_t initialResourceCapacity = 0);

    void setNumberOfThreadsInternal(const size_t numberOfThreads);

public:

    typedef TT TripleTable;

    RDFStore(const Parameters& dataStoreParameters);

    virtual ~RDFStore();

    virtual bool isConcurrent() const;

    virtual const DataStoreType& getType() const;

    virtual const Parameters& getDataStoreParameters() const;

    virtual EqualityAxiomatizationType getEqualityAxiomatizationType() const;

    virtual void initialize(const size_t initialTripleCapacity = 0, const size_t initialResourceCapacity = 0);

    virtual void reindex(const bool dropIDB, const size_t initialTripleCapacity = 0, const size_t initialResourceCapacity = 0);

    virtual size_t getNumberOfThreads() const;

    virtual void setNumberOfThreads(const size_t numberOfThreads);

    virtual MemoryManager& getMemoryManager() const;

    virtual Dictionary& getDictionary() const;

    virtual const EqualityManager& getEqualityManager() const;

    virtual void makeFactsExplicit();

    virtual void clearRulesAndMakeFactsExplicit();

    virtual bool addRule(const Rule& rule);

    virtual bool addRules(const DatalogProgram& datalogProgram);

    virtual bool removeRule(const Rule& rule);

    virtual bool removeRules(const DatalogProgram& datalogProgram);

    virtual void recompileRules();

    virtual void applyRules(const bool processComponentsByLevels, MaterializationMonitor* const materializationMonitor = 0);

    virtual void applyRulesIncrementally(const bool processComponentsByLevels, const bool useDRedAlgorithm = false, IncrementalMonitor* const incrementalMonitor = 0);

    virtual std::unique_ptr<RuleIterator> createRuleIterator() const;

    virtual const std::map<std::string, TupleTable*>& getTupleTablesByName() const;

    virtual bool containsTupleTable(const std::string& predicateName) const;

    virtual TupleTable& getTupleTable(const std::string& predicateName) const;

    virtual std::unique_ptr<ExplanationProvider> createExplanationProvider() const;

    virtual std::unique_ptr<QueryIterator> compileQuery(const Query& query, const Parameters& parameters, TupleIteratorMonitor* const tupleIteratorMonitor = nullptr) const;

    virtual std::unique_ptr<QueryIterator> compileQuery(const Query& query, TermArray& termArray, const Parameters& parameters, TupleIteratorMonitor* const tupleIteratorMonitor = nullptr) const;

    virtual std::unique_ptr<QueryIterator> compileQuery(Prefixes& prefixes, const char* queryText, const Parameters& parameters, TupleIteratorMonitor* const tupleIteratorMonitor = nullptr) const;

    virtual std::unique_ptr<QueryIterator> compileQuery(Prefixes& prefixes, const char* queryText, TermArray& termArray, const Parameters& parameters, TupleIteratorMonitor* const tupleIteratorMonitor = nullptr) const;

    virtual void updateStatistics();

    virtual void saveFormatted(OutputStream& outputStream) const;

    virtual void loadFormatted(InputStream& inputStream, const bool formatKindAlreadyCheched, const bool dataStoreParametersAlreadyCheched);

    virtual void saveUnformatted(OutputStream& outputStream) const;

    virtual void loadUnformatted(InputStream& inputStream, const bool formatKindAlreadyCheched, const size_t numberOfThreads);

    virtual std::unique_ptr<ComponentStatistics> getComponentStatistics() const;

    __ALIGNED(RDFStore<TT>)

};

#endif // RDFSTORE_H_
