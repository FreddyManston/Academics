// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef INPUTIMPORTER_H_
#define INPUTIMPORTER_H_

#include "../Common.h"
#include "../logic/Logic.h"
#include "../dictionary/DictionaryUsageContext.h"
#include "../formats/InputConsumer.h"
#include "DatalogProgramDecomposer.h"

class DataStore;
class EqualityManager;
class TupleReceiver;
class TupleTable;
class Dictionary;

// AbstractInputImporter

class AbstractInputImporter : public InputConsumer {

protected:

    DictionaryUsageContext m_dictionaryUsageContext;
    std::vector<ResourceID> m_argumentsBuffer;
    std::vector<ArgumentIndex> m_argumentIndexes;
    Dictionary* const m_dictionary;
    const EqualityManager* const m_equalityManager;
    std::ostream* m_errors;
    ThreadContext* m_threadContext;
    std::string m_currentFile;
    size_t m_numberOfUpdates;
    size_t m_numberOfUniqueUpdates;
    size_t m_numberOfErrors;
    DatalogProgramDecomposer m_datalogProgramDecomposer;
    bool m_decomposeRules;
    bool m_renameBlankNodes;
    std::string m_blankNodePrefix;

    void resolve(const size_t line, const size_t column, const ResourceText& resourceText, ResourceID& resourceID, bool& hasError);

    virtual bool doImport() = 0;

    virtual bool doImport(const Rule& rule) = 0;

public:

    AbstractInputImporter(Dictionary* const dictionary, const EqualityManager* const equalityManager, std::ostream* const errors);

    always_inline size_t getNumberOfUpdates() const {
        return m_numberOfUpdates;
    }

    always_inline size_t getNumberOfUniqueUpdates() const {
        return m_numberOfUniqueUpdates;
    }

    always_inline size_t getNumberOfErrors() const {
        return m_numberOfErrors;
    }

    always_inline void setCurrentFile(const std::string& currentFile) {
        m_currentFile = currentFile;
    }

    always_inline const std::string& getCurrentFile() const {
        return m_currentFile;
    }

    always_inline bool getDecomposeRules() const {
        return m_decomposeRules;
    }

    always_inline void setDecomposeRules(const bool decomposeRules) {
        m_decomposeRules = decomposeRules;
    }

    always_inline bool getRenameBlankNodes() const {
        return m_renameBlankNodes;
    }

    void setRenameBlankNodes(const bool renameBlankNodes);

    virtual void start();

    virtual void reportError(const size_t line, const size_t column, const char* const errorDescription);

    virtual void consumeTriple(const size_t line, const size_t column, const ResourceText& subject, const ResourceText& predicate, const ResourceText& object);

    virtual void consumeRule(const size_t line, const size_t column, const Rule& rule);

};

// DataStoreInputImporter

class DataStoreInputImporter : public AbstractInputImporter {

protected:

    DataStore& m_dataStore;

public:

    DataStoreInputImporter(DataStore& dataStore, std::ostream* errors);

};

// InputImporter

class InputImporter : public DataStoreInputImporter {

protected:

    TupleReceiver& m_tupleReceiver;

    virtual bool doImport();

    virtual bool doImport(const Rule& rule);

public:

    InputImporter(DataStore& dataStore, std::ostream* errors);

    InputImporter(DataStore& dataStore, std::ostream* errors, TupleReceiver& tupleReceiver);

};

// AdditionInputImporter

class AdditionInputImporter : public DataStoreInputImporter {

protected:

    TupleTable& m_tripleTable;

    virtual bool doImport();

    virtual bool doImport(const Rule& rule);

public:

    AdditionInputImporter(DataStore& dataStore, std::ostream* errors);

};

// DeletionInputImporter

class DeletionInputImporter : public DataStoreInputImporter {

protected:

    TupleTable& m_tripleTable;

    virtual bool doImport();

    virtual bool doImport(const Rule& rule);

public:

    DeletionInputImporter(DataStore& dataStore, std::ostream* errors);

};

// DatalogProgramInputImpoter

class DatalogProgramInputImporter : public InputConsumer {

protected:

    LogicFactory m_factory;
    DatalogProgram& m_datalogProgram;
    std::vector<Atom>& m_facts;
    std::ostream* m_errors;
    std::string m_currentFile;
    size_t m_numberOfErrors;

public:

    DatalogProgramInputImporter(LogicFactory factory, DatalogProgram& datalogProgram, std::vector<Atom>& facts, std::ostream* errors);

    always_inline size_t getNumberOfErrors() const {
        return m_numberOfErrors;
    }

    always_inline void setCurrentFile(const std::string& currentFile) {
        m_currentFile = currentFile;
    }

    always_inline const std::string& getCurrentFile() const {
        return m_currentFile;
    }
    
    virtual void reportError(const size_t line, const size_t column, const char* const errorDescription);

    virtual void consumeTriple(const size_t line, const size_t column, const ResourceText& subject, const ResourceText& predicate, const ResourceText& object);

    virtual void consumeRule(const size_t line, const size_t column, const Rule& rule);

};

#endif // INPUTIMPORTER_H_
