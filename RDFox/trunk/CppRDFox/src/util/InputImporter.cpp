// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../RDFStoreException.h"
#include "../dictionary/Dictionary.h"
#include "../equality/EqualityManager.h"
#include "../storage/DataStore.h"
#include "../storage/TupleTable.h"
#include "../tasks/Tasks.h"
#include "Prefixes.h"
#include "ThreadContext.h"
#include "InputImporter.h"

// AbstractInputImporter

void AbstractInputImporter::resolve(const size_t line, const size_t column, const ResourceText& resourceText, ResourceID& resourceID, bool& hasError) {
    try {
        if (resourceText.m_resourceType == BLANK_NODE && m_renameBlankNodes)
            resourceID = m_dictionary->resolveResource(*m_threadContext, &m_dictionaryUsageContext, m_blankNodePrefix + resourceText.m_lexicalForm, D_BLANK_NODE);
        else
            resourceID = m_dictionary->resolveResource(*m_threadContext, &m_dictionaryUsageContext, resourceText);
    }
    catch (const RDFStoreException& error) {
        hasError = true;
        std::ostringstream message;
        message << "Invalid resource " << resourceText.toString(Prefixes::s_defaultPrefixes) << ": " << error;
        reportError(line, column, message.str().c_str());
    }
}

AbstractInputImporter::AbstractInputImporter(Dictionary* const dictionary, const EqualityManager* const equalityManager, std::ostream* const errors) :
    m_dictionaryUsageContext(),
    m_argumentsBuffer(3, INVALID_RESOURCE_ID),
    m_argumentIndexes(),
    m_dictionary(dictionary),
    m_equalityManager(equalityManager),
    m_errors(errors),
    m_threadContext(0),
    m_numberOfUpdates(0),
    m_numberOfUniqueUpdates(0),
    m_numberOfErrors(0),
    m_datalogProgramDecomposer(),
    m_decomposeRules(false),
    m_renameBlankNodes(false),
    m_blankNodePrefix()
{
    m_argumentIndexes.push_back(0);
    m_argumentIndexes.push_back(1);
    m_argumentIndexes.push_back(2);
}

void AbstractInputImporter::setRenameBlankNodes(const bool renameBlankNodes) {
    m_renameBlankNodes = renameBlankNodes;
    std::ostringstream buffer;
    buffer << "bn" << ::getTimePoint() << '_';
    m_blankNodePrefix = buffer.str();
}

void AbstractInputImporter::start() {
    m_threadContext = &ThreadContext::getCurrentThreadContext();
}

void AbstractInputImporter::reportError(const size_t line, const size_t column, const char* const errorDescription) {
    ++m_numberOfErrors;
    if (m_errors)
        *m_errors << "Error in file '" << m_currentFile << "' at line = " << line << ", column = " << column << ": " << errorDescription << std::endl;
    else {
        std::ostringstream message;
        message << "Error in file '" << m_currentFile << "' at line = " << line << ", column = " << column << ": " << errorDescription;
        throw RDF_STORE_EXCEPTION(message.str());
    }
}

void AbstractInputImporter::consumeTriple(const size_t line, const size_t column, const ResourceText& subject, const ResourceText& predicate, const ResourceText& object) {
    bool hasError = false;
    resolve(line, column, subject, m_argumentsBuffer[0], hasError);
    resolve(line, column, predicate, m_argumentsBuffer[1], hasError);
    resolve(line, column, object, m_argumentsBuffer[2], hasError);
    if (!hasError) {
        ++m_numberOfUpdates;
        if (doImport())
            ++m_numberOfUniqueUpdates;
    }
}

void AbstractInputImporter::consumeRule(const size_t line, const size_t column, const Rule& rule) {
    if (m_decomposeRules) {
        DatalogProgram outputDatalogProgram;
        m_datalogProgramDecomposer.decomposeRule(rule, outputDatalogProgram);
        for (DatalogProgram::iterator iterator = outputDatalogProgram.begin(); iterator != outputDatalogProgram.end(); ++iterator) {
            ++m_numberOfUpdates;
            if (doImport(*iterator))
                ++m_numberOfUniqueUpdates;
        }
    }
    else {
        ++m_numberOfUpdates;
        if (doImport(rule))
            ++m_numberOfUniqueUpdates;
    }
}

// DataStoreInputImporter

DataStoreInputImporter::DataStoreInputImporter(DataStore& dataStore, std::ostream* errors) :
    AbstractInputImporter(&dataStore.getDictionary(), &dataStore.getEqualityManager(), errors),
    m_dataStore(dataStore)
{
}

// InputImporter

InputImporter::InputImporter(DataStore& dataStore, std::ostream* errors) : InputImporter(dataStore, errors, dataStore.getTupleTable("internal$rdf")) {
}

InputImporter::InputImporter(DataStore& dataStore, std::ostream* errors, TupleReceiver& tupleReceiver) : DataStoreInputImporter(dataStore, errors), m_tupleReceiver(tupleReceiver) {
}

bool InputImporter::doImport() {
    return ::addTuple(*m_threadContext, m_tupleReceiver, *m_equalityManager, m_argumentsBuffer, m_argumentIndexes).first;
}

bool InputImporter::doImport(const Rule& rule) {
    return m_dataStore.addRule(rule);
}

// AdditionInputImporter

bool AdditionInputImporter::doImport() {
    return m_tripleTable.scheduleForAddition(*m_threadContext, m_argumentsBuffer, m_argumentIndexes);
}

bool AdditionInputImporter::doImport(const Rule& rule) {
    return m_dataStore.addRule(rule);
}

AdditionInputImporter::AdditionInputImporter(DataStore& dataStore, std::ostream* errors) : DataStoreInputImporter(dataStore, errors), m_tripleTable(dataStore.getTupleTable("internal$rdf")) {
}

// DeletionInputImporter

bool DeletionInputImporter::doImport() {
    return m_tripleTable.scheduleForDeletion(*m_threadContext, m_argumentsBuffer, m_argumentIndexes);
}

bool DeletionInputImporter::doImport(const Rule& rule) {
    return m_dataStore.removeRule(rule);
}

DeletionInputImporter::DeletionInputImporter(DataStore& dataStore, std::ostream* errors) : DataStoreInputImporter(dataStore, errors), m_tripleTable(dataStore.getTupleTable("internal$rdf")) {
}

// DatalogProgramInputImporter

DatalogProgramInputImporter::DatalogProgramInputImporter(LogicFactory factory, DatalogProgram& datalogProgram, std::vector<Atom>& facts, std::ostream* errors) :
    m_factory(factory),
    m_datalogProgram(datalogProgram),
    m_facts(facts),
    m_errors(errors),
    m_currentFile(),
    m_numberOfErrors(0)
{
}

void DatalogProgramInputImporter::reportError(const size_t line, const size_t column, const char* const errorDescription) {
    ++m_numberOfErrors;
    if (m_errors)
        *m_errors << "Error in file '" << m_currentFile << "' at line = " << line << ", column = " << column << ": " << errorDescription << std::endl;
    else {
        std::ostringstream message;
        message << "Error in file '" << m_currentFile << "' at line = " << line << ", column = " << column << ": " << errorDescription;
        throw RDF_STORE_EXCEPTION(message.str());
    }
}

void DatalogProgramInputImporter::consumeTriple(const size_t line, const size_t column, const ResourceText& subject, const ResourceText& predicate, const ResourceText& object) {
    Term subjectTerm = m_factory->getResourceByName(subject);
    Term predicateTerm = m_factory->getResourceByName(predicate);
    Term objectTerm = m_factory->getResourceByName(object);
    m_facts.push_back(m_factory->getRDFAtom(subjectTerm, predicateTerm, objectTerm));
}

void DatalogProgramInputImporter::consumeRule(const size_t line, const size_t column, const Rule& rule) {
    m_datalogProgram.push_back(rule);
}
