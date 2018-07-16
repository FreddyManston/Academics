// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#define _CRT_SECURE_NO_WARNINGS

#include "../../include/RDFoxDataStore.h"
#include "../Common.h"
#include "../RDFStoreException.h"
#include "../dictionary/Dictionary.h"
#include "../equality/EqualityManager.h"
#include "../storage/Parameters.h"
#include "../storage/DataStore.h"
#include "../storage/DataStoreType.h"
#include "../storage/TupleTable.h"
#include "../tasks/Tasks.h"
#include "../util/File.h"
#include "../util/ThreadLocalPointer.h"
#include "../util/APILogEntry.h"
#include "../util/Prefixes.h"
#include "api.h"

ThreadLocalPointer<std::string> s_RDFoxDataStoreErrorMessage;

void RDFoxDataStore_GetLastError(char* const messageBuffer, size_t* const messageBufferSize) {
    copyTextToMemory(*s_RDFoxDataStoreErrorMessage, messageBuffer, *messageBufferSize);
}

bool RDFoxDataStore_Create(RDFoxDataStore* const vDataStore, const char* const storeType, const char ** const parametersArray, const size_t parametersCount) {
    API_FUNCTION_START
    Parameters parameters;
    ::getParameters(parametersArray, parametersCount, parameters);
    *vDataStore = reinterpret_cast<void*>(::newDataStore(storeType, parameters).release());
    API_FUNCTION_END("Error while creating the store.")
}

bool RDFoxDataStore_Initialize(const RDFoxDataStore vDataStore) {
    API_FUNCTION_START
    DataStore& dataStore = *reinterpret_cast<DataStore*>(vDataStore);
    dataStore.initialize();
    if (::logAPICalls()) {
        APIDataStoreLogEntry entry(dataStore);
        entry.getOutput()
            << "# RDFoxDataStore_Initialize()" << std::endl
            << "init " << dataStore.getType().getDataStoreTypeName();
        const Parameters& dataStoreParameters = dataStore.getDataStoreParameters();
        auto dataStoreParametersEnd = dataStoreParameters.end();
        for (auto iterator = dataStoreParameters.begin(); iterator != dataStoreParametersEnd; ++iterator)
            entry.getOutput() << ' ' << iterator->first << ' ' << iterator->second;
        entry.getOutput() << std::endl << std::endl;
    }
    API_FUNCTION_END("Error while initializing the store.")
}

bool RDFoxDataStore_GetDictionary(RDFoxDataStoreDictionary* const vDictionary, const RDFoxDataStore vDataStore) {
    API_FUNCTION_START
    *vDictionary = reinterpret_cast<void*>(&reinterpret_cast<DataStore*>(vDataStore)->getDictionary());
    API_FUNCTION_END("Error while retrieving the dictionary.")
}

bool RDFoxDataStore_SetNumberOfThreads(const RDFoxDataStore vDataStore, const size_t numberOfThreads) {
    API_FUNCTION_START
    DataStore& dataStore = *reinterpret_cast<DataStore*>(vDataStore);
    dataStore.setNumberOfThreads(numberOfThreads);
    if (::logAPICalls()) {
        APIDataStoreLogEntry entry(dataStore);
        entry.getOutput()
            << "# RDFoxDataStore_SetNumberOfThreads(" << numberOfThreads << ")" << std::endl
            << "threads " << numberOfThreads << std::endl
            << std::endl;
    }
    API_FUNCTION_END("Error while retrieving the number of tuples.")
}

static always_inline bool addTriple(DataStore& dataStore, const ResourceID s, const ResourceID p, const ResourceID o, const RDFoxUpdateType updateType) {
    std::vector<ResourceID> argumentsBuffer;
    argumentsBuffer.push_back(s);
    argumentsBuffer.push_back(p);
    argumentsBuffer.push_back(o);
    std::vector<ArgumentIndex> argumentIndexes;
    argumentIndexes.push_back(0);
    argumentIndexes.push_back(1);
    argumentIndexes.push_back(2);
    TupleTable& tupleTable = dataStore.getTupleTable("internal$rdf");
    if (static_cast<UpdateType>(updateType) == ADD)
        return ::addTuple(dataStore.getTupleTable("internal$rdf"), dataStore.getEqualityManager(), argumentsBuffer, argumentIndexes).first;
    else if (static_cast<UpdateType>(updateType) == SCHEDULE_FOR_ADDITION)
        return tupleTable.scheduleForAddition(argumentsBuffer, argumentIndexes);
    else
        return tupleTable.scheduleForDeletion(argumentsBuffer, argumentIndexes);
}

static void logImport(APIDataStoreLogEntry& entry, const UpdateType updateType) {
    entry.getOutput() << "import";
    switch (updateType) {
    case ADD:
        break;
    case SCHEDULE_FOR_ADDITION:
        entry.getOutput() << " +";
        break;
    case SCHEDULE_FOR_DELETION:
        entry.getOutput() << " -";
        break;
    default:
        entry.getOutput() << " <invalid mode>";
    }
}

bool RDFoxDataStore_AddTriplesByResourceIDs(const RDFoxDataStore vDataStore, const size_t numberOfTriples, const RDFoxDataStoreResourceID* resourceIDs, const RDFoxUpdateType updateType) {
    API_FUNCTION_START
    DataStore& dataStore = *reinterpret_cast<DataStore*>(vDataStore);
    for (size_t index = 0; index < numberOfTriples; index++)
    	addTriple(dataStore, resourceIDs[3 * index + 0], resourceIDs[3 * index + 1], resourceIDs[3 * index + 2], updateType);
    if (::logAPICalls()) {
        APIDataStoreLogEntry entry(dataStore);
        entry.getOutput() << "# RDFoxDataStore_AddTripleByResourceIDs(";
        bool first = true;
        for (size_t index = 0; index < numberOfTriples; index++, first = false)
        	entry.getOutput() << (first ? "" : ", ") << resourceIDs[index];
        entry.getOutput() << ")" << std::endl;
        logImport(entry, static_cast<UpdateType>(updateType));
        entry.getOutput() << " ! ";
        for (size_t index = 0; index < numberOfTriples; index++) {
        	entry.printResource(dataStore, resourceIDs[3 * index + 0]);
        	entry.getOutput() << " ";
        	entry.printResource(dataStore, resourceIDs[3 * index + 1]);
        	entry.getOutput() << " ";
        	entry.printResource(dataStore, resourceIDs[3 * index + 2]);
        	entry.getOutput() << " . ";
        }
        entry.getOutput() << std::endl << std::endl;
    }
    API_FUNCTION_END("Error while adding tuple.")
}

bool RDFoxDataStore_AddTriplesByResourceValues(const RDFoxDataStore vDataStore, const size_t numberOfTriples, const char** const lexicalForms, const RDFoxDataStoreDatatypeID* datatypeIDs, const RDFoxUpdateType updateType) {
    API_FUNCTION_START
    DataStore& dataStore = *reinterpret_cast<DataStore*>(vDataStore);
    Dictionary& dictionary = dataStore.getDictionary();
    for (size_t index = 0; index < numberOfTriples; index++)
    	addTriple(dataStore, dictionary.resolveResource(lexicalForms[3 * index + 0], datatypeIDs[3 * index + 0]), dictionary.resolveResource(lexicalForms[3 * index + 1], datatypeIDs[3 * index + 1]), dictionary.resolveResource(lexicalForms[3 * index + 2], datatypeIDs[3 * index + 2]), updateType);
    if (::logAPICalls()) {
        APIDataStoreLogEntry entry(dataStore);
        entry.getOutput() << "# RDFoxDataStore_AddTripleByResourceValues()" << std::endl;
        logImport(entry, static_cast<UpdateType>(updateType));
    	entry.getOutput() << " ! ";
        for (size_t index = 0; index < numberOfTriples; index++) {
        	entry.printResource(lexicalForms[3 * index + 0], datatypeIDs[3 * index + 0]);
        	entry.getOutput() << " ";
        	entry.printResource(lexicalForms[3 * index + 1], datatypeIDs[3 * index + 1]);
        	entry.getOutput() << " ";
        	entry.printResource(lexicalForms[3 * index + 2], datatypeIDs[3 * index + 2]);
        	entry.getOutput() << " .";
        }
        entry.getOutput() << std::endl << std::endl;
    }
    API_FUNCTION_END("Error while adding tuple.")
}

bool RDFoxDataStore_AddTriplesByResources(const RDFoxDataStore vDataStore, const size_t numberOfTriples, const RDFoxDataStoreResourceTypeID* resourceTypeIDs, const char** const lexicalForms, const char** const datatypeIRIs, const RDFoxUpdateType updateType) {
    API_FUNCTION_START
    DataStore& dataStore = *reinterpret_cast<DataStore*>(vDataStore);
    Dictionary& dictionary = dataStore.getDictionary();
    for (size_t index = 0; index < numberOfTriples; index++)
    	addTriple(dataStore, dictionary.resolveResource(static_cast<ResourceType>(resourceTypeIDs[3 * index + 0]), lexicalForms[3 * index + 0], datatypeIRIs[3 * index + 0]), dictionary.resolveResource(static_cast<ResourceType>(resourceTypeIDs[3 * index + 1]), lexicalForms[3 * index + 1], datatypeIRIs[3 * index + 1]), dictionary.resolveResource(static_cast<ResourceType>(resourceTypeIDs[3 * index + 2]), lexicalForms[3 * index + 2], datatypeIRIs[3 * index + 2]), updateType);
    if (::logAPICalls()) {
        APIDataStoreLogEntry entry(dataStore);
        entry.getOutput() << "# RDFoxDataStore_AddTripleByResourceValues()" << std::endl;
        logImport(entry, static_cast<UpdateType>(updateType));
        entry.getOutput() << " ! ";
        for (size_t index = 0; index < numberOfTriples; index++) {
        	entry.printResource(static_cast<ResourceType>(resourceTypeIDs[3 * index + 0]), lexicalForms[3 * index + 0], datatypeIRIs[3 * index + 0]);
        	entry.getOutput() << " ";
        	entry.printResource(static_cast<ResourceType>(resourceTypeIDs[3 * index + 1]), lexicalForms[3 * index + 1], datatypeIRIs[3 * index + 1]);
        	entry.getOutput() << " ";
        	entry.printResource(static_cast<ResourceType>(resourceTypeIDs[3 * index + 2]), lexicalForms[3 * index + 2], datatypeIRIs[3 * index + 2]);
        	entry.getOutput() << " .";
        }
        entry.getOutput() << std::endl << std::endl;
    }
    API_FUNCTION_END("Error while adding tuple.")
}

bool RDFoxDataStore_ImportFiles(const RDFoxDataStore vDataStore, const size_t fileCount, const char** const fileNames, const RDFoxUpdateType updateType, const bool decomposeRules, const bool renameBlankNodes) {
    API_FUNCTION_START
    DataStore& dataStore = *reinterpret_cast<DataStore*>(vDataStore);
    Prefixes prefixes;
    std::ostringstream dummyOutput;
    std::ostringstream errors;
    size_t numberOfUpdates;
    size_t numberOfUniqueUpdates;
    if (!::importFiles(dataStore, std::vector<std::string>(fileNames, fileNames + fileCount), 0, false, static_cast<UpdateType>(updateType), decomposeRules, renameBlankNodes, prefixes, dummyOutput, errors, numberOfUpdates, numberOfUniqueUpdates))
        throw RDF_STORE_EXCEPTION("Error during import: " + errors.str());
    if (::logAPICalls()) {
        APIDataStoreLogEntry entry(dataStore);
        entry.getOutput() << "# RDFoxDataStore_ImportFiles()" << std::endl;
        entry.setDecomposeRules(decomposeRules);
        entry.setRenameBlankNodes(renameBlankNodes);
        logImport(entry, static_cast<UpdateType>(updateType));
        for (size_t fileIndex = 0; fileIndex < fileCount; ++fileIndex) {
            entry.getOutput() << " \\" << std::endl << "    ";
            entry.printString(fileNames[fileIndex]);
        }
        entry.getOutput() << std::endl << std::endl;
    }
    API_FUNCTION_END("Error while importing data.")
}

bool RDFoxDataStore_ImportText(const RDFoxDataStore vDataStore, const char* const text, const RDFoxUpdateType updateType, const bool decomposeRules, const bool renameBlankNodes, const char ** const prefixesArray, const size_t prefixesCount) {
    API_FUNCTION_START
    DataStore& dataStore = *reinterpret_cast<DataStore*>(vDataStore);
    Prefixes prefixes;
    ::getPrefixes(prefixesArray, prefixesCount, prefixes);
    std::ostringstream dummyOutput;
    std::ostringstream errors;
    size_t numberOfUpdates;
    size_t numberOfUniqueUpdates;
    if (!::importText(dataStore, text, ::strlen(text), 0, static_cast<UpdateType>(updateType), decomposeRules, renameBlankNodes, prefixes, dummyOutput, errors, numberOfUpdates, numberOfUniqueUpdates))
        throw RDF_STORE_EXCEPTION("Error during import: " + errors.str());
    if (::logAPICalls()) {
        APIDataStoreLogEntry entry(dataStore);
        entry.getOutput() << "# RDFoxDataStore_ImportText()" << std::endl;
        entry.setDecomposeRules(decomposeRules);
        entry.setRenameBlankNodes(renameBlankNodes);
        logImport(entry, static_cast<UpdateType>(updateType));
        entry.getOutput() << " ! \\" << std::endl;
        for (const char* textPointer = text; *textPointer != 0; ++textPointer) {
            if (*textPointer == '\n')
                entry.getOutput() << "\\";
            entry.getOutput() << *textPointer;
        }
        entry.getOutput() << std::endl << std::endl;
    }
    API_FUNCTION_END("Error while importing data.")
}

bool RDFoxDataStore_ApplyRules(const RDFoxDataStore vDataStore, const bool incrementally) {
    API_FUNCTION_START
    DataStore& dataStore = *reinterpret_cast<DataStore*>(vDataStore);
    if (incrementally)
        dataStore.applyRulesIncrementally(true);
    else
        dataStore.applyRules(true);
    if (::logAPICalls()) {
        APIDataStoreLogEntry entry(dataStore);
        entry.getOutput() << "# RDFoxDataStore_ApplyRules(" << (incrementally ? "true" : "false") << ")" << std::endl << "mat";
        if (incrementally)
            entry.getOutput() << " inc";
        entry.getOutput() << std::endl << std::endl;
    }
    API_FUNCTION_END("Error while materializing data.")
}

void RDFoxDataStore_Dispose(const RDFoxDataStore vDataStore) {
    DataStore* dataStore = reinterpret_cast<DataStore*>(vDataStore);
    if (::logAPICalls()) {
        APIDataStoreLogEntry entry(*dataStore);
        entry.getOutput() << "# RDFoxDataStore_Dispose()" << std::endl << "drop" << std::endl << std::endl;
    }
    delete dataStore;
}

bool RDFoxDataStore_Save(const RDFoxDataStore vDataStore, const char* const fileName) {
    API_FUNCTION_START
    DataStore* dataStore = reinterpret_cast<DataStore*>(vDataStore);
    File outputFile;
    outputFile.open(fileName, File::CREATE_NEW_OR_TRUNCATE_EXISTING_FILE, false, true, false, false);
    File::OutputStreamType outputStream(outputFile);
    dataStore->saveFormatted(outputStream);
    API_FUNCTION_END("Error while saving store")
}

bool RDFoxDataStore_Load(RDFoxDataStore* const vDataStore, const char* const fileName) {
    API_FUNCTION_START
    File inputFile;
    inputFile.open(fileName, File::OPEN_EXISTING_FILE, true, false, true, false);
    File::InputStreamType inputStream(inputFile);
    *vDataStore = reinterpret_cast<RDFoxDataStore>(::newDataStoreFromFormattedFile(inputStream, false).release());
    API_FUNCTION_END("Error while loading store")
}
