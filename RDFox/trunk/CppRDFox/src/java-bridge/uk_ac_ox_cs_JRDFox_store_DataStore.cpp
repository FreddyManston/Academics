// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "uk_ac_ox_cs_JRDFox_store_DataStore.h"
#include "JRDFoxCommon.h"

#include "../RDFStoreException.h"
#include "../dictionary/Dictionary.h"
#include "../storage/Parameters.h"
#include "../storage/DataStore.h"
#include "../storage/DataStoreType.h"
#include "../storage/TupleTable.h"
#include "../formats/InputOutput.h"
#include "../tasks/Tasks.h"
#include "../util/DatalogProgramDecomposer.h"
#include "../util/File.h"
#include "../util/APILogEntry.h"
#include "../util/Prefixes.h"

JNIEXPORT void JNICALL Java_uk_ac_ox_cs_JRDFox_store_DataStore_nCreate(JNIEnv* env, jclass, jstring jStoreType, jobjectArray jDataStoreParameters, jlongArray jPointers) {
    JNI_METHOD_START("Java_uk_ac_ox_cs_JRDFox_store_DataStore_nCreate")
    std::string storeType;
    uk_ac_ox_cs_JRDFox_Common_GetString(env, jStoreType, storeType);
    Parameters dataStoreParameters;
    uk_ac_ox_cs_JRDFox_Common_GetParameters(env, jDataStoreParameters, dataStoreParameters);
    DataStore* dataStore = ::newDataStore(storeType.c_str(), dataStoreParameters).release();
    JavaLongArray pointers(env, jPointers);
    pointers[0] = reinterpret_cast<jlong>(dataStore);
    pointers[1] = reinterpret_cast<jlong>(&dataStore->getDictionary());
    JNI_METHOD_END("Error creating the store.")
}

JNIEXPORT void JNICALL Java_uk_ac_ox_cs_JRDFox_store_DataStore_nInitialize(JNIEnv* env, jclass, jlong dataStorePtr) {
    JNI_METHOD_START("Java_uk_ac_ox_cs_JRDFox_store_DataStore_nInitialize")
    DataStore& dataStore = *reinterpret_cast<DataStore*>(dataStorePtr);
    dataStore.initialize();
    if (::logAPICalls()) {
        APIDataStoreLogEntry entry(dataStore);
        entry.getOutput()
            << "# Java_uk_ac_ox_cs_JRDFox_store_DataStore_nInitialize()" << std::endl
            << "init " << dataStore.getType().getDataStoreTypeName();
        const Parameters& dataStoreParameters = dataStore.getDataStoreParameters();
        auto dataStoreParametersEnd = dataStoreParameters.end();
        for (auto iterator = dataStoreParameters.begin(); iterator != dataStoreParametersEnd; ++iterator)
            entry.getOutput() << ' ' << iterator->first << ' ' << iterator->second;
        entry.getOutput() << std::endl << std::endl;
    }
    JNI_METHOD_END("Error initializing the store.")
}

JNIEXPORT void JNICALL Java_uk_ac_ox_cs_JRDFox_store_DataStore_nSetNumberOfThreads(JNIEnv* env, jclass, jlong dataStorePtr, jint numberOfThreads) {
    JNI_METHOD_START("Java_uk_ac_ox_cs_JRDFox_store_DataStore_nSetNumberOfThreads")
    DataStore& dataStore = *reinterpret_cast<DataStore*>(dataStorePtr);
    dataStore.setNumberOfThreads(static_cast<size_t>(numberOfThreads));
    if (::logAPICalls()) {
        APIDataStoreLogEntry entry(dataStore);
        entry.getOutput()
            << "# Java_uk_ac_ox_cs_JRDFox_store_DataStore_nSetNumberOfThreads(" << numberOfThreads << ")" << std::endl
            << "threads " << numberOfThreads << std::endl
            << std::endl;
    }
    JNI_METHOD_END("Error setting the number of workers.")
}

JNIEXPORT void JNICALL Java_uk_ac_ox_cs_JRDFox_store_DataStore_nAddTriples(JNIEnv* env, jclass, jlong dataStorePtr, jobjectArray jLexicalForms, jintArray jDatatypeIDs, jint numberOfResources, jint updateType) {
    JNI_METHOD_START("Java_uk_ac_ox_cs_JRDFox_store_DataStore_nAddTriples")
    JavaIntArray datatypeIDs(env, jDatatypeIDs);
    JavaStringArray lexicalForms(env, jLexicalForms);
    assert(numberOfResources % 3 == 0);
    assert(numberOfResources <= datatypeIDs.getLength());
    assert(numberOfResources <= lexicalForms.getLength());
    std::vector<ResourceID> argumentsBuffer(3, INVALID_RESOURCE_ID);
    std::vector<ArgumentIndex> argumentIndexes;
    argumentIndexes.push_back(0);
    argumentIndexes.push_back(1);
    argumentIndexes.push_back(2);
    DataStore& dataStore = *reinterpret_cast<DataStore*>(dataStorePtr);
    TupleTable& tupleTable = dataStore.getTupleTable("internal$rdf");
    Dictionary& dictionary = dataStore.getDictionary();
    std::string lexicalForm;
    for (int32_t tupleIndex = 0; tupleIndex < numberOfResources; tupleIndex += 3) {
        for (size_t termIndex = 0; termIndex < 3; ++termIndex) {
            lexicalForms.get(tupleIndex + termIndex, lexicalForm);
            argumentsBuffer[termIndex] = dictionary.resolveResource(lexicalForm, static_cast<DatatypeID>(datatypeIDs[tupleIndex + termIndex]));
        }
        if (updateType == SCHEDULE_FOR_ADDITION)
            tupleTable.scheduleForAddition(argumentsBuffer, argumentIndexes);
        else if (updateType == SCHEDULE_FOR_DELETION)
            tupleTable.scheduleForDeletion(argumentsBuffer, argumentIndexes);
        else
            ::addTuple(tupleTable, dataStore.getEqualityManager(), argumentsBuffer, argumentIndexes);
        if (::logAPICalls()) {
            APIDataStoreLogEntry entry(dataStore);
            entry.getOutput() << "# Java_uk_ac_ox_cs_JRDFox_store_DataStore_nAddTriples()" << std::endl << "import ";
            if (updateType == SCHEDULE_FOR_ADDITION)
                entry.getOutput() << "+ ";
            else if (updateType == SCHEDULE_FOR_DELETION)
                entry.getOutput() << "- ";
            entry.getOutput() << " ! ";
            lexicalForms.get(tupleIndex + 0, lexicalForm);
            entry.printResource(lexicalForm.c_str(), static_cast<DatatypeID>(datatypeIDs[tupleIndex + 0]));
            entry.getOutput() << " ";
            lexicalForms.get(tupleIndex + 1, lexicalForm);
            entry.printResource(lexicalForm.c_str(), static_cast<DatatypeID>(datatypeIDs[tupleIndex + 1]));
            entry.getOutput() << " ";
            lexicalForms.get(tupleIndex + 2, lexicalForm);
            entry.printResource(lexicalForm.c_str(), static_cast<DatatypeID>(datatypeIDs[tupleIndex + 2]));
            entry.getOutput() << " ." << std::endl << std::endl;
        }

    }
    JNI_METHOD_END("Error adding a tuple.")
}

JNIEXPORT void JNICALL Java_uk_ac_ox_cs_JRDFox_store_DataStore_nAddTriplesByResourceIDs(JNIEnv* env, jclass, jlong dataStorePtr, jlongArray jResourceIDs, jint numberOfResources, jint updateType) {
    JNI_METHOD_START("Java_uk_ac_ox_cs_JRDFox_store_DataStore_nAddTriplesByResourceIDs")
    JavaLongArray resourceIDs(env, jResourceIDs);
    assert(numberOfResources % 3 == 0);
    assert(numberOfResources <= resourceIDs.getLength());
    std::vector<ResourceID> argumentsBuffer;
    std::vector<ArgumentIndex> argumentIndexes;
    for (ArgumentIndex index = 0; index < 3; index++) {
        argumentsBuffer.push_back(0);
        argumentIndexes.push_back(index);
    }
    DataStore& dataStore = *reinterpret_cast<DataStore*>(dataStorePtr);
    TupleTable& tupleTable = dataStore.getTupleTable("internal$rdf");
    for (int32_t tupleIndex = 0; tupleIndex < numberOfResources; tupleIndex += 3) {
        for (size_t termIndex = 0; termIndex < 3; ++termIndex)
            argumentsBuffer[termIndex] = resourceIDs[tupleIndex + termIndex];
        if (updateType == SCHEDULE_FOR_ADDITION)
            tupleTable.scheduleForAddition(argumentsBuffer, argumentIndexes);
        else if (updateType == SCHEDULE_FOR_DELETION)
            tupleTable.scheduleForDeletion(argumentsBuffer, argumentIndexes);
        else
            ::addTuple(tupleTable, dataStore.getEqualityManager(), argumentsBuffer, argumentIndexes);
        if (::logAPICalls()) {
            APIDataStoreLogEntry entry(dataStore);
            entry.getOutput() << "# Java_uk_ac_ox_cs_JRDFox_store_DataStore_nAddTriplesByResourceIDs()" << std::endl << "import ";
            if (updateType == SCHEDULE_FOR_ADDITION)
                entry.getOutput() << "+ ";
            else if (updateType == SCHEDULE_FOR_DELETION)
                entry.getOutput() << "- ";
            entry.getOutput() << " ! ";
            entry.printResource(dataStore, resourceIDs[tupleIndex + 0]);
            entry.getOutput() << " ";
            entry.printResource(dataStore, resourceIDs[tupleIndex + 1]);
            entry.getOutput() << " ";
            entry.printResource(dataStore, resourceIDs[tupleIndex + 2]);
            entry.getOutput() << " ." << std::endl << std::endl;
        }
    }
    JNI_METHOD_END("Error adding a tuple.")
}

JNIEXPORT void JNICALL Java_uk_ac_ox_cs_JRDFox_store_DataStore_nImportFiles(JNIEnv* env, jclass, jlong dataStorePtr, jobjectArray jFileNames, jint updateType, jboolean decomposeRules, jboolean renameBlankNodes) {
    JNI_METHOD_START("Java_uk_ac_ox_cs_JRDFox_store_DataStore_nImportFiles")
    DataStore& dataStore = *reinterpret_cast<DataStore*>(dataStorePtr);
    Prefixes prefixes;
    std::ostringstream duymmyOutput;
    std::ostringstream errors;
    size_t numberOfUpdates;
    size_t numberOfUniqueUpdates;
    JavaStringArray fileNamesArray(env, jFileNames);
    const size_t fileNamesLength = fileNamesArray.getLength();
    std::vector<std::string> fileNames(fileNamesLength);
    for (size_t index = 0; index < fileNamesLength; index++)
        fileNamesArray.get(index, fileNames[index]);
    if (!::importFiles(dataStore, fileNames, 0, false, static_cast<UpdateType>(updateType), decomposeRules == JNI_TRUE, renameBlankNodes == JNI_TRUE, prefixes, duymmyOutput, errors, numberOfUpdates, numberOfUniqueUpdates))
        throw RDF_STORE_EXCEPTION("Error during import: " + errors.str());
    if (::logAPICalls()) {
        APIDataStoreLogEntry entry(dataStore);
        entry.getOutput() << "# Java_uk_ac_ox_cs_JRDFox_store_DataStore_nImportTurtleFiles()" << std::endl;
        entry.setDecomposeRules(decomposeRules == JNI_TRUE);
        entry.setRenameBlankNodes(renameBlankNodes == JNI_TRUE);
        entry.getOutput() << "import";
        if (updateType == SCHEDULE_FOR_ADDITION)
            entry.getOutput() << " +";
        else if (updateType == SCHEDULE_FOR_DELETION)
            entry.getOutput() << " -";
        if (fileNames.size() == 1)
            entry.printString(fileNames[0].c_str());
        else {
            for (std::vector<std::string>::const_iterator iterator = fileNames.begin(); iterator != fileNames.end(); ++iterator) {
                entry.getOutput() << " \\" << std::endl << "    ";
                entry.printString(iterator->c_str());
            }
        }
        entry.getOutput() << std::endl << std::endl;
    }
    JNI_METHOD_END("Importation error.")
}

JNIEXPORT void JNICALL Java_uk_ac_ox_cs_JRDFox_store_DataStore_nImportText(JNIEnv* env, jclass, jlong dataStorePtr, jstring jText, jint updateType, jboolean decomposeRules, jboolean renameBlankNodes, jobjectArray jPrefixes) {
    JNI_METHOD_START("Java_uk_ac_ox_cs_JRDFox_store_DataStore_nImportText")
    DataStore& dataStore = *reinterpret_cast<DataStore*>(dataStorePtr);
    Prefixes prefixes;
    uk_ac_ox_cs_JRDFox_Common_GetPrefixes(env, jPrefixes, prefixes);
    std::ostringstream dummyOutput;
    std::ostringstream errors;
    size_t numberOfUpdates;
    size_t numberOfUniqueUpdates;
    std::string text;
    uk_ac_ox_cs_JRDFox_Common_GetString(env, jText, text);
    if (!::importText(dataStore, text.c_str(), text.length(), 0, static_cast<UpdateType>(updateType), decomposeRules == JNI_TRUE, renameBlankNodes == JNI_TRUE, prefixes, dummyOutput, errors, numberOfUpdates, numberOfUniqueUpdates))
        throw RDF_STORE_EXCEPTION("Error during import: " + errors.str());
    if (::logAPICalls()) {
        APIDataStoreLogEntry entry(dataStore);
        entry.getOutput() << "# Java_uk_ac_ox_cs_JRDFox_store_DataStore_nImportText()" << std::endl;
        entry.setDecomposeRules(decomposeRules == JNI_TRUE);
        entry.setRenameBlankNodes(renameBlankNodes == JNI_TRUE);
        entry.getOutput() << "import";
        if (updateType == SCHEDULE_FOR_ADDITION)
            entry.getOutput() << " +";
        if (updateType == SCHEDULE_FOR_DELETION)
            entry.getOutput() << " -";
        entry.getOutput() << " ! \\" << std::endl;
        for (const char* programText = text.c_str(); *programText != 0; ++programText) {
            if (*programText == '\n')
                entry.getOutput() << "\\";
            entry.getOutput() << *programText;
        }
        entry.getOutput() << std::endl << std::endl;
        entry.getOutput() << std::endl;
    }
    JNI_METHOD_END("Importation error.")
}


JNIEXPORT void JNICALL Java_uk_ac_ox_cs_JRDFox_store_DataStore_nExport(JNIEnv* env, jclass, jlong dataStorePtr, jstring jFileName, jstring jFormatName) {
    JNI_METHOD_START("Java_uk_ac_ox_cs_JRDFox_store_DataStore_nExport")
    DataStore& dataStore = *reinterpret_cast<DataStore*>(dataStorePtr);
    std::string fileName;
    uk_ac_ox_cs_JRDFox_Common_GetString(env, jFileName, fileName);
    std::string formatName;
    uk_ac_ox_cs_JRDFox_Common_GetString(env, jFormatName, formatName);
    File outputFile;
    outputFile.open(fileName, File::CREATE_NEW_OR_TRUNCATE_EXISTING_FILE, false, true, true);
    File::OutputStreamType outputStream(outputFile);
    Prefixes prefixes;
    ::save(dataStore, prefixes, outputStream, formatName);
    outputStream.flush();
    outputFile.close();
    if (::logAPICalls()) {
        APIDataStoreLogEntry entry(dataStore);
        entry.getOutput() << "# Java_uk_ac_ox_cs_JRDFox_store_DataStore_nExport()" << std::endl << "export ";
        entry.printString(fileName.c_str());
        entry.getOutput() << " ";
        entry.printString(formatName.c_str());
        entry.getOutput() << std::endl << std::endl;
    }
    JNI_METHOD_END("Error during exporting the store.")
}

JNIEXPORT void JNICALL Java_uk_ac_ox_cs_JRDFox_store_DataStore_nApplyRules(JNIEnv* env, jclass, jlong dataStorePtr, jboolean incrementally) {
    JNI_METHOD_START("Java_uk_ac_ox_cs_JRDFox_store_DataStore_nApplyRules")
    DataStore& dataStore = *reinterpret_cast<DataStore*>(dataStorePtr);
    if (incrementally)
        dataStore.applyRulesIncrementally(true);
    else
        dataStore.applyRules(true);
    if (::logAPICalls()) {
        APIDataStoreLogEntry entry(dataStore);
        entry.getOutput() << "# Java_uk_ac_ox_cs_JRDFox_store_DataStore_nApplyRules(" << (incrementally ? "true" : "false") << ")" << std::endl << "mat";
        if (incrementally)
            entry.getOutput() << " inc";
        entry.getOutput() << std::endl << std::endl;
    }
    JNI_METHOD_END("Error during materialization.")
}

JNIEXPORT void JNICALL Java_uk_ac_ox_cs_JRDFox_store_DataStore_nUpdateStatistics(JNIEnv* env, jclass, jlong dataStorePtr) {
    JNI_METHOD_START("Java_uk_ac_ox_cs_JRDFox_store_DataStore_nUpdateStatistics")
    DataStore& dataStore = *reinterpret_cast<DataStore*>(dataStorePtr);
    dataStore.updateStatistics();
    if (::logAPICalls()) {
        APIDataStoreLogEntry entry(dataStore);
        entry.getOutput() << "# Java_uk_ac_ox_cs_JRDFox_store_DataStore_nUpdateStatistics()" << std::endl << "updatestats" << std::endl << std::endl;
    }
    JNI_METHOD_END("Error during updating statistics.")
}

JNIEXPORT void JNICALL Java_uk_ac_ox_cs_JRDFox_store_DataStore_nMakeFactsExplicit(JNIEnv* env, jclass, jlong dataStorePtr) {
    JNI_METHOD_START("Java_uk_ac_ox_cs_JRDFox_store_DataStore_nMakeFactsExplicit")
    DataStore& dataStore = *reinterpret_cast<DataStore*>(dataStorePtr);
    dataStore.makeFactsExplicit();
    if (::logAPICalls()) {
        APIDataStoreLogEntry entry(dataStore);
        entry.getOutput() << "# Java_uk_ac_ox_cs_JRDFox_store_DataStore_nMakeFactsExplicit()" << std::endl << "explicate" << std::endl << std::endl;
    }
    JNI_METHOD_END("Error during clearing rules.")
}

JNIEXPORT void JNICALL Java_uk_ac_ox_cs_JRDFox_store_DataStore_nClearRulesAndMakeFactsExplicit(JNIEnv* env, jclass, jlong dataStorePtr) {
    JNI_METHOD_START("Java_uk_ac_ox_cs_JRDFox_store_DataStore_nClearRulesAndMakeFactsExplicit")
    DataStore& dataStore = *reinterpret_cast<DataStore*>(dataStorePtr);
    dataStore.clearRulesAndMakeFactsExplicit();
    if (::logAPICalls()) {
        APIDataStoreLogEntry entry(dataStore);
        entry.getOutput() << "# Java_uk_ac_ox_cs_JRDFox_store_DataStore_nClearRulesAndMakeFactsExplicit()" << std::endl << "explicate clr" << std::endl << std::endl;
    }
    JNI_METHOD_END("Error during clearing rules.")
}

JNIEXPORT void JNICALL Java_uk_ac_ox_cs_JRDFox_store_DataStore_nSave(JNIEnv* env, jclass, jlong dataStorePtr, jstring jFileName) {
    JNI_METHOD_START("Java_uk_ac_ox_cs_JRDFox_store_DataStore_nSave")
    DataStore& dataStore = *reinterpret_cast<DataStore*>(dataStorePtr);
    std::string fileName;
    uk_ac_ox_cs_JRDFox_Common_GetString(env, jFileName, fileName);
    File outputFile;
    outputFile.open(fileName, File::CREATE_NEW_OR_TRUNCATE_EXISTING_FILE, false, true, false, false);
    File::OutputStreamType outputStream(outputFile);
    dataStore.saveFormatted(outputStream);
    if (::logAPICalls()) {
        APIDataStoreLogEntry entry(dataStore);
        entry.getOutput() << "# Java_uk_ac_ox_cs_JRDFox_store_DataStore_nSave()" << std::endl << "fsave ";
        entry.printString(fileName.c_str());
        entry.getOutput() << std::endl << std::endl;
    }
    JNI_METHOD_END("Error during saving the store.")
}

JNIEXPORT void JNICALL Java_uk_ac_ox_cs_JRDFox_store_DataStore_nLoad(JNIEnv* env, jclass, jstring jFileName, jlongArray jPointers) {
    JNI_METHOD_START("Java_uk_ac_ox_cs_JRDFox_store_DataStore_nLoad")
    std::string fileName;
    uk_ac_ox_cs_JRDFox_Common_GetString(env, jFileName, fileName);
    File inputFile;
    inputFile.open(fileName, File::OPEN_EXISTING_FILE, true, false, true, false);
    File::InputStreamType inputStream(inputFile);
    std::unique_ptr<DataStore> dataStore = ::newDataStoreFromFormattedFile(inputStream, false);
    if (::logAPICalls()) {
        APIDataStoreLogEntry entry(*dataStore);
        entry.getOutput() << "# Java_uk_ac_ox_cs_JRDFox_store_DataStore_nLoad()" << std::endl << "load ";
        entry.printString(fileName.c_str());
        entry.getOutput() << std::endl << std::endl;
    }
    JavaLongArray pointers(env, jPointers);
    pointers[1] = reinterpret_cast<jlong>(&dataStore->getDictionary());
    pointers[0] = reinterpret_cast<jlong>(dataStore.release());
    JNI_METHOD_END("Error during loading the store.")
}

JNIEXPORT void JNICALL Java_uk_ac_ox_cs_JRDFox_store_DataStore_nDispose(JNIEnv* env, jclass, jlong dataStorePtr) {
    JNI_METHOD_START("Java_uk_ac_ox_cs_JRDFox_store_DataStore_nDispose")
    DataStore* dataStore = reinterpret_cast<DataStore*>(dataStorePtr);
    if (dataStore != 0) {
        if (::logAPICalls()) {
            APIDataStoreLogEntry entry(*dataStore);
            entry.getOutput() << "# Java_uk_ac_ox_cs_JRDFox_store_DataStore_nDispose()" << std::endl << "drop" << std::endl << std::endl;
        }
        delete dataStore;
    }
    JNI_METHOD_END("Error during loading the store.")
}
