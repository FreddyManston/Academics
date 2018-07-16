// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "uk_ac_ox_cs_JRDFox_store_TupleIterator.h"
#include "uk_ac_ox_cs_JRDFox_store_Dictionary.h"

#include "JRDFoxCommon.h"
#include "../storage/DataStore.h"
#include "../storage/TupleIterator.h"
#include "../storage/Parameters.h"
#include "../logic/Logic.h"
#include "../formats/turtle/SPARQLParser.h"
#include "../querying/QueryIterator.h"
#include "../util/Prefixes.h"
#include "../util/APILogEntry.h"

JNIEXPORT jlong JNICALL Java_uk_ac_ox_cs_JRDFox_store_TupleIterator_nCompileQuery(JNIEnv* env, jclass, jlong dataStorePtr, jstring jQueryText, jobjectArray jPrefixes, jobjectArray jParameters, jintArray jArity) {
    JNI_METHOD_START("Java_uk_ac_ox_cs_JRDFox_store_TupleIterator_nCompileQuery")
    std::string queryText;
    uk_ac_ox_cs_JRDFox_Common_GetString(env, jQueryText, queryText);
    Prefixes prefixes;
    uk_ac_ox_cs_JRDFox_Common_GetPrefixes(env, jPrefixes, prefixes);
    Parameters parameters;
    uk_ac_ox_cs_JRDFox_Common_GetParameters(env, jParameters, parameters);
    DataStore& dataStore = *reinterpret_cast<DataStore*>(dataStorePtr);
    TupleIterator* tupleIterator = dataStore.compileQuery(prefixes, queryText.c_str(), parameters).release();
    JavaIntArray arity(env, jArity);
    arity[0] = static_cast<jint>(tupleIterator->getArity());
    if (::logAPICalls()) {
        APIDataStoreLogEntry entry(dataStore);
        entry.getOutput() << "# Java_uk_ac_ox_cs_JRDFox_store_TupleIterator_nCompileQuery()" << std::endl;
        for (auto iterator = parameters.begin(); iterator != parameters.end(); ++iterator)
            entry.getOutput() << "set query." << iterator->first << ' ' << iterator->second << std::endl;
        entry.getOutput() << "run ! \\" << std::endl;
        for (const char* characterPointer = queryText.c_str(); *characterPointer != 0; ++characterPointer) {
            if (*characterPointer == '\n')
                entry.getOutput() << "\\";
            entry.getOutput() << *characterPointer;
        }
        entry.getOutput() << std::endl << std::endl;
    }
    JNI_RMETHOD_END("Error while compiling query. ", reinterpret_cast<jlong>(tupleIterator), 0)
}

JNIEXPORT jint JNICALL Java_uk_ac_ox_cs_JRDFox_store_TupleIterator_nOpen(JNIEnv* env, jclass, jlong jTupleIteratorPtr, jint windowSize, jlong jResourceIDsPtr, jlong jMultiplicitiesPtr) {
    JNI_METHOD_START("Java_uk_ac_ox_cs_JRDFox_store_TupleIterator_nOpen")
    TupleIterator* tupleIterator = reinterpret_cast<TupleIterator*>(jTupleIteratorPtr);
    jlong* resourceIDs = reinterpret_cast<jlong*>(jResourceIDsPtr);
    jlong* multiplicities = reinterpret_cast<jlong*>(jMultiplicitiesPtr);
    const std::vector<ResourceID>& argumentsBuffer = tupleIterator->getArgumentsBuffer();
    const std::vector<ArgumentIndex>& argumentIndexes = tupleIterator->getArgumentIndexes();
    jint tupleIndex = 0;
    size_t tupleResourceIndex = 0;
    size_t multiplicity = tupleIterator->open();
    while (multiplicity != 0) {
        multiplicities[tupleIndex++] = static_cast<jlong>(multiplicity);
        for (auto iterator = argumentIndexes.begin(); iterator != argumentIndexes.end(); ++iterator)
            resourceIDs[tupleResourceIndex++] = static_cast<jlong>(argumentsBuffer[*iterator]);
        if (tupleIndex >= windowSize)
            break;
        multiplicity = tupleIterator->advance();
    }
    JNI_RMETHOD_END("Error while opening the iterator.", tupleIndex, 0)
}

JNIEXPORT jint JNICALL Java_uk_ac_ox_cs_JRDFox_store_TupleIterator_nAdvance(JNIEnv* env, jclass, jlong jTupleIteratorPtr, jint windowSize, jlong jResourceIDsPtr, jlong jMultiplicitiesPtr) {
    JNI_METHOD_START("Java_uk_ac_ox_cs_JRDFox_store_TupleIterator_nAdvance")
    TupleIterator* tupleIterator = reinterpret_cast<TupleIterator*>(jTupleIteratorPtr);
    jlong* resourceIDs = reinterpret_cast<jlong*>(jResourceIDsPtr);
    jlong* multiplicities = reinterpret_cast<jlong*>(jMultiplicitiesPtr);
    const std::vector<ResourceID>& argumentsBuffer = tupleIterator->getArgumentsBuffer();
    const std::vector<ArgumentIndex>& argumentIndexes = tupleIterator->getArgumentIndexes();
    jint tupleIndex = 0;
    size_t tupleResourceIndex = 0;
    size_t multiplicity;
    while (tupleIndex < windowSize && (multiplicity = tupleIterator->advance()) > 0) {
        multiplicities[tupleIndex++] = multiplicity;
        for (auto iterator = argumentIndexes.begin(); iterator != argumentIndexes.end(); ++iterator)
            resourceIDs[tupleResourceIndex++] = static_cast<jlong>(argumentsBuffer[*iterator]);
    }
    JNI_RMETHOD_END("Error while advancing the iterator.", tupleIndex, 0)
}

JNIEXPORT jint JNICALL Java_uk_ac_ox_cs_JRDFox_store_TupleIterator_nGetResources(JNIEnv* env, jclass, jlong tupleIteratorPtr, jlong resourceIDsPtr, jint firstIndex, jint limitIndex, jlong stringByteBufferPtr, jint stringByteBufferLength, jlong stringLengthsBufferPtr, jlong datatypeIDsBufferPtr) {
    JNI_METHOD_START("Java_uk_ac_ox_cs_JRDFox_store_Dictionary_nGetResources")
    jint firstUnprocessedIndex = getResources(&reinterpret_cast<QueryIterator*>(tupleIteratorPtr)->getResourceValueCache(), resourceIDsPtr, firstIndex, limitIndex, stringByteBufferPtr, stringByteBufferLength, stringLengthsBufferPtr, datatypeIDsBufferPtr);
    JNI_RMETHOD_END("Error getting ground terms from the dictionary.", firstUnprocessedIndex, 0)
}

JNIEXPORT void JNICALL Java_uk_ac_ox_cs_JRDFox_store_TupleIterator_nDispose(JNIEnv* env, jclass, jlong tupleIteratorPtr) {
    JNI_METHOD_START("Java_uk_ac_ox_cs_JRDFox_store_TupleIterator_nDispose")
    if (tupleIteratorPtr != 0)
        delete reinterpret_cast<TupleIterator*>(tupleIteratorPtr);
    JNI_METHOD_END("Error while disposing of the iterator.")
}
