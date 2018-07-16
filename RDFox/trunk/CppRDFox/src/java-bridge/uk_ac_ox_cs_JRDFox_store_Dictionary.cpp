// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#define _CRT_SECURE_NO_WARNINGS

#include "uk_ac_ox_cs_JRDFox_store_Dictionary.h"
#include "JRDFoxCommon.h"

#include "../dictionary/Dictionary.h"
#include "../Common.h"

JNIEXPORT jlong JNICALL Java_uk_ac_ox_cs_JRDFox_store_Dictionary_nGetMaxResourceID(JNIEnv* env, jclass, jlong dictionaryPtr) {
    JNI_METHOD_START("Java_uk_ac_ox_cs_JRDFox_store_Dictionary_nGetMaxResourceID")
    JNI_RMETHOD_END("Error getting MaxResourceID.", static_cast<jlong>(reinterpret_cast<Dictionary*>(dictionaryPtr)->getMaxResourceID()), 0)
}

JNIEXPORT jint JNICALL Java_uk_ac_ox_cs_JRDFox_store_Dictionary_nGetResources(JNIEnv* env, jclass, jlong dictionaryPtr, jlong resourceIDsPtr, jint firstIndex, jint limitIndex, jlong stringByteBufferPtr, jint stringByteBufferLength, jlong stringLengthsBufferPtr, jlong datatypeIDsBufferPtr) {
    JNI_METHOD_START("Java_uk_ac_ox_cs_JRDFox_store_Dictionary_nGetResources")
    jint firstUnprocessedIndex = getResources(reinterpret_cast<Dictionary*>(dictionaryPtr), resourceIDsPtr, firstIndex, limitIndex, stringByteBufferPtr, stringByteBufferLength, stringLengthsBufferPtr, datatypeIDsBufferPtr);
    JNI_RMETHOD_END("Error getting ground terms from the dictionary.", firstUnprocessedIndex, 0)
}

JNIEXPORT void JNICALL Java_uk_ac_ox_cs_JRDFox_store_Dictionary_nResolveResources (JNIEnv* env, jclass, jlong dictionaryPtr, jlongArray jResourceIDs, jobjectArray jLexicalForms, jintArray jDatatypeIDs) {
    JNI_METHOD_START("Java_uk_ac_ox_cs_JRDFox_store_Dictionary_nResolveResources")
    JavaLongArray  resourceIDs(env, jResourceIDs);
    JavaStringArray lexicalForms(env, jLexicalForms);
    JavaIntArray datatypeIDs(env, jDatatypeIDs);
    std::string lexicalForm;
    for (size_t termIndex = 0; termIndex < resourceIDs.getLength(); termIndex++) {
        lexicalForms.get(termIndex, lexicalForm);
        resourceIDs[termIndex] = static_cast<jint>(reinterpret_cast<Dictionary*>(dictionaryPtr)->resolveResource(lexicalForm.c_str(), static_cast<DatatypeID>(datatypeIDs[termIndex])));
    }
    JNI_METHOD_END("Error getting ground terms from the dictionary.")
}
