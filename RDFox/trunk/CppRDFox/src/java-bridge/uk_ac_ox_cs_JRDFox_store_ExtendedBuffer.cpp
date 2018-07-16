// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#define _CRT_SECURE_NO_WARNINGS

#include "uk_ac_ox_cs_JRDFox_store_ExtendedBuffer.h"
#include "JRDFoxCommon.h"

JNIEXPORT jlong JNICALL Java_uk_ac_ox_cs_JRDFox_store_ExtendedBuffer_nGetBufferPtr(JNIEnv* env, jclass, jobject buffer) {
    JNI_METHOD_START("Java_uk_ac_ox_cs_JRDFox_store_Dictionary_nGetBufferPtr")
    JNI_RMETHOD_END("Error getting buffer pointer.", reinterpret_cast<jlong>(env->GetDirectBufferAddress(buffer)), 0)
}
