// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef UK_AC_OX_CS_JRDFOX_H_
#define UK_AC_OX_CS_JRDFOX_H_

#include <jni.h>

#include "../util/Prefixes.h"
#include "../RDFStoreException.h"
#include "../Common.h"
#include "../util/APILogEntry.h"
#include "../storage/Parameters.h"

class JavaSideException { };

#define JNI_METHOD_START(methodName)\
    std::string JNI_METHOD_NAME = methodName;\
    if (::logAPICalls()) {\
    	APILogEntry logEntry;\
    	(logEntry.getOutput() << std::endl << "# " << JNI_METHOD_NAME << " {" << std::endl << std::endl).flush();\
    }\
    try {


#define JNI_METHOD_END(errorMessage)\
		if (::logAPICalls()) {\
			APILogEntry logEntry;\
			(logEntry.getOutput() << std::endl << "# } \\\\ " << JNI_METHOD_NAME << std::endl << std::endl).flush();\
		}\
    }\
    catch (const JavaSideException&) { }\
    catch (const RDFStoreException& e) {\
        uk_ac_ox_cs_JRDFox_Common_ThrowRDFoxException(env, e.what());\
    }\
    catch (...) {\
        uk_ac_ox_cs_JRDFox_Common_ThrowRDFoxException(env, errorMessage);\
    }

#define JNI_RMETHOD_END(errorMessage, returnValue, defaultReturnValue)\
        if (::logAPICalls()) {\
        	APILogEntry logEntry;\
            (logEntry.getOutput() << std::endl << "# } \\\\ " << JNI_METHOD_NAME << std::endl << std::endl).flush();\
        }\
        return returnValue;\
    }\
    catch (const JavaSideException&) { }\
    catch (const RDFStoreException& e) {\
        uk_ac_ox_cs_JRDFox_Common_ThrowRDFoxException(env, e.what());\
    }\
    catch (...) {\
        uk_ac_ox_cs_JRDFox_Common_ThrowRDFoxException(env, errorMessage);\
    }\
    return defaultReturnValue;


#define CHECKJAVASIDEEXCEPTION(env)  \
    if (env->ExceptionOccurred()) {\
        env->ExceptionClear();\
        throw JavaSideException();\
    }\

always_inline void uk_ac_ox_cs_JRDFox_Common_ThrowRDFoxException(JNIEnv* env, const char* const message) {
    jclass jrdfoxExceptionClass = env->FindClass("uk/ac/ox/cs/JRDFox/JRDFoxException");
    if (jrdfoxExceptionClass != 0 && !env->ExceptionCheck())
        env->ThrowNew(jrdfoxExceptionClass, message);
    else {
        jclass exceptionClass = env->FindClass("java/lang/Exception");
        if (exceptionClass != 0 && !env->ExceptionCheck())
            env->ThrowNew(exceptionClass, message);
        else
            std::cerr << "exception in RDFox: " << message << std::endl;
    }
}

always_inline void uk_ac_ox_cs_JRDFox_Common_GetString(JNIEnv* env, jstring jstr, std::string& result) {
    const char* str = env->GetStringUTFChars(jstr, 0);
    CHECKJAVASIDEEXCEPTION(env)
    result = str;
    env->ReleaseStringUTFChars(jstr, str);
    CHECKJAVASIDEEXCEPTION(env)
}

class JavaIntArray {

protected:

    JNIEnv* m_env;
    jintArray m_intArray;
    jint* m_data;
    size_t m_size;

public:

    always_inline JavaIntArray(JNIEnv* env, jintArray intArray) : m_env(env), m_intArray(intArray), m_size(static_cast<size_t>(-1)) {
        m_data = m_env->GetIntArrayElements(m_intArray, NULL);
        CHECKJAVASIDEEXCEPTION(m_env);
    }

    always_inline ~JavaIntArray() {
        m_env->ReleaseIntArrayElements(m_intArray, m_data, 0);
    }

    always_inline jint& operator[](const size_t index) {
        return m_data[index];
    }

    always_inline const jint& operator[](const size_t index) const {
        return m_data[index];
    }

    always_inline size_t getLength() {
        if (m_size == static_cast<size_t>(-1)) {
            m_size = m_env->GetArrayLength(m_intArray);
            CHECKJAVASIDEEXCEPTION(m_env);
        }
        return m_size;
    }

};

class JavaLongArray {

protected:

    JNIEnv* m_env;
    jlongArray m_longArray;
    jlong* m_data;
    size_t m_size;

public:

    always_inline JavaLongArray(JNIEnv* env,  jlongArray longArray) : m_env(env), m_longArray(longArray), m_size(static_cast<size_t>(-1)) {
        m_data = m_env->GetLongArrayElements(m_longArray, 0);
        CHECKJAVASIDEEXCEPTION(m_env);
    }

    always_inline ~JavaLongArray() {
        m_env->ReleaseLongArrayElements(m_longArray, m_data, 0);
    }

    always_inline jlong& operator[](const size_t index) {
        return m_data[index];
    }

    always_inline const jlong& operator[](const size_t index) const {
        return m_data[index];
    }

    always_inline size_t getLength() {
        if (m_size == static_cast<size_t>(-1)) {
            m_size = m_env->GetArrayLength(m_longArray);
            CHECKJAVASIDEEXCEPTION(m_env);
        }
        return m_size;
    }

};

class JavaStringArray {

protected:

    JNIEnv* m_env;
    jobjectArray m_stringArray;
    size_t m_size;

public:

    always_inline JavaStringArray(JNIEnv* env, jobjectArray stringArray) : m_env(env), m_stringArray(stringArray), m_size(static_cast<size_t>(-1)) {
    }

    always_inline const void get(const size_t index, std::string& result) const {
        jstring jvalue = static_cast<jstring>(m_env->GetObjectArrayElement(m_stringArray, static_cast<jsize>(index)));
        CHECKJAVASIDEEXCEPTION(m_env);
        uk_ac_ox_cs_JRDFox_Common_GetString(m_env, jvalue, result);
    }

    always_inline void set(const size_t index, const std::string& value) {
        jstring jvalue = m_env->NewStringUTF(value.c_str());
        CHECKJAVASIDEEXCEPTION(m_env);
        m_env->SetObjectArrayElement(m_stringArray, static_cast<jsize>(index), jvalue);
        CHECKJAVASIDEEXCEPTION(m_env);
    }

    always_inline size_t getLength() {
        if (m_size == static_cast<size_t>(-1)) {
            m_size = m_env->GetArrayLength(m_stringArray);
            CHECKJAVASIDEEXCEPTION(m_env);
        }
        return m_size;
    }

};

always_inline void uk_ac_ox_cs_JRDFox_Common_GetPrefixes(JNIEnv* env, jobjectArray jPrefixes, Prefixes& prefixes) {
    JavaStringArray prefixesArray(env, jPrefixes);
    const size_t prefixesCount = prefixesArray.getLength();
    if (prefixesCount % 2 == 1)
        throw RDF_STORE_EXCEPTION("Mismatch in prefixes encoding.");
    std::string prefixName;
    std::string prefixIRI;
    for (size_t prefixIndex = 0; prefixIndex < prefixesCount; prefixIndex += 2) {
        prefixesArray.get(prefixIndex, prefixName);
        prefixesArray.get(prefixIndex + 1, prefixIRI);
        prefixes.declarePrefix(prefixName, prefixIRI);
    }
}

always_inline void uk_ac_ox_cs_JRDFox_Common_GetParameters(JNIEnv* env, jobjectArray jParameters, Parameters& parameters) {
    JavaStringArray parametersArray(env, jParameters);
    const size_t parametersCount = parametersArray.getLength();
    if (parametersCount % 2 == 1)
        throw RDF_STORE_EXCEPTION("Mismatch in prefixes encoding.");
    std::string key;
    std::string value;
    for (size_t parameterIndex = 0; parameterIndex < parametersCount; parameterIndex += 2) {
        parametersArray.get(parameterIndex, key);
        parametersArray.get(parameterIndex + 1, value);
        parameters.setString(key, value);
    }
}

template<typename ResourceProviderClass>
always_inline jint getResources(ResourceProviderClass* resourceProvider, jlong resourceIDsPtr, jint firstIndex, jint limitIndex, jlong stringByteBufferPtr, jint stringByteBufferLength, jlong stringLengthsBufferPtr, jlong datatypeIDsBufferPtr) {
    jlong* resourceIDs = reinterpret_cast<jlong*>(resourceIDsPtr);
    char* stringByteBuffer = reinterpret_cast<char*>(stringByteBufferPtr);
    jint* stringLengthsBuffer = reinterpret_cast<jint*>(stringLengthsBufferPtr);
    jshort* datatypeIDsBuffer = reinterpret_cast<jshort*>(datatypeIDsBufferPtr);
    std::string lexicalForm;
    DatatypeID datatypeID;
    int stringBufferPosition = 0;
    jint resourceIndex;
    for (resourceIndex = firstIndex; resourceIndex < limitIndex; resourceIndex++) {
        if (!resourceProvider->getResource(static_cast<ResourceID>(resourceIDs[resourceIndex]), lexicalForm, datatypeID))
            throw RDF_STORE_EXCEPTION("Invalid resource id.");
        datatypeIDsBuffer[resourceIndex] = static_cast<jshort>(datatypeID);
        stringLengthsBuffer[resourceIndex] = stringByteBufferLength - stringBufferPosition;
        if (!::copyTextToMemory(lexicalForm, stringByteBuffer + stringBufferPosition, stringLengthsBuffer[resourceIndex]))
            break;
        stringBufferPosition += stringLengthsBuffer[resourceIndex];
    }
    return resourceIndex;
}

#endif /* UK_AC_OX_CS_JRDFOX_H_ */
