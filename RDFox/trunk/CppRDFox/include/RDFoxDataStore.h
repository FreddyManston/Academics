// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef RDFOXDATASTORE_H_
#define RDFOXDATASTORE_H_

#include <stdint.h>
#include <stdlib.h>

#ifdef WIN32
    #define APICONVENTION __stdcall
    #ifdef EXPORT_API
        #define APIFUNCTION __declspec(dllexport)
    #else
        #define APIFUNCTION __declspec(dllimport)
    #endif
#else
    #define APICONVENTION
    #ifdef EXPORT_API
        #define APIFUNCTION __attribute__((visibility("default")))
    #else
        #define APIFUNCTION
    #endif
#endif

#ifdef __cplusplus
extern "C" {
#endif

typedef void* RDFoxDataStore;
typedef void* RDFoxDataStoreDictionary;
typedef void* RDFoxDataStoreTupleIterator;
typedef uint64_t RDFoxDataStoreResourceID;
typedef uint8_t RDFoxDataStoreDatatypeID;
typedef uint8_t RDFoxDataStoreResourceTypeID;
typedef uint8_t RDFoxUpdateType;

// Error handling

APIFUNCTION void APICONVENTION RDFoxDataStore_GetLastError(char* const messageBuffer, size_t* const messageBufferSize);


// RDFoxDataStore

APIFUNCTION bool APICONVENTION RDFoxDataStore_Create(RDFoxDataStore* const vDataStore, const char* const storeType, const char ** const parametersArray, const size_t parametersCount);

APIFUNCTION bool APICONVENTION RDFoxDataStore_Initialize(const RDFoxDataStore vDataStore);

APIFUNCTION bool APICONVENTION RDFoxDataStore_GetDictionary(RDFoxDataStoreDictionary* const vDictionary, const RDFoxDataStore vDataStore);
    
APIFUNCTION bool APICONVENTION RDFoxDataStore_SetNumberOfThreads(const RDFoxDataStore vDataStore, const size_t numberOfThreads);

APIFUNCTION bool APICONVENTION RDFoxDataStore_AddTriplesByResourceIDs(const RDFoxDataStore vDataStore, const size_t numberOfTriples, const RDFoxDataStoreResourceID* resourceIDs, const RDFoxUpdateType updateType);

APIFUNCTION bool APICONVENTION RDFoxDataStore_AddTriplesByResourceValues(const RDFoxDataStore vDataStore, const size_t numberOfTriples, const char** const lexicalForms, const RDFoxDataStoreDatatypeID* datatypeIDs, const RDFoxUpdateType updateType);

APIFUNCTION bool APICONVENTION RDFoxDataStore_AddTriplesByResources(const RDFoxDataStore vDataStore, const size_t numberOfTriples, const RDFoxDataStoreResourceTypeID* resourceTypeIDs, const char** const lexicalForms, const char** const datatypeIRIs, const RDFoxUpdateType updateType);

APIFUNCTION bool APICONVENTION RDFoxDataStore_ImportFiles(const RDFoxDataStore vDataStore, const size_t fileCount, const char** const fileNames, const RDFoxUpdateType updateType, const bool decomposeRules, const bool renameBlankNodes);

APIFUNCTION bool APICONVENTION RDFoxDataStore_ImportText(const RDFoxDataStore vDataStore, const char* const text, const RDFoxUpdateType updateType, const bool decomposeRules, const bool renameBlankNodes, const char ** const prefixesArray, const size_t prefixesCount);

APIFUNCTION bool APICONVENTION RDFoxDataStore_ApplyRules(const RDFoxDataStore vDataStore, const bool incrementally);

APIFUNCTION void APICONVENTION RDFoxDataStore_Dispose(const RDFoxDataStore vDataStore);
    
APIFUNCTION bool APICONVENTION RDFoxDataStore_Save(RDFoxDataStore, const char* const fileName);
    
APIFUNCTION bool APICONVENTION RDFoxDataStore_Load(RDFoxDataStore*, const char* const fileName);


// RDFoxDataStoreDictionary

APIFUNCTION bool APICONVENTION RDFoxDataStoreDictionary_GetResource(const RDFoxDataStoreDictionary vDictionary, const RDFoxDataStoreResourceID resourceID, RDFoxDataStoreDatatypeID* datatypeID, char* const lexicalFormBuffer, size_t* const lexicalFormBufferLength);

APIFUNCTION bool APICONVENTION RDFoxDataStoreDictionary_GetDatatypeIRI(const RDFoxDataStoreDatatypeID datatypeID, char* const datatypeIRIBuffer, size_t* const datatypeIRIBufferLength);

APIFUNCTION bool APICONVENTION RDFoxDataStoreDictionary_ResolveResourceValues(RDFoxDataStoreResourceID* const resourceIDs, const RDFoxDataStoreDictionary vDictionary, const char** const lexicalForms, const RDFoxDataStoreDatatypeID* datatypeIDs, size_t numberOfResources);

APIFUNCTION bool APICONVENTION RDFoxDataStoreDictionary_ResolveResources(RDFoxDataStoreResourceID* const resourceIDs, const RDFoxDataStoreDictionary vDictionary, const RDFoxDataStoreResourceTypeID* resourceTypeIDs, const char** const lexicalForms, const char** const datatypeIRIs, size_t numberOfResources);


// TupleIterator

APIFUNCTION bool APICONVENTION RDFoxDataStoreTupleIterator_CompileQuery(RDFoxDataStoreTupleIterator* const vTupleIterator, RDFoxDataStore vDataStore, const char* const queryText, const char ** const parametersArray, const size_t parametersCount, const char ** const prefixesArray, const size_t prefixesCount);

APIFUNCTION bool APICONVENTION RDFoxDataStoreTupleIterator_GetArity(size_t* const arity, const RDFoxDataStoreTupleIterator vTupleIterator);

APIFUNCTION bool APICONVENTION RDFoxDataStoreTupleIterator_Open(size_t* const multiplicity, const RDFoxDataStoreTupleIterator vTupleIterator, const size_t arity, RDFoxDataStoreResourceID* const resourceIDs);

APIFUNCTION bool APICONVENTION RDFoxDataStoreTupleIterator_GetNext(size_t* const multiplicity, const RDFoxDataStoreTupleIterator vTupleIterator, const size_t arity, RDFoxDataStoreResourceID* const resourceIDs);
    
APIFUNCTION bool APICONVENTION RDFoxDataStoreTupleIterator_GetResource(const RDFoxDataStoreTupleIterator vTupleIterator, const RDFoxDataStoreResourceID resourceID, RDFoxDataStoreDatatypeID* datatypeID, char* const lexicalFormBuffer, size_t* const lexicalFormBufferSize);

APIFUNCTION void APICONVENTION RDFoxDataStoreTupleIterator_Dispose(const RDFoxDataStoreTupleIterator vTupleIterator);

#ifdef __cplusplus
}
#endif


#endif /* RDFOXDATASTORE_H_ */
