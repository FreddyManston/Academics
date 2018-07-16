// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#define _CRT_SECURE_NO_WARNINGS

#include "../../include/RDFoxDataStore.h"
#include "../dictionary/Dictionary.h"
#include "../Common.h"
#include "api.h"

bool RDFoxDataStoreDictionary_GetResource(const RDFoxDataStoreDictionary vDictionary, const RDFoxDataStoreResourceID resourceID, RDFoxDataStoreDatatypeID* datatypeID, char* const lexicalFormBuffer, size_t* const lexicalFormBufferLength) {
    API_FUNCTION_START
    Dictionary& dictionary = *reinterpret_cast<Dictionary*>(vDictionary);
    std::string lexicalForm;
    if (!dictionary.getResource(resourceID, lexicalForm, *datatypeID))
        throw RDF_STORE_EXCEPTION("ResourceID not in dictionary.");
    copyTextToMemory(lexicalForm, lexicalFormBuffer, *lexicalFormBufferLength);
    API_FUNCTION_END("Error while retrieving a resource.")
}

bool RDFoxDataStoreDictionary_GetDatatypeIRI(const RDFoxDataStoreDatatypeID datatypeID, char* const datatypeIRIBuffer, size_t* const datatypeIRIBufferSize) {
    API_FUNCTION_START
    copyTextToMemory(Dictionary::getDatatypeIRI(datatypeID), datatypeIRIBuffer, *datatypeIRIBufferSize);
    API_FUNCTION_END("Error while retrieving a datatype.")
}

bool RDFoxDataStoreDictionary_ResolveResourceValues(RDFoxDataStoreResourceID* const resourceIDs, const RDFoxDataStoreDictionary vDictionary, const char** const lexicalForms, const RDFoxDataStoreDatatypeID* const datatypeIDs, const size_t numberOfResources) {
    API_FUNCTION_START
	Dictionary& dictionary = *reinterpret_cast<Dictionary*>(vDictionary);
	for (size_t index = 0; index < numberOfResources; index++)
		resourceIDs[index] = dictionary.resolveResource(lexicalForms[index], datatypeIDs[index]);
    API_FUNCTION_END("Error while resolving a resource.")
}

bool RDFoxDataStoreDictionary_ResolveResources(RDFoxDataStoreResourceID* const resourceIDs, const RDFoxDataStoreDictionary vDictionary, const RDFoxDataStoreResourceTypeID* const resourceTypeIDs, const char** const lexicalForms, const char** const datatypeIRIs, const size_t numberOfResources) {
    API_FUNCTION_START
    Dictionary& dictionary = *reinterpret_cast<Dictionary*>(vDictionary);
    for (size_t index = 0; index < numberOfResources; index++)
    	resourceIDs[index] = dictionary.resolveResource(static_cast<ResourceType>(resourceTypeIDs[index]), lexicalForms[index], datatypeIRIs[index]);
    API_FUNCTION_END("Error while resolving a resource.")
}
