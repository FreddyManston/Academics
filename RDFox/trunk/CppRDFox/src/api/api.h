// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef API_H_
#define API_H_

#include "../Common.h"
#include "../RDFStoreException.h"
#include "../util/ThreadLocalPointer.h"
#include "../util/Prefixes.h"
#include "../storage/Parameters.h"

extern ThreadLocalPointer<std::string> s_RDFoxDataStoreErrorMessage;


#define API_FUNCTION_START\
    try {\

#define API_FUNCTION_ERROR_MESSAGE\


#define API_FUNCTION_END(errorMessage)\
        return true;\
    }\
    catch (const std::exception& e) {\
        s_RDFoxDataStoreErrorMessage->assign(std::string(errorMessage) + ": " + e.what());\
    }\
    catch(...) {\
        s_RDFoxDataStoreErrorMessage->assign(errorMessage);\
    }\
    return false;

always_inline void getPrefixes(const char ** const prefixesArray, const size_t prefixesCount, Prefixes& prefixes) {
	if (prefixesCount % 2 != 0)
		throw RDF_STORE_EXCEPTION("Wrong argument: prefixesCount should be an even number");
	for (size_t index = 0; index < prefixesCount; index += 2)
		if (!prefixes.declarePrefix(prefixesArray[index], prefixesArray[index + 1]))
			throw RDF_STORE_EXCEPTION("Invalid prefix definition");
}

always_inline void getParameters(const char ** const parametersArray, const size_t parametersCount, Parameters& parameters) {
	if (parametersCount % 2 != 0)
		throw RDF_STORE_EXCEPTION("Wrong argument: parameterCount should be an even number");
	for (size_t index = 0; index < parametersCount; index += 2)
		parameters.setString(parametersArray[index], parametersArray[index + 1]);
}

#endif /* API_H_ */
