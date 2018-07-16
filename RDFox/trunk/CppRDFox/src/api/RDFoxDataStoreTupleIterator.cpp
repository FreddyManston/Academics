// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../../include/RDFoxDataStore.h"
#include "../dictionary/Dictionary.h"
#include "../dictionary/ResourceValueCache.h"
#include "../querying/QueryIterator.h"
#include "../storage/DataStore.h"
#include "../storage/Parameters.h"
#include "../storage/TupleIterator.h"
#include "../util/Prefixes.h"
#include "../util/APILogEntry.h"
#include "api.h"

bool RDFoxDataStoreTupleIterator_CompileQuery(RDFoxDataStoreTupleIterator* const vTupleIterator, RDFoxDataStore vDataStore, const char* const queryText, const char ** const parametersArray, const size_t parametersCount, const char ** const prefixesArray, const size_t prefixesCount) {
    API_FUNCTION_START
    Parameters parameters;
    ::getParameters(parametersArray, parametersCount, parameters);
    Prefixes prefixes;
    ::getPrefixes(prefixesArray, prefixesCount, prefixes);
    DataStore& dataStore = *reinterpret_cast<DataStore*>(vDataStore);
    *vTupleIterator = reinterpret_cast<RDFoxDataStoreTupleIterator>(dataStore.compileQuery(prefixes, queryText, parameters).release());
    if (::logAPICalls()) {
        APIDataStoreLogEntry entry(dataStore);
        entry.getOutput() << "# RDFoxDataStoreTupleIterator_CompileQuery()" << std::endl;
        for (auto iterator = parameters.begin(); iterator != parameters.end(); ++iterator)
            entry.getOutput() << "set query." << iterator->first << ' ' << iterator->second << std::endl;
        entry.getOutput() << "run ! \\" << std::endl;
        for (const char* characterPointer = queryText; *characterPointer != 0; ++characterPointer) {
            if (*characterPointer == '\n')
                entry.getOutput() << "\\";
            entry.getOutput() << *characterPointer;
        }
        entry.getOutput() << std::endl << std::endl;
    }
    API_FUNCTION_END("Error while compiling the query.")
}

bool RDFoxDataStoreTupleIterator_GetArity(size_t* const arity, const RDFoxDataStoreTupleIterator vTupleIterator) {
    API_FUNCTION_START
    *arity = reinterpret_cast<TupleIterator*>(vTupleIterator)->getArity();
    API_FUNCTION_END("Error while retrieving the arity of the tuple iterator.")
}

bool RDFoxDataStoreTupleIterator_Open(size_t* const multiplicity, const RDFoxDataStoreTupleIterator vTupleIterator, const size_t arity, RDFoxDataStoreResourceID* const resourceIDs) {
    API_FUNCTION_START
    TupleIterator* iterator = reinterpret_cast<TupleIterator*>(vTupleIterator);
    if(arity != iterator->getArity())
        throw RDF_STORE_EXCEPTION("Mismatch between the provided arity and the iterator's arity.");
    *multiplicity = iterator->open();
    if (*multiplicity > 0)
        for (size_t index = 0; index < iterator->getArity(); index++)
            resourceIDs[index] = iterator->getArgumentsBuffer()[iterator->getArgumentIndexes()[index]];
    API_FUNCTION_END("Error while opening the iterator.")
}

bool RDFoxDataStoreTupleIterator_GetNext(size_t* const multiplicity, const RDFoxDataStoreTupleIterator vTupleIterator, const size_t arity, RDFoxDataStoreResourceID* const resourceIDs) {
    API_FUNCTION_START
    TupleIterator* iterator = reinterpret_cast<TupleIterator*>(vTupleIterator);
    if(arity != iterator->getArity())
        throw RDF_STORE_EXCEPTION("Mismatch between the provided arity and the iterator's arity.");
    *multiplicity = iterator->advance();
    if (*multiplicity > 0)
        for (size_t index = 0; index < iterator->getArity(); index++)
            resourceIDs[index] = iterator->getArgumentsBuffer()[iterator->getArgumentIndexes()[index]];
    API_FUNCTION_END("Error while advancing the iterator.")
}

bool RDFoxDataStoreTupleIterator_GetResource(const RDFoxDataStoreTupleIterator vTupleIterator, const RDFoxDataStoreResourceID resourceID, RDFoxDataStoreDatatypeID* datatypeID, char* const lexicalFormBuffer, size_t* const lexicalFormBufferSize) {
    API_FUNCTION_START
    const ResourceValueCache& resourceValueCache = reinterpret_cast<QueryIterator*>(vTupleIterator)->getResourceValueCache();
    std::string lexicalForm;
    if (!resourceValueCache.getResource(resourceID, lexicalForm, *datatypeID))
        throw RDF_STORE_EXCEPTION("ResourceID not in dictionary");
    copyTextToMemory(lexicalForm, lexicalFormBuffer, *lexicalFormBufferSize);
    API_FUNCTION_END("Error while retrieving a resource.")
}

void RDFoxDataStoreTupleIterator_Dispose(const RDFoxDataStoreTupleIterator vTupleIterator) {
    delete reinterpret_cast<TupleIterator*>(vTupleIterator);
}
