// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef APILOGENTRY_H_
#define APILOGENTRY_H_

#include "../all.h"

class DataStore;
class Prefixes;

always_inline bool logAPICalls() {
#ifdef APILOGGING
    return true;
#else
    return false;
#endif
}

class APILogEntry : private Unmovable {

public:

	APILogEntry();

    ~APILogEntry();

    std::ostream& getOutput();

};

class APIDataStoreLogEntry : private Unmovable {

public:

    APIDataStoreLogEntry(const DataStore& dataStore);

    ~APIDataStoreLogEntry();

    const Prefixes& getPrefixes();

    void declarePrefix(const std::string& prefixName, const std::string& prefixIRI);

    std::ostream& getOutput();

    void setDecomposeRules(const bool decomposeRules);

    void setRenameBlankNodes(const bool renameBlankNodes);

    void printString(const char* value);

    void printResource(const char* const lexicalForm, const DatatypeID datatypeID);

    void printResource(const ResourceType resourceType, const char* const lexicalForm, const char* const datatypeIRI);

    void printResource(DataStore& dataStore, const ResourceID resourceID);

};

#endif /* APILOGENTRY_H_ */
