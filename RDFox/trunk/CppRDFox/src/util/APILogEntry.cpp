// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../RDFStoreException.h"
#include "../dictionary/Dictionary.h"
#include "../storage/DataStore.h"
#include "../util/Mutex.h"
#include "../util/Prefixes.h"
#include "APILogEntry.h"

static Mutex s_mutex;
static std::ofstream s_output;
static Prefixes s_prefixes;
static std::unordered_map<const DataStore*, size_t> s_storesToSpaceNumbers;
static size_t s_nextSpaceNumber = 1;
static size_t s_currentSpaceNumber = 0;
static bool s_decomposeRules = false;
static bool s_renameBlankNodes = false;

APILogEntry::APILogEntry() {
    s_mutex.lock();
    try {
        if (!s_output.is_open()) {
            s_output.open("RDFoxAPILog.txt", std::ofstream::out | std::ofstream::trunc);
            if (!s_output.is_open())
                throw RDF_STORE_EXCEPTION("Cannot open API log file 'RDFoxAPILog.txt'.");
            s_prefixes.declareStandardPrefixes();
        }
    }
    catch (...) {
        s_mutex.unlock();
        throw;
    }
}

APILogEntry::~APILogEntry() {
    s_output.flush();
    s_mutex.unlock();
}

std::ostream& APILogEntry::getOutput() {
    return s_output;
}

APIDataStoreLogEntry::APIDataStoreLogEntry(const DataStore& dataStore) {
    s_mutex.lock();
    try {
        if (!s_output.is_open()) {
            s_output.open("RDFoxAPILog.txt", std::ofstream::out | std::ofstream::trunc);
            if (!s_output.is_open())
                throw RDF_STORE_EXCEPTION("Cannot open API log file 'RDFoxAPILog.txt'.");
            s_prefixes.declareStandardPrefixes();
        }
        size_t& spaceNumber = s_storesToSpaceNumbers[&dataStore];
        if (spaceNumber == 0)
            spaceNumber = s_nextSpaceNumber++;
        if (spaceNumber != s_currentSpaceNumber) {
            s_output << "set active \"store" << spaceNumber << "\"" << std::endl << std::endl;
            s_currentSpaceNumber = spaceNumber;
        }
    }
    catch (...) {
        s_mutex.unlock();
        throw;
    }
}

APIDataStoreLogEntry::~APIDataStoreLogEntry() {
    s_output.flush();
    s_mutex.unlock();
}

const Prefixes& APIDataStoreLogEntry::getPrefixes() {
    return s_prefixes;
}

void APIDataStoreLogEntry::declarePrefix(const std::string& prefixName, const std::string& prefixIRI) {
    s_prefixes.declarePrefix(prefixName, prefixIRI);
    s_output << "prefix " << prefixName << " <" << prefixIRI << ">" << std::endl;
}

std::ostream& APIDataStoreLogEntry::getOutput() {
    return s_output;
}

void APIDataStoreLogEntry::setDecomposeRules(const bool decomposeRules) {
    if (s_decomposeRules != decomposeRules) {
        s_output << "set decompose-rules " << (decomposeRules ? "true" : "false") << std::endl << std::endl;
        s_decomposeRules = decomposeRules;
    }
}

void APIDataStoreLogEntry::setRenameBlankNodes(const bool renameBlankNodes) {
    if (s_renameBlankNodes != renameBlankNodes) {
        s_output << "set rename-blank-nodes " << (renameBlankNodes ? "true" : "false") << std::endl << std::endl;
        s_renameBlankNodes = renameBlankNodes;
    }
}

void APIDataStoreLogEntry::printString(const char* value) {
    s_output << "\"";
    while (*value != 0) {
        if (*value == '"')
            s_output << "\\\"";
        else
            s_output << *value;
        ++value;
    }
    s_output << "\"";
}

void APIDataStoreLogEntry::printResource(const char* const lexicalForm, const DatatypeID datatypeID) {
    if (datatypeID == D_IRI_REFERENCE)
        s_output << s_prefixes.encodeIRI(lexicalForm);
    else if (datatypeID == D_BLANK_NODE)
        s_output << "_:" << lexicalForm;
    else if (datatypeID == D_XSD_INTEGER)
        s_output << lexicalForm;
    else {
        printString(lexicalForm);
        s_output << "^^" << s_prefixes.encodeIRI(Dictionary::getDatatypeIRI(datatypeID));
    }
}

void APIDataStoreLogEntry::printResource(const ResourceType resourceType, const char* const lexicalForm, const char* const datatypeIRI) {
    switch (resourceType) {
    case UNDEFINED_RESOURCE:
        s_output << "UNDEF";
        break;
    case IRI_REFERENCE:
        s_output << s_prefixes.encodeIRI(lexicalForm);
        break;
    case BLANK_NODE:
        s_output << "_:" << lexicalForm;
        break;
    case LITERAL:
        printString(lexicalForm);
        s_output << "^^" << s_prefixes.encodeIRI(datatypeIRI);
        break;
    }
}

void APIDataStoreLogEntry::printResource(DataStore& dataStore, const ResourceID resourceID) {
    std::string lexicalForm;
    DatatypeID datatypeID;
    if (dataStore.getDictionary().getResource(resourceID, lexicalForm, datatypeID))
        printResource(lexicalForm.c_str(), datatypeID);
    else
        s_output << "<unknown resource ID " << resourceID << ">";
}
