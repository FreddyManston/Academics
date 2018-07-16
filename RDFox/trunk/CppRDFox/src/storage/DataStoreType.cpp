// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../RDFStoreException.h"
#include "../util/InputStream.h"
#include "../util/OutputStream.h"
#include "Parameters.h"
#include "DataStoreType.h"

// DataStore

DataStore::~DataStore() {
}

// DataStoreTypeRegistry

class DataStoreTypeRegistry {

protected:

    std::unordered_map<std::string, const DataStoreType*> m_dataStoreTypes;

public:

    DataStoreTypeRegistry() : m_dataStoreTypes() {
    }

    void reigsterDataStoreType(const DataStoreType& dataStoreType) {
        m_dataStoreTypes[dataStoreType.getDataStoreTypeName()] = &dataStoreType;
    }

    std::unique_ptr<DataStore> newDataStore(const char* const dataStoreTypeName, const Parameters& dataStoreParameters) const {
        std::unordered_map<std::string, const DataStoreType*>::const_iterator iterator = m_dataStoreTypes.find(dataStoreTypeName);
        if (iterator == m_dataStoreTypes.end()) {
            std::ostringstream message;
            message << "Unknown data store type with name '" << dataStoreTypeName << "'.";
            throw RDF_STORE_EXCEPTION(message.str());
        }
        else
            return iterator->second->newDataStore(dataStoreParameters);
    }

};

static DataStoreTypeRegistry& getDataStoreTypeRegistry() {
    static DataStoreTypeRegistry s_dataStoreTypeRegistry;
    return s_dataStoreTypeRegistry;
}

std::unique_ptr<DataStore> newDataStore(const char* const dataStoreTypeName, const Parameters& dataStoreParameters) {
    return getDataStoreTypeRegistry().newDataStore(dataStoreTypeName, dataStoreParameters);
}

std::unique_ptr<DataStore> newDataStoreFromFormattedFile(InputStream& inputStream, const bool formatKindAlreadyCheched) {
    if (!formatKindAlreadyCheched && !inputStream.checkNextString(CURRENT_FORMATTED_STORE_VERSION))
        throw RDF_STORE_EXCEPTION("The specified file does not seem to contain a data store.");
    std::string dataStoreTypeName;
    inputStream.readString(dataStoreTypeName, 4096);
    Parameters dataStoreParameters;
    dataStoreParameters.load(inputStream);
    std::unique_ptr<DataStore> dataStore(::newDataStore(dataStoreTypeName.c_str(), dataStoreParameters));
    dataStore->loadFormatted(inputStream, true, true);
    return dataStore;
}

// DataStoreType

DataStoreType::DataStoreType(const char* const dataStoreTypeName) : m_dataStoreTypeName(dataStoreTypeName) {
    getDataStoreTypeRegistry().reigsterDataStoreType(*this);
}

DataStoreType::~DataStoreType() {
}

const char* DataStoreType::getDataStoreTypeName() const {
    return m_dataStoreTypeName.c_str();
}
