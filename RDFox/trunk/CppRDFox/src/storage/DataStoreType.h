// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef DATASTORETYPE_H_
#define DATASTORETYPE_H_

#include "../Common.h"
#include "DataStore.h"

class DataStoreType : private Unmovable {

protected:

    const std::string m_dataStoreTypeName;

public:

    DataStoreType(const char* const dataStoreTypeName);

    virtual ~DataStoreType();

    const char* getDataStoreTypeName() const;

    virtual std::unique_ptr<DataStore> newDataStore(const Parameters& parameters) const = 0;

};

#endif /* DATASTORETYPE_H_ */
