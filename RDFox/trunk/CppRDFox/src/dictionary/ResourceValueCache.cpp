// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../RDFStoreException.h"
#include "ResourceValueCacheImpl.h"

ResourceValueCache::ResourceValueCache(const Dictionary& dictionary, MemoryManager& memoryManager) :
    m_dictionary(dictionary),
    m_nextFreeLocation(0),
    m_hashTable(memoryManager, HASH_TABLE_LOAD_FACTOR, memoryManager)
{
}

ResourceValueCache::~ResourceValueCache() {
}

void ResourceValueCache::initialize() {
    m_nextFreeLocation = 0;
    if (!m_hashTable.getPolicy().m_data.initializeLarge())
        throw RDF_STORE_EXCEPTION("Cannot initialize the data array in ResourceValueCache.");
    if (!m_hashTable.initialize(HASH_TABLE_INITIAL_SIZE))
        throw RDF_STORE_EXCEPTION("Cannot initialize the hash table in ResourceValueCache.");
}

void ResourceValueCache::clear() {
    if (m_nextFreeLocation != 0)
        initialize();
}
