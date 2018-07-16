// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "ResourceIDMapperImpl.h"

ResourceIDMapper::ResourceIDMapper(MemoryManager& memoryManager) :
    m_localToGlobal(memoryManager),
    m_globalToLocal(memoryManager, HASH_TABLE_LOAD_FACTOR)
{
}

void ResourceIDMapper::initialize() {
    if (!m_localToGlobal.initializeLarge())
        throw RDF_STORE_EXCEPTION("Cannot initialise the mapping of local to global resource IDs.");
    if (!m_globalToLocal.initialize(HASH_TABLE_INITIAL_SIZE))
        throw RDF_STORE_EXCEPTION("Cannot initialise the mapping of global to local resource IDs.");
}
