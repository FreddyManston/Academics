// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../util/InputStream.h"
#include "../util/OutputStream.h"
#include "DataPoolImpl.h"

DataPool::DataPool(MemoryManager& memoryManager) : m_nextFreeLocation(1), m_data(memoryManager) {
}

void DataPool::initialize() {
    m_nextFreeLocation = 1;
    if (!m_data.initializeLarge())
        throw RDF_STORE_EXCEPTION("Cannot initialize DataPool.");
}

void DataPool::save(OutputStream& outputStream) const {
    outputStream.writeString("DataPool");
    outputStream.writeMemoryRegion(m_data);
    outputStream.write<uint64_t>(m_nextFreeLocation);
}

void DataPool::load(InputStream& inputStream) {
    if (!inputStream.checkNextString("DataPool"))
        throw RDF_STORE_EXCEPTION("Invalid input file: cannot load DataPool.");
    inputStream.readMemoryRegion(m_data);
    m_nextFreeLocation = inputStream.read<uint64_t>();
}

std::unique_ptr<ComponentStatistics> DataPool::getComponentStatistics() const {
    std::unique_ptr<ComponentStatistics> result(new ComponentStatistics("DataPool"));
    result->addIntegerItem("Size", m_nextFreeLocation);
    return result;
}
