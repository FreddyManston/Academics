// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef EQUALITYMANAGERIMPL_H_
#define EQUALITYMANAGERIMPL_H_

#include "../util/ComponentStatistics.h"
#include "../util/InputStream.h"
#include "../util/OutputStream.h"
#include "EqualityManager.h"

// EqualityManager

EqualityManager::EqualityManager(MemoryManager& memoryManager) : m_data(memoryManager) {
}

bool EqualityManager::initialize() {
    return m_data.initializeLarge();
}

void EqualityManager::save(OutputStream& outputStream) const {
    outputStream.writeString("EqualityManager");
    outputStream.writeMemoryRegion(m_data);
}

void EqualityManager::load(InputStream& inputStream) {
    if (!inputStream.checkNextString("EqualityManager"))
        throw RDF_STORE_EXCEPTION("Invalid input file: cannot load EqualityManager.");
    inputStream.readMemoryRegion(m_data);
}

std::unique_ptr<ComponentStatistics> EqualityManager::getComponentStatistics() const {
    std::vector<size_t> numberOfClassesBySize;
    const ResourceID afterLastResourceID = getAfterLastResourceID();
    for (ResourceID resourceID = 1; resourceID < afterLastResourceID; ++resourceID)
        if (isNormal(resourceID)) {
            const size_t equivalenceClassSize = getEquivalenceClassSize(resourceID);
            while (numberOfClassesBySize.size() <= equivalenceClassSize)
                numberOfClassesBySize.push_back(0);
            numberOfClassesBySize[equivalenceClassSize]++;
        }
    std::unique_ptr<ComponentStatistics> result(new ComponentStatistics("EqualityManager"));
    result->addIntegerItem("Size", m_data.getEndIndex() * sizeof(ResourceID));
    for (size_t index = 0; index < numberOfClassesBySize.size(); ++index)
        if (numberOfClassesBySize[index] != 0) {
            std::ostringstream name;
            name << "Equivalence classes of size " << index;
            result->addIntegerItem(name.str(), numberOfClassesBySize[index]);
        }
    return result;
}

#endif /* EQUALITYMANAGERIMPL_H_ */
