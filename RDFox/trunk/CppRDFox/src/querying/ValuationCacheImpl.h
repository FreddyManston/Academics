// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef VALUATIONCACHEIMPL_H_
#define VALUATIONCACHEIMPL_H_

#include "../RDFStoreException.h"
#include "../util/HashTableImpl.h"
#include "ValuationCache.h"

// ValuationDataPool

always_inline ValuationDataPool::ValuationDataPool(MemoryManager& memoryManager) : m_data(memoryManager), m_nextFreeLocation(1) {
}

always_inline MemoryManager& ValuationDataPool::getMemoryManager() const {
    return m_data.getMemoryManager();
}

always_inline void ValuationDataPool::initialize() {
    m_nextFreeLocation = sizeof(uint64_t);
    if (!m_data.initializeLarge())
        throw RDF_STORE_EXCEPTION("Cannot initialize ValuationDataPool.");
}

always_inline uint8_t* ValuationDataPool::newValuationData(const size_t valuationDataSize) {
    assert(valuationDataSize != 0);
    uint8_t* result = m_data + m_nextFreeLocation;
    if (!m_data.ensureEndAtLeast(m_nextFreeLocation, valuationDataSize))
        throw RDF_STORE_EXCEPTION("Memory exhausted.");
    m_nextFreeLocation += valuationDataSize;
    return result;
}

// InputValuationManager

always_inline void InputValuationManager::copy(const uint8_t* const sourceBucket, uint8_t* const targetBucket) {
    reinterpret_cast<Bucket*>(targetBucket)->setInputValuation(reinterpret_cast<const Bucket*>(sourceBucket)->getInputValuation());
}

always_inline void InputValuationManager::invalidate(uint8_t* const bucket) {
    reinterpret_cast<Bucket*>(bucket)->setInputValuation(0);
}

always_inline bool InputValuationManager::isEmpty(const uint8_t* const bucket) {
    return reinterpret_cast<const Bucket*>(bucket)->getInputValuation() == 0;
}

always_inline size_t InputValuationManager::hashCode(const uint8_t* const bucket) const {
    const InputValuation* const inputValuation = reinterpret_cast<const Bucket*>(bucket)->getInputValuation();
    const ResourceID* inputValuationValues = inputValuation->getValues();
    size_t hash = 0;
    for (uint16_t index = m_inputValuationSize; index > 0; --index) {
        hash += *(inputValuationValues++);
        hash += (hash << 10);
        hash ^= (hash >> 6);
    }
    hash += (hash << 3);
    hash ^= (hash >> 11);
    hash += (hash << 15);
    return hash;
}

always_inline size_t InputValuationManager::hashCodeFor(const ResourceID valuesBuffer[], const ArgumentIndex valuesIndexes[]) const {
    size_t hash = 0;
    for (uint16_t index = 0; index < m_inputValuationSize; ++index) {
        hash += valuesBuffer[valuesIndexes[index]];
        hash += (hash << 10);
        hash ^= (hash >> 6);
    }
    hash += (hash << 3);
    hash ^= (hash >> 11);
    hash += (hash << 15);
    return hash;
}

always_inline bool InputValuationManager::isValidAndNotContains(const uint8_t* const bucket, const size_t valuesHashCode, const ResourceID valuesBuffer[], const ArgumentIndex valuesIndexes[]) const {
    const InputValuation* const inputValuation = reinterpret_cast<const Bucket*>(bucket)->getInputValuation();
    if (inputValuation != 0) {
        const ResourceID* inputValuationValues = inputValuation->getValues();
        for (uint16_t index = 0; index < m_inputValuationSize; ++index)
            if (valuesBuffer[valuesIndexes[index]] != (*inputValuationValues++))
                return true;
    }
    return false;
}

always_inline InputValuationManager::InputValuationManager(MemoryManager& memoryManager, ValuationDataPool& ValuationDataPool, const uint16_t inputValuationSize) :
    HashTable<InputValuationManager>(memoryManager), m_valuationDataPool(ValuationDataPool), m_inputValuationSize(inputValuationSize)
{
}

always_inline void InputValuationManager::initialize() {
    if (!HashTable<InputValuationManager>::initialize(HASH_TABLE_INITIAL_SIZE))
        throw RDF_STORE_EXCEPTION("Cannot initialize InputValuationManager.");
}

always_inline uint16_t InputValuationManager::getInputValuationSize() const {
    return m_inputValuationSize;
}

always_inline InputValuation* InputValuationManager::resolveInputValuation(const ResourceID valuesBuffer[], const ArgumentIndex valuesIndexes[], bool& alreadyExists) {
    size_t valuesHashCode;
    Bucket* const inputValuationBucket = reinterpret_cast<Bucket*>(getBucketFor(valuesHashCode, valuesBuffer, valuesIndexes));
    InputValuation* inputValuation = inputValuationBucket->getInputValuation();
    if (inputValuation == 0) {
        alreadyExists = false;
        inputValuation = reinterpret_cast<InputValuation*>(m_valuationDataPool.newValuationData(sizeof(InputValuation) + sizeof(ResourceID) * m_inputValuationSize));
        inputValuation->setFirstOutputValuation(0);
        ResourceID* inputValuationValues = inputValuation->getValues();
        for (uint16_t index = 0; index < m_inputValuationSize; ++index)
            *(inputValuationValues++) = valuesBuffer[valuesIndexes[index]];
        inputValuationBucket->setInputValuation(inputValuation);
        m_numberOfUsedBuckets++;
        if (!resizeIfNeeded())
            throw RDF_STORE_EXCEPTION("Memory exhausted.");
    }
    else
        alreadyExists = true;
    return inputValuation;
}

// OutputValuationManager

always_inline void OutputValuationManager::copy(const uint8_t* const sourceBucket, uint8_t* const targetBucket) {
    reinterpret_cast<Bucket*>(targetBucket)->setOutputValuation(reinterpret_cast<const Bucket*>(sourceBucket)->getOutputValuation());
}

always_inline void OutputValuationManager::invalidate(uint8_t* const bucket) {
    reinterpret_cast<Bucket*>(bucket)->setOutputValuation(0);
}

always_inline bool OutputValuationManager::isEmpty(const uint8_t* const bucket) {
    return reinterpret_cast<const Bucket*>(bucket)->getOutputValuation() == 0;
}

always_inline size_t OutputValuationManager::hashCode(const uint8_t* const bucket) const {
    const OutputValuation* const outputValuation = reinterpret_cast<const Bucket*>(bucket)->getOutputValuation();
    const ResourceID* outputValuationValues = outputValuation->getValues();
    size_t hash = reinterpret_cast<size_t>(outputValuation->getInputValuation());
    hash += (hash << 10);
    hash ^= (hash >> 6);
    for (uint16_t index = m_outputValuationSize; index > 0; --index) {
        hash += *(outputValuationValues++);
        hash += (hash << 10);
        hash ^= (hash >> 6);
    }
    hash += (hash << 3);
    hash ^= (hash >> 11);
    hash += (hash << 15);
    return hash;
}

always_inline size_t OutputValuationManager::hashCodeFor(const InputValuation* inputValuation, const ResourceID valuesBuffer[], const ArgumentIndex valuesIndexes[]) const {
    size_t hash = reinterpret_cast<size_t>(inputValuation);
    hash += (hash << 10);
    hash ^= (hash >> 6);
    for (uint16_t index = 0; index < m_outputValuationSize; ++index) {
        hash += valuesBuffer[valuesIndexes[index]];
        hash += (hash << 10);
        hash ^= (hash >> 6);
    }
    hash += (hash << 3);
    hash ^= (hash >> 11);
    hash += (hash << 15);
    return hash;
}

always_inline bool OutputValuationManager::isValidAndNotContains(const uint8_t* const bucket, const size_t valuesHashCode, const InputValuation* inputValuation, const ResourceID valuesBuffer[], const ArgumentIndex valuesIndexes[]) const {
    const OutputValuation* const outputValuation = reinterpret_cast<const Bucket*>(bucket)->getOutputValuation();
    if (outputValuation != 0) {
        if (outputValuation->getInputValuation() != inputValuation)
            return true;
        const ResourceID* outputValuationValues = outputValuation->getValues();
        for (uint16_t index = 0; index < m_outputValuationSize; ++index)
            if (valuesBuffer[valuesIndexes[index]] != (*outputValuationValues++))
                return true;
    }
    return false;
}

always_inline OutputValuationManager::OutputValuationManager(MemoryManager& memoryManager, ValuationDataPool& ValuationDataPool, InputValuationManager& inputValuationManager, const uint16_t outputValuationSize) :
    HashTable<OutputValuationManager>(memoryManager), m_valuationDataPool(ValuationDataPool), m_inputValuationManager(inputValuationManager), m_outputValuationSize(outputValuationSize)
{
}

always_inline void OutputValuationManager::initialize() {
    if (!HashTable<OutputValuationManager>::initialize(HASH_TABLE_INITIAL_SIZE))
        throw RDF_STORE_EXCEPTION("Cannot initialize OutputValuationManager.");
}

always_inline uint16_t OutputValuationManager::getOutputValuationSize() const {
    return m_outputValuationSize;
}

always_inline OutputValuation* OutputValuationManager::resolveOutputValuation(InputValuation* inputValuation, const ResourceID valuesBuffer[], const ArgumentIndex valuesIndexes[], const size_t multiplicity, bool& alreadyExists) {
    size_t valuesHashCode;
    Bucket* const outputValuationBucket = reinterpret_cast<Bucket*>(getBucketFor(valuesHashCode, inputValuation, valuesBuffer, valuesIndexes));
    OutputValuation* outputValuation = outputValuationBucket->getOutputValuation();
    if (outputValuation == 0) {
        alreadyExists = false;
        outputValuation = reinterpret_cast<OutputValuation*>(m_valuationDataPool.newValuationData(sizeof(OutputValuation) + sizeof(ResourceID) * m_outputValuationSize));
        outputValuation->setInputValuation(inputValuation);
        outputValuation->setMultiplicity(multiplicity);
        ResourceID* outputValuationValues = outputValuation->getValues();
        for (uint16_t index = 0; index < m_outputValuationSize; ++index)
            *(outputValuationValues++) = valuesBuffer[valuesIndexes[index]];
        outputValuation->setNextOutputValuation(inputValuation->getFirstOutputValuation());
        inputValuation->setFirstOutputValuation(outputValuation);
        outputValuationBucket->setOutputValuation(outputValuation);
        m_numberOfUsedBuckets++;
        if (!resizeIfNeeded())
            throw RDF_STORE_EXCEPTION("Memory exhausted.");
    }
    else {
        alreadyExists = true;
        outputValuation->addMultiplicity(multiplicity);
    }
    return outputValuation;
}

always_inline void OutputValuationManager::loadOutputValuation(const OutputValuation* outputValuation, ResourceID valuesBuffer[], const ArgumentIndex valuesIndexes[]) const {
    ResourceID* outputValuationValues = outputValuation->getValues();
    for (uint16_t index = 0; index < m_outputValuationSize; ++index)
        valuesBuffer[valuesIndexes[index]] = *(outputValuationValues++);
}

// ValuationCache

always_inline ValuationCache::ValuationCache(MemoryManager& memoryManager, const uint16_t inputValuationSize, const uint16_t outputValuationSize) :
    m_valuationDataPool(memoryManager),
    m_inputValuationManager(memoryManager, m_valuationDataPool, inputValuationSize),
    m_outputValuationManager(memoryManager, m_valuationDataPool, m_inputValuationManager, outputValuationSize)
{
}

always_inline MemoryManager& ValuationCache::getMemoryManager() const {
    return m_valuationDataPool.getMemoryManager();
}

always_inline void ValuationCache::initialize() {
    m_valuationDataPool.initialize();
    m_inputValuationManager.initialize();
    m_outputValuationManager.initialize();
}

#endif /* VALUATIONCACHEIMPL_H_ */
