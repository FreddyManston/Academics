// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef VALUATIONCACHE_H_
#define VALUATIONCACHE_H_

#include "../Common.h"
#include "../util/HashTable.h"
#include "../util/MemoryRegion.h"

class MemoryManager;

// ValuationPool

class ValuationDataPool : private Unmovable {

protected:

    MemoryRegion<uint8_t> m_data;
    size_t m_nextFreeLocation;

public:

    ValuationDataPool(MemoryManager& memoryManager);

    MemoryManager& getMemoryManager() const;

    void initialize();

    uint8_t* newValuationData(const size_t valuationDataSize);

};

// InputValuationManager

class OutputValuation;

class InputValuation {

protected:

    OutputValuation* m_firstOutputValuation;

public:

    always_inline OutputValuation* getFirstOutputValuation() const {
        return read8Aligned4(m_firstOutputValuation);
    }

    always_inline void setFirstOutputValuation(OutputValuation* const firstOutputValuation) {
        write8Aligned4(m_firstOutputValuation, firstOutputValuation);
    }

    always_inline ResourceID* getValues() const {
        return const_cast<ResourceID*>(reinterpret_cast<const ResourceID*>(this + 1));
    }
};

class InputValuationManager : protected HashTable<InputValuationManager> {

protected:

    struct Bucket {
        InputValuation* m_inputValuation;

        always_inline InputValuation* getInputValuation() const {
            return m_inputValuation;
        }

        always_inline void setInputValuation(InputValuation* inputValuation) {
            m_inputValuation = inputValuation;
        }

    };

    friend class HashTable<InputValuationManager>;

    static const size_t BUCKET_SIZE = sizeof(Bucket);

    static void copy(const uint8_t* const sourceBucket, uint8_t* const targetBucket);

    static void invalidate(uint8_t* const bucket);

    static bool isEmpty(const uint8_t* const bucket);

    size_t hashCode(const uint8_t* const bucket) const;

    size_t hashCodeFor(const ResourceID valuesBuffer[], const ArgumentIndex valuesIndexes[]) const;

    bool isValidAndNotContains(const uint8_t* const bucket, const size_t valuesHashCode, const ResourceID valuesBuffer[], const ArgumentIndex valuesIndexes[]) const;

    ValuationDataPool& m_valuationDataPool;
    const uint16_t m_inputValuationSize;

public:

    InputValuationManager(MemoryManager& memoryManager, ValuationDataPool& valuationDataPool, const uint16_t inputValuationSize);

    void initialize();

    uint16_t getInputValuationSize() const;

    InputValuation* resolveInputValuation(const ResourceID valuesBuffer[], const ArgumentIndex valuesIndexes[], bool& alreadyExists);

};

// OutputValuation

class OutputValuation {

protected:

    InputValuation* m_inputValuation;
    OutputValuation* m_nextOutputValuation;
    size_t m_multiplicity;

public:

    always_inline InputValuation* getInputValuation() const {
        return read8Aligned4(m_inputValuation);
    }

    always_inline void setInputValuation(InputValuation* const inputValuation) {
        write8Aligned4(m_inputValuation, inputValuation);
    }

    always_inline OutputValuation* getNextOutputValuation() const {
        return read8Aligned4(m_nextOutputValuation);
    }

    always_inline void setNextOutputValuation(OutputValuation* const nextOutputValuation) {
        write8Aligned4(m_nextOutputValuation, nextOutputValuation);
    }

    always_inline size_t getMultiplicity() const {
        return read8Aligned4(m_multiplicity);
    }

    always_inline void setMultiplicity(const size_t multiplicity) {
        write8Aligned4(m_multiplicity, multiplicity);
    }

    always_inline void addMultiplicity(const size_t amount) {
        increment8Aligned4(m_multiplicity, amount);
    }

    always_inline ResourceID* getValues() const {
        return const_cast<ResourceID*>(reinterpret_cast<const ResourceID*>(this + 1));
    }

    always_inline bool valuesMatch(const ResourceID value1, const ResourceID value2) const {
        return value1 == INVALID_RESOURCE_ID || value2 == INVALID_RESOURCE_ID || value1 == value2;
    }

    always_inline bool valuesMatch(std::vector<ResourceID>::iterator compareWith, std::vector<size_t>::iterator compareWithOffset, std::vector<size_t>::iterator compareWithOffsetEnd) const {
        const ResourceID* values = getValues();
        while (compareWithOffset != compareWithOffsetEnd) {
            if (!valuesMatch(values[*compareWithOffset], *compareWith))
                return false;
            ++compareWith;
            ++compareWithOffset;
        }
        return true;
    }

};

class OutputValuationManager : protected HashTable<OutputValuationManager> {

protected:

    struct Bucket {
        OutputValuation* m_outputValuation;

        always_inline OutputValuation* getOutputValuation() const {
            return m_outputValuation;
        }

        always_inline void setOutputValuation(OutputValuation* outputValuation) {
            m_outputValuation = outputValuation;
        }

    };

    friend class HashTable<OutputValuationManager>;

    static const size_t BUCKET_SIZE = sizeof(Bucket);

    static void copy(const uint8_t* const sourceBucket, uint8_t* const targetBucket);

    static void invalidate(uint8_t* const bucket);

    static bool isEmpty(const uint8_t* const bucket);

    size_t hashCode(const uint8_t* const bucket) const;

    size_t hashCodeFor(const InputValuation* inputValuation, const ResourceID valuesBuffer[], const ArgumentIndex valuesIndexes[]) const;

    bool isValidAndNotContains(const uint8_t* const bucket, const size_t valuesHashCode, const InputValuation* inputValuation, const ResourceID valuesBuffer[], const ArgumentIndex valuesIndexes[]) const;

    ValuationDataPool& m_valuationDataPool;
    InputValuationManager& m_inputValuationManager;
    const uint16_t m_outputValuationSize;

public:

    OutputValuationManager(MemoryManager& memoryManager, ValuationDataPool& valuationDataPool, InputValuationManager& inputValuationManager, const uint16_t outputValuationSize);

    void initialize();

    uint16_t getOutputValuationSize() const;

    OutputValuation* resolveOutputValuation(InputValuation* inputValuation, const ResourceID valuesBuffer[], const ArgumentIndex valuesIndexes[], const size_t multiplicity, bool& alreadyExists);

    void loadOutputValuation(const OutputValuation* outputValuation, ResourceID valuesBuffer[], const ArgumentIndex valuesIndexes[]) const;

};

// ValuationCache

class ValuationCache : private Unmovable {

protected:

    ValuationDataPool m_valuationDataPool;

public:

    InputValuationManager m_inputValuationManager;
    OutputValuationManager m_outputValuationManager;

    ValuationCache(MemoryManager& memoryManager, const uint16_t inputValuationSize, const uint16_t outputValuationSize);

    MemoryManager& getMemoryManager() const;

    void initialize();

};

#endif /* VALUATIONCACHE_H_ */
