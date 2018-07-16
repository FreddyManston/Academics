// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifdef WITH_TEST

#define AUTO_TEST   InterningManagerTest

#include <CppTest/AutoTest.h>

#include "../../src/util/MemoryManager.h"
#include "../../src/util/InterningManagerImpl.h"
#include "../../src/util/HashTableImpl.h"

class Int {

public:

    size_t m_value;

    Int(size_t value) : m_value(value) {
    }

    always_inline size_t hash() const {
        return m_value;
    }

};

class IntInterningManager : public InterningManager<Int, IntInterningManager> {

protected:

    friend class HashTable<IntInterningManager>;
    friend class InterningManager<Int, IntInterningManager>;

    always_inline static size_t getObjectHashCode(const Int* const object) {
        return object->hash();
    }

    always_inline static size_t hashCodeFor(size_t value) {
        return value;
    }

    always_inline static bool isEqual(const Int* object, const size_t valueHashCode, const size_t value) {
        return object->m_value == value;
    }

    always_inline static Int* makeNew(const size_t valueHashCode, const size_t value) {
        return new Int(value);
    }

public:

    IntInterningManager(MemoryManager& memoryManager) : InterningManager<Int, IntInterningManager>(memoryManager) {
    }

};

class InterningManagerTest {

protected:

    MemoryManager m_memoryManager;
    IntInterningManager m_interningManager;

public:

    InterningManagerTest() : m_memoryManager(1024 * 1024 * 1024), m_interningManager(m_memoryManager) {
    }

    void initialize() {
        m_interningManager.initialize();
    }

    Int* get(size_t value) {
        return m_interningManager.get(value);
    }

    void dispose(Int* object) {
        m_interningManager.dispose(object);
    }

};

TEST(testBasic) {
    Int* i1 = get(1);
    Int* i1again = get(1);
    ASSERT_TRUE(i1 == i1again);
    ASSERT_EQUAL(static_cast<size_t>(1), m_interningManager.getNumberOfObjects());

    Int* i2 = get(2);
    ASSERT_TRUE(i1 != i2);
    ASSERT_EQUAL(static_cast<size_t>(2), m_interningManager.getNumberOfObjects());

    m_interningManager.dispose(i1);
    ASSERT_EQUAL(static_cast<size_t>(1), m_interningManager.getNumberOfObjects());

    Int* i1new = get(1);
    ASSERT_TRUE(i1 != i1new);
    ASSERT_EQUAL(static_cast<size_t>(2), m_interningManager.getNumberOfObjects());

    delete i1;
    delete i2;
    delete i1new;
}

TEST(testResize) {
    const size_t numberOfAllocations = 3 * HASH_TABLE_INITIAL_SIZE;
    std::unique_ptr<std::unique_ptr<Int>[]> objects(new std::unique_ptr<Int>[numberOfAllocations]);

    for (size_t index = 0; index < numberOfAllocations; index++)
        objects[index].reset(get(index));
    ASSERT_EQUAL(static_cast<size_t>(numberOfAllocations), m_interningManager.getNumberOfObjects());

    for (size_t index = 0; index < numberOfAllocations; index++)
        ASSERT_TRUE(objects[index].get() == get(index));
}

#endif
