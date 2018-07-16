// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef NONREENTRANTSPINLOCK_H_
#define NONREENTRANTSPINLOCK_H_

#include "../all.h"

class NonReentrantSpinLock {

protected:

    int8_t m_mutexFlag;

public:

    NonReentrantSpinLock() : m_mutexFlag(0) {
    }

    always_inline void lock() {
        do {
            while (::atomicRead(m_mutexFlag)) {
            }
        } while (::atomicExchange(m_mutexFlag, 1));
    }

    always_inline void unlock() {
        ::atomicWrite(m_mutexFlag, 0);
    }

    always_inline void reset() {
        ::atomicWrite(m_mutexFlag, 0);
    }

    always_inline bool isLocked() const {
        return ::atomicRead(m_mutexFlag) == 1;
    }

    __ALIGNED(NonReentrantSpinLock)

};

class NonReentrantSpinLockHolder : private Unmovable {

protected:

    NonReentrantSpinLock& m_lock;

public:

    always_inline NonReentrantSpinLockHolder(NonReentrantSpinLock& lock) : m_lock(lock) {
        m_lock.lock();
    }

    always_inline ~NonReentrantSpinLockHolder() {
        m_lock.unlock();
    }

};

#endif /* NONREENTRANTSPINLOCK_H_ */
