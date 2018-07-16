// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef MUTEX_H_
#define MUTEX_H_

#include "../all.h"

class MutexHolder;
class MutexReleaser;
class NullMutexHolder;
class NullMutexReleaser;

// Mutex

class Mutex : private Unmovable {

public:

    typedef MutexHolder MutexHolderType;
    typedef MutexReleaser MutexReleaserType;

#ifdef WIN32

        friend class Condition;

    protected:

        CRITICAL_SECTION m_criticalSection;

    public:

        always_inline Mutex() {
            ::InitializeCriticalSection(&m_criticalSection);
        }

        always_inline ~Mutex() {
            ::DeleteCriticalSection(&m_criticalSection);
        }

        always_inline void lock() {
            ::EnterCriticalSection(&m_criticalSection);
        }

        always_inline bool tryLock() {
            return ::TryEnterCriticalSection(&m_criticalSection) != 0;
        }

        always_inline void unlock() {
            ::LeaveCriticalSection(&m_criticalSection);
        }

#else

        friend class Condition;

    protected:

        pthread_mutex_t m_mutex;

    public:

        always_inline Mutex() {
            ::pthread_mutex_init(&m_mutex, nullptr);
        }

        always_inline ~Mutex() {
            ::pthread_mutex_destroy(&m_mutex);
        }

        always_inline void lock() {
            ::pthread_mutex_lock(&m_mutex);
        }

        always_inline bool tryLock() {
            return ::pthread_mutex_trylock(&m_mutex) == 0;
        }

        always_inline void unlock() {
            ::pthread_mutex_unlock(&m_mutex);
        }

#endif

};

// MutexHolder

class MutexHolder : private Unmovable {

protected:

    Mutex& m_mutex;

public:

    always_inline MutexHolder(Mutex& mutex) : m_mutex(mutex) {
        m_mutex.lock();
    }

    always_inline ~MutexHolder() {
        m_mutex.unlock();
    }

};

// MutexReleaser

class MutexReleaser : private Unmovable {

protected:

    Mutex& m_mutex;

public:

    always_inline MutexReleaser(Mutex& mutex) : m_mutex(mutex) {
        m_mutex.unlock();
    }

    always_inline ~MutexReleaser() {
        m_mutex.lock();
    }
    
};

// NullMutex

class NullMutex : private Unmovable {

public:

    typedef NullMutexHolder MutexHolderType;
    typedef NullMutexReleaser MutexReleaserType;

    always_inline NullMutex() {
    }

    always_inline ~NullMutex() {
    }

    always_inline void lock() {
    }

    always_inline bool tryLock() {
        return true;
    }

    always_inline void unlock() {
    }

};

// NullMutexHolder

class NullMutexHolder : private Unmovable {

public:

    always_inline NullMutexHolder(NullMutex& mutex) {
    }

};

// NullMutexReleaser

class NullMutexReleaser : private Unmovable {

public:

    always_inline NullMutexReleaser(NullMutex& mutex) {
    }
    
};

// MutexSelector

template<bool isConcurrent>
struct MutexSelector;

template<>
struct MutexSelector<true> {
    typedef Mutex MutexType;
    typedef MutexHolder MutexHolderType;
    typedef MutexReleaser MutexReleaserType;
};

template<>
struct MutexSelector<false> {
    typedef NullMutex MutexType;
    typedef NullMutexHolder MutexHolderType;
    typedef NullMutexReleaser MutexReleaserType;
};

#endif // MUTEX_H_
