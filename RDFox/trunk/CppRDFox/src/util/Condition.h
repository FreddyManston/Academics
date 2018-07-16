// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef CONDITION_H_
#define CONDITION_H_

#include "../all.h"
#include "Mutex.h"

#ifdef WIN32

    class Condition : private Unmovable {

    protected:

        CONDITION_VARIABLE m_conditionVariable;

    public:

        Condition() {
            ::InitializeConditionVariable(&m_conditionVariable);
        }

        ~Condition() {
        }

        always_inline void wait(Mutex& mutex) {
            ::SleepConditionVariableCS(&m_conditionVariable, &mutex.m_criticalSection, INFINITE);
        }

        always_inline bool wait(Mutex& mutex, const size_t milliseconds) {
            return ::SleepConditionVariableCS(&m_conditionVariable, &mutex.m_criticalSection, static_cast<DWORD>(milliseconds)) != 0;
        }

        always_inline void signalOne() {
            ::WakeConditionVariable(&m_conditionVariable);
        }

        always_inline void signalAll() {
            ::WakeAllConditionVariable(&m_conditionVariable);
        }

    };

#else

    class Condition : private Unmovable {

    protected:

        pthread_cond_t m_condition;

    public:

        Condition() {
            ::pthread_cond_init(&m_condition, NULL);
        }

        ~Condition() {
            ::pthread_cond_destroy(&m_condition);
        }

        always_inline void wait(Mutex& mutex) {
            ::pthread_cond_wait(&m_condition, &mutex.m_mutex);
        }

        always_inline bool wait(Mutex& mutex, const size_t milliseconds) {
            timeval now;
            ::gettimeofday(&now, nullptr);
            const long absoluteTimeNS = now.tv_usec * 1000 + static_cast<long>(milliseconds) * static_cast<long>(1000000);
            timespec timeout = { now.tv_sec + (absoluteTimeNS / 1000000000), absoluteTimeNS % 1000000000 };
            return ::pthread_cond_timedwait(&m_condition, &mutex.m_mutex, &timeout) == 0;
        }

        always_inline void signalOne() {
            ::pthread_cond_signal(&m_condition);
        }

        always_inline void signalAll() {
            ::pthread_cond_broadcast(&m_condition);
        }

    };

#endif

#endif /* CONDITION_H_ */
