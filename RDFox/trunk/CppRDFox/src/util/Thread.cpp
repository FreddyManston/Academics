// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../Common.h"
#include "../RDFStoreException.h"
#include "Thread.h"

always_inline static void runThread(Thread* thread) {
    thread->run();
    if (thread->getAutoCleanup())
        delete thread;
}

Thread::Thread() : Thread(false) {
}

#ifdef WIN32

    static DWORD WINAPI threadStarter(LPVOID lpParameter) {
        runThread(reinterpret_cast<Thread*>(lpParameter));
        return 0;
    }

    Thread::Thread(const bool autoCleanup) : m_threadHandle(NULL), m_autoCleanup(autoCleanup) {
    }

    Thread::~Thread() {
        if (wasStarted())
            ::CloseHandle(m_threadHandle);
    }

    bool Thread::wasStarted() const {
        return m_threadHandle != NULL;
    }

    void Thread::start() {
        if (!wasStarted()) {
            DWORD threadID;
            m_threadHandle = ::CreateThread(NULL, 0, threadStarter, reinterpret_cast<void*>(this), 0, &threadID);
            if (m_threadHandle == NULL && m_autoCleanup) {
                delete this;
                throw RDF_STORE_EXCEPTION("Cannot start an auto-cleanup thread.");
            }
        }
    }

    void Thread::join() {
        if (wasStarted())
            ::WaitForSingleObject(m_threadHandle, INFINITE);
    }

#else

    static void* threadStarter(void* parameter) {
        runThread(reinterpret_cast<Thread*>(parameter));
        return 0;
    }

    Thread::Thread(const bool autoCleanup) : m_wasStarted(false), m_autoCleanup(autoCleanup) {
    }

    Thread::~Thread() {
    }

    bool Thread::wasStarted() const {
        return m_wasStarted;
    }

    void Thread::start() {
        if (!wasStarted()) {
            if (::pthread_create(&m_threadHandle, NULL, threadStarter, reinterpret_cast<void*>(this)) == 0)
                m_wasStarted = true;
            else if (m_autoCleanup) {
                delete this;
                throw RDF_STORE_EXCEPTION("Cannot start an auto-cleanup thread.");
            }
        }
    }

    void Thread::join() {
        if (wasStarted())
            ::pthread_join(m_threadHandle, NULL);
    }

#endif
