// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef THREAD_H_
#define THREAD_H_

#include "../all.h"

class Thread : private Unmovable {

protected:

#ifdef WIN32
    HANDLE m_threadHandle;
#else
    bool m_wasStarted;
    pthread_t m_threadHandle;
#endif

    const bool m_autoCleanup;

public:

    Thread();

    Thread(const bool autoCleanup);

    always_inline bool getAutoCleanup() const {
        return m_autoCleanup;
    }

    virtual ~Thread();

    virtual void run() = 0;

    virtual bool wasStarted() const;

    virtual void start();

    virtual void join();

};

#endif /* THREAD_H_ */
