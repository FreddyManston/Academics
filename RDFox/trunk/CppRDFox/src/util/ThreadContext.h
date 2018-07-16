// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef THREADCONTEXT_H_
#define THREADCONTEXT_H_

#include "../all.h"
#include "ThreadLocalPointer.h"
#include "NonReentrantSpinLock.h"

class ThreadContext;

typedef void (*ObjectThreadDestructor)(ThreadContext& threadContext, void*);

typedef uint32_t ObjectID;

const ObjectID MAX_OBJECT_ID = 65535;

typedef uint32_t ThreadContextID;

const ThreadContextID MAX_THREAD_CONTEXT_ID = 65535;

struct ObjectDescriptor : private Unmovable {
    aligned_uint32_t m_exclusiveThreadContextID;
    ObjectID m_objectID;

    ObjectDescriptor(ObjectThreadDestructor objectThreadDestructor = 0, void* objectThreadDestructorData = 0);

    ~ObjectDescriptor();

};

class ThreadContext : private Unmovable {

protected:

    friend class ThreadContextManager;

    NonReentrantSpinLock m_objectLocks[static_cast<size_t>(MAX_OBJECT_ID) + 1];
    ThreadContextID m_threadContextID;
    ThreadContext* m_previousThreadContext;
    ThreadContext* m_nextThreadContext;

public:

    static ThreadLocalPointer<ThreadContext> s_threadContext;

    ThreadContext();

    ~ThreadContext();

    always_inline ThreadContext& getNextThreadContext() const {
        return *m_nextThreadContext;
    }

    always_inline ThreadContextID getThreadContextID() const {
        return m_threadContextID;
    }

    always_inline void lockObjectShared(const ObjectDescriptor& objectDescriptor) {
        while (::atomicRead(objectDescriptor.m_exclusiveThreadContextID) != 0) {
        }
        m_objectLocks[objectDescriptor.m_objectID].lock();
    }

    always_inline void unlockObjectShared(const ObjectDescriptor& objectDescriptor) {
        m_objectLocks[objectDescriptor.m_objectID].unlock();
    }

    bool tryPromoteSharedLockToExclusive(ObjectDescriptor& objectDescriptor);

    void downgradeExclusiveLockToShared(ObjectDescriptor& objectDescriptor);

    always_inline static ThreadContext& getCurrentThreadContext() {
        return *s_threadContext;
    }

    static ThreadContext* startThreadContextEnumeration();

    static void finishThreadContextEnumeration();

    __ALIGNED(ThreadContext);

};

#endif /* THREADCONTEXT_H_ */
