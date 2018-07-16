// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../RDFStoreException.h"
#include "ThreadContext.h"
#include "Mutex.h"

// ThreadContextManager

class ThreadContextManager : private Unmovable {

protected:

    struct ObjectRegistration {
        ObjectThreadDestructor m_objectThreadDestructor;
        void* m_objectThreadDestructorData;
        bool m_inUse;

        always_inline ObjectRegistration() : m_objectThreadDestructor(0), m_objectThreadDestructorData(0), m_inUse(false) {
        }

    };

    static const int64_t MANAGER_UPDATE_IN_PROGRESS = -1;

    aligned_int64_t m_updateCounter;
    ThreadContext* m_firstThreadContext;
    uint32_t m_lastUsedThreadContextID;
    ObjectRegistration m_objectRegistrations[static_cast<size_t>(MAX_OBJECT_ID) + 1];
    std::vector<bool> m_usedThreadContextIDs;

    ThreadContextManager() : m_updateCounter(0), m_firstThreadContext(0), m_lastUsedThreadContextID(0), m_usedThreadContextIDs() {
        m_usedThreadContextIDs.insert(m_usedThreadContextIDs.end(), static_cast<size_t>(MAX_THREAD_CONTEXT_ID) + 1, false);
    }

    struct ManagerUpdate {
        always_inline ManagerUpdate() {
            s_threadContextManager.startManagerUpdate();
        }

        always_inline ~ManagerUpdate() {
            s_threadContextManager.finishManagerUpdate();
        }
    };

public:

    static ThreadContextManager s_threadContextManager;

    always_inline void startConcurrentUpdate() {
        int64_t currentUpdateCounterValue;
        do {
            while ((currentUpdateCounterValue = ::atomicRead(m_updateCounter)) < 0) {
            }
        } while (!::atomicConditionalSet(m_updateCounter, currentUpdateCounterValue, currentUpdateCounterValue + 1));
    }

    always_inline void finishConcurrentUpdate() {
        ::atomicDecrement(m_updateCounter);
    }

    always_inline void startManagerUpdate() {
        do {
            while (::atomicRead(m_updateCounter)) {
            }
        } while (!::atomicConditionalSet(m_updateCounter, 0, MANAGER_UPDATE_IN_PROGRESS));
    }

    always_inline void finishManagerUpdate() {
        ::atomicWrite(m_updateCounter, 0);
    }

    always_inline ObjectID acquireObjectID(ObjectThreadDestructor objectThreadDestructor, void* objectThreadDestructorData) {
        ManagerUpdate managerUpdate;
        for (size_t objectID = 0; objectID <= MAX_OBJECT_ID ; ++objectID)
            if (!m_objectRegistrations[objectID].m_inUse) {
                m_objectRegistrations[objectID].m_objectThreadDestructor = objectThreadDestructor;
                m_objectRegistrations[objectID].m_objectThreadDestructorData = objectThreadDestructorData;
                m_objectRegistrations[objectID].m_inUse = true;
                return static_cast<ObjectID>(objectID);
            }
        throw RDF_STORE_EXCEPTION("The maximum number of ObjectIDs exceeded.");
    }

    always_inline void releaseObjectID(const ObjectID objectID) {
        ManagerUpdate managerUpdate;
        m_objectRegistrations[objectID].m_objectThreadDestructor = 0;
        m_objectRegistrations[objectID].m_objectThreadDestructorData = 0;
        m_objectRegistrations[objectID].m_inUse = false;
    }

    always_inline void addThreadContext(ThreadContext* threadContext) {
        ManagerUpdate managerUpdate;
        threadContext->m_nextThreadContext = m_firstThreadContext;
        if (m_firstThreadContext)
            m_firstThreadContext->m_previousThreadContext = threadContext;
        m_firstThreadContext = threadContext;
        for (size_t threadContextID = 1; threadContextID <= MAX_THREAD_CONTEXT_ID; ++threadContextID)
            if (!m_usedThreadContextIDs[threadContextID]) {
                m_usedThreadContextIDs[threadContextID] = true;
                threadContext->m_threadContextID = static_cast<ThreadContextID>(threadContextID);
                return;
            }
        throw RDF_STORE_EXCEPTION("The maximum number of thread contexts exceeded.");
    }

    always_inline void removeThreadContext(ThreadContext* threadContext) {
        ManagerUpdate managerUpdate;
        // Call the destructors for all objects in the thread context
        for (size_t objectID = 0; objectID <= MAX_OBJECT_ID; ++objectID)
            if (m_objectRegistrations[objectID].m_objectThreadDestructor != 0)
                m_objectRegistrations[objectID].m_objectThreadDestructor(*threadContext, m_objectRegistrations[objectID].m_objectThreadDestructorData);
        // Now deregister the thread context
        if (threadContext->m_previousThreadContext)
            threadContext->m_previousThreadContext->m_nextThreadContext = threadContext->m_nextThreadContext;
        else
            m_firstThreadContext = threadContext->m_nextThreadContext;
        if (threadContext->m_nextThreadContext)
            threadContext->m_nextThreadContext->m_previousThreadContext = threadContext->m_previousThreadContext;
        threadContext->m_nextThreadContext = 0;
        threadContext->m_previousThreadContext = 0;
        // release the thread context ID
        m_usedThreadContextIDs[threadContext->m_threadContextID] = false;
    }

    always_inline ThreadContext* getFirstThreadContext() const {
        return m_firstThreadContext;
    }


    __ALIGNED(ThreadContextManager)

};

ThreadContextManager ThreadContextManager::s_threadContextManager;

// ObjectDescriptor

ObjectDescriptor::ObjectDescriptor(ObjectThreadDestructor objectThreadDestructor, void* objectThreadDestructorData) :
    m_exclusiveThreadContextID(0),
    m_objectID(ThreadContextManager::s_threadContextManager.acquireObjectID(objectThreadDestructor, objectThreadDestructorData))
{
}

ObjectDescriptor::~ObjectDescriptor() {
    ThreadContextManager::s_threadContextManager.releaseObjectID(m_objectID);
}

// ThreadContext

ThreadContext::ThreadContext() : m_objectLocks(), m_threadContextID(0), m_previousThreadContext(0), m_nextThreadContext(0) {
    ThreadContextManager::s_threadContextManager.addThreadContext(this);
}

ThreadContext::~ThreadContext() {
    ThreadContextManager::s_threadContextManager.removeThreadContext(this);
}

bool ThreadContext::tryPromoteSharedLockToExclusive(ObjectDescriptor& objectDescriptor) {
    if (::atomicConditionalSet(objectDescriptor.m_exclusiveThreadContextID, 0, m_threadContextID)) {
        ThreadContextManager::s_threadContextManager.startConcurrentUpdate();
        ThreadContext* currentThreadContext = ThreadContextManager::s_threadContextManager.getFirstThreadContext();
        while (currentThreadContext) {
            if (currentThreadContext != this)
                currentThreadContext->m_objectLocks[objectDescriptor.m_objectID].lock();
            currentThreadContext = currentThreadContext->m_nextThreadContext;
        }
        return true;
    }
    else {
        unlockObjectShared(objectDescriptor);
        lockObjectShared(objectDescriptor);
        return false;
    }
}

void ThreadContext::downgradeExclusiveLockToShared(ObjectDescriptor& objectDescriptor) {
    ThreadContext* currentThreadContext = ThreadContextManager::s_threadContextManager.getFirstThreadContext();
    while (currentThreadContext) {
        if (currentThreadContext != this)
            currentThreadContext->m_objectLocks[objectDescriptor.m_objectID].unlock();
        currentThreadContext = currentThreadContext->m_nextThreadContext;
    }
    ThreadContextManager::s_threadContextManager.finishConcurrentUpdate();
    ::atomicWrite(objectDescriptor.m_exclusiveThreadContextID, 0);
}

ThreadContext* ThreadContext::startThreadContextEnumeration() {
    ThreadContextManager::s_threadContextManager.startConcurrentUpdate();
    return ThreadContextManager::s_threadContextManager.getFirstThreadContext();
}

void ThreadContext::finishThreadContextEnumeration() {
    ThreadContextManager::s_threadContextManager.finishConcurrentUpdate();
}

ThreadLocalPointer<ThreadContext> ThreadContext::s_threadContext;
