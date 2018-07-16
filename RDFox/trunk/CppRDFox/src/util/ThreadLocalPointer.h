// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef THREADLOCALPOINTER_H_
#define THREADLOCALPOINTER_H_

#include "../all.h"

template<class OT, bool autoCreate = true>
class ThreadLocalPointer : private Unmovable {

protected:

    ThreadLocalKey m_threadLocalKey;

    static void THREAD_LOCAL_DESTRUCTOR cleanUpFunction(void* data) {
        delete reinterpret_cast<ObjectType*>(data);
    }

public:

    typedef OT ObjectType;
    static const bool AUTO_CREATE = autoCreate;

    always_inline ThreadLocalPointer() : m_threadLocalKey(::allocateThreadLocalKey(cleanUpFunction)) {
    }

    always_inline ~ThreadLocalPointer() {
        ::freeThreadLocalKey(m_threadLocalKey);
    }

    always_inline ObjectType* get() {
        ObjectType* value = reinterpret_cast<ObjectType*>(::getThreadLocalKeyValue(m_threadLocalKey));
        if (autoCreate && value == 0) {
            value = new ObjectType();
            ::setThreadLocalKeyValue(m_threadLocalKey, value);
        }
        return value;
    }

    always_inline ObjectType* operator->() {
        return get();
    }

    always_inline ObjectType& operator*() {
        return *get();
    }

    always_inline void reset(ObjectType* newValue) {
        ObjectType* oldValue = reinterpret_cast<ObjectType*>(::getThreadLocalKeyValue(m_threadLocalKey));
        if (oldValue != newValue) {
            ::setThreadLocalKeyValue(m_threadLocalKey, newValue);
            delete oldValue;
        }
    }
};

#endif /* THREADLOCALPOINTER_H_ */
