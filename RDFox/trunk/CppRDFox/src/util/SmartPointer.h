// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef SMARTPOINTER_H_
#define SMARTPOINTER_H_

#include "../all.h"

template<class T>
class DefaultReferenceManager {

public:

    always_inline void addReference(T* const object) const {
        ++object->m_referenceCount;
    }

    always_inline bool removeReference(T* const object) const {
        return (--object->m_referenceCount) == 0;
    }

    always_inline size_t getReferenceCount(T* const object) const {
        return object->m_referenceCount;
    }

};

template<class T, class RM = DefaultReferenceManager<T> >
class SmartPointer {

public:

    typedef T PointeeType;
    typedef RM ReferenceManagerType;
    typedef SmartPointer<T, RM> SmartPointerType;

protected:

    template<class U, class URM>
    friend class SmartPointer;

    template<class Tptr, class U, class URM>
    friend typename Tptr::SmartPointerType static_pointer_cast(SmartPointer<U, URM>&& pointer);

    template<class Tptr, class U, class URM>
    friend typename Tptr::SmartPointerType dynamic_pointer_cast(SmartPointer<U, URM>&& pointer);

    template<class Tptr, class U, class URM>
    friend typename Tptr::SmartPointerType const_pointer_cast(SmartPointer<U, URM>&& pointer);

    struct ReferenceManagerObject : public ReferenceManagerType {
        PointeeType* m_object;

        ReferenceManagerObject(const ReferenceManagerType& referenceManager, PointeeType* const object) : ReferenceManagerType(referenceManager), m_object(object) {
        }

    };

    ReferenceManagerObject m_referenceManagerObject;

    always_inline explicit SmartPointer(PointeeType* object, int, const ReferenceManagerType& referenceManager = ReferenceManagerType()) : m_referenceManagerObject(referenceManager, object) {
    }

public:

    always_inline explicit SmartPointer(PointeeType* object = nullptr, const ReferenceManagerType& referenceManager = ReferenceManagerType()) : m_referenceManagerObject(referenceManager, object) {
        if (m_referenceManagerObject.m_object)
            m_referenceManagerObject.addReference(m_referenceManagerObject.m_object);
    }

    always_inline SmartPointer(const SmartPointerType& rhs, const ReferenceManagerType& referenceManager = ReferenceManagerType()) : m_referenceManagerObject(referenceManager, rhs.m_referenceManagerObject.m_object) {
        if (m_referenceManagerObject.m_object)
            m_referenceManagerObject.addReference(m_referenceManagerObject.m_object);
    }

    template<class U, class URM>
    always_inline SmartPointer(const SmartPointer<U, URM>& rhs, const ReferenceManagerType& referenceManager = ReferenceManagerType()) : m_referenceManagerObject(referenceManager, rhs.m_referenceManagerObject.m_object) {
        if (m_referenceManagerObject.m_object)
            m_referenceManagerObject.addReference(m_referenceManagerObject.m_object);
    }

    always_inline SmartPointer(SmartPointerType&& rhs, const ReferenceManagerType& referenceManager = ReferenceManagerType()) : m_referenceManagerObject(referenceManager, rhs.m_referenceManagerObject.m_object) {
        rhs.m_referenceManagerObject.m_object = nullptr;
    }

    template<class U, class URM>
    always_inline SmartPointer(SmartPointer<U, URM>&& rhs, const ReferenceManagerType& referenceManager = ReferenceManagerType()) : m_referenceManagerObject(referenceManager, rhs.m_referenceManagerObject.m_object) {
        rhs.m_referenceManagerObject.m_object = nullptr;
    }

    always_inline ~SmartPointer() {
        if (m_referenceManagerObject.m_object != nullptr && m_referenceManagerObject.removeReference(m_referenceManagerObject.m_object))
            delete m_referenceManagerObject.m_object;
    }

    always_inline SmartPointerType& operator=(const SmartPointerType& rhs) {
        reset(rhs.m_referenceManagerObject.m_object);
        return *this;
    }

    template<class U, class URM>
    always_inline SmartPointerType& operator=(const SmartPointer<U, URM>& rhs) {
        reset(rhs.m_referenceManagerObject.m_object);
        return *this;
    }

    always_inline SmartPointerType& operator=(SmartPointerType&& rhs) {
        SmartPointerType(std::move(rhs)).swap(*this);
        return *this;
    }

    template<class U, class URM>
    always_inline SmartPointerType& operator=(SmartPointer<U, URM>&& rhs) {
        SmartPointerType(std::move(rhs)).swap(*this);
        return *this;
    }

    always_inline void reset() {
        reset(static_cast<PointeeType*>(nullptr));
    }

    template<class U>
    always_inline void reset(U* object) {
        SmartPointerType(object).swap(*this);
    }

    template<class U, class URM>
    always_inline void reset(const SmartPointer<U, URM>& rhs) {
        reset(rhs.m_object);
    }

    always_inline PointeeType* get() const {
        return m_referenceManagerObject.m_object;
    }

    always_inline bool is_null() const {
        return m_referenceManagerObject.m_object == nullptr;
    }

    always_inline PointeeType* operator->() const {
        return m_referenceManagerObject.m_object;
    }

    always_inline PointeeType& operator*() const {
        return *m_referenceManagerObject.m_object;
    }

    always_inline size_t use_count() const {
        return m_referenceManagerObject.m_object == nullptr ? 0 : m_referenceManagerObject.getReferenceCount(m_referenceManagerObject.m_object);
    }

    always_inline bool unique() const {
        return use_count() == 1;
    }

    always_inline void swap(SmartPointerType& rhs) {
        PointeeType* temp = m_referenceManagerObject.m_object;
        m_referenceManagerObject.m_object = rhs.m_referenceManagerObject.m_object;
        rhs.m_referenceManagerObject.m_object = temp;
    }

};

template<class T, class RM, class U, class URM>
always_inline bool operator==(const SmartPointer<T, RM>& lhs, const SmartPointer<U, URM>& rhs) {
    return (*lhs.get()) == (*rhs.get());
}

template<class T, class RM, class U, class URM>
always_inline bool operator!=(const SmartPointer<T, RM>& lhs, const SmartPointer<U, URM>& rhs) {
    return (*lhs.get()) != (*rhs.get());
}

template<class T, class RM, class U, class URM>
always_inline bool operator<(const SmartPointer<T, RM>& lhs, const SmartPointer<U, URM>& rhs) {
    return (*lhs.get()) < (*rhs.get());
}

template<class T, class RM, class U, class URM>
always_inline bool operator<=(const SmartPointer<T, RM>& lhs, const SmartPointer<U, URM>& rhs) {
    return (*lhs.get()) <= (*rhs.get());
}

template<class T, class RM, class U, class URM>
always_inline bool operator>(const SmartPointer<T, RM>& lhs, const SmartPointer<U, URM>& rhs) {
    return (*lhs.get()) > (*rhs.get());
}

template<class T, class RM, class U, class URM>
always_inline bool operator>=(const SmartPointer<T, RM>& lhs, const SmartPointer<U, URM>& rhs) {
    return (*lhs.get()) >= (*rhs.get());
}

template<class T, class RM>
always_inline std::ostream& operator<<(std::ostream& stream, const SmartPointer<T, RM>& pointer) {
    stream << *pointer;
    return stream;
}

template<class Tptr, class U, class URM>
always_inline typename Tptr::SmartPointerType static_pointer_cast(const SmartPointer<U, URM>& pointer) {
    return typename Tptr::SmartPointerType(static_cast<typename Tptr::PointeeType*>(pointer.get()));
}

template<class Tptr, class U, class URM>
always_inline typename Tptr::SmartPointerType static_pointer_cast(SmartPointer<U, URM>&& pointer) {
    typename Tptr::PointeeType* object = static_cast<typename Tptr::PointeeType*>(pointer.m_referenceManagerObject.m_object);
    pointer.m_referenceManagerObject.m_object = nullptr;
    return typename Tptr::SmartPointerType(object, 0);
}

template<class Tptr, class U, class URM>
always_inline typename Tptr::SmartPointerType dynamic_pointer_cast(const SmartPointer<U, URM>& pointer) {
    return typename Tptr::SmartPointerType(dynamic_cast<typename Tptr::PointeeType*>(pointer.get()));
}

template<class Tptr, class U, class URM>
always_inline typename Tptr::SmartPointerType dynamic_pointer_cast(SmartPointer<U, URM>&& pointer) {
    typename Tptr::PointeeType* object = dynamic_cast<typename Tptr::PointeeType*>(pointer.m_object);
    if (object != nullptr)
        pointer.m_object = nullptr;
    return typename Tptr::SmartPointerType(object, 0);
}

template<class Tptr, class U, class URM>
always_inline typename Tptr::SmartPointerType const_pointer_cast(const SmartPointer<U, URM>& pointer) {
    return typename Tptr::SmartPointerType(const_cast<typename Tptr::PointeeType*>(pointer.get()));
}

template<class Tptr, class U, class URM>
always_inline typename Tptr::SmartPointerType const_pointer_cast(SmartPointer<U, URM>&& pointer) {
    typename Tptr::PointeeType* object = const_cast<typename Tptr::PointeeType*>(pointer.m_object);
    pointer.m_object = nullptr;
    return typename Tptr::SmartPointerType(object, 0);
}

template<class Tptr, class U, class URM>
always_inline typename Tptr::PointeeType* to_pointer_cast(const SmartPointer<U, URM>& pointer) {
    return static_cast<typename Tptr::PointeeType*>(pointer.get());
}

template<class Tptr, class U, class URM>
always_inline typename Tptr::PointeeType& to_reference_cast(const SmartPointer<U, URM>& pointer) {
    return *static_cast<typename Tptr::PointeeType*>(pointer.get());
}


namespace std {

template<class T, class RM>
struct hash<SmartPointer<T, RM> > {
    always_inline size_t operator()(const SmartPointer<T, RM>& key) const {
        return key->hash();
    }
};

}

#endif /* SMARTPOINTER_H_ */
