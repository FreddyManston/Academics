// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef LOGICOBJECT_H_
#define LOGICOBJECT_H_

#include "../util/Prefixes.h"

class Prefixes;

class _LogicObject : private Unmovable {

    friend class _LogicFactory;
    template<class T>
    friend class DefaultReferenceManager;

protected:

    mutable int32_t m_referenceCount;
    LogicFactory m_factory;
    size_t m_hash;

    _LogicObject(_LogicFactory* const factory, const size_t hash);

    virtual LogicObject doClone(const LogicFactory& logicFactory) const = 0;

public:

    virtual ~_LogicObject();

    always_inline LogicFactory& getFactory() const {
        return const_cast<LogicFactory&>(m_factory);
    }

    always_inline LogicObject clone(const LogicFactory& logicFactory) const {
        return doClone(logicFactory);
    }

    virtual void accept(LogicObjectVisitor& visitor) const = 0;

    virtual std::string toString(const Prefixes& prefixes) const = 0;

    always_inline size_t hash() const {
        return m_hash;
    }

    always_inline friend bool operator==(const _LogicObject& lhs, const _LogicObject& rhs) {
        return &lhs == &rhs;
    }

    always_inline friend bool operator!=(const _LogicObject& lhs, const _LogicObject& rhs) {
        return &lhs != &rhs;
    }

};

always_inline std::ostream& operator<<(std::ostream& stream, const _LogicObject& object) {
    Prefixes prefixes;
    stream << object.toString(prefixes);
    return stream;
}

#endif /* LOGICOBJECT_H_ */
