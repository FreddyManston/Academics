// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef PREDICATE_H_
#define PREDICATE_H_

#include "LogicObject.h"

class _Predicate : public _LogicObject {

    friend class _LogicFactory::FactoryInterningManager<Predicate>;

protected:

    std::string m_name;

    _Predicate(_LogicFactory* const factory, const size_t hash, const char* const name);

    static size_t hashCodeFor(const char* const name);

    bool isEqual(const char* const name) const;

    virtual LogicObject doClone(const LogicFactory& logicFactory) const;

public:

    virtual ~_Predicate();

    always_inline Predicate clone(const LogicFactory& logicFactory) const {
        return static_pointer_cast<Predicate>(doClone(logicFactory));
    }

    virtual const std::string& getName() const;

    virtual void accept(LogicObjectVisitor& visitor) const;

    virtual std::string toString(const Prefixes& prefixes) const;

};

#endif /* PREDICATE_H_ */
