// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "Logic.h"

_Atom::_Atom(_LogicFactory* const factory, const size_t hash, const Predicate& predicate, const std::vector<Term>& arguments) :
    _AtomicFormula(factory, hash, arguments),
    m_predicate(predicate)
{
}

_Atom::_Atom(_LogicFactory* const factory, const size_t hash, const Predicate& rdfPredicate, const Term& subject, const Term& predicate, const Term& object) :
    _AtomicFormula(factory, hash),
    m_predicate(rdfPredicate)
{
    m_arguments.reserve(3);
    m_arguments.push_back(subject);
    m_arguments.push_back(predicate);
    m_arguments.push_back(object);
}

size_t _Atom::hashCodeFor(const Predicate& predicate, const std::vector<Term>& arguments) {
    size_t result = 0;

    result += predicate->hash();
    result += (result << 10);
    result ^= (result >> 6);

    for (auto iterator = arguments.begin(); iterator != arguments.end(); ++iterator) {
        result += (*iterator)->hash();
        result += (result << 10);
        result ^= (result >> 6);
    }

    result += (result << 3);
    result ^= (result >> 11);
    result += (result << 15);
    return result;
}

bool _Atom::isEqual(const Predicate& predicate, const std::vector<Term>& arguments) const {
    if (m_predicate != predicate || m_arguments.size() != arguments.size())
        return false;
    for (auto iterator1 = m_arguments.begin(), iterator2 = arguments.begin(); iterator1 != m_arguments.end(); ++iterator1, ++iterator2)
        if ((*iterator1) != (*iterator2))
            return false;
    return true;
}

size_t _Atom::hashCodeFor(const Predicate& rdfPredicate, const Term& subject, const Term& predicate, const Term& object) {
    size_t result = 0;

    result += rdfPredicate->hash();
    result += (result << 10);
    result ^= (result >> 6);

    result += subject->hash();
    result += (result << 10);
    result ^= (result >> 6);

    result += predicate->hash();
    result += (result << 10);
    result ^= (result >> 6);

    result += object->hash();
    result += (result << 10);
    result ^= (result >> 6);

    result += (result << 3);
    result ^= (result >> 11);
    result += (result << 15);
    return result;
}

bool _Atom::isEqual(const Predicate& rdfPredicate, const Term& subject, const Term& predicate, const Term& object) const {
    return
        m_predicate == rdfPredicate &&
        m_arguments.size() == 3 &&
        m_arguments[0] == subject &&
        m_arguments[1] == predicate &&
        m_arguments[2] == object;
}

LogicObject _Atom::doClone(const LogicFactory& logicFactory) const {
    std::vector<Term> arguments;
    for (auto iterator = m_arguments.begin(); iterator != m_arguments.end(); ++iterator)
        arguments.push_back((*iterator)->clone(logicFactory));
    return logicFactory->getAtom(m_predicate->clone(logicFactory), arguments);
}

Formula _Atom::doApply(const Substitution& substitution, const bool renameImplicitExistentialVariables, size_t& formulaWithImplicitExistentialVariablesCounter) const {
    std::vector<Term> newArguments;
    for (auto iterator = m_arguments.begin(); iterator != m_arguments.end(); ++iterator)
        newArguments.push_back(static_pointer_cast<Term>((*iterator)->applyEx(substitution, renameImplicitExistentialVariables, formulaWithImplicitExistentialVariablesCounter)));
    return m_factory->getAtom(m_predicate, newArguments);
}

_Atom::~_Atom() {
    m_factory->dispose(this);
}

const Predicate& _Atom::getPredicate() const {
    return m_predicate;
}

FormulaType _Atom::getType() const {
    return ATOM_FORMULA;
}

void _Atom::accept(LogicObjectVisitor& visitor) const {
    visitor.visit(Atom(this));
}

std::string _Atom::toString(const Prefixes& prefixes) const {
    std::ostringstream buffer;
    if (m_arguments.size() == 3 && m_predicate->getName() == "internal$rdf")
        buffer << '[' << m_arguments[0]->toString(prefixes) << ", " << m_arguments[1]->toString(prefixes) << ", " << m_arguments[2]->toString(prefixes) << ']';
    else {
        buffer << m_predicate->toString(prefixes);
        if (m_arguments.size() != 0) {
            buffer << '(';
            for (auto iterator = m_arguments.begin(); iterator != m_arguments.end(); ++iterator) {
                if (iterator != m_arguments.begin())
                    buffer << ", ";
                buffer << (*iterator)->toString(prefixes);
            }
            buffer << ')';
        }
    }
    return buffer.str();
}
