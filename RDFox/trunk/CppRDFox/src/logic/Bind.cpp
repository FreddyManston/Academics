// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../util/BuiltinExpressionVariableCollector.h"
#include "Logic.h"

_Bind::_Bind(_LogicFactory* const factory, const size_t hash, const BuiltinExpression& builtinExpression, const Term& boundTerm) :
    _AtomicFormula(factory, hash),
    m_builtinExpression(builtinExpression),
    m_boundTerm(boundTerm)
{
    BuiltinExpressionVariableCollector builtinExpressionVariableCollector(m_arguments);
    m_arguments.push_back(m_boundTerm);
    m_builtinExpression->accept(builtinExpressionVariableCollector);
}

size_t _Bind::hashCodeFor(const BuiltinExpression& builtinExpression, const Term& boundTerm) {
    size_t result = 0;

    result += builtinExpression->hash();
    result += (result << 10);
    result ^= (result >> 6);

    result += boundTerm->hash();
    result += (result << 10);
    result ^= (result >> 6);

    result += (result << 3);
    result ^= (result >> 11);
    result += (result << 15);
    return result;
}

bool _Bind::isEqual(const BuiltinExpression& builtinExpression, const Term& boundTerm) const {
    return m_builtinExpression == builtinExpression && m_boundTerm == boundTerm;
}

LogicObject _Bind::doClone(const LogicFactory& logicFactory) const {
    return logicFactory->getBind(m_builtinExpression->clone(logicFactory), m_boundTerm->clone(logicFactory));
}

Formula _Bind::doApply(const Substitution& substitution, const bool renameImplicitExistentialVariables, size_t& formulaWithImplicitExistentialVariablesCounter) const {
    BuiltinExpression newBuiltinExpression = m_builtinExpression->applyEx(substitution, renameImplicitExistentialVariables, formulaWithImplicitExistentialVariablesCounter);
    Term newBoundTerm = m_boundTerm->applyEx(substitution, renameImplicitExistentialVariables, formulaWithImplicitExistentialVariablesCounter);
    return m_factory->getBind(newBuiltinExpression, newBoundTerm);
}

_Bind::~_Bind() {
    m_factory->dispose(this);
}

const BuiltinExpression& _Bind::getBuiltinExpression() const {
    return m_builtinExpression;
}

const Term& _Bind::getBoundTerm() const {
    return m_boundTerm;
}

FormulaType _Bind::getType() const {
    return BIND_FORMULA;
}

bool _Bind::isGround() const {
    return m_builtinExpression->isGround() && m_boundTerm->isGround();
}

void _Bind::accept(LogicObjectVisitor& visitor) const {
    visitor.visit(Bind(this));
}

std::string _Bind::toString(const Prefixes& prefixes) const {
    std::ostringstream buffer;
    buffer << "BIND(" << m_builtinExpression->toString(prefixes) << " AS " << m_boundTerm->toString(prefixes) << ")";
    return buffer.str();
}
