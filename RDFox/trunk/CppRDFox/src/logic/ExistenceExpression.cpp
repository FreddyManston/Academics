// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../builtins/BuiltinExpressionEvaluator.h"
#include "Logic.h"

_ExistenceExpression::_ExistenceExpression(_LogicFactory* const factory, const size_t hash, const bool positive, const Formula& formula) :
    _BuiltinExpression(factory, hash),
    m_positive(positive),
    m_formula(formula)
{
}

size_t _ExistenceExpression::hashCodeFor(const bool positive, const Formula& formula) {
    size_t result = static_cast<size_t>(14695981039346656037ULL);

    result += static_cast<size_t>(positive);
    result += (result << 10);
    result ^= (result >> 6);

    result += formula->hash();
    result += (result << 10);
    result ^= (result >> 6);

    result += (result << 3);
    result ^= (result >> 11);
    result += (result << 15);
    return result;
}

bool _ExistenceExpression::isEqual(const bool positive, const Formula& formula) const {
    return m_positive == positive && m_formula == formula;
}

LogicObject _ExistenceExpression::doClone(const LogicFactory& logicFactory) const {
    return logicFactory->getExistenceExpression(m_positive, m_formula->clone(logicFactory));
}

BuiltinExpression _ExistenceExpression::doApply(const Substitution& substitution, const bool renameImplicitExistentialVariables, size_t& formulaWithImplicitExistentialVariablesCounter) const {
    Formula newFormula = m_formula->applyEx(substitution, renameImplicitExistentialVariables, formulaWithImplicitExistentialVariablesCounter);
    return m_factory->getExistenceExpression(m_positive, newFormula);
}

_ExistenceExpression::~_ExistenceExpression() {
    m_factory->dispose(this);
}

bool _ExistenceExpression::isPositive() const {
    return m_positive;
}

const Formula& _ExistenceExpression::getFormula() const {
    return m_formula;
}

BuiltinExpressionType _ExistenceExpression::getType() const {
    return EXISTENCE_EXPRESSION;
}

bool _ExistenceExpression::isGround() const {
    return m_formula->isGround();
}

void _ExistenceExpression::getFreeVariablesEx(std::unordered_set<Variable>& freeVariables) const {
    m_formula->getFreeVariablesEx(freeVariables);
}

void _ExistenceExpression::accept(LogicObjectVisitor& visitor) const {
    visitor.visit(ExistenceExpression(this));
}

std::string _ExistenceExpression::toString(const Prefixes& prefixes) const {
    std::ostringstream buffer;
    BuiltinExpressionEvaluator::toString(BuiltinExpression(this), prefixes, buffer);
    return buffer.str();
}
