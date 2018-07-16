// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../util/BuiltinExpressionVariableCollector.h"
#include "Logic.h"

_Filter::_Filter(_LogicFactory* const factory, const size_t hash, const BuiltinExpression& builtinExpression) :
    _AtomicFormula(factory, hash),
    m_builtinExpression(builtinExpression)
{
    BuiltinExpressionVariableCollector builtinExpressionVariableCollector(m_arguments);
    m_builtinExpression->accept(builtinExpressionVariableCollector);
}

size_t _Filter::hashCodeFor(const BuiltinExpression& builtinExpression) {
    size_t result = 0;

    result += builtinExpression->hash();
    result += (result << 10);
    result ^= (result >> 6);

    result += (result << 3);
    result ^= (result >> 11);
    result += (result << 15);
    return result;
}

bool _Filter::isEqual(const BuiltinExpression& builtinExpression) const {
    return m_builtinExpression == builtinExpression;
}

LogicObject _Filter::doClone(const LogicFactory& logicFactory) const {
    return logicFactory->getFilter(m_builtinExpression->clone(logicFactory));
}

Formula _Filter::doApply(const Substitution& substitution, const bool renameImplicitExistentialVariables, size_t& formulaWithImplicitExistentialVariablesCounter) const {
    BuiltinExpression newBuiltinExpression = m_builtinExpression->applyEx(substitution, renameImplicitExistentialVariables, formulaWithImplicitExistentialVariablesCounter);
    return m_factory->getFilter(newBuiltinExpression);
}

_Filter::~_Filter() {
    m_factory->dispose(this);
}

const BuiltinExpression& _Filter::getBuiltinExpression() const {
    return m_builtinExpression;
}

FormulaType _Filter::getType() const {
    return FILTER_FORMULA;
}

bool _Filter::isGround() const {
    return m_builtinExpression->isGround();
}

void _Filter::accept(LogicObjectVisitor& visitor) const {
    visitor.visit(Filter(this));
}

std::string _Filter::toString(const Prefixes& prefixes) const {
    std::ostringstream buffer;
    buffer << "FILTER(" << m_builtinExpression->toString(prefixes) << ")";
    return buffer.str();
}
