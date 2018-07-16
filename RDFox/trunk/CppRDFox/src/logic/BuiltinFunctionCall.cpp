// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../builtins/BuiltinExpressionEvaluator.h"
#include "Logic.h"

_BuiltinFunctionCall::_BuiltinFunctionCall(_LogicFactory* const factory, const size_t hash, const char* const functionName, const std::vector<BuiltinExpression>& arguments) :
    _BuiltinExpression(factory, hash),
    m_functionName(functionName),
    m_arguments(arguments)
{
}

size_t _BuiltinFunctionCall::hashCodeFor(const char* const functionName, const std::vector<BuiltinExpression>& arguments) {
    size_t result = stringHashCode(functionName);
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

bool _BuiltinFunctionCall::isEqual(const char* const functionName, const std::vector<BuiltinExpression>& arguments) const {
    if (m_functionName != functionName || m_arguments.size() != arguments.size())
        return false;
    for (auto iterator1 = m_arguments.begin(), iterator2 = arguments.begin(); iterator1 != m_arguments.end(); ++iterator1, ++iterator2)
        if ((*iterator1) != (*iterator2))
            return false;
    return true;
}

LogicObject _BuiltinFunctionCall::doClone(const LogicFactory& logicFactory) const {
    std::vector<BuiltinExpression> newArguments;
    for (auto iterator = m_arguments.begin(); iterator != m_arguments.end(); ++iterator)
        newArguments.push_back((*iterator)->clone(logicFactory));
    return logicFactory->getBuiltinFunctionCall(m_functionName, newArguments);
}

BuiltinExpression _BuiltinFunctionCall::doApply(const Substitution& substitution, const bool renameImplicitExistentialVariables, size_t& formulaWithImplicitExistentialVariablesCounter) const {
    std::vector<BuiltinExpression> newArguments;
    for (auto iterator = m_arguments.begin(); iterator != m_arguments.end(); ++iterator)
        newArguments.push_back((*iterator)->applyEx(substitution, renameImplicitExistentialVariables, formulaWithImplicitExistentialVariablesCounter));
    return m_factory->getBuiltinFunctionCall(m_functionName, newArguments);
}

_BuiltinFunctionCall::~_BuiltinFunctionCall() {
    m_factory->dispose(this);
}

const std::string& _BuiltinFunctionCall::getFunctionName() const {
    return m_functionName;
}

const std::vector<BuiltinExpression>& _BuiltinFunctionCall::getArguments() const {
    return m_arguments;
}

size_t _BuiltinFunctionCall::getNumberOfArguments() const {
    return m_arguments.size();
}

const BuiltinExpression& _BuiltinFunctionCall::getArgument(const size_t index) const {
    return m_arguments[index];
}

BuiltinExpressionType _BuiltinFunctionCall::getType() const {
    return BUILTIN_FUNCTION_CALL;
}

bool _BuiltinFunctionCall::isGround() const {
    for (auto iterator = m_arguments.begin(); iterator != m_arguments.end(); ++iterator)
        if (!(*iterator)->isGround())
            return false;
    return true;
}

void _BuiltinFunctionCall::getFreeVariablesEx(std::unordered_set<Variable>& freeVariables) const {
    for (auto iterator = m_arguments.begin(); iterator != m_arguments.end(); ++iterator)
        (*iterator)->getFreeVariablesEx(freeVariables);
}

void _BuiltinFunctionCall::accept(LogicObjectVisitor& visitor) const {
    visitor.visit(BuiltinFunctionCall(this));
}

std::string _BuiltinFunctionCall::toString(const Prefixes& prefixes) const {
    std::ostringstream buffer;
    BuiltinExpressionEvaluator::toString(BuiltinExpression(this), prefixes, buffer);
    return buffer.str();
}
