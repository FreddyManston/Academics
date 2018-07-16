// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "Logic.h"

_AggregateBind::_AggregateBind(_LogicFactory* const factory, const size_t hash, const char* const functionName, const bool distinct, const std::vector<BuiltinExpression>& arguments, const Term& boundTerm) :
    _LogicObject(factory, hash),
    m_functionName(functionName),
    m_distinct(distinct),
    m_arguments(arguments),
    m_boundTerm(boundTerm)
{
}

_AggregateBind::_AggregateBind(_LogicFactory* const factory, const size_t hash, const char* const functionName, const bool distinct, const BuiltinExpression& argument, const Term& boundTerm) :
    _LogicObject(factory, hash),
    m_functionName(functionName),
    m_distinct(distinct),
    m_arguments(),
    m_boundTerm(boundTerm)
{
    m_arguments.push_back(argument);
}

size_t _AggregateBind::hashCodeFor(const char* const functionName, const bool distinct, const std::vector<BuiltinExpression>& arguments, const Term& boundTerm) {
    size_t result = stringHashCode(functionName);
    result += (result << 10);
    result ^= (result >> 6);

    result += (distinct ? 11 : 0);
    result += (result << 10);
    result ^= (result >> 6);

    result += boundTerm->hash();
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

bool _AggregateBind::isEqual(const char* const functionName, const bool distinct, const std::vector<BuiltinExpression>& arguments, const Term& boundTerm) const {
    if (m_functionName != functionName || m_distinct != distinct || m_boundTerm != boundTerm || m_arguments.size() != arguments.size())
        return false;
    for (auto iterator1 = m_arguments.begin(), iterator2 = arguments.begin(); iterator1 != m_arguments.end(); ++iterator1, ++iterator2)
        if (*iterator1 != *iterator2)
            return false;
    return true;
}

size_t _AggregateBind::hashCodeFor(const char* const functionName, const bool distinct, const BuiltinExpression& argument, const Term& boundTerm) {
    size_t result = stringHashCode(functionName);
    result += (result << 10);
    result ^= (result >> 6);

    result += (distinct ? 11 : 0);
    result += (result << 10);
    result ^= (result >> 6);

    result += argument->hash();
    result += (result << 10);
    result ^= (result >> 6);

    result += (result << 3);
    result ^= (result >> 11);
    result += (result << 15);
    return result;
}

bool _AggregateBind::isEqual(const char* const functionName, const bool distinct, const BuiltinExpression& argument, const Term& boundTerm) const {
    return m_functionName == functionName && m_distinct == distinct && m_boundTerm == boundTerm && m_arguments.size() == 1 && m_arguments[0] == argument;
}

LogicObject _AggregateBind::doClone(const LogicFactory& logicFactory) const {
    std::vector<BuiltinExpression> newArguments;
    newArguments.reserve(m_arguments.size());
    for (auto iterator = m_arguments.begin(); iterator != m_arguments.end(); ++iterator)
        newArguments.push_back((*iterator)->clone(logicFactory));
    Term newBoundTerm = m_boundTerm->clone(logicFactory);
    return logicFactory->getAggregateBind(m_functionName, m_distinct, newArguments, newBoundTerm);
}

AggregateBind _AggregateBind::doApply(const Substitution& substitution, const bool renameImplicitExistentialVariables, size_t& formulaWithImplicitExistentialVariablesCounter) const {
    std::vector<BuiltinExpression> newArguments;
    newArguments.reserve(m_arguments.size());
    for (auto iterator = m_arguments.begin(); iterator != m_arguments.end(); ++iterator)
        newArguments.push_back((*iterator)->applyEx(substitution, renameImplicitExistentialVariables, formulaWithImplicitExistentialVariablesCounter));
    Term newBoundTerm = m_boundTerm->applyEx(substitution, renameImplicitExistentialVariables, formulaWithImplicitExistentialVariablesCounter);
    return m_factory->getAggregateBind(m_functionName, m_distinct,newArguments, newBoundTerm);
}

_AggregateBind::~_AggregateBind() {
}

const std::string& _AggregateBind::getFunctionName() const {
    return m_functionName;
}

bool _AggregateBind::isDistinct() const {
    return m_distinct;
}

const std::vector<BuiltinExpression>& _AggregateBind::getArguments() const {
    return m_arguments;
}

size_t _AggregateBind::getNumberOfArguments() const {
    return m_arguments.size();
}

const BuiltinExpression& _AggregateBind::getArgument(const size_t index) const {
    return m_arguments[index];
}

const Term& _AggregateBind::getBoundTerm() const {
    return m_boundTerm;
}

bool _AggregateBind::isGround() {
    if (!m_boundTerm->isGround())
        return false;
    for (auto iterator = m_arguments.begin(); iterator != m_arguments.end(); ++iterator)
        if (!(*iterator)->isGround())
            return false;
    return true;
}

std::unordered_set<Variable> _AggregateBind::getFreeVariables() const {
    std::unordered_set<Variable> result;
    getFreeVariablesEx(result);
    return result;
}

void _AggregateBind::getFreeVariablesEx(std::unordered_set<Variable>& freeVariables) const {
    m_boundTerm->getFreeVariablesEx(freeVariables);
    for (auto iterator = m_arguments.begin(); iterator != m_arguments.end(); ++iterator)
        (*iterator)->getFreeVariablesEx(freeVariables);
}

void _AggregateBind::accept(LogicObjectVisitor& visitor) const {
    visitor.visit(AggregateBind(this));
}

std::string _AggregateBind::toString(const Prefixes& prefixes) const {
    std::ostringstream buffer;
    buffer << "BIND " << prefixes.encodeIRI(m_functionName) << '(';
    if (m_distinct)
        buffer << "DISTINCT";
    for (auto iterator = m_arguments.begin(); iterator != m_arguments.end(); ++iterator) {
        if (iterator == m_arguments.begin()) {
            if (m_distinct)
                buffer << ' ';
        }
        else
            buffer << ", ";
        buffer << (*iterator)->toString(prefixes);
    }
    buffer << ')';
    buffer << " AS " << m_boundTerm->toString(prefixes);
    return buffer.str();
}
