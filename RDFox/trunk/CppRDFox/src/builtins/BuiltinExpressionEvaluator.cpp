// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../RDFStoreException.h"
#include "../dictionary/ResourceValueCache.h"
#include "../querying/TermArray.h"
#include "../querying/QueryCompiler.h"
#include "../storage/DataStore.h"
#include "../storage/Parameters.h"
#include "../util/Prefixes.h"
#include "BuiltinExpressionEvaluator.h"

// BuiltinFunctionDescriptor

BuiltinFunctionDescriptor::BuiltinFunctionDescriptor(const char* const functionName, const size_t precedence) : m_functionName(functionName), m_precedence(precedence) {
    BuiltinExpressionEvaluator::getBuiltinFunctionDescriptors()[m_functionName] = this;
}

BuiltinFunctionDescriptor::~BuiltinFunctionDescriptor() {
}

// VariableEvaluator

class VariableEvaluator : public BuiltinExpressionEvaluator {

protected:

    const ResourceValueCache& m_resourceValueCache;
    const std::vector<ResourceID>& m_argumentsBuffer;
    const size_t m_argumentIndex;

public:

    VariableEvaluator(const ResourceValueCache& resourceValueCache, const std::vector<ResourceID>& argumentsBuffer, const TermArray& termArray, const Variable& variable);

    VariableEvaluator(const VariableEvaluator& other, CloneReplacements& cloneReplacements);

    virtual std::unique_ptr<BuiltinExpressionEvaluator> clone(CloneReplacements& cloneReplacements) const;

    virtual bool evaluate(ThreadContext& threadContext, ResourceValue& result);

};

VariableEvaluator::VariableEvaluator(const ResourceValueCache& resourceValueCache, const std::vector<ResourceID>& argumentsBuffer, const TermArray& termArray, const Variable& variable) :
    m_resourceValueCache(resourceValueCache),
    m_argumentsBuffer(argumentsBuffer),
    m_argumentIndex(termArray.getPosition(variable))
{
}

VariableEvaluator::VariableEvaluator(const VariableEvaluator& other, CloneReplacements& cloneReplacements) :
    m_resourceValueCache(*cloneReplacements.getReplacement(&other.m_resourceValueCache)),
    m_argumentsBuffer(*cloneReplacements.getReplacement(&other.m_argumentsBuffer)),
    m_argumentIndex(other.m_argumentIndex)
{
}

std::unique_ptr<BuiltinExpressionEvaluator> VariableEvaluator::clone(CloneReplacements& cloneReplacements) const {
    return std::unique_ptr<BuiltinExpressionEvaluator>(new VariableEvaluator(*this, cloneReplacements));
}

bool VariableEvaluator::evaluate(ThreadContext& threadContext, ResourceValue& result) {
    if (m_resourceValueCache.getResource(m_argumentsBuffer[m_argumentIndex], result))
        return false;
    else
        return true;
}

// ConstantEvaluator

class ConstantEvaluator : public BuiltinExpressionEvaluator {

protected:

    const ResourceValue m_resourceValue;

public:

    ConstantEvaluator(const ResourceValue& resourceValue);

    ConstantEvaluator(const ConstantEvaluator& other, CloneReplacements& cloneReplacements);

    virtual std::unique_ptr<BuiltinExpressionEvaluator> clone(CloneReplacements& cloneReplacements) const;

    virtual bool evaluate(ThreadContext& threadContext, ResourceValue& result);

};

ConstantEvaluator::ConstantEvaluator(const ResourceValue& resourceValue) : m_resourceValue(resourceValue) {
}

ConstantEvaluator::ConstantEvaluator(const ConstantEvaluator& other, CloneReplacements& cloneReplacements) : m_resourceValue(other.m_resourceValue) {
}

std::unique_ptr<BuiltinExpressionEvaluator> ConstantEvaluator::clone(CloneReplacements& cloneReplacements) const {
    return std::unique_ptr<BuiltinExpressionEvaluator>(new ConstantEvaluator(*this, cloneReplacements));
}

bool ConstantEvaluator::evaluate(ThreadContext& threadContext, ResourceValue& result) {
    result = m_resourceValue;
    return false;
}

// ExistenceExpressionEvaluator

template<bool isPositive>
class ExistenceExpressionEvaluator : public BuiltinExpressionEvaluator {

protected:

    std::vector<std::pair<ArgumentIndex, ResourceID> > m_unsureBindings;
    std::unique_ptr<TupleIterator> m_tupleIterator;
    std::vector<ResourceID>& m_argumentsBuffer;

public:

    ExistenceExpressionEvaluator(const ArgumentIndexSet& allInputArguments, const ArgumentIndexSet& surelyBoundInputArguments, std::unique_ptr<TupleIterator> tupleIterator);

    ExistenceExpressionEvaluator(const ExistenceExpressionEvaluator& other, CloneReplacements& cloneReplacements);

    virtual std::unique_ptr<BuiltinExpressionEvaluator> clone(CloneReplacements& cloneReplacements) const;

    virtual bool evaluate(ThreadContext& threadContext, ResourceValue& result);

};

template<bool isPositive>
ExistenceExpressionEvaluator<isPositive>::ExistenceExpressionEvaluator(const ArgumentIndexSet& allInputArguments, const ArgumentIndexSet& surelyBoundInputArguments, std::unique_ptr<TupleIterator> tupleIterator) :
    m_unsureBindings(),
    m_tupleIterator(std::move(tupleIterator)),
    m_argumentsBuffer(m_tupleIterator->getArgumentsBuffer())
{
    for (ArgumentIndexSet::const_iterator iterator = allInputArguments.begin(); iterator != allInputArguments.end(); ++iterator)
        if (!surelyBoundInputArguments.contains(*iterator))
            m_unsureBindings.push_back(std::make_pair(*iterator, INVALID_RESOURCE_ID));
}

template<bool isPositive>
ExistenceExpressionEvaluator<isPositive>::ExistenceExpressionEvaluator(const ExistenceExpressionEvaluator<isPositive>& other, CloneReplacements& cloneReplacements) :
    m_unsureBindings(other.m_unsureBindings),
    m_tupleIterator(other.m_tupleIterator->clone(cloneReplacements)),
    m_argumentsBuffer(m_tupleIterator->getArgumentsBuffer())
{
}

template<bool isPositive>
std::unique_ptr<BuiltinExpressionEvaluator> ExistenceExpressionEvaluator<isPositive>::clone(CloneReplacements& cloneReplacements) const {
    return std::unique_ptr<BuiltinExpressionEvaluator>(new ExistenceExpressionEvaluator<isPositive>(*this, cloneReplacements));
}

template<bool isPositive>
bool ExistenceExpressionEvaluator<isPositive>::evaluate(ThreadContext& threadContext, ResourceValue& result) {
    for (std::vector<std::pair<ArgumentIndex, ResourceID> >::iterator iterator = m_unsureBindings.begin(); iterator != m_unsureBindings.end(); ++iterator)
        iterator->second = m_argumentsBuffer[iterator->first];
    const bool isNonempty = (m_tupleIterator->open(threadContext) != 0);
    for (std::vector<std::pair<ArgumentIndex, ResourceID> >::iterator iterator = m_unsureBindings.begin(); iterator != m_unsureBindings.end(); ++iterator)
        m_argumentsBuffer[iterator->first] = iterator->second;
    result.setBoolean((isPositive && isNonempty) || (!isPositive && !isNonempty));
    return false;
}

template class ExistenceExpressionEvaluator<false>;
template class ExistenceExpressionEvaluator<true>;

// BuiltinExpressionEvaluator

std::unordered_map<std::string, BuiltinFunctionDescriptor*>& BuiltinExpressionEvaluator::getBuiltinFunctionDescriptors() {
    static std::unordered_map<std::string, BuiltinFunctionDescriptor*> s_builtinFunctionDescriptors;
    return s_builtinFunctionDescriptors;
}

const BuiltinFunctionDescriptor& BuiltinExpressionEvaluator::getBuiltinFunctionDescriptor(const std::string& functionName) {
    const std::unordered_map<std::string, BuiltinFunctionDescriptor*>& builtinFunctionDescriptors = getBuiltinFunctionDescriptors();
    std::unordered_map<std::string, BuiltinFunctionDescriptor*>::const_iterator iterator = builtinFunctionDescriptors.find(functionName);
    if (iterator == builtinFunctionDescriptors.end()) {
        std::ostringstream message;
        message << "Builtin function with IRI '" << functionName << "' does not exist.";
        throw RDF_STORE_EXCEPTION(message.str());
    }
    else
        return *iterator->second;
}

std::unique_ptr<BuiltinExpressionEvaluator> BuiltinExpressionEvaluator::compile(TupleIteratorMonitor* const tupleIteratorMonitor, const DataStore& dataStore, ResourceValueCache& resourceValueCache, const TermArray& termArray, const ArgumentIndexSet& allInputArguments, const ArgumentIndexSet& surelyBoundInputArguments, std::vector<ResourceID>& argumentsBuffer, const BuiltinExpression& builtinExpression) {
    if (builtinExpression->getType() == BUILTIN_FUNCTION_CALL) {
        BuiltinFunctionCall builtinFunctionCall = static_pointer_cast<BuiltinFunctionCall>(builtinExpression);
        unique_ptr_vector<BuiltinExpressionEvaluator> arguments;
        for (size_t argumentIndex = 0; argumentIndex < builtinFunctionCall->getNumberOfArguments(); ++argumentIndex) {
            const BuiltinExpression& argument = builtinFunctionCall->getArgument(argumentIndex);
            arguments.push_back(compile(tupleIteratorMonitor, dataStore, resourceValueCache, termArray, allInputArguments, surelyBoundInputArguments, argumentsBuffer, argument));
        }
        return getBuiltinFunctionDescriptor(builtinFunctionCall->getFunctionName()).createBuiltinExpressionEvaluator(dataStore, resourceValueCache, termArray, allInputArguments, surelyBoundInputArguments, argumentsBuffer, std::move(arguments));
    }
    else if (builtinExpression->getType() == EXISTENCE_EXPRESSION) {
        ExistenceExpression existenceExpression = static_pointer_cast<ExistenceExpression>(builtinExpression);
        Parameters parameters;
        parameters.setString("domain", "IDBrep");
        parameters.setBoolean("reorder-conjunctions", true);
        parameters.setBoolean("bushy", false);
        parameters.setBoolean("root-has-answers", false);
        parameters.setBoolean("distinct", false);
        parameters.setBoolean("cardinality", false);
        parameters.setBoolean("normalizeConstants", true);
        parameters.setBoolean("normalizeConstantsInQueryHead", false);
        QueryCompiler queryCompiler(dataStore, parameters, tupleIteratorMonitor);
        ArgumentIndexSet freeVariables;
        queryCompiler.getFreeVariables(existenceExpression->getFormula(), termArray, freeVariables);
        ArgumentIndexSet answerVariables(freeVariables);
        std::unique_ptr<TupleIterator> tupleIterator = queryCompiler.processFormula(resourceValueCache, existenceExpression->getFormula(), termArray, allInputArguments, surelyBoundInputArguments, answerVariables, freeVariables, argumentsBuffer);
        if (existenceExpression->isPositive())
            return std::unique_ptr<BuiltinExpressionEvaluator>(new ExistenceExpressionEvaluator<true>(allInputArguments, surelyBoundInputArguments, std::move(tupleIterator)));
        else
            return std::unique_ptr<BuiltinExpressionEvaluator>(new ExistenceExpressionEvaluator<false>(allInputArguments, surelyBoundInputArguments, std::move(tupleIterator)));
    }
    else if (builtinExpression->getType() == VARIABLE)
        return std::unique_ptr<BuiltinExpressionEvaluator>(new VariableEvaluator(resourceValueCache, argumentsBuffer, termArray, static_pointer_cast<Variable>(builtinExpression)));
    else {
        ResourceValue resourceValue;
        if (builtinExpression->getType() == RESOURCE_BY_NAME)
            Dictionary::parseResourceValue(resourceValue, to_pointer_cast<ResourceByName>(builtinExpression)->getResourceText());
        else if (!resourceValueCache.getResource(to_pointer_cast<ResourceByID>(builtinExpression)->getResourceID(), resourceValue)) {
            std::ostringstream message;
            message << "Resource with ID '" << to_pointer_cast<ResourceByID>(builtinExpression)->getResourceID() << "' is not known.";
            throw RDF_STORE_EXCEPTION(message.str());
        }
        return std::unique_ptr<BuiltinExpressionEvaluator>(new ConstantEvaluator(resourceValue));
    }
}

void BuiltinExpressionEvaluator::toString(const BuiltinExpression& builtinExpression, const Prefixes& prefixes, std::ostream& output) {
    if (builtinExpression->getType() == BUILTIN_FUNCTION_CALL) {
        BuiltinFunctionCall builtinFunctionCall = static_pointer_cast<BuiltinFunctionCall>(builtinExpression);
        getBuiltinFunctionDescriptor(builtinFunctionCall->getFunctionName()).toString(builtinFunctionCall, prefixes, output);
    }
    else if (builtinExpression->getType() == EXISTENCE_EXPRESSION) {
        ExistenceExpression existenceExpression = static_pointer_cast<ExistenceExpression>(builtinExpression);
        if (!existenceExpression->isPositive())
            output << "NOT ";
        output << "EXISTS { " << existenceExpression->getFormula()->toString(prefixes) << " }";
    }
    else
        output << to_pointer_cast<Term>(builtinExpression)->toString(prefixes);
}

size_t BuiltinExpressionEvaluator::getPrecedence(const BuiltinExpression& builtinExpression) {
    if (builtinExpression->getType() == BUILTIN_FUNCTION_CALL) {
        BuiltinFunctionCall builtinFunctionCall = static_pointer_cast<BuiltinFunctionCall>(builtinExpression);
        return getBuiltinFunctionDescriptor(builtinFunctionCall->getFunctionName()).getPrecedence();
    }
    else
        return 1000;
}

BuiltinExpressionEvaluator::~BuiltinExpressionEvaluator() {
}
