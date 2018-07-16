// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef COMMONBUILTINEXPRESSIONEVALUATORS_H_
#define COMMONBUILTINEXPRESSIONEVALUATORS_H_

#include "../RDFStoreException.h"
#include "BuiltinExpressionEvaluator.h"

// UnaryFunctionEvaluator

template<class Derived>
class UnaryFunctionEvaluator : public BuiltinExpressionEvaluator {

protected:

    std::unique_ptr<BuiltinExpressionEvaluator> m_argument;

public:

    UnaryFunctionEvaluator(unique_ptr_vector<BuiltinExpressionEvaluator> arguments) : m_argument(std::move(arguments[0])) {
    }

    virtual size_t getNumberOfArguments() const {
        return 1;
    }

    virtual const BuiltinExpressionEvaluator& getArgument(const size_t index) const {
        switch (index) {
        case 1:
            return *m_argument;
        default:
            throw RDF_STORE_EXCEPTION("Invalid index of the argument.");
        }
    }

    virtual std::unique_ptr<BuiltinExpressionEvaluator> clone(CloneReplacements& cloneReplacements) const {
        unique_ptr_vector<BuiltinExpressionEvaluator> newArguments;
        newArguments.push_back(m_argument->clone(cloneReplacements));
        return std::unique_ptr<BuiltinExpressionEvaluator>(new Derived(std::move(newArguments)));
    }

};

// BinaryFunctionEvaluator

template<class Derived>
class BinaryFunctionEvaluator : public BuiltinExpressionEvaluator {

protected:

    std::unique_ptr<BuiltinExpressionEvaluator> m_argument1;
    std::unique_ptr<BuiltinExpressionEvaluator> m_argument2;

public:

    BinaryFunctionEvaluator(unique_ptr_vector<BuiltinExpressionEvaluator> arguments) : m_argument1(std::move(arguments[0])), m_argument2(std::move(arguments[1])) {
    }

    virtual size_t getNumberOfArguments() const {
        return 2;
    }

    virtual const BuiltinExpressionEvaluator& getArgument(const size_t index) const {
        switch (index) {
        case 1:
            return *m_argument1;
        case 2:
            return *m_argument2;
        default:
            throw RDF_STORE_EXCEPTION("Invalid index of the argument.");
        }
    }

    virtual std::unique_ptr<BuiltinExpressionEvaluator> clone(CloneReplacements& cloneReplacements) const {
        unique_ptr_vector<BuiltinExpressionEvaluator> newArguments;
        newArguments.push_back(m_argument1->clone(cloneReplacements));
        newArguments.push_back(m_argument2->clone(cloneReplacements));
        return std::unique_ptr<BuiltinExpressionEvaluator>(new Derived(std::move(newArguments)));
    }

};

// NAryFunctionEvaluator

template<class Derived>
class NAryFunctionEvaluator : public BuiltinExpressionEvaluator {

protected:

    unique_ptr_vector<BuiltinExpressionEvaluator> m_arguments;

public:

    NAryFunctionEvaluator(unique_ptr_vector<BuiltinExpressionEvaluator> arguments) : m_arguments(std::move(arguments)) {
    }

    virtual size_t getNumberOfArguments() const {
        return m_arguments.size();
    }

    virtual const BuiltinExpressionEvaluator& getArgument(const size_t index) const {
        return *m_arguments[index];
    }

    virtual std::unique_ptr<BuiltinExpressionEvaluator> clone(CloneReplacements& cloneReplacements) const {
        unique_ptr_vector<BuiltinExpressionEvaluator> newArguments;
        for (unique_ptr_vector<BuiltinExpressionEvaluator>::const_iterator iterator = m_arguments.begin(); iterator != m_arguments.end(); ++iterator)
            newArguments.push_back((*iterator)->clone(cloneReplacements));
        return std::unique_ptr<BuiltinExpressionEvaluator>(new Derived(std::move(newArguments)));
    }

};

// UnaryOperatorDescriptor

template<class BEE>
class UnaryOperatorDescriptor : public BuiltinFunctionDescriptor {

protected:

    const std::string m_operatorSymbol;

public:

    UnaryOperatorDescriptor(const char* const functionName, const size_t precedence, const char* const operatorSymbol) : BuiltinFunctionDescriptor(functionName, precedence), m_operatorSymbol(operatorSymbol) {
    }

    virtual void toString(const BuiltinFunctionCall& builtinFunctionCall, const Prefixes& prefixes, std::ostream& output) const {
        output << m_operatorSymbol;
        if (BuiltinExpressionEvaluator::getPrecedence(builtinFunctionCall->getArgument(0)) < m_precedence) {
            output << "(";
            BuiltinExpressionEvaluator::toString(builtinFunctionCall->getArgument(0), prefixes, output);
            output << ")";
        }
        else
            BuiltinExpressionEvaluator::toString(builtinFunctionCall->getArgument(0), prefixes, output);
    }

    virtual std::unique_ptr<BuiltinExpressionEvaluator> createBuiltinExpressionEvaluator(const DataStore& dataStore, ResourceValueCache& resourceValueCache, const TermArray& termArray, const ArgumentIndexSet& allInputArguments, const ArgumentIndexSet& surelyBoundInputArguments, std::vector<ResourceID>& argumentsBuffer, unique_ptr_vector<BuiltinExpressionEvaluator> arguments) const {
        if (arguments.size() != 1) {
            std::ostringstream message;
            message << "Invalid number of arguments (" << arguments.size() << ") for unary operator '" << m_operatorSymbol << "'.";
            throw RDF_STORE_EXCEPTION(message.str());
        }
        return std::unique_ptr<BuiltinExpressionEvaluator>(new BEE(std::move(arguments)));
    }

};

// UnaryOperatorDescriptor

template<class BEE>
class BinaryOperatorDescriptor : public BuiltinFunctionDescriptor {

protected:

    const std::string m_operatorSymbol;

public:

    BinaryOperatorDescriptor(const char* const functionName, const size_t precedence, const char* const operatorSymbol) : BuiltinFunctionDescriptor(functionName, precedence), m_operatorSymbol(operatorSymbol) {
    }

    virtual void toString(const BuiltinFunctionCall& builtinFunctionCall, const Prefixes& prefixes, std::ostream& output) const {
        if (BuiltinExpressionEvaluator::getPrecedence(builtinFunctionCall->getArgument(0)) < m_precedence) {
            output << "(";
            BuiltinExpressionEvaluator::toString(builtinFunctionCall->getArgument(0), prefixes, output);
            output << ")";
        }
        else
            BuiltinExpressionEvaluator::toString(builtinFunctionCall->getArgument(0), prefixes, output);
        output << " " << m_operatorSymbol << " ";
        if (BuiltinExpressionEvaluator::getPrecedence(builtinFunctionCall->getArgument(1)) < m_precedence) {
            output << "(";
            BuiltinExpressionEvaluator::toString(builtinFunctionCall->getArgument(1), prefixes, output);
            output << ")";
        }
        else
            BuiltinExpressionEvaluator::toString(builtinFunctionCall->getArgument(1), prefixes, output);
    }

    virtual std::unique_ptr<BuiltinExpressionEvaluator> createBuiltinExpressionEvaluator(const DataStore& dataStore, ResourceValueCache& resourceValueCache, const TermArray& termArray, const ArgumentIndexSet& allInputArguments, const ArgumentIndexSet& surelyBoundInputArguments, std::vector<ResourceID>& argumentsBuffer, unique_ptr_vector<BuiltinExpressionEvaluator> arguments) const {
        if (arguments.size() != 2) {
            std::ostringstream message;
            message << "Invalid number of arguments (" << arguments.size() << ") for binary operator '" << m_operatorSymbol << "'.";
            throw RDF_STORE_EXCEPTION(message.str());
        }
        return std::unique_ptr<BuiltinExpressionEvaluator>(new BEE( std::move(arguments)));
    }

};

// AbstractFunctionDescriptor

class AbstractBuiltinFunctionDescriptor : public BuiltinFunctionDescriptor {

public:

    AbstractBuiltinFunctionDescriptor(const char* const functionName, const size_t precedence) : BuiltinFunctionDescriptor(functionName, precedence) {
    }

    virtual void toString(const BuiltinFunctionCall& builtinFunctionCall, const Prefixes& prefixes, std::ostream& output) const {
        output << prefixes.encodeIRI(m_functionName) << "(";
        for (size_t index = 0; index < builtinFunctionCall->getNumberOfArguments(); ++index) {
            if (index != 0)
                output << ", ";
            BuiltinExpressionEvaluator::toString(builtinFunctionCall->getArgument(index), prefixes, output);
        }
        output << ")";
    }

};


// GenericBuiltinFunctionDescriptor

template<class BEE, size_t minArity, size_t maxArity>
class GenericBuiltinFunctionDescriptor : public AbstractBuiltinFunctionDescriptor {

public:

    GenericBuiltinFunctionDescriptor(const char* const functionName, const size_t precedence) : AbstractBuiltinFunctionDescriptor(functionName, precedence) {
    }

    virtual void toString(const BuiltinFunctionCall& builtinFunctionCall, const Prefixes& prefixes, std::ostream& output) const {
        output << prefixes.encodeIRI(m_functionName) << "(";
        for (size_t index = 0; index < builtinFunctionCall->getNumberOfArguments(); ++index) {
            if (index != 0)
                output << ", ";
            BuiltinExpressionEvaluator::toString(builtinFunctionCall->getArgument(index), prefixes, output);
        }
        output << ")";
    }

    virtual std::unique_ptr<BuiltinExpressionEvaluator> createBuiltinExpressionEvaluator(const DataStore& dataStore, ResourceValueCache& resourceValueCache, const TermArray& termArray, const ArgumentIndexSet& allInputArguments, const ArgumentIndexSet& surelyBoundInputArguments, std::vector<ResourceID>& argumentsBuffer, unique_ptr_vector<BuiltinExpressionEvaluator> arguments) const {
        if (minArity > arguments.size() || arguments.size() > maxArity) {
            std::ostringstream message;
            message << "Invalid number of arguments (" << arguments.size() << ") for builtin function '" << m_functionName << "'.";
            throw RDF_STORE_EXCEPTION(message.str());
        }
        return std::unique_ptr<BuiltinExpressionEvaluator>(new BEE(std::move(arguments)));
    }

};

// Common macros

#define LOAD_ARGUMENT(argumentValue,argument)\
    ResourceValue argumentValue;\
    if (argument->evaluate(threadContext, argumentValue) || argumentValue.isUndefined())\
        return true;

#define LOAD_ARGUMENT_OF_DATATYPE(argumentValue,argument,datatypeID)\
    LOAD_ARGUMENT(argumentValue,argument);\
    if (argumentValue.getDatatypeID() != datatypeID)\
        return true;

#define UNARY_FUNCTION_START_BASE(FC, functionName)\
    class FC : public UnaryFunctionEvaluator<FC> {\
    \
    public:\
    \
        FC(unique_ptr_vector<BuiltinExpressionEvaluator> arguments) : UnaryFunctionEvaluator<FC>(std::move(arguments)) {\
        }\
    \
        virtual bool evaluate(ThreadContext& threadContext, ResourceValue& result);\
    \
    };\
    \
    static GenericBuiltinFunctionDescriptor<FC, 1, 1> s_registration_##FC(functionName, 1000);\
    \
    bool FC::evaluate(ThreadContext& threadContext, ResourceValue& result) {

#define UNARY_FUNCTION_START(FC, functionName)\
    UNARY_FUNCTION_START_BASE(FC, functionName)\
    LOAD_ARGUMENT(argumentValue,m_argument)

#define UNARY_FUNCTION_END\
        return false;\
    }

#define UNARY_OPERATOR_START_BASE(FC, functionName, operatorSymbol, precedence)\
    class FC : public UnaryFunctionEvaluator<FC> {\
    \
    public:\
    \
        FC(unique_ptr_vector<BuiltinExpressionEvaluator> arguments) : UnaryFunctionEvaluator<FC>(std::move(arguments)) {\
        }\
    \
        virtual bool evaluate(ThreadContext& threadContext, ResourceValue& result);\
    \
    };\
    \
    static UnaryOperatorDescriptor<FC> s_registration_##FC(functionName, precedence, operatorSymbol);\
    \
    bool FC::evaluate(ThreadContext& threadContext, ResourceValue& result) {

#define UNARY_OPERATOR_START(FC, functionName, operatorSymbol, precedence)\
    UNARY_OPERATOR_START_BASE(FC, functionName, operatorSymbol, precedence)\
    LOAD_ARGUMENT(argumentValue,m_argument)

#define UNARY_OPERATOR_END \
        return false;\
    }

#define BINARY_FUNCTION_START_BASE(FC, functionName)\
    class FC : public BinaryFunctionEvaluator<FC> {\
    \
    public:\
    \
        FC(unique_ptr_vector<BuiltinExpressionEvaluator> arguments) : BinaryFunctionEvaluator<FC>(std::move(arguments)) {\
        }\
    \
        virtual bool evaluate(ThreadContext& threadContext, ResourceValue& result);\
    \
    };\
    \
    static GenericBuiltinFunctionDescriptor<FC, 2, 2> s_registration_##FC(functionName, 1000);\
    \
    bool FC::evaluate(ThreadContext& threadContext, ResourceValue& result) {

#define BINARY_FUNCTION_START(FC, functionName)\
    BINARY_FUNCTION_START_BASE(FC, functionName)\
    LOAD_ARGUMENT(argument1Value,m_argument1)\
    LOAD_ARGUMENT(argument2Value,m_argument2)

#define BINARY_FUNCTION_END\
        return false;\
    }

#define BINARY_OPERATOR_START_BASE(FC, functionName, operatorSymbol, precedence)\
    class FC : public BinaryFunctionEvaluator<FC> {\
    \
    public:\
    \
        FC(unique_ptr_vector<BuiltinExpressionEvaluator> arguments) : BinaryFunctionEvaluator<FC>(std::move(arguments)) {\
        }\
    \
        virtual bool evaluate(ThreadContext& threadContext, ResourceValue& result);\
    \
    };\
    \
    static BinaryOperatorDescriptor<FC> s_registration_##FC(functionName, precedence, operatorSymbol);\
    \
    bool FC::evaluate(ThreadContext& threadContext, ResourceValue& result) {

#define BINARY_OPERATOR_START(FC, functionName, operatorSymbol, precedence)\
    BINARY_OPERATOR_START_BASE(FC, functionName, operatorSymbol, precedence)\
    LOAD_ARGUMENT(argument1Value,m_argument1)\
    LOAD_ARGUMENT(argument2Value,m_argument2)

#define BINARY_OPERATOR_END \
        return false;\
    }

#define COMPUTE_BY_TYPE(CC)\
    switch (argument1Value.getDatatypeID()) {\
    case D_IRI_REFERENCE:\
        switch (argument2Value.getDatatypeID()) {\
        case D_IRI_REFERENCE:\
            return CC::compute<D_IRI_REFERENCE, D_IRI_REFERENCE>(argument1Value, argument2Value, result);\
        case D_BLANK_NODE:\
            return CC::compute<D_IRI_REFERENCE, D_BLANK_NODE>(argument1Value, argument2Value, result);\
        case D_XSD_STRING:\
            return CC::compute<D_IRI_REFERENCE, D_XSD_STRING>(argument1Value, argument2Value, result);\
        case D_RDF_PLAIN_LITERAL:\
            return CC::compute<D_IRI_REFERENCE, D_RDF_PLAIN_LITERAL>(argument1Value, argument2Value, result);\
        case D_XSD_INTEGER:\
            return CC::compute<D_IRI_REFERENCE, D_XSD_INTEGER>(argument1Value, argument2Value, result);\
        case D_XSD_FLOAT:\
            return CC::compute<D_IRI_REFERENCE, D_XSD_FLOAT>(argument1Value, argument2Value, result);\
        case D_XSD_DOUBLE:\
            return CC::compute<D_IRI_REFERENCE, D_XSD_DOUBLE>(argument1Value, argument2Value, result);\
        case D_XSD_BOOLEAN:\
            return CC::compute<D_IRI_REFERENCE, D_XSD_BOOLEAN>(argument1Value, argument2Value, result);\
        case D_XSD_DATE_TIME:\
            return CC::compute<D_IRI_REFERENCE, D_XSD_DATE_TIME>(argument1Value, argument2Value, result);\
        case D_XSD_DURATION:\
            return CC::compute<D_IRI_REFERENCE, D_XSD_DURATION>(argument1Value, argument2Value, result);\
        }\
        return true;\
    case D_BLANK_NODE:\
        switch (argument2Value.getDatatypeID()) {\
        case D_IRI_REFERENCE:\
            return CC::compute<D_BLANK_NODE, D_IRI_REFERENCE>(argument1Value, argument2Value, result);\
        case D_BLANK_NODE:\
            return CC::compute<D_BLANK_NODE, D_BLANK_NODE>(argument1Value, argument2Value, result);\
        case D_XSD_STRING:\
            return CC::compute<D_BLANK_NODE, D_XSD_STRING>(argument1Value, argument2Value, result);\
        case D_RDF_PLAIN_LITERAL:\
            return CC::compute<D_BLANK_NODE, D_RDF_PLAIN_LITERAL>(argument1Value, argument2Value, result);\
        case D_XSD_INTEGER:\
            return CC::compute<D_BLANK_NODE, D_XSD_INTEGER>(argument1Value, argument2Value, result);\
        case D_XSD_FLOAT:\
            return CC::compute<D_BLANK_NODE, D_XSD_FLOAT>(argument1Value, argument2Value, result);\
        case D_XSD_DOUBLE:\
            return CC::compute<D_BLANK_NODE, D_XSD_DOUBLE>(argument1Value, argument2Value, result);\
        case D_XSD_BOOLEAN:\
            return CC::compute<D_BLANK_NODE, D_XSD_BOOLEAN>(argument1Value, argument2Value, result);\
        case D_XSD_DATE_TIME:\
            return CC::compute<D_BLANK_NODE, D_XSD_DATE_TIME>(argument1Value, argument2Value, result);\
        case D_XSD_DURATION:\
            return CC::compute<D_BLANK_NODE, D_XSD_DURATION>(argument1Value, argument2Value, result);\
        }\
        return true;\
    case D_XSD_STRING:\
        switch (argument2Value.getDatatypeID()) {\
        case D_IRI_REFERENCE:\
            return CC::compute<D_XSD_STRING, D_IRI_REFERENCE>(argument1Value, argument2Value, result);\
        case D_BLANK_NODE:\
            return CC::compute<D_XSD_STRING, D_BLANK_NODE>(argument1Value, argument2Value, result);\
        case D_XSD_STRING:\
            return CC::compute<D_XSD_STRING, D_XSD_STRING>(argument1Value, argument2Value, result);\
        case D_RDF_PLAIN_LITERAL:\
            return CC::compute<D_XSD_STRING, D_RDF_PLAIN_LITERAL>(argument1Value, argument2Value, result);\
        case D_XSD_INTEGER:\
            return CC::compute<D_XSD_STRING, D_XSD_INTEGER>(argument1Value, argument2Value, result);\
        case D_XSD_FLOAT:\
            return CC::compute<D_XSD_STRING, D_XSD_FLOAT>(argument1Value, argument2Value, result);\
        case D_XSD_DOUBLE:\
            return CC::compute<D_XSD_STRING, D_XSD_DOUBLE>(argument1Value, argument2Value, result);\
        case D_XSD_BOOLEAN:\
            return CC::compute<D_XSD_STRING, D_XSD_BOOLEAN>(argument1Value, argument2Value, result);\
        case D_XSD_DATE_TIME:\
            return CC::compute<D_XSD_STRING, D_XSD_DATE_TIME>(argument1Value, argument2Value, result);\
        case D_XSD_DURATION:\
            return CC::compute<D_XSD_STRING, D_XSD_DURATION>(argument1Value, argument2Value, result);\
        }\
        return true;\
    case D_RDF_PLAIN_LITERAL:\
        switch (argument2Value.getDatatypeID()) {\
        case D_IRI_REFERENCE:\
            return CC::compute<D_RDF_PLAIN_LITERAL, D_IRI_REFERENCE>(argument1Value, argument2Value, result);\
        case D_BLANK_NODE:\
            return CC::compute<D_RDF_PLAIN_LITERAL, D_BLANK_NODE>(argument1Value, argument2Value, result);\
        case D_XSD_STRING:\
            return CC::compute<D_RDF_PLAIN_LITERAL, D_XSD_STRING>(argument1Value, argument2Value, result);\
        case D_RDF_PLAIN_LITERAL:\
            return CC::compute<D_RDF_PLAIN_LITERAL, D_RDF_PLAIN_LITERAL>(argument1Value, argument2Value, result);\
        case D_XSD_INTEGER:\
            return CC::compute<D_RDF_PLAIN_LITERAL, D_XSD_INTEGER>(argument1Value, argument2Value, result);\
        case D_XSD_FLOAT:\
            return CC::compute<D_RDF_PLAIN_LITERAL, D_XSD_FLOAT>(argument1Value, argument2Value, result);\
        case D_XSD_DOUBLE:\
            return CC::compute<D_XSD_STRING, D_XSD_DOUBLE>(argument1Value, argument2Value, result);\
        case D_XSD_BOOLEAN:\
            return CC::compute<D_RDF_PLAIN_LITERAL, D_XSD_BOOLEAN>(argument1Value, argument2Value, result);\
        case D_XSD_DATE_TIME:\
            return CC::compute<D_RDF_PLAIN_LITERAL, D_XSD_DATE_TIME>(argument1Value, argument2Value, result);\
        case D_XSD_DURATION:\
            return CC::compute<D_RDF_PLAIN_LITERAL, D_XSD_DURATION>(argument1Value, argument2Value, result);\
        }\
        return true;\
    case D_XSD_INTEGER:\
        switch (argument2Value.getDatatypeID()) {\
        case D_IRI_REFERENCE:\
            return CC::compute<D_XSD_INTEGER, D_IRI_REFERENCE>(argument1Value, argument2Value, result);\
        case D_BLANK_NODE:\
            return CC::compute<D_XSD_INTEGER, D_BLANK_NODE>(argument1Value, argument2Value, result);\
        case D_XSD_STRING:\
            return CC::compute<D_XSD_INTEGER, D_XSD_STRING>(argument1Value, argument2Value, result);\
        case D_RDF_PLAIN_LITERAL:\
            return CC::compute<D_XSD_INTEGER, D_RDF_PLAIN_LITERAL>(argument1Value, argument2Value, result);\
        case D_XSD_INTEGER:\
            return CC::compute<D_XSD_INTEGER, D_XSD_INTEGER>(argument1Value, argument2Value, result);\
        case D_XSD_FLOAT:\
            return CC::compute<D_XSD_INTEGER, D_XSD_FLOAT>(argument1Value, argument2Value, result);\
        case D_XSD_DOUBLE:\
            return CC::compute<D_XSD_INTEGER, D_XSD_DOUBLE>(argument1Value, argument2Value, result);\
        case D_XSD_BOOLEAN:\
            return CC::compute<D_XSD_INTEGER, D_XSD_BOOLEAN>(argument1Value, argument2Value, result);\
        case D_XSD_DATE_TIME:\
            return CC::compute<D_XSD_INTEGER, D_XSD_DATE_TIME>(argument1Value, argument2Value, result);\
        case D_XSD_DURATION:\
            return CC::compute<D_XSD_INTEGER, D_XSD_DURATION>(argument1Value, argument2Value, result);\
        }\
        return true;\
    case D_XSD_FLOAT:\
        switch (argument2Value.getDatatypeID()) {\
        case D_IRI_REFERENCE:\
            return CC::compute<D_XSD_FLOAT, D_IRI_REFERENCE>(argument1Value, argument2Value, result);\
        case D_BLANK_NODE:\
            return CC::compute<D_XSD_FLOAT, D_BLANK_NODE>(argument1Value, argument2Value, result);\
        case D_XSD_STRING:\
            return CC::compute<D_XSD_FLOAT, D_XSD_STRING>(argument1Value, argument2Value, result);\
        case D_RDF_PLAIN_LITERAL:\
            return CC::compute<D_XSD_FLOAT, D_RDF_PLAIN_LITERAL>(argument1Value, argument2Value, result);\
        case D_XSD_INTEGER:\
            return CC::compute<D_XSD_FLOAT, D_XSD_INTEGER>(argument1Value, argument2Value, result);\
        case D_XSD_FLOAT:\
            return CC::compute<D_XSD_FLOAT, D_XSD_FLOAT>(argument1Value, argument2Value, result);\
        case D_XSD_DOUBLE:\
            return CC::compute<D_XSD_FLOAT, D_XSD_DOUBLE>(argument1Value, argument2Value, result);\
        case D_XSD_BOOLEAN:\
            return CC::compute<D_XSD_FLOAT, D_XSD_BOOLEAN>(argument1Value, argument2Value, result);\
        case D_XSD_DATE_TIME:\
            return CC::compute<D_XSD_FLOAT, D_XSD_DATE_TIME>(argument1Value, argument2Value, result);\
        case D_XSD_DURATION:\
            return CC::compute<D_XSD_FLOAT, D_XSD_DURATION>(argument1Value, argument2Value, result);\
        }\
        return true;\
    case D_XSD_DOUBLE:\
        switch (argument2Value.getDatatypeID()) {\
        case D_IRI_REFERENCE:\
            return CC::compute<D_XSD_DOUBLE, D_IRI_REFERENCE>(argument1Value, argument2Value, result);\
        case D_BLANK_NODE:\
            return CC::compute<D_XSD_DOUBLE, D_BLANK_NODE>(argument1Value, argument2Value, result);\
        case D_XSD_STRING:\
            return CC::compute<D_XSD_DOUBLE, D_XSD_STRING>(argument1Value, argument2Value, result);\
        case D_RDF_PLAIN_LITERAL:\
            return CC::compute<D_XSD_DOUBLE, D_RDF_PLAIN_LITERAL>(argument1Value, argument2Value, result);\
        case D_XSD_INTEGER:\
            return CC::compute<D_XSD_DOUBLE, D_XSD_INTEGER>(argument1Value, argument2Value, result);\
        case D_XSD_FLOAT:\
            return CC::compute<D_XSD_DOUBLE, D_XSD_FLOAT>(argument1Value, argument2Value, result);\
        case D_XSD_DOUBLE:\
            return CC::compute<D_XSD_DOUBLE, D_XSD_DOUBLE>(argument1Value, argument2Value, result);\
        case D_XSD_BOOLEAN:\
            return CC::compute<D_XSD_DOUBLE, D_XSD_BOOLEAN>(argument1Value, argument2Value, result);\
        case D_XSD_DATE_TIME:\
            return CC::compute<D_XSD_DOUBLE, D_XSD_DATE_TIME>(argument1Value, argument2Value, result);\
        case D_XSD_DURATION:\
            return CC::compute<D_XSD_DOUBLE, D_XSD_DURATION>(argument1Value, argument2Value, result);\
        }\
        return true;\
    case D_XSD_BOOLEAN:\
        switch (argument2Value.getDatatypeID()) {\
        case D_IRI_REFERENCE:\
            return CC::compute<D_XSD_BOOLEAN, D_IRI_REFERENCE>(argument1Value, argument2Value, result);\
        case D_BLANK_NODE:\
            return CC::compute<D_XSD_BOOLEAN, D_BLANK_NODE>(argument1Value, argument2Value, result);\
        case D_XSD_STRING:\
            return CC::compute<D_XSD_BOOLEAN, D_XSD_STRING>(argument1Value, argument2Value, result);\
        case D_RDF_PLAIN_LITERAL:\
            return CC::compute<D_XSD_BOOLEAN, D_RDF_PLAIN_LITERAL>(argument1Value, argument2Value, result);\
        case D_XSD_INTEGER:\
            return CC::compute<D_XSD_BOOLEAN, D_XSD_INTEGER>(argument1Value, argument2Value, result);\
        case D_XSD_FLOAT:\
            return CC::compute<D_XSD_BOOLEAN, D_XSD_FLOAT>(argument1Value, argument2Value, result);\
        case D_XSD_DOUBLE:\
            return CC::compute<D_XSD_BOOLEAN, D_XSD_DOUBLE>(argument1Value, argument2Value, result);\
        case D_XSD_BOOLEAN:\
            return CC::compute<D_XSD_BOOLEAN, D_XSD_BOOLEAN>(argument1Value, argument2Value, result);\
        case D_XSD_DATE_TIME:\
            return CC::compute<D_XSD_BOOLEAN, D_XSD_DATE_TIME>(argument1Value, argument2Value, result);\
        case D_XSD_DURATION:\
            return CC::compute<D_XSD_BOOLEAN, D_XSD_DURATION>(argument1Value, argument2Value, result);\
        }\
        return true;\
    case D_XSD_DATE_TIME:\
        switch (argument2Value.getDatatypeID()) {\
        case D_IRI_REFERENCE:\
            return CC::compute<D_XSD_DATE_TIME, D_IRI_REFERENCE>(argument1Value, argument2Value, result);\
        case D_BLANK_NODE:\
            return CC::compute<D_XSD_DATE_TIME, D_BLANK_NODE>(argument1Value, argument2Value, result);\
        case D_XSD_STRING:\
            return CC::compute<D_XSD_DATE_TIME, D_XSD_STRING>(argument1Value, argument2Value, result);\
        case D_RDF_PLAIN_LITERAL:\
            return CC::compute<D_XSD_DATE_TIME, D_RDF_PLAIN_LITERAL>(argument1Value, argument2Value, result);\
        case D_XSD_INTEGER:\
            return CC::compute<D_XSD_DATE_TIME, D_XSD_INTEGER>(argument1Value, argument2Value, result);\
        case D_XSD_FLOAT:\
            return CC::compute<D_XSD_DATE_TIME, D_XSD_FLOAT>(argument1Value, argument2Value, result);\
        case D_XSD_DOUBLE:\
            return CC::compute<D_XSD_DATE_TIME, D_XSD_DOUBLE>(argument1Value, argument2Value, result);\
        case D_XSD_BOOLEAN:\
            return CC::compute<D_XSD_DATE_TIME, D_XSD_BOOLEAN>(argument1Value, argument2Value, result);\
        case D_XSD_DATE_TIME:\
            return CC::compute<D_XSD_DATE_TIME, D_XSD_DATE_TIME>(argument1Value, argument2Value, result);\
        case D_XSD_DURATION:\
            return CC::compute<D_XSD_DATE_TIME, D_XSD_DURATION>(argument1Value, argument2Value, result);\
        }\
        return true;\
    case D_XSD_DURATION:\
        switch (argument2Value.getDatatypeID()) {\
        case D_IRI_REFERENCE:\
            return CC::compute<D_XSD_DURATION, D_IRI_REFERENCE>(argument1Value, argument2Value, result);\
        case D_BLANK_NODE:\
            return CC::compute<D_XSD_DURATION, D_BLANK_NODE>(argument1Value, argument2Value, result);\
        case D_XSD_STRING:\
            return CC::compute<D_XSD_DURATION, D_XSD_STRING>(argument1Value, argument2Value, result);\
        case D_RDF_PLAIN_LITERAL:\
            return CC::compute<D_XSD_DURATION, D_RDF_PLAIN_LITERAL>(argument1Value, argument2Value, result);\
        case D_XSD_INTEGER:\
            return CC::compute<D_XSD_DURATION, D_XSD_INTEGER>(argument1Value, argument2Value, result);\
        case D_XSD_FLOAT:\
            return CC::compute<D_XSD_DURATION, D_XSD_FLOAT>(argument1Value, argument2Value, result);\
        case D_XSD_DOUBLE:\
            return CC::compute<D_XSD_DURATION, D_XSD_DOUBLE>(argument1Value, argument2Value, result);\
        case D_XSD_BOOLEAN:\
            return CC::compute<D_XSD_DURATION, D_XSD_BOOLEAN>(argument1Value, argument2Value, result);\
        case D_XSD_DATE_TIME:\
            return CC::compute<D_XSD_DURATION, D_XSD_DATE_TIME>(argument1Value, argument2Value, result);\
        case D_XSD_DURATION:\
            return CC::compute<D_XSD_DURATION, D_XSD_DURATION>(argument1Value, argument2Value, result);\
        }\
        return true;\
    }\
    return true;

#define DECLARE_COMPUTE_OPERATOR(FC)\
    struct FC##Compute {\
        template<DatatypeID DT1, DatatypeID DT2>\
        static bool compute(const ResourceValue& argument1Value, const ResourceValue& argument2Value, ResourceValue& result);\
    };\
    \
    template<DatatypeID DT1, DatatypeID DT2>\
    always_inline bool FC##Compute::compute(const ResourceValue& argument1Value, const ResourceValue& argument2Value, ResourceValue& result) {\
        return true;\
    }

#define COMPUTE_OPERATION(FC, DT1, DT2)\
    template<>\
    always_inline bool FC##Compute::compute<DT1, DT2>(const ResourceValue& argument1Value, const ResourceValue& argument2Value, ResourceValue& result)


#define NUMERIC_COMPUTE_OPERATIONS(FC, op)\
    COMPUTE_OPERATION(FC, D_XSD_INTEGER, D_XSD_INTEGER) {\
        result.setInteger(argument1Value.getInteger() op argument2Value.getInteger());\
        return false;\
    }\
    COMPUTE_OPERATION(FC, D_XSD_INTEGER, D_XSD_FLOAT) {\
        result.setFloat(argument1Value.getInteger() op argument2Value.getFloat());\
        return false;\
    }\
    COMPUTE_OPERATION(FC, D_XSD_INTEGER, D_XSD_DOUBLE) {\
        result.setDouble(argument1Value.getInteger() op argument2Value.getDouble());\
        return false;\
    }\
    COMPUTE_OPERATION(FC, D_XSD_FLOAT, D_XSD_INTEGER) {\
        result.setFloat(argument1Value.getFloat() op argument2Value.getInteger());\
        return false;\
    }\
    COMPUTE_OPERATION(FC, D_XSD_FLOAT, D_XSD_FLOAT) {\
        result.setFloat(argument1Value.getFloat() op argument2Value.getFloat());\
        return false;\
    }\
    COMPUTE_OPERATION(FC, D_XSD_FLOAT, D_XSD_DOUBLE) {\
        result.setDouble(argument1Value.getFloat() op argument2Value.getDouble());\
        return false;\
    }\
    COMPUTE_OPERATION(FC, D_XSD_DOUBLE, D_XSD_INTEGER) {\
        result.setDouble(argument1Value.getDouble() op argument2Value.getInteger());\
        return false;\
    }\
    COMPUTE_OPERATION(FC, D_XSD_DOUBLE, D_XSD_FLOAT) {\
        result.setDouble(argument1Value.getDouble() op argument2Value.getFloat());\
        return false;\
    }\
    COMPUTE_OPERATION(FC, D_XSD_DOUBLE, D_XSD_DOUBLE) {\
        result.setDouble(argument1Value.getDouble() op argument2Value.getDouble());\
        return false;\
    }

#define IMPLEMENT_COMPUTE_OPERATOR(FC, functionName, operatorSymbol, precedence)\
    BINARY_OPERATOR_START(FC, functionName, operatorSymbol, precedence)\
    COMPUTE_BY_TYPE(FC##Compute)\
    BINARY_OPERATOR_END


#endif /* COMMONBUILTINEXPRESSIONEVALUATORS_H_ */
