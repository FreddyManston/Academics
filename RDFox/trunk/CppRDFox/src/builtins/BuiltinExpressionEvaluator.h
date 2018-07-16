// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef BUILTINEXPRESSIONEVALUATOR_H_
#define BUILTINEXPRESSIONEVALUATOR_H_

#include "../Common.h"
#include "../RDFStoreException.h"
#include "../dictionary/Dictionary.h"
#include "../storage/DataStore.h"
#include "../storage/TupleIterator.h"
#include "../logic/Logic.h"

class TermArray;
class TupleIteratorMonitor;
class ResourceValueCache;
class ThreadContext;

// BuiltinFunctionDescriptor

class BuiltinExpressionEvaluator;

class BuiltinFunctionDescriptor : private Unmovable {

protected:

    const std::string m_functionName;
    const size_t m_precedence;

public:

    BuiltinFunctionDescriptor(const char* const functionName, const size_t precedence);

    virtual ~BuiltinFunctionDescriptor();

    always_inline const std::string& getFunctionName() const {
        return m_functionName;
    }

    always_inline size_t getPrecedence() const {
        return m_precedence;
    }

    virtual void toString(const BuiltinFunctionCall& builtinFunctionCall, const Prefixes& prefixes, std::ostream& output) const = 0;

    virtual std::unique_ptr<BuiltinExpressionEvaluator> createBuiltinExpressionEvaluator(const DataStore& dataStore, ResourceValueCache& resourceValueCache, const TermArray& termArray, const ArgumentIndexSet& allInputArguments, const ArgumentIndexSet& surelyBoundInputArguments, std::vector<ResourceID>& argumentsBuffer, unique_ptr_vector<BuiltinExpressionEvaluator> arguments) const = 0;

};

// BuiltinExpressionEvaluator

class BuiltinExpressionEvaluator : private Unmovable {

protected:

    friend class BuiltinFunctionDescriptor;

    static std::unordered_map<std::string, BuiltinFunctionDescriptor*>& getBuiltinFunctionDescriptors();

    static const BuiltinFunctionDescriptor& getBuiltinFunctionDescriptor(const std::string& functionName);

public:

    static std::unique_ptr<BuiltinExpressionEvaluator> compile(TupleIteratorMonitor* const tupleIteratorMonitor, const DataStore& dataStore, ResourceValueCache& resourceValueCache, const TermArray& termArray, const ArgumentIndexSet& allInputArguments, const ArgumentIndexSet& surelyBoundInputArguments, std::vector<ResourceID>& argumentsBuffer, const BuiltinExpression& builtinExpression);

    static void toString(const BuiltinExpression& builtinExpression, const Prefixes& prefixes, std::ostream& output);

    static size_t getPrecedence(const BuiltinExpression& builtinExpression);

    virtual ~BuiltinExpressionEvaluator();

    virtual std::unique_ptr<BuiltinExpressionEvaluator> clone(CloneReplacements& cloneReplacements) const = 0;

    virtual bool evaluate(ThreadContext& threadContext, ResourceValue& result) = 0;

};

#endif /* BUILTINEXPRESSIONEVALUATOR_H_ */
