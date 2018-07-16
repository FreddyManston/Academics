// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../dictionary/Dictionary.h"
#include "CommonBuiltinExpressionEvaluators.h"

// BoundEvaluator

UNARY_FUNCTION_START_BASE(BoundEvaluator, "BOUND")
    ResourceValue argumentValue;
    if (m_argument->evaluate(threadContext, argumentValue))
        return true;
    result.setBoolean(!argumentValue.isUndefined());
UNARY_FUNCTION_END

// IfEvaluator

class IfEvaluator : public NAryFunctionEvaluator<IfEvaluator> {

public:

    IfEvaluator(unique_ptr_vector<BuiltinExpressionEvaluator> arguments) : NAryFunctionEvaluator<IfEvaluator>(std::move(arguments)) {
    }

    virtual bool evaluate(ThreadContext& threadContext, ResourceValue& result);

};

bool IfEvaluator::evaluate(ThreadContext& threadContext, ResourceValue& result) {
    LOAD_ARGUMENT(argument0Value, m_arguments[0]);
    switch (Dictionary::getEffectiveBooleanValue(argument0Value)) {
    case EBV_ERROR:
        return true;
    case EBV_TRUE:
        if (m_arguments[1]->evaluate(threadContext, result))
            return true;
        break;
    case EBV_FALSE:
        if (m_arguments[2]->evaluate(threadContext, result))
            return true;
        break;
    }
    return false;
}

static GenericBuiltinFunctionDescriptor<IfEvaluator, 3, 3> s_registration_IfEvaluator("IF", 1000);

// CoalesceEvaluator

class CoalesceEvaluator : public NAryFunctionEvaluator<CoalesceEvaluator> {

public:

    CoalesceEvaluator(unique_ptr_vector<BuiltinExpressionEvaluator> arguments) : NAryFunctionEvaluator<CoalesceEvaluator>(std::move(arguments)) {
    }

    virtual bool evaluate(ThreadContext& threadContext, ResourceValue& result);

};

bool CoalesceEvaluator::evaluate(ThreadContext& threadContext, ResourceValue& result) {
    for (unique_ptr_vector<BuiltinExpressionEvaluator>::const_iterator iterator = m_arguments.begin(); iterator != m_arguments.end(); ++iterator)
        if (!(*iterator)->evaluate(threadContext, result))
            return false;
    return true;
}

static GenericBuiltinFunctionDescriptor<CoalesceEvaluator, 1, static_cast<size_t>(-1)> s_registration_CoalesceEvaluator("COALESCE", 1000);

// SameTermEvaluator

BINARY_FUNCTION_START_BASE(SameTermEvaluator, "sameTerm")
    ResourceValue argument1Value;
    ResourceValue argument2Value;
    if (m_argument1->evaluate(threadContext, argument1Value) || m_argument2->evaluate(threadContext, argument2Value))
        return true;
    result.setBoolean(argument1Value == argument2Value);
BINARY_FUNCTION_END

// InEvaluator

class InEvaluator : public NAryFunctionEvaluator<InEvaluator> {

public:

    InEvaluator(unique_ptr_vector<BuiltinExpressionEvaluator> arguments) : NAryFunctionEvaluator<InEvaluator>(std::move(arguments)) {
    }

    virtual bool evaluate(ThreadContext& threadContext, ResourceValue& result);

};

bool InEvaluator::evaluate(ThreadContext& threadContext, ResourceValue& result) {
    LOAD_ARGUMENT(argument0Value, m_arguments[0]);
    for (unique_ptr_vector<BuiltinExpressionEvaluator>::const_iterator iterator = m_arguments.begin() + 1; iterator != m_arguments.end(); ++iterator) {
        ResourceValue argumentNValue;
        if ((*iterator)->evaluate(threadContext, argumentNValue))
            return true;
        if (argument0Value == argumentNValue) {
            result.setBoolean(true);
            return false;
        }
    }
    result.setBoolean(false);
    return false;
}

static GenericBuiltinFunctionDescriptor<InEvaluator, 1, static_cast<size_t>(-1)> s_registration_InEvaluator("internal$in", 1000);

// NotInEvaluator

class NotInEvaluator : public NAryFunctionEvaluator<NotInEvaluator> {

public:

    NotInEvaluator(unique_ptr_vector<BuiltinExpressionEvaluator> arguments) : NAryFunctionEvaluator<NotInEvaluator>(std::move(arguments)) {
    }

    virtual bool evaluate(ThreadContext& threadContext, ResourceValue& result);

};

bool NotInEvaluator::evaluate(ThreadContext& threadContext, ResourceValue& result) {
    LOAD_ARGUMENT(argument0Value, m_arguments[0]);
    for (unique_ptr_vector<BuiltinExpressionEvaluator>::const_iterator iterator = m_arguments.begin() + 1; iterator != m_arguments.end(); ++iterator) {
        ResourceValue argumentNValue;
        if ((*iterator)->evaluate(threadContext, argumentNValue))
            return true;
        if (argument0Value == argumentNValue) {
            result.setBoolean(false);
            return false;
        }
    }
    result.setBoolean(true);
    return false;
}

static GenericBuiltinFunctionDescriptor<NotInEvaluator, 1, static_cast<size_t>(-1)> s_registration_NotInEvaluator("internal$not-in", 1000);
