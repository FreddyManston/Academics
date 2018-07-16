// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "CommonBuiltinExpressionEvaluators.h"
#include "../dictionary/Dictionary.h"
#include "../util/XSDDateTime.h"
#include "../util/XSDDuration.h"

// NotEvaluator

UNARY_OPERATOR_START(NotEvaluator, "internal$logical-not", "!", 300)
    const EffectiveBooleanValue argumentEffectiveBooleanValue = Dictionary::getEffectiveBooleanValue(argumentValue);
    if (argumentEffectiveBooleanValue == EBV_ERROR)
        return true;
    result.setBoolean(argumentEffectiveBooleanValue == EBV_FALSE);
UNARY_OPERATOR_END

// NumericUnaryPlusEvaluator

UNARY_OPERATOR_START(NumericUnaryPlusEvaluator, "internal$numeric-unary-plus", "+", 900)
    switch (argumentValue.getDatatypeID()) {
    case D_XSD_FLOAT:
        result.setFloat(argumentValue.getFloat());
        break;
    case D_XSD_DOUBLE:
        result.setDouble(argumentValue.getDouble());
        break;
    case D_XSD_INTEGER:
        result.setInteger(argumentValue.getInteger());
        break;
    default:
        return true;
    }
UNARY_OPERATOR_END

// NumericUnaryMinusEvaluator

UNARY_OPERATOR_START(NumericUnaryMinusEvaluator, "internal$numeric-unary-minus", "-", 900)
    switch (argumentValue.getDatatypeID()) {
    case D_XSD_FLOAT:
        result.setFloat(-argumentValue.getFloat());
        break;
    case D_XSD_DOUBLE:
        result.setDouble(-argumentValue.getDouble());
        break;
    case D_XSD_INTEGER:
        result.setInteger(-argumentValue.getInteger());
        break;
    default:
        return true;
    }
UNARY_OPERATOR_END

// LogicalOrEvaluator

BINARY_OPERATOR_START(LogicalOrEvaluator, "internal$logical-or", "||", 100)
    const EffectiveBooleanValue argument1EffectiveBooleanValue = Dictionary::getEffectiveBooleanValue(argument1Value);
    const EffectiveBooleanValue argument2EffectiveBooleanValue = Dictionary::getEffectiveBooleanValue(argument2Value);
    if (argument1EffectiveBooleanValue == EBV_ERROR || argument2EffectiveBooleanValue == EBV_ERROR)
        return true;
    result.setBoolean(argument1EffectiveBooleanValue == EBV_TRUE || argument2EffectiveBooleanValue == EBV_TRUE);
BINARY_OPERATOR_END

// LogicalAndEvaluator

BINARY_OPERATOR_START(LogicalAndEvaluator, "internal$logical-and", "&&", 200)
    const EffectiveBooleanValue argument1EffectiveBooleanValue = Dictionary::getEffectiveBooleanValue(argument1Value);
    const EffectiveBooleanValue argument2EffectiveBooleanValue = Dictionary::getEffectiveBooleanValue(argument2Value);
    if (argument1EffectiveBooleanValue == EBV_ERROR || argument2EffectiveBooleanValue == EBV_ERROR)
    return true;
    result.setBoolean(argument1EffectiveBooleanValue == EBV_TRUE && argument2EffectiveBooleanValue == EBV_TRUE);
BINARY_OPERATOR_END

// EqualEvaluator

BINARY_OPERATOR_START_BASE(EqualEvaluator, "internal$equal", "=", 400)
    ResourceValue argument1Value;
    ResourceValue argument2Value;
    if (m_argument1->evaluate(threadContext, argument1Value) || m_argument2->evaluate(threadContext, argument2Value))
        return true;
    result.setBoolean(argument1Value == argument2Value);
BINARY_OPERATOR_END

// NotEqualEvaluator

BINARY_OPERATOR_START_BASE(NotEqualEvaluator, "internal$not-equal", "!=", 400)
    ResourceValue argument1Value;
    ResourceValue argument2Value;
    if (m_argument1->evaluate(threadContext, argument1Value) || m_argument2->evaluate(threadContext, argument2Value))
        return true;
    result.setBoolean(argument1Value != argument2Value);
BINARY_OPERATOR_END

// LessThanEvaluator

DECLARE_COMPUTE_OPERATOR(LessThanEvaluator)

NUMERIC_COMPUTE_OPERATIONS(LessThanEvaluator, <)

COMPUTE_OPERATION(LessThanEvaluator, D_XSD_STRING, D_XSD_STRING) {
    result.setBoolean(::strcmp(argument1Value.getString(), argument2Value.getString()) < 0);
    return false;
}

COMPUTE_OPERATION(LessThanEvaluator, D_XSD_STRING, D_RDF_PLAIN_LITERAL) {
    std::string lexicalForm1 = argument1Value.getString();
    lexicalForm1.push_back('@');
    result.setBoolean(lexicalForm1.compare(argument2Value.getString()) < 0);
    return false;
}

COMPUTE_OPERATION(LessThanEvaluator, D_RDF_PLAIN_LITERAL, D_XSD_STRING) {
    std::string lexicalForm2 = argument2Value.getString();
    lexicalForm2.push_back('@');
    result.setBoolean(::strcmp(argument1Value.getString(), lexicalForm2.c_str()) < 0);
    return false;
}

COMPUTE_OPERATION(LessThanEvaluator, D_RDF_PLAIN_LITERAL, D_RDF_PLAIN_LITERAL) {
    result.setBoolean(::strcmp(argument1Value.getString(), argument2Value.getString()) < 0);
    return false;
}

COMPUTE_OPERATION(LessThanEvaluator, D_XSD_BOOLEAN, D_XSD_BOOLEAN) {
    result.setBoolean(argument1Value.getBoolean() < argument2Value.getBoolean());
    return false;
}

COMPUTE_OPERATION(LessThanEvaluator, D_XSD_DATE_TIME, D_XSD_DATE_TIME) {
    XSDDateTimeComparison comparison = argument1Value.getData<XSDDateTime>().compare(argument2Value.getData<XSDDateTime>());
    result.setBoolean(comparison == XSD_DATE_TIME_LESS_THAN);
    return false;
}

COMPUTE_OPERATION(LessThanEvaluator, D_XSD_DURATION, D_XSD_DURATION) {
    XSDDurationComparison comparison = argument1Value.getData<XSDDuration>().compare(argument2Value.getData<XSDDuration>());
    result.setBoolean(comparison == XSD_DURATION_LESS_THAN);
    return false;
}

IMPLEMENT_COMPUTE_OPERATOR(LessThanEvaluator, "internal$less-than", "<", 400)

// LessEqualThanEvaluator

DECLARE_COMPUTE_OPERATOR(LessEqualThanEvaluator)

NUMERIC_COMPUTE_OPERATIONS(LessEqualThanEvaluator, <=)

COMPUTE_OPERATION(LessEqualThanEvaluator, D_XSD_STRING, D_XSD_STRING) {
    result.setBoolean(::strcmp(argument1Value.getString(), argument2Value.getString()) <= 0);
    return false;
}

COMPUTE_OPERATION(LessEqualThanEvaluator, D_XSD_STRING, D_RDF_PLAIN_LITERAL) {
    std::string lexicalForm1 = argument1Value.getString();
    lexicalForm1.push_back('@');
    result.setBoolean(::strcmp(lexicalForm1.c_str(), argument2Value.getString()) <= 0);
    return false;
}

COMPUTE_OPERATION(LessEqualThanEvaluator, D_RDF_PLAIN_LITERAL, D_XSD_STRING) {
    std::string lexicalForm2 = argument2Value.getString();
    lexicalForm2.push_back('@');
    result.setBoolean(::strcmp(argument1Value.getString(), lexicalForm2.c_str()) <= 0);
    return false;
}

COMPUTE_OPERATION(LessEqualThanEvaluator, D_RDF_PLAIN_LITERAL, D_RDF_PLAIN_LITERAL) {
    result.setBoolean(::strcmp(argument1Value.getString(), argument2Value.getString()) <= 0);
    return false;
}

COMPUTE_OPERATION(LessEqualThanEvaluator, D_XSD_BOOLEAN, D_XSD_BOOLEAN) {
    result.setBoolean(argument1Value.getBoolean() <= argument2Value.getBoolean());
    return false;
}

COMPUTE_OPERATION(LessEqualThanEvaluator, D_XSD_DATE_TIME, D_XSD_DATE_TIME) {
    XSDDateTimeComparison comparison = argument1Value.getData<XSDDateTime>().compare(argument2Value.getData<XSDDateTime>());
    result.setBoolean(comparison == XSD_DATE_TIME_LESS_THAN || comparison == XSD_DATE_TIME_EQUAL);
    return false;
}

COMPUTE_OPERATION(LessEqualThanEvaluator, D_XSD_DURATION, D_XSD_DURATION) {
    XSDDurationComparison comparison = argument1Value.getData<XSDDuration>().compare(argument2Value.getData<XSDDuration>());
    result.setBoolean(comparison == XSD_DURATION_LESS_THAN || comparison == XSD_DURATION_EQUAL);
    return false;
}

IMPLEMENT_COMPUTE_OPERATOR(LessEqualThanEvaluator, "internal$less-equal-than", "<=", 400)

// GreaterThanEvaluator

DECLARE_COMPUTE_OPERATOR(GreaterThanEvaluator)

NUMERIC_COMPUTE_OPERATIONS(GreaterThanEvaluator, >)

COMPUTE_OPERATION(GreaterThanEvaluator, D_XSD_STRING, D_XSD_STRING) {
    result.setBoolean(::strcmp(argument1Value.getString(), argument2Value.getString()) > 0);
    return false;
}

COMPUTE_OPERATION(GreaterThanEvaluator, D_XSD_STRING, D_RDF_PLAIN_LITERAL) {
    std::string lexicalForm1 = argument1Value.getString();
    lexicalForm1.push_back('@');
    result.setBoolean(::strcmp(lexicalForm1.c_str(), argument2Value.getString()) > 0);
    return false;
}

COMPUTE_OPERATION(GreaterThanEvaluator, D_RDF_PLAIN_LITERAL, D_XSD_STRING) {
    std::string lexicalForm2 = argument2Value.getString();
    lexicalForm2.push_back('@');
    result.setBoolean(::strcmp(argument1Value.getString(), lexicalForm2.c_str()) > 0);
    return false;
}

COMPUTE_OPERATION(GreaterThanEvaluator, D_RDF_PLAIN_LITERAL, D_RDF_PLAIN_LITERAL) {
    result.setBoolean(::strcmp(argument1Value.getString(), argument2Value.getString()) > 0);
    return false;
}

COMPUTE_OPERATION(GreaterThanEvaluator, D_XSD_BOOLEAN, D_XSD_BOOLEAN) {
    result.setBoolean(argument1Value.getBoolean() > argument2Value.getBoolean());
    return false;
}

COMPUTE_OPERATION(GreaterThanEvaluator, D_XSD_DATE_TIME, D_XSD_DATE_TIME) {
    XSDDateTimeComparison comparison = argument1Value.getData<XSDDateTime>().compare(argument2Value.getData<XSDDateTime>());
    result.setBoolean(comparison == XSD_DATE_TIME_GREATER_THAN);
    return false;
}

COMPUTE_OPERATION(GreaterThanEvaluator, D_XSD_DURATION, D_XSD_DURATION) {
    XSDDurationComparison comparison = argument1Value.getData<XSDDuration>().compare(argument2Value.getData<XSDDuration>());
    result.setBoolean(comparison == XSD_DURATION_GREATER_THAN);
    return false;
}

IMPLEMENT_COMPUTE_OPERATOR(GreaterThanEvaluator, "internal$greater-than", ">", 400)

// GreaterEqualThanEvaluator

DECLARE_COMPUTE_OPERATOR(GreaterEqualThanEvaluator)

NUMERIC_COMPUTE_OPERATIONS(GreaterEqualThanEvaluator, >=)

COMPUTE_OPERATION(GreaterEqualThanEvaluator, D_XSD_STRING, D_XSD_STRING) {
    result.setBoolean(::strcmp(argument1Value.getString(), argument2Value.getString()) >= 0);
    return false;
}

COMPUTE_OPERATION(GreaterEqualThanEvaluator, D_XSD_STRING, D_RDF_PLAIN_LITERAL) {
    std::string lexicalForm1 = argument1Value.getString();
    lexicalForm1.push_back('@');
    result.setBoolean(::strcmp(lexicalForm1.c_str(), argument2Value.getString()) >= 0);
    return false;
}

COMPUTE_OPERATION(GreaterEqualThanEvaluator, D_RDF_PLAIN_LITERAL, D_XSD_STRING) {
    std::string lexicalForm2 = argument2Value.getString();
    lexicalForm2.push_back('@');
    result.setBoolean(::strcmp(argument1Value.getString(), lexicalForm2.c_str()) >= 0);
    return false;
}

COMPUTE_OPERATION(GreaterEqualThanEvaluator, D_RDF_PLAIN_LITERAL, D_RDF_PLAIN_LITERAL) {
    result.setBoolean(::strcmp(argument1Value.getString(), argument2Value.getString()) >= 0);
    return false;
}

COMPUTE_OPERATION(GreaterEqualThanEvaluator, D_XSD_BOOLEAN, D_XSD_BOOLEAN) {
    result.setBoolean(argument1Value.getBoolean() >= argument2Value.getBoolean());
    return false;
}

COMPUTE_OPERATION(GreaterEqualThanEvaluator, D_XSD_DATE_TIME, D_XSD_DATE_TIME) {
    XSDDateTimeComparison comparison = argument1Value.getData<XSDDateTime>().compare(argument2Value.getData<XSDDateTime>());
    result.setBoolean(comparison == XSD_DATE_TIME_GREATER_THAN || comparison == XSD_DATE_TIME_EQUAL);
    return false;
}

COMPUTE_OPERATION(GreaterEqualThanEvaluator, D_XSD_DURATION, D_XSD_DURATION) {
    XSDDurationComparison comparison = argument1Value.getData<XSDDuration>().compare(argument2Value.getData<XSDDuration>());
    result.setBoolean(comparison == XSD_DURATION_GREATER_THAN || comparison == XSD_DURATION_EQUAL);
    return false;
}

IMPLEMENT_COMPUTE_OPERATOR(GreaterEqualThanEvaluator, "internal$greater-equal-than", ">=", 400)

// AddEvaluator

DECLARE_COMPUTE_OPERATOR(AddEvaluator)

NUMERIC_COMPUTE_OPERATIONS(AddEvaluator, +)

COMPUTE_OPERATION(AddEvaluator, D_XSD_DATE_TIME, D_XSD_DURATION) {
    result.setData(D_XSD_DATE_TIME, argument1Value.getData<XSDDateTime>().addDuration(argument2Value.getData<XSDDuration>()));
    return false;
}

IMPLEMENT_COMPUTE_OPERATOR(AddEvaluator, "internal$add", "+", 500)

// SubtractEvaluator

DECLARE_COMPUTE_OPERATOR(SubtractEvaluator)

NUMERIC_COMPUTE_OPERATIONS(SubtractEvaluator, -)

IMPLEMENT_COMPUTE_OPERATOR(SubtractEvaluator, "internal$subtract", "-", 500)

// MultiplyEvaluator

DECLARE_COMPUTE_OPERATOR(MultiplyEvaluator)

NUMERIC_COMPUTE_OPERATIONS(MultiplyEvaluator, *)

IMPLEMENT_COMPUTE_OPERATOR(MultiplyEvaluator, "internal$multiply", "*", 600)

// DivideEvaluator

DECLARE_COMPUTE_OPERATOR(DivideEvaluator)

COMPUTE_OPERATION(DivideEvaluator, D_XSD_INTEGER, D_XSD_INTEGER) {
    if (argument2Value.getInteger() == 0)
        return true;
    else {
        result.setInteger(argument1Value.getInteger() / argument2Value.getInteger());
        return false;
    }
}

COMPUTE_OPERATION(DivideEvaluator, D_XSD_INTEGER, D_XSD_FLOAT) {
    result.setFloat(argument1Value.getInteger() / argument2Value.getFloat());
    return false;
}

COMPUTE_OPERATION(DivideEvaluator, D_XSD_INTEGER, D_XSD_DOUBLE) {
    result.setDouble(argument1Value.getInteger() / argument2Value.getDouble());
    return false;
}

COMPUTE_OPERATION(DivideEvaluator, D_XSD_FLOAT, D_XSD_INTEGER) {
    result.setFloat(argument1Value.getFloat() / argument2Value.getInteger());
    return false;
}

COMPUTE_OPERATION(DivideEvaluator, D_XSD_FLOAT, D_XSD_FLOAT) {
    result.setFloat(argument1Value.getFloat() / argument2Value.getFloat());
    return false;
}

COMPUTE_OPERATION(DivideEvaluator, D_XSD_FLOAT, D_XSD_DOUBLE) {
    result.setDouble(argument1Value.getFloat() / argument2Value.getDouble());
    return false;
}

COMPUTE_OPERATION(DivideEvaluator, D_XSD_DOUBLE, D_XSD_INTEGER) {
    result.setDouble(argument1Value.getDouble() / argument2Value.getInteger());
    return false;
}

COMPUTE_OPERATION(DivideEvaluator, D_XSD_DOUBLE, D_XSD_FLOAT) {
    result.setDouble(argument1Value.getDouble() / argument2Value.getFloat());
    return false;
}

COMPUTE_OPERATION(DivideEvaluator, D_XSD_DOUBLE, D_XSD_DOUBLE) {
    result.setDouble(argument1Value.getDouble() / argument2Value.getDouble());
    return false;
}

IMPLEMENT_COMPUTE_OPERATOR(DivideEvaluator, "internal$divide", "/", 600)
