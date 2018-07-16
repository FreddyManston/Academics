// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "CommonBuiltinExpressionEvaluators.h"

// AbsEvaluator

template<typename T>
always_inline T numericAbs(const T value) {
    return value >= 0 ? value : -value;
}

UNARY_FUNCTION_START(AbsEvaluator, "ABS")
    switch (argumentValue.getDatatypeID()) {
    case D_XSD_INTEGER:
        result.setInteger(numericAbs(argumentValue.getInteger()));
        break;
    case D_XSD_FLOAT:
        result.setFloat(numericAbs(argumentValue.getFloat()));
        break;
    case D_XSD_DOUBLE:
        result.setDouble(numericAbs(argumentValue.getDouble()));
        break;
    default:
        return true;
    }
UNARY_FUNCTION_END

static GenericBuiltinFunctionDescriptor<AbsEvaluator, 1, 1> s_registration_AbsEvaluatorXPathNS("http://www.w3.org/2005/xpath-functions#numeric-abs", 1000);

// RoundEvaluator

UNARY_FUNCTION_START(RoundEvaluator, "ROUND")
    switch (argumentValue.getDatatypeID()) {
    case D_XSD_INTEGER:
        result.setInteger(argumentValue.getInteger());
        break;
    case D_XSD_FLOAT:
        result.setFloat(::roundf(argumentValue.getFloat()));
        break;
    case D_XSD_DOUBLE:
        result.setDouble(::round(argumentValue.getDouble()));
        break;
    default:
        return true;
    }
UNARY_FUNCTION_END

static GenericBuiltinFunctionDescriptor<RoundEvaluator, 1, 1> s_registration_RoundEvaluatorXPathNS("http://www.w3.org/2005/xpath-functions#numeric-round", 1000);

// CeilEvaluator

UNARY_FUNCTION_START(CeilEvaluator, "CEIL")
    switch (argumentValue.getDatatypeID()) {
    case D_XSD_INTEGER:
        result.setInteger(argumentValue.getInteger());
        break;
    case D_XSD_FLOAT:
        result.setFloat(::ceilf(argumentValue.getFloat()));
        break;
    case D_XSD_DOUBLE:
        result.setDouble(::ceil(argumentValue.getDouble()));
        break;
    default:
        return true;
    }
UNARY_FUNCTION_END

static GenericBuiltinFunctionDescriptor<CeilEvaluator, 1, 1> s_registration_CeilEvaluatorXPathNS("http://www.w3.org/2005/xpath-functions#numeric-ceil", 1000);

// FloorEvaluator

UNARY_FUNCTION_START(FloorEvaluator, "FLOOR")
    switch (argumentValue.getDatatypeID()) {
    case D_XSD_INTEGER:
        result.setInteger(argumentValue.getInteger());
        break;
    case D_XSD_FLOAT:
        result.setFloat(::floorf(argumentValue.getFloat()));
        break;
    case D_XSD_DOUBLE:
        result.setDouble(::floor(argumentValue.getDouble()));
        break;
    default:
        return true;
    }
UNARY_FUNCTION_END

static GenericBuiltinFunctionDescriptor<FloorEvaluator, 1, 1> s_registration_FloorEvaluatorXPathNS("http://www.w3.org/2005/xpath-functions#numeric-floor", 1000);

// SinEvaluator

UNARY_FUNCTION_START(SinEvaluator, "SIN")
    switch (argumentValue.getDatatypeID()) {
    case D_XSD_INTEGER:
        result.setDouble(::sin(static_cast<double>(argumentValue.getInteger())));
        break;
    case D_XSD_FLOAT:
        result.setFloat(::sinf(argumentValue.getFloat()));
        break;
    case D_XSD_DOUBLE:
        result.setDouble(::sin(argumentValue.getDouble()));
        break;
    default:
        return true;
    }
UNARY_FUNCTION_END

// CosEvaluator

UNARY_FUNCTION_START(CosEvaluator, "COS")
    switch (argumentValue.getDatatypeID()) {
    case D_XSD_INTEGER:
        result.setDouble(::cos(static_cast<double>(argumentValue.getInteger())));
        break;
    case D_XSD_FLOAT:
        result.setFloat(::cosf(argumentValue.getFloat()));
        break;
    case D_XSD_DOUBLE:
        result.setDouble(::cos(argumentValue.getDouble()));
        break;
    default:
        return true;
    }
UNARY_FUNCTION_END

// TanEvaluator

UNARY_FUNCTION_START(TanEvaluator, "TAN")
    switch (argumentValue.getDatatypeID()) {
    case D_XSD_INTEGER:
        result.setDouble(::tan(static_cast<double>(argumentValue.getInteger())));
        break;
    case D_XSD_FLOAT:
        result.setFloat(::tanf(argumentValue.getFloat()));
        break;
    case D_XSD_DOUBLE:
        result.setDouble(::tan(argumentValue.getDouble()));
        break;
    default:
        return true;
    }
UNARY_FUNCTION_END

// AsinEvaluator

UNARY_FUNCTION_START(AsinEvaluator, "ASIN")
    switch (argumentValue.getDatatypeID()) {
    case D_XSD_INTEGER:
        result.setDouble(::asin(static_cast<double>(argumentValue.getInteger())));
        break;
    case D_XSD_FLOAT:
        result.setFloat(::asinf(argumentValue.getFloat()));
        break;
    case D_XSD_DOUBLE:
        result.setDouble(::asin(argumentValue.getDouble()));
        break;
    default:
        return true;
    }
UNARY_FUNCTION_END

// AcosEvaluator

UNARY_FUNCTION_START(AcosEvaluator, "ACOS")
    switch (argumentValue.getDatatypeID()) {
    case D_XSD_INTEGER:
        result.setDouble(::acos(static_cast<double>(argumentValue.getInteger())));
        break;
    case D_XSD_FLOAT:
        result.setFloat(::acosf(argumentValue.getFloat()));
        break;
    case D_XSD_DOUBLE:
        result.setDouble(::acos(argumentValue.getDouble()));
        break;
    default:
        return true;
    }
UNARY_FUNCTION_END

// AtanEvaluator

UNARY_FUNCTION_START(AtanEvaluator, "ATAN")
    switch (argumentValue.getDatatypeID()) {
    case D_XSD_INTEGER:
        result.setDouble(::atan(static_cast<double>(argumentValue.getInteger())));
        break;
    case D_XSD_FLOAT:
        result.setFloat(::atanf(argumentValue.getFloat()));
        break;
    case D_XSD_DOUBLE:
        result.setDouble(::atan(argumentValue.getDouble()));
        break;
    default:
        return true;
    }
UNARY_FUNCTION_END

// Atan2Evaluator

BINARY_FUNCTION_START(Atan2Evaluator, "ATAN2")
    double argument1;
    switch (argument1Value.getDatatypeID()) {
    case D_XSD_INTEGER:
        argument1 = static_cast<double>(argument1Value.getInteger());
        break;
    case D_XSD_FLOAT:
        argument1 = argument1Value.getFloat();
        break;
    case D_XSD_DOUBLE:
        argument1 = argument1Value.getDouble();
        break;
    default:
        return true;
    }
    double argument2;
    switch (argument2Value.getDatatypeID()) {
    case D_XSD_INTEGER:
        argument2 = static_cast<double>(argument2Value.getInteger());
        break;
    case D_XSD_FLOAT:
        argument2 = argument2Value.getFloat();
        break;
    case D_XSD_DOUBLE:
        argument2 = argument1Value.getDouble();
        break;
    default:
        return true;
    }
    if (argument1Value.getDatatypeID() == D_XSD_DOUBLE || argument2Value.getDatatypeID() == D_XSD_DOUBLE)
        result.setDouble(::atan2(argument1, argument2));
    else
        result.setFloat(static_cast<float>(::atan2(argument1, argument2)));
BINARY_FUNCTION_END

// ExpEvaluator

UNARY_FUNCTION_START(ExpEvaluator, "EXP")
    switch (argumentValue.getDatatypeID()) {
    case D_XSD_INTEGER:
        result.setDouble(::exp(static_cast<double>(argumentValue.getInteger())));
        break;
    case D_XSD_FLOAT:
        result.setFloat(::expf(argumentValue.getFloat()));
        break;
    case D_XSD_DOUBLE:
        result.setDouble(::exp(argumentValue.getDouble()));
        break;
    default:
        return true;
    }
UNARY_FUNCTION_END

// LogEvaluator

UNARY_FUNCTION_START(LogEvaluator, "LOG")
    switch (argumentValue.getDatatypeID()) {
    case D_XSD_INTEGER:
        result.setDouble(::log(static_cast<double>(argumentValue.getInteger())));
        break;
    case D_XSD_FLOAT:
        result.setFloat(::logf(argumentValue.getFloat()));
        break;
    case D_XSD_DOUBLE:
        result.setDouble(::log(argumentValue.getDouble()));
        break;
    default:
        return true;
    }
UNARY_FUNCTION_END

// Log10Evaluator

UNARY_FUNCTION_START(Log10Evaluator, "LOG10")
    switch (argumentValue.getDatatypeID()) {
    case D_XSD_INTEGER:
        result.setDouble(::log10(static_cast<double>(argumentValue.getInteger())));
        break;
    case D_XSD_FLOAT:
        result.setFloat(::log10f(argumentValue.getFloat()));
        break;
    case D_XSD_DOUBLE:
        result.setDouble(::log10(argumentValue.getDouble()));
        break;
    default:
        return true;
    }
UNARY_FUNCTION_END

// PowEvaluator

BINARY_FUNCTION_START(PowEvaluator, "POW")
    double argument1;
    switch (argument1Value.getDatatypeID()) {
    case D_XSD_INTEGER:
        argument1 = static_cast<double>(argument1Value.getInteger());
        break;
    case D_XSD_FLOAT:
        argument1 = argument1Value.getFloat();
        break;
    case D_XSD_DOUBLE:
        argument1 = argument1Value.getDouble();
        break;
    default:
        return true;
    }
    double argument2;
    switch (argument2Value.getDatatypeID()) {
    case D_XSD_INTEGER:
        argument2 = static_cast<double>(argument2Value.getInteger());
        break;
    case D_XSD_FLOAT:
        argument2 = argument2Value.getFloat();
        break;
    case D_XSD_DOUBLE:
        argument2 = argument2Value.getDouble();
        break;
    default:
        return true;
    }
    result.setDouble(::pow(argument1, argument2));
BINARY_FUNCTION_END

// SqrtEvaluator

UNARY_FUNCTION_START(SqrtEvaluator, "SQRT")
    switch (argumentValue.getDatatypeID()) {
    case D_XSD_INTEGER:
        result.setDouble(::sqrt(static_cast<double>(argumentValue.getInteger())));
        break;
    case D_XSD_FLOAT:
        result.setFloat(::sqrtf(argumentValue.getFloat()));
        break;
    case D_XSD_DOUBLE:
        result.setDouble(::sqrt(argumentValue.getDouble()));
        break;
    default:
        return true;
    }
UNARY_FUNCTION_END
