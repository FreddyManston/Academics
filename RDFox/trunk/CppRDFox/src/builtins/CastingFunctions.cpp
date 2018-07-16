// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../RDFStoreException.h"
#include "../dictionary/Dictionary.h"
#include "CommonBuiltinExpressionEvaluators.h"

// XSD_BooleanEvaluator

UNARY_FUNCTION_START(XSD_BooleanEvaluator, "http://www.w3.org/2001/XMLSchema#boolean")
    switch (argumentValue.getDatatypeID()) {
    case D_XSD_STRING:
        try {
            Dictionary::parseResourceValue(result, argumentValue.getString(), D_XSD_BOOLEAN);
        }
        catch (const RDFStoreException&) {
            return true;
        }
        break;
    case D_XSD_INTEGER:
    case D_XSD_FLOAT:
    case D_XSD_DOUBLE:
    case D_XSD_BOOLEAN:
        {
            const EffectiveBooleanValue effectiveBooleanValue = Dictionary::getEffectiveBooleanValue(argumentValue);
            if (effectiveBooleanValue == EBV_ERROR)
                return true;
            result.setBoolean(effectiveBooleanValue == EBV_TRUE);
        }
        break;
    default:
        return true;
    }
UNARY_FUNCTION_END

// XSD_FloatEvaluator

UNARY_FUNCTION_START(XSD_FloatEvaluator, "http://www.w3.org/2001/XMLSchema#float")
    switch (argumentValue.getDatatypeID()) {
    case D_XSD_STRING:
        try {
            Dictionary::parseResourceValue(result, argumentValue.getString(), D_XSD_FLOAT);
        }
        catch (const RDFStoreException&) {
            return true;
        }
        break;
    case D_XSD_INTEGER:
        result.setFloat(static_cast<float>(argumentValue.getInteger()));
        break;
    case D_XSD_FLOAT:
        result.setFloat(argumentValue.getFloat());
        break;
    case D_XSD_DOUBLE:
        result.setFloat(static_cast<float>(argumentValue.getDouble()));
        break;
    case D_XSD_BOOLEAN:
        result.setFloat(argumentValue.getBoolean() ? 1.0f : 0.0f);
        break;
    default:
        return true;
    }
UNARY_FUNCTION_END

// XSD_DoubleEvaluator

UNARY_FUNCTION_START(XSD_DoubleEvaluator, "http://www.w3.org/2001/XMLSchema#double")
    switch (argumentValue.getDatatypeID()) {
    case D_XSD_STRING:
        try {
            Dictionary::parseResourceValue(result, argumentValue.getString(), D_XSD_DOUBLE);
        }
        catch (const RDFStoreException&) {
            return true;
        }
        break;
    case D_XSD_INTEGER:
        result.setDouble(static_cast<double>(argumentValue.getInteger()));
        break;
    case D_XSD_FLOAT:
        result.setDouble(static_cast<double>(argumentValue.getFloat()));
        break;
    case D_XSD_DOUBLE:
        result.setDouble(argumentValue.getDouble());
        break;
    case D_XSD_BOOLEAN:
        result.setDouble(argumentValue.getBoolean() ? 1.0 : 0.0);
        break;
    default:
        return true;
    }
UNARY_FUNCTION_END

// XSD_IntegerEvaluator

UNARY_FUNCTION_START(XSD_IntegerEvaluator, "http://www.w3.org/2001/XMLSchema#integer")
    switch (argumentValue.getDatatypeID()) {
    case D_XSD_STRING:
        try {
            Dictionary::parseResourceValue(result, argumentValue.getString(), D_XSD_INTEGER);
        }
        catch (const RDFStoreException&) {
            return true;
        }
        break;
    case D_XSD_INTEGER:
        result.setInteger(argumentValue.getInteger());
        break;
    case D_XSD_FLOAT:
        result.setInteger(static_cast<int64_t>(argumentValue.getFloat()));
        break;
    case D_XSD_DOUBLE:
        result.setInteger(static_cast<int64_t>(argumentValue.getDouble()));
        break;
    case D_XSD_BOOLEAN:
        result.setInteger(argumentValue.getBoolean() ? 1 : 0);
        break;
    default:
        return true;
    }
UNARY_FUNCTION_END

// XSD_StringEvaluator

UNARY_FUNCTION_START(XSD_StringEvaluator, "http://www.w3.org/2001/XMLSchema#string")
    switch (argumentValue.getDatatypeID()) {
    case D_IRI_REFERENCE:
    case D_BLANK_NODE:
    case D_XSD_STRING:
    case D_RDF_PLAIN_LITERAL:
        result.setString(D_XSD_STRING, argumentValue.getString());
        break;
    case D_XSD_INTEGER:
        {
            std::ostringstream buffer;
            buffer << argumentValue.getInteger();
            result.setString(D_XSD_STRING, buffer.str());
        }
        break;
    case D_XSD_FLOAT:
        {
            std::ostringstream buffer;
            buffer << argumentValue.getFloat();
            result.setString(D_XSD_STRING, buffer.str());
        }
        break;
    case D_XSD_DOUBLE:
        {
            std::ostringstream buffer;
            buffer << argumentValue.getDouble();
            result.setString(D_XSD_STRING, buffer.str());
        }
        break;
    case D_XSD_BOOLEAN:
        result.setString(D_XSD_STRING, argumentValue.getBoolean() ? "true" : "false");
        break;
    default:
        return true;
    }
UNARY_FUNCTION_END

// IRIEvaluator

UNARY_FUNCTION_START(IRIEvaluator, "IRI")
    if (argumentValue.getDatatypeID() != D_XSD_STRING)
        return true;
    result.setString(D_IRI_REFERENCE, argumentValue.getString());
UNARY_FUNCTION_END
