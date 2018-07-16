// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../dictionary/Dictionary.h"
#include "../RDFStoreException.h"
#include "CommonBuiltinExpressionEvaluators.h"

// IsIRIEvaluator

UNARY_FUNCTION_START(IsIRIEvaluator, "isIRI")
    result.setBoolean(argumentValue.getDatatypeID() == D_IRI_REFERENCE);
UNARY_FUNCTION_END

static GenericBuiltinFunctionDescriptor<IsIRIEvaluator, 1, 1> s_registration_IsIRIEvaluator_IsURI("isURI", 1000);

// IsBlankEvaluator

UNARY_FUNCTION_START(IsBlankEvaluator, "isBlank")
    result.setBoolean(argumentValue.getDatatypeID() == D_BLANK_NODE);
UNARY_FUNCTION_END

// IsLiteralEvaluator

UNARY_FUNCTION_START(IsLiteralEvaluator, "isLiteral")
    result.setBoolean(argumentValue.getDatatypeID() != D_IRI_REFERENCE && argumentValue.getDatatypeID() != D_BLANK_NODE);
UNARY_FUNCTION_END

// IsNumericEvaluator

UNARY_FUNCTION_START(IsNumericEvaluator, "isNumeric")
    result.setBoolean(argumentValue.getDatatypeID() == D_XSD_INTEGER || argumentValue.getDatatypeID() == D_XSD_FLOAT || argumentValue.getDatatypeID() == D_XSD_DOUBLE);
UNARY_FUNCTION_END

// StrEvaluator

UNARY_FUNCTION_START(StrEvaluator, "STR")
    std::string lexicalForm;
    Dictionary::toLexicalForm(argumentValue, lexicalForm);
    result.setString(D_XSD_STRING, lexicalForm);
UNARY_FUNCTION_END

// LangEvaluator

UNARY_FUNCTION_START(LangEvaluator, "LANG")
    switch (argumentValue.getDatatypeID()) {
    case D_RDF_PLAIN_LITERAL:
        {
            const char* const begin = argumentValue.getString();
            const char* const end = begin + argumentValue.getStringLength();
            const char* current = end - 1;
            while (begin <= current && *current != '@')
                --current;
            if (current < begin)
                return true;
            result.setString(D_XSD_STRING, current + 1, (end - current) - 1);
        }
        break;
    default:
        result.setString(D_XSD_STRING, "");
        break;
    }
UNARY_FUNCTION_END

// DatatypeEvaluator

UNARY_FUNCTION_START(DatatypeEvaluator, "DATATYPE")
    switch (argumentValue.getDatatypeID()) {
    case D_IRI_REFERENCE:
    case D_BLANK_NODE:
        return true;
    default:
        result.setString(D_IRI_REFERENCE, Dictionary::getDatatypeIRI(argumentValue.getDatatypeID()));
        break;
    }
UNARY_FUNCTION_END

// StrdtEvaluator

BINARY_FUNCTION_START(StrdtEvaluator, "STRDT")
    if (argument1Value.getDatatypeID() != D_XSD_STRING || argument2Value.getDatatypeID() != D_IRI_REFERENCE)
        return true;
    try {
        Dictionary::parseResourceValue(result, LITERAL, argument1Value.getString(), argument2Value.getString());
    }
    catch (const RDFStoreException&) {
        return true;
    }
BINARY_FUNCTION_END

// StrlangEvaluator

BINARY_FUNCTION_START(StrlangEvaluator, "STRLANG")
    if (argument1Value.getDatatypeID() != D_XSD_STRING || argument2Value.getDatatypeID() != D_XSD_STRING)
        return true;
    std::string lexicalForm(argument1Value.getString());
    lexicalForm.push_back('@');
    lexicalForm.append(argument2Value.getString());
    result.setString(D_RDF_PLAIN_LITERAL, lexicalForm);
BINARY_FUNCTION_END
