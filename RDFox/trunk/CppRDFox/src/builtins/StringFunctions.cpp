// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "CommonBuiltinExpressionEvaluators.h"

// Utility functions

always_inline bool containsStringLiteral(const ResourceValue& resourceValue) {
    return resourceValue.getDatatypeID() == D_XSD_STRING || resourceValue.getDatatypeID() == D_RDF_PLAIN_LITERAL;
}

always_inline void parseStringLiteral(const ResourceValue& resourceValue, size_t& lexicalFormNoLanguageTagLength, size_t& languageTagStart) {
    if (resourceValue.getDatatypeID() == D_XSD_STRING) {
        lexicalFormNoLanguageTagLength = resourceValue.getStringLength();
        languageTagStart = std::string::npos;
    }
    else {
        const char* const begin = resourceValue.getString();
        const char* current = begin + resourceValue.getStringLength() - 1;
        while (begin <= current && *current != '@')
            --current;
        lexicalFormNoLanguageTagLength = current - begin;
        languageTagStart = lexicalFormNoLanguageTagLength + 1;
    }
}

always_inline void getStringLiteral(ResourceValue& resourceValue, const std::string& lexicalFormNoLanguageTag, const size_t lexicalFormNoLanguageTagStart, const size_t lexicalFormNoLanguageTagLength, const std::string& lexicalFormForLanguageTag, const size_t languageTagStart) {
    std::string lexicalForm(lexicalFormNoLanguageTag, lexicalFormNoLanguageTagStart, lexicalFormNoLanguageTagLength);
    if (languageTagStart == std::string::npos)
        resourceValue.setString(D_XSD_STRING, lexicalForm);
    else {
        lexicalForm.push_back('@');
        lexicalForm.append(lexicalFormForLanguageTag, languageTagStart, std::string::npos);
        resourceValue.setString(D_RDF_PLAIN_LITERAL, lexicalForm);
    }
}

always_inline void getStringLiteral(ResourceValue& resourceValue, const std::string& lexicalFormNoLanguageTag, const std::string& lexicalFormForLanguageTag, const size_t languageTagStart) {
    if (languageTagStart == std::string::npos)
        resourceValue.setString(D_XSD_STRING, lexicalFormNoLanguageTag);
    else {
        std::string lexicalForm(lexicalFormNoLanguageTag);
        lexicalForm.push_back('@');
        lexicalForm.append(lexicalFormForLanguageTag, languageTagStart, std::string::npos);
        resourceValue.setString(D_RDF_PLAIN_LITERAL, lexicalForm);
    }
}

always_inline void getStringLiteral(ResourceValue& resourceValue, const std::string& lexicalFormNoLanguageTag, const std::string& languageTag) {
    if (languageTag.length() == 0)
        resourceValue.setString(D_XSD_STRING, lexicalFormNoLanguageTag);
    else {
        std::string lexicalForm(lexicalFormNoLanguageTag);
        lexicalForm.push_back('@');
        lexicalForm.append(languageTag);
        resourceValue.setString(D_RDF_PLAIN_LITERAL, lexicalForm);
    }
}

// StrlenEvaluator

UNARY_FUNCTION_START(StrlenEvaluator, "STRLEN")
    if (!containsStringLiteral(argumentValue))
        return true;
    size_t lexicalFormNoLanguageTagLength;
    size_t languageTagStart;
    ::parseStringLiteral(argumentValue, lexicalFormNoLanguageTagLength, languageTagStart);
    result.setInteger(static_cast<int64_t>(lexicalFormNoLanguageTagLength));
UNARY_FUNCTION_END

static GenericBuiltinFunctionDescriptor<StrlenEvaluator, 1, 1> s_registration_StrlenEvaluatorXPathNS("http://www.w3.org/2005/xpath-functions#string-length", 1000);

// SubstrEvaluator

class SubstrEvaluator : public NAryFunctionEvaluator<SubstrEvaluator> {

public:

    SubstrEvaluator(unique_ptr_vector<BuiltinExpressionEvaluator> arguments) : NAryFunctionEvaluator<SubstrEvaluator>(std::move(arguments)) {
    }

    virtual bool evaluate(ThreadContext& threadContext, ResourceValue& result);

};

bool SubstrEvaluator::evaluate(ThreadContext& threadContext, ResourceValue& result) {
    LOAD_ARGUMENT(argument0Value, m_arguments[0]);
    if (!containsStringLiteral(argument0Value))
        return true;
    size_t lexicalFormNoLanguageTagLength;
    size_t languageTagStart;
    ::parseStringLiteral(argument0Value, lexicalFormNoLanguageTagLength, languageTagStart);
    LOAD_ARGUMENT(argument1Value, m_arguments[1]);
    if (argument1Value.getDatatypeID() != D_XSD_INTEGER)
        return true;
    size_t startingLocation = static_cast<size_t>(argument1Value.getInteger());
    if (startingLocation <= 0)
        return true;
    // The following is because character indexes in a string as 1-based in SPARQL
    --startingLocation;
    size_t length;
    if (m_arguments.size() == 3) {
        LOAD_ARGUMENT(argument2Value, m_arguments[2]);
        if (argument2Value.getDatatypeID() != D_XSD_INTEGER)
            return true;
        length = static_cast<size_t>(argument2Value.getInteger());
        if (startingLocation + length > lexicalFormNoLanguageTagLength)
            return true;
    }
    else {
        if (startingLocation > lexicalFormNoLanguageTagLength)
            return true;
        length = lexicalFormNoLanguageTagLength - startingLocation;
    }
    getStringLiteral(result, argument0Value.getString(), startingLocation, length, argument0Value.getString(), languageTagStart);
    return false;
}

static GenericBuiltinFunctionDescriptor<SubstrEvaluator, 2, 3> s_registration_SubstrEvaluator("SUBSTR", 1000);

static GenericBuiltinFunctionDescriptor<SubstrEvaluator, 2, 3> s_registration_SubstrEvaluatorXPathNS("http://www.w3.org/2005/xpath-functions#substring", 1000);

// UcaseEvaluator

UNARY_FUNCTION_START(UcaseEvaluator, "UCASE")
    if (!containsStringLiteral(argumentValue))
        return true;
    size_t lexicalFormNoLanguageTagLength;
    size_t languageTagStart;
    ::parseStringLiteral(argumentValue, lexicalFormNoLanguageTagLength, languageTagStart);
    std::string lexicalFormNoLanguageTag(argumentValue.getString(), 0, lexicalFormNoLanguageTagLength);
    ::toUpperCase(lexicalFormNoLanguageTag);
    ::getStringLiteral(result, lexicalFormNoLanguageTag, argumentValue.getString(), languageTagStart);
UNARY_FUNCTION_END

static GenericBuiltinFunctionDescriptor<UcaseEvaluator, 1, 1> s_registration_UcaseEvaluatorXPathNS("http://www.w3.org/2005/xpath-functions#upper-case", 1000);

// LcaseEvaluator

UNARY_FUNCTION_START(LcaseEvaluator, "LCASE")
    if (!containsStringLiteral(argumentValue))
        return true;
    size_t lexicalFormNoLanguageTagLength;
    size_t languageTagStart;
    ::parseStringLiteral(argumentValue, lexicalFormNoLanguageTagLength, languageTagStart);
    std::string lexicalFormNoLanguageTag(argumentValue.getString(), 0, lexicalFormNoLanguageTagLength);
    ::toLowerCase(lexicalFormNoLanguageTag);
    ::getStringLiteral(result, lexicalFormNoLanguageTag, argumentValue.getString(), languageTagStart);
UNARY_FUNCTION_END

static GenericBuiltinFunctionDescriptor<LcaseEvaluator, 1, 1> s_registration_LcaseEvaluatorXPathNS("http://www.w3.org/2005/xpath-functions#lower-case", 1000);

// StrstartsEvaluator

BINARY_FUNCTION_START(StrstartsEvaluator, "STRSTARTS")
    if (!containsStringLiteral(argument1Value) || !containsStringLiteral(argument2Value))
        return true;
    size_t lexicalFormNoLanguageTagLength1;
    size_t languageTagStart1;
    ::parseStringLiteral(argument1Value, lexicalFormNoLanguageTagLength1, languageTagStart1);
    const std::string& argument1LexicalForm = argument1Value.getString();
    size_t lexicalFormNoLanguageTagLength2;
    size_t languageTagStart2;
    ::parseStringLiteral(argument2Value, lexicalFormNoLanguageTagLength2, languageTagStart2);
    const std::string& argument2LexicalForm = argument2Value.getString();
    if (languageTagStart2 != std::string::npos && (languageTagStart1 == std::string::npos || argument1LexicalForm.compare(languageTagStart1, std::string::npos, argument2LexicalForm, languageTagStart2, std::string::npos) != 0))
        return true;
    if (lexicalFormNoLanguageTagLength2 > lexicalFormNoLanguageTagLength1) {
        result.setBoolean(false);
        return false;
    }
    std::string::const_iterator iterator1 = argument1LexicalForm.begin();
    std::string::const_iterator iterator2 = argument2LexicalForm.begin();
    const std::string::const_iterator iterator2End = iterator2 + lexicalFormNoLanguageTagLength2;
    while (iterator2 != iterator2End) {
        if (*iterator1 != *iterator2) {
            result.setBoolean(false);
            return false;
        }
        ++iterator1;
        ++iterator2;
    }
    result.setBoolean(true);
BINARY_FUNCTION_END

static GenericBuiltinFunctionDescriptor<StrstartsEvaluator, 1, 1> s_registration_StrstartsEvaluatorXPathNS("http://www.w3.org/2005/xpath-functions#starts-with", 1000);

// StrendsEvaluator

BINARY_FUNCTION_START(StrendsEvaluator, "STRENDS")
    if (!containsStringLiteral(argument1Value) || !containsStringLiteral(argument2Value))
        return true;
    size_t lexicalFormNoLanguageTagLength1;
    size_t languageTagStart1;
    ::parseStringLiteral(argument1Value, lexicalFormNoLanguageTagLength1, languageTagStart1);
    const std::string& argument1LexicalForm = argument1Value.getString();
    size_t lexicalFormNoLanguageTagLength2;
    size_t languageTagStart2;
    ::parseStringLiteral(argument2Value, lexicalFormNoLanguageTagLength2, languageTagStart2);
    const std::string& argument2LexicalForm = argument2Value.getString();
    if (languageTagStart2 != std::string::npos && (languageTagStart1 == std::string::npos || argument1LexicalForm.compare(languageTagStart1, std::string::npos, argument2LexicalForm, languageTagStart2, std::string::npos) != 0))
        return true;
    if (lexicalFormNoLanguageTagLength2 > lexicalFormNoLanguageTagLength1) {
        result.setBoolean(false);
        return false;
    }
    std::string::const_iterator iterator1 = argument1LexicalForm.begin() + lexicalFormNoLanguageTagLength1;
    std::string::const_iterator iterator2 = argument2LexicalForm.begin() + lexicalFormNoLanguageTagLength2;
    const std::string::const_iterator iterator2End = argument2LexicalForm.begin();
    while (iterator2 != iterator2End) {
        --iterator1;
        --iterator2;
        if (*iterator1 != *iterator2) {
            result.setBoolean(false);
            return false;
        }
    }
    result.setBoolean(true);
BINARY_FUNCTION_END

static GenericBuiltinFunctionDescriptor<StrendsEvaluator, 1, 1> s_registration_StrendsEvaluatorXPathNS("http://www.w3.org/2005/xpath-functions#ends-with", 1000);

// ContainsEvaluator

BINARY_FUNCTION_START(ContainsEvaluator, "CONTAINS")
    if (!containsStringLiteral(argument1Value) || !containsStringLiteral(argument2Value))
        return true;
    size_t lexicalFormNoLanguageTagLength1;
    size_t languageTagStart1;
    ::parseStringLiteral(argument1Value, lexicalFormNoLanguageTagLength1, languageTagStart1);
    const std::string& argument1LexicalForm = argument1Value.getString();
    size_t lexicalFormNoLanguageTagLength2;
    size_t languageTagStart2;
    ::parseStringLiteral(argument2Value, lexicalFormNoLanguageTagLength2, languageTagStart2);
    const std::string& argument2LexicalForm = argument2Value.getString();
    if (languageTagStart2 != std::string::npos && (languageTagStart1 == std::string::npos || argument1LexicalForm.compare(languageTagStart1, std::string::npos, argument2LexicalForm, languageTagStart2, std::string::npos) != 0))
        return true;
    const size_t position = argument1LexicalForm.find(argument2LexicalForm.c_str(), 0, lexicalFormNoLanguageTagLength2);
    result.setBoolean(position != std::string::npos && position + lexicalFormNoLanguageTagLength2 <= lexicalFormNoLanguageTagLength1);
BINARY_FUNCTION_END

static GenericBuiltinFunctionDescriptor<ContainsEvaluator, 1, 1> s_registration_ContainsEvaluatorXPathNS("http://www.w3.org/2005/xpath-functions#contains", 1000);

// StrbeforeEvaluator

BINARY_FUNCTION_START(StrbeforeEvaluator, "STRBEFORE")
    if (!containsStringLiteral(argument1Value) || !containsStringLiteral(argument2Value))
        return true;
    size_t lexicalFormNoLanguageTagLength1;
    size_t languageTagStart1;
    ::parseStringLiteral(argument1Value, lexicalFormNoLanguageTagLength1, languageTagStart1);
    const std::string& argument1LexicalForm = argument1Value.getString();
    size_t lexicalFormNoLanguageTagLength2;
    size_t languageTagStart2;
    ::parseStringLiteral(argument2Value, lexicalFormNoLanguageTagLength2, languageTagStart2);
    const std::string& argument2LexicalForm = argument2Value.getString();
    if (languageTagStart2 != std::string::npos && (languageTagStart1 == std::string::npos || argument1LexicalForm.compare(languageTagStart1, std::string::npos, argument2LexicalForm, languageTagStart2, std::string::npos) != 0))
        return true;
    size_t position = argument1LexicalForm.find(argument2LexicalForm.c_str(), 0, lexicalFormNoLanguageTagLength2);
    if (position == std::string::npos || position + lexicalFormNoLanguageTagLength2 > lexicalFormNoLanguageTagLength1)
        position = 0;
    ::getStringLiteral(result, argument1LexicalForm, 0, position, argument1LexicalForm, languageTagStart1);
BINARY_FUNCTION_END

static GenericBuiltinFunctionDescriptor<StrbeforeEvaluator, 1, 1> s_registration_StrbeforeEvaluatorXPathNS("http://www.w3.org/2005/xpath-functions#substring-before", 1000);

// StrafterEvaluator

BINARY_FUNCTION_START(StrafterEvaluator, "STRAFTER")
    if (!containsStringLiteral(argument1Value) || !containsStringLiteral(argument2Value))
        return true;
    size_t lexicalFormNoLanguageTagLength1;
    size_t languageTagStart1;
    ::parseStringLiteral(argument1Value, lexicalFormNoLanguageTagLength1, languageTagStart1);
    const std::string& argument1LexicalForm = argument1Value.getString();
    size_t lexicalFormNoLanguageTagLength2;
    size_t languageTagStart2;
    ::parseStringLiteral(argument2Value, lexicalFormNoLanguageTagLength2, languageTagStart2);
    const std::string& argument2LexicalForm = argument2Value.getString();
    if (languageTagStart2 != std::string::npos && (languageTagStart1 == std::string::npos || argument1LexicalForm.compare(languageTagStart1, std::string::npos, argument2LexicalForm, languageTagStart2, std::string::npos) != 0))
        return true;
    size_t position = argument1LexicalForm.find(argument2LexicalForm.c_str(), 0, lexicalFormNoLanguageTagLength2);
    if (position == std::string::npos || position + lexicalFormNoLanguageTagLength2 > lexicalFormNoLanguageTagLength1)
        position = lexicalFormNoLanguageTagLength1;
    else
        position += lexicalFormNoLanguageTagLength2;
    ::getStringLiteral(result, argument1LexicalForm,position, lexicalFormNoLanguageTagLength1 - position, argument1LexicalForm, languageTagStart1);
BINARY_FUNCTION_END

static GenericBuiltinFunctionDescriptor<StrafterEvaluator, 1, 1> s_registration_StrafterEvaluatorXPathNS("http://www.w3.org/2005/xpath-functions#substring-after", 1000);

// EncodeForUriEvaluator

UNARY_FUNCTION_START(EncodeForUriEvaluator, "ENCODE_FOR_URI")
    if (!containsStringLiteral(argumentValue))
        return true;
    size_t lexicalFormNoLanguageTagLength;
    size_t languageTagStart;
    ::parseStringLiteral(argumentValue, lexicalFormNoLanguageTagLength, languageTagStart);
    const std::string& argumentLexicalForm = argumentValue.getString();
    const std::string::const_iterator iteratorEnd = argumentLexicalForm.begin() + lexicalFormNoLanguageTagLength;
    std::string resultLexicalForm;
    for (std::string::const_iterator iterator = argumentLexicalForm.begin(); iterator != iteratorEnd; ++iterator) {
        const unsigned char c = *iterator;
        if (('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z') || ('0' <= c && c <= '9') || c == '-' || c == '_' || c == '.' || c == '~')
            resultLexicalForm.push_back(c);
        else {
            const unsigned char firstDigit = c >> 4;
            const unsigned char secondDigit = c & 0xf;
            resultLexicalForm.push_back('%');
            if (firstDigit < 10)
                resultLexicalForm.push_back('0' + firstDigit);
            else
                resultLexicalForm.push_back('A' - 10 + firstDigit);
            if (secondDigit < 10)
                resultLexicalForm.push_back('0' + secondDigit);
            else
                resultLexicalForm.push_back('A' - 10 + secondDigit);
        }
    }
    ::getStringLiteral(result, resultLexicalForm, argumentLexicalForm, languageTagStart);
UNARY_FUNCTION_END

static GenericBuiltinFunctionDescriptor<EncodeForUriEvaluator, 1, 1> s_registration_EncodeForUriEvaluatorXPathNS("http://www.w3.org/2005/xpath-functions#encode-for-uri", 1000);

// ConcatEvaluator

class ConcatEvaluator : public NAryFunctionEvaluator<ConcatEvaluator> {

public:

    ConcatEvaluator(unique_ptr_vector<BuiltinExpressionEvaluator> arguments) : NAryFunctionEvaluator<ConcatEvaluator>(std::move(arguments)) {
    }

    virtual bool evaluate(ThreadContext& threadContext, ResourceValue& result);

};

bool ConcatEvaluator::evaluate(ThreadContext& threadContext, ResourceValue& result) {
    std::string resultingLexicalFormNoLanguageTag;
    std::string resultingLanguageTag;
    bool first = true;
    ResourceValue argumentNValue;
    for (unique_ptr_vector<BuiltinExpressionEvaluator>::const_iterator iterator = m_arguments.begin(); iterator != m_arguments.end(); ++iterator) {
        if ((*iterator)->evaluate(threadContext, argumentNValue) || !containsStringLiteral(argumentNValue))
            return true;
        size_t lexicalFormNoLanguageTagLength;
        size_t languageTagStart;
        ::parseStringLiteral(argumentNValue, lexicalFormNoLanguageTagLength, languageTagStart);
        resultingLexicalFormNoLanguageTag.append(argumentNValue.getString(), 0, lexicalFormNoLanguageTagLength);
        if (first) {
            if (languageTagStart != std::string::npos)
                resultingLanguageTag = (argumentNValue.getString() + languageTagStart);
            first = false;
        }
        else {
            if (languageTagStart == std::string::npos || resultingLanguageTag.compare(0, std::string::npos, argumentNValue.getString(), languageTagStart, std::string::npos) != 0)
                resultingLanguageTag.clear();
        }
    }
    getStringLiteral(result, resultingLexicalFormNoLanguageTag, resultingLanguageTag);
    return false;
}

static GenericBuiltinFunctionDescriptor<ConcatEvaluator, 1, static_cast<size_t>(-1)> s_registration_ConcatEvaluator("CONCAT", 1000);

// LangMatchesEvaluator

BINARY_FUNCTION_START(LangMatchesEvaluator, "langMatches")
    if (argument1Value.getDatatypeID() != D_XSD_STRING || argument2Value.getDatatypeID() != D_XSD_STRING)
        return true;
    const std::string& argument2LexicalForm = argument2Value.getString();
    const size_t argument2Length = argument2LexicalForm.length();
    if (argument2Length == 0)
        return true;
    if (argument2LexicalForm == "*")
        result.setBoolean(true);
    else {
        const std::string& argument1LexicalForm = argument1Value.getString();
        const size_t argument1Length = argument1LexicalForm.length();
        if (argument1Length < argument2Length)
            result.setBoolean(false);
        else {
            std::string::const_iterator iterator1 = argument1LexicalForm.begin();
            std::string::const_iterator iterator2 = argument2LexicalForm.begin();
            const std::string::const_iterator iterator2End = iterator2 + argument2Length;
            while (iterator2 != iterator2End) {
                if (::tolower(*iterator1) != ::tolower(*iterator2)) {
                    result.setBoolean(false);
                    return false;
                }
                ++iterator1;
                ++iterator2;
            }
            result.setBoolean(argument1Length == argument2Length || *iterator1 == '-');
        }
    }
BINARY_FUNCTION_END
