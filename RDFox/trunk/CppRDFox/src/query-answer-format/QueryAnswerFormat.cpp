// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../RDFStoreException.h"
#include "../dictionary/Dictionary.h"
#include "../dictionary/ResourceValueCache.h"
#include "../storage/TupleIterator.h"
#include "../formats/turtle/TurtleSyntax.h"
#include "../util/OutputStream.h"
#include "QueryAnswerFormat.h"

class Dictionary;
class TupleIterator;

const static char HEX[16] = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F' };

// VariableAnswersOnlyFormat

class VariableAnswersOnlyFormat : public QueryAnswerFormat {

public:

    VariableAnswersOnlyFormat(OutputStream& outputStream, const bool isAskQuery, const std::vector<Term>& answerTerms) :
        QueryAnswerFormat(outputStream, isAskQuery, answerTerms)
    {
        for (auto iterator = m_answerTerms.begin(); iterator != m_answerTerms.end(); ++iterator)
            if ((*iterator)->getType() != VARIABLE)
                throw RDF_STORE_EXCEPTION("This format supports only queries that returns only variables.");
    }

};

// XMLFormat

class XMLFormat : public VariableAnswersOnlyFormat {

protected:

    bool m_firstResult;

public:

    XMLFormat(OutputStream& outputStream, const bool isAskQuery, const std::vector<Term>& answerTerms) :
        VariableAnswersOnlyFormat(outputStream, isAskQuery, answerTerms),
        m_firstResult(true)
    {
    }

    void write(std::string::const_iterator start, std::string::const_iterator end);

    void write(const std::string& string);

    virtual void printPrologue();

    virtual void printResult(const ResourceValueCache& resourceValueCache, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const size_t multiplicity);

    virtual void printEpilogue();

};

always_inline void XMLFormat::write(std::string::const_iterator start, std::string::const_iterator end) {
    for (; start != end; ++start) {
        const char c = *start;
        switch (c) {
        case '&':
            m_outputStream << "&amp;";
            break;
        case '\"':
            m_outputStream << "&quot;";
            break;
        case '\'':
            m_outputStream << "&apos;";
            break;
        case '<':
            m_outputStream << "&lt;";
            break;
        case '>':
            m_outputStream << "&gt;";
            break;
        default:
            m_outputStream << c;
            break;
        }
    }
}

always_inline void XMLFormat::write(const std::string& string) {
    write(string.begin(), string.end());
}

void XMLFormat::printPrologue() {
    m_outputStream <<
        "<?xml version=\"1.0\"?>\n"
        "<sparql xmlns=\"http://www.w3.org/2005/sparql-results#\">\n"
        "<head>\n";
    for (auto iterator = m_answerTerms.begin(); iterator != m_answerTerms.end(); ++iterator) {
        m_outputStream << "  <variable name=\"";
        write(to_pointer_cast<Variable>(*iterator)->getName());
        m_outputStream << "\"/>\n";
    }
    m_outputStream << "</head>\n";
    if (!m_isAskQuery)
        m_outputStream << "<results>\n";
}

void XMLFormat::printResult(const ResourceValueCache& resourceValueCache, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const size_t multiplicity) {
    if (m_isAskQuery) {
        if (m_firstResult)
            m_outputStream << "    <boolean>false</boolean>\n";
    }
    else {
        std::string lexicalForm;
        DatatypeID datatypeID;
        for (size_t index = 0; index < multiplicity; ++index) {
            m_outputStream << " <result>\n";
            auto variableIterator = m_answerTerms.begin();
            for (auto iterator = argumentIndexes.begin(); iterator != argumentIndexes.end(); ++iterator) {
                m_outputStream << "  <binding name=\"";
                write(to_pointer_cast<Variable>(*variableIterator)->getName());
                m_outputStream << "\">";
                if (resourceValueCache.getResource(argumentsBuffer[*iterator], lexicalForm, datatypeID)) {
                    switch (datatypeID) {
                    case D_IRI_REFERENCE:
                        m_outputStream << "<uri>";
                        write(lexicalForm);
                        m_outputStream << "</uri>";
                        break;
                    case D_BLANK_NODE:
                        m_outputStream << "<bnode>";
                        write(lexicalForm);
                        m_outputStream << "</bnode>";
                        break;
                    case D_XSD_STRING:
                        m_outputStream << "<literal>";
                        write(lexicalForm);
                        m_outputStream << "</literal>";
                        break;
                    case D_RDF_PLAIN_LITERAL:
                        {
                            const std::string::const_iterator atPosition = lexicalForm.begin() + lexicalForm.find_last_of('@');
                            m_outputStream << "<literal xml:lang=\"";
                            write(atPosition + 1, lexicalForm.end());
                            m_outputStream << "\">";
                            write(lexicalForm.begin(), atPosition);
                            m_outputStream << "</literal>";
                        }
                        break;
                    default:
                        {
                            const std::string& datatypeIRI = Dictionary::getDatatypeIRI(datatypeID);
                            m_outputStream << "<literal datatype=\"";
                            write(datatypeIRI);
                            m_outputStream << "\">";
                            write(lexicalForm);
                            m_outputStream << "</literal>";
                        }
                        break;
                    }
                }
                else {
                    std::ostringstream message;
                    message << "Resource ID " << argumentsBuffer[*iterator] << " cannot be resolved.";
                    throw RDF_STORE_EXCEPTION(message.str());
                }
                m_outputStream << "</binding>\n";
                ++variableIterator;
            }
            m_outputStream << " </result>\n";
        }
    }
    m_firstResult = false;
}

void XMLFormat::printEpilogue() {
    if (m_isAskQuery) {
        if (m_firstResult)
            m_outputStream << "    <boolean>false</boolean>\n";
    }
    else
        m_outputStream << "</results>\n";
    m_outputStream << "</sparql>\n";
    m_outputStream.flush();
}

// JSONFormat

class JSONFormat : public VariableAnswersOnlyFormat {

protected:

    bool m_firstResult;

public:

    JSONFormat(OutputStream& outputStream, const bool isAskQuery, const std::vector<Term>& answerTerms) :
        VariableAnswersOnlyFormat(outputStream, isAskQuery, answerTerms),
        m_firstResult(true)
    {
    }

    void write(std::string::const_iterator start, std::string::const_iterator end);

    void write(const std::string& string);

    virtual void printPrologue();

    virtual void printResult(const ResourceValueCache& resourceValueCache, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const size_t multiplicity);

    virtual void printEpilogue();

};

always_inline void JSONFormat::write(std::string::const_iterator start, std::string::const_iterator end) {
    m_outputStream << '\"';
    for (; start != end; ++start) {
        const char c = *start;
        if ((0 <= static_cast<uint8_t>(c) && static_cast<uint8_t>(c) <= 0x001Fu) || (0x007Fu <= static_cast<uint8_t>(c) && static_cast<uint8_t>(c) <= 0x009Fu)) {
            switch (c) {
            case '\"':
            case '\\':
            case '\b':
            case '\f':
            case '\n':
            case '\r':
            case '\t':
                m_outputStream << '\\' << c;
                break;
            default:
                m_outputStream << "\\u00" << HEX[static_cast<uint8_t>(c) >> 4] << HEX[static_cast<uint8_t>(c) & 0x0Fu];
                break;
            }
        }
        else
            m_outputStream << c;
    }
    m_outputStream << '\"';
}

always_inline void JSONFormat::write(const std::string& string) {
    write(string.begin(), string.end());
}

void JSONFormat::printPrologue() {
    m_outputStream <<
        "{\n"
        "  \"head\": { ";
    if (!m_answerTerms.empty()) {
        m_outputStream << "\"vars\": [ ";
        bool first = true;
        for (auto iterator = m_answerTerms.begin(); iterator != m_answerTerms.end(); ++iterator) {
            if (first)
                first = false;
            else
                m_outputStream << ", ";
            write(to_pointer_cast<Variable>(*iterator)->getName());
        }
        m_outputStream << " ]";
    }
    m_outputStream << " },\n";
    if (!m_isAskQuery) {
        m_outputStream <<
            "  \"results\": {\n"
            "    \"bindings\": [";
    }
}

void JSONFormat::printResult(const ResourceValueCache& resourceValueCache, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const size_t multiplicity) {
    if (m_isAskQuery) {
        if (m_firstResult)
            m_outputStream << "    \"boolean\": true";
    }
    else {
        std::string lexicalForm;
        DatatypeID datatypeID;
        for (size_t index = 0; index < multiplicity; ++index) {
            if (!m_firstResult)
                m_outputStream << " ,";
            m_outputStream << "\n      {";
            auto variableIterator = m_answerTerms.begin();
            for (auto iterator = argumentIndexes.begin(); iterator != argumentIndexes.end(); ++iterator) {
                if (iterator != argumentIndexes.begin())
                    m_outputStream << " ,";
                m_outputStream << "\n        ";
                write(to_pointer_cast<Variable>(*variableIterator)->getName());
                m_outputStream << ": { ";
                if (resourceValueCache.getResource(argumentsBuffer[*iterator], lexicalForm, datatypeID)) {
                    switch (datatypeID) {
                    case D_IRI_REFERENCE:
                        m_outputStream << "\"type\" : \"uri\", \"value\" : ";
                        write(lexicalForm);
                        break;
                    case D_BLANK_NODE:
                        m_outputStream << "\"type\" : \"bnode\", \"value\" : ";
                        write(lexicalForm);
                        break;
                    case D_XSD_STRING:
                        m_outputStream << "\"type\" : \"literal\", \"value\" : ";
                        write(lexicalForm);
                        break;
                    case D_RDF_PLAIN_LITERAL:
                        {
                            const std::string::const_iterator atPosition = lexicalForm.begin() + lexicalForm.find_last_of('@');
                            m_outputStream << "\"type\" : \"literal\", \"value\" : ";
                            write(lexicalForm.begin(), atPosition);
                            m_outputStream << ", \"xml:lang\" : ";
                            write(atPosition + 1, lexicalForm.end());
                        }
                        break;
                    default:
                        {
                            const std::string& datatypeIRI = Dictionary::getDatatypeIRI(datatypeID);
                            m_outputStream << "\"type\" : \"literal\", \"value\" : ";
                            write(lexicalForm);
                            m_outputStream << ", \"datatype\" : ";
                            write(datatypeIRI.begin(), datatypeIRI.end());
                        }
                        break;
                    }
                }
                else {
                    std::ostringstream message;
                    message << "Resource ID " << argumentsBuffer[*iterator] << " cannot be resolved.";
                    throw RDF_STORE_EXCEPTION(message.str());
                }
                m_outputStream << " }";
                ++variableIterator;
            }
            m_outputStream << "\n      }";
        }
    }
    m_firstResult = false;

}

void JSONFormat::printEpilogue() {
    if (m_isAskQuery) {
        if (m_firstResult)
            m_outputStream << "    \"boolean\": false";
    }
    else {
        m_outputStream <<
            "\n    ]\n"
            "  }";
    }
    m_outputStream << "\n}\n";
    m_outputStream.flush();
}

// CSVFormat

class CSVFormat : public QueryAnswerFormat {

protected:

    bool m_firstResult;

public:

    CSVFormat(OutputStream& outputStream, const bool isAskQuery, const std::vector<Term>& answerTerms) :
        QueryAnswerFormat(outputStream, isAskQuery, answerTerms),
        m_firstResult(true)
    {
    }

    void write(std::string::const_iterator start, std::string::const_iterator end);

    void write(const std::string& string);

    virtual void printPrologue();

    virtual void printResult(const ResourceValueCache& resourceValueCache, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const size_t multiplicity);

    virtual void printEpilogue();
    
};

always_inline void CSVFormat::write(std::string::const_iterator start, std::string::const_iterator end) {
    bool useQuote = false;
    for (std::string::const_iterator current = start; current != end; ++current) {
        const char c = *current;
        if (c == '\n' || c == '\r' || c == '\"' || c == ',') {
            useQuote = true;
            m_outputStream << '\"';
            break;
        }
    }
    for (; start != end; ++start) {
        const char c = *start;
        if (c < ' ')
            m_outputStream << ' ';
        else {
            if (c == '\"')
                m_outputStream << '\"';
            m_outputStream << c;
        }
    }
    if (useQuote)
        m_outputStream << '\"';
}

always_inline void CSVFormat::write(const std::string& string) {
    write(string.begin(), string.end());
}

void CSVFormat::printPrologue() {
    for (auto iterator = m_answerTerms.begin(); iterator != m_answerTerms.end(); ++iterator) {
        if (iterator != m_answerTerms.begin())
            m_outputStream << ',';
        write(to_pointer_cast<Variable>(*iterator)->getName());
    }
    m_outputStream << "\r\n";
}

void CSVFormat::printResult(const ResourceValueCache& resourceValueCache, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const size_t multiplicity) {
    if (m_isAskQuery) {
        if (m_firstResult)
            m_outputStream << "\r\n";
    }
    else {
        std::string lexicalForm;
        DatatypeID datatypeID;
        for (size_t index = 0; index < multiplicity; ++index) {
            for (auto iterator = argumentIndexes.begin(); iterator != argumentIndexes.end(); ++iterator) {
                if (iterator != argumentIndexes.begin())
                    m_outputStream << ',';
                if (resourceValueCache.getResource(argumentsBuffer[*iterator], lexicalForm, datatypeID)) {
                    switch (datatypeID) {
                    case D_INVALID_DATATYPE_ID:
                        break;
                    case D_BLANK_NODE:
                        {
                            std::string nodeName("_:");
                            nodeName.append(lexicalForm);
                            write(nodeName);
                        }
                        break;
                    case D_RDF_PLAIN_LITERAL:
                        write(lexicalForm.begin(), lexicalForm.begin() + lexicalForm.find_last_of('@'));
                        break;
                    default:
                        write(lexicalForm);
                        break;
                    }
                }
                else {
                    std::ostringstream message;
                    message << "Resource ID " << argumentsBuffer[*iterator] << " cannot be resolved.";
                    throw RDF_STORE_EXCEPTION(message.str());
                }
            }
            m_outputStream << "\r\n";
        }
    }
    m_firstResult = false;
}

void CSVFormat::printEpilogue() {
    m_outputStream.flush();
}

// CSVSimplifiedFormat

class CSVSimplifiedFormat : public QueryAnswerFormat {

protected:

    const Prefixes& m_prefixes;
    bool m_firstResult;

public:

    CSVSimplifiedFormat(OutputStream& outputStream, const bool isAskQuery, const std::vector<Term>& answerTerms, const Prefixes& prefixes) :
        QueryAnswerFormat(outputStream, isAskQuery, answerTerms),
        m_prefixes(prefixes),
        m_firstResult(true)
    {
    }

    void write(std::string::const_iterator start, std::string::const_iterator end);

    void write(const std::string& string);

    virtual void printPrologue();

    virtual void printResult(const ResourceValueCache& resourceValueCache, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const size_t multiplicity);

    virtual void printEpilogue();

};

always_inline void CSVSimplifiedFormat::write(std::string::const_iterator start, std::string::const_iterator end) {
    for (; start != end; ++start) {
        const char c = *start;
        if (c < ' ' || c == '"' || c == ',')
            m_outputStream << ' ';
        else
            m_outputStream << c;
    }
}

always_inline void CSVSimplifiedFormat::write(const std::string& string) {
    write(string.begin(), string.end());
}

void CSVSimplifiedFormat::printPrologue() {
    for (auto iterator = m_answerTerms.begin(); iterator != m_answerTerms.end(); ++iterator) {
        if (iterator != m_answerTerms.begin())
            m_outputStream << ',';
        write(to_pointer_cast<Variable>(*iterator)->getName());
    }
    m_outputStream << "\r\n";
}

void CSVSimplifiedFormat::printResult(const ResourceValueCache& resourceValueCache, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const size_t multiplicity) {
    if (m_isAskQuery) {
        if (m_firstResult)
            m_outputStream << "\r\n";
    }
    else {
        std::string lexicalForm;
        DatatypeID datatypeID;
        for (size_t index = 0; index < multiplicity; ++index) {
            for (auto iterator = argumentIndexes.begin(); iterator != argumentIndexes.end(); ++iterator) {
                if (iterator != argumentIndexes.begin())
                    m_outputStream << ',';
                if (resourceValueCache.getResource(argumentsBuffer[*iterator], lexicalForm, datatypeID)) {
                    switch (datatypeID) {
                        case D_INVALID_DATATYPE_ID:
                            break;
                        case D_IRI_REFERENCE:
                            write(m_prefixes.getLocalName(lexicalForm), lexicalForm.end());
                            break;
                        case D_BLANK_NODE:
                        {
                            std::string nodeName("_:");
                            nodeName.append(lexicalForm);
                            write(nodeName);
                        }
                            break;
                        case D_RDF_PLAIN_LITERAL:
                            write(lexicalForm.begin(), lexicalForm.begin() + lexicalForm.find_last_of('@'));
                            break;
                        default:
                            write(lexicalForm);
                            break;
                    }
                }
                else {
                    std::ostringstream message;
                    message << "Resource ID " << argumentsBuffer[*iterator] << " cannot be resolved.";
                    throw RDF_STORE_EXCEPTION(message.str());
                }
            }
            m_outputStream << "\r\n";
        }
    }
    m_firstResult = false;
}

void CSVSimplifiedFormat::printEpilogue() {
    m_outputStream.flush();
}

// TSVFormat

class TSVFormat : public QueryAnswerFormat {

protected:

    bool m_firstResult;

public:

    TSVFormat(OutputStream& outputStream, const bool isAskQuery, const std::vector<Term>& answerTerms) :
        QueryAnswerFormat(outputStream, isAskQuery, answerTerms),
        m_firstResult(true)
    {
    }

    void write(std::string::const_iterator start, std::string::const_iterator end);

    void write(const std::string& string);

    void writeURI(const std::string& uri);

    virtual void printPrologue();

    virtual void printResult(const ResourceValueCache& resourceValueCache, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const size_t multiplicity);

    virtual void printEpilogue();
    
};

void TSVFormat::write(std::string::const_iterator start, std::string::const_iterator end) {
    for (; start != end; ++start) {
        const char c = *start;
        switch (c) {
        case '\t':
            m_outputStream << "\\t";
            break;
        case '\b':
            m_outputStream << "\\b";
            break;
        case '\n':
            m_outputStream << "\\n";
            break;
        case '\r':
            m_outputStream << "\\r";
            break;
        case '\f':
            m_outputStream << "\\f";
            break;
        case '\"':
            m_outputStream << "\\\"";
            break;
        case '\\':
            m_outputStream << "\\\\";
        default:
            m_outputStream << c;
            break;
        }
    }
}

always_inline void TSVFormat::write(const std::string& string) {
    write(string.begin(), string.end());
}

always_inline void TSVFormat::writeURI(const std::string& uri) {
    for (auto start = uri.begin(); start != uri.end(); ++start) {
        const char c = *start;
        if ((static_cast<uint8_t>(c) & 0x80u) != 0 || TurtleSyntax::is_IRIREF(static_cast<CodePoint>(c)))
            m_outputStream << c;
        else
            m_outputStream << "\\u00" << HEX[static_cast<uint8_t>(c) >> 4] << HEX[static_cast<uint8_t>(c) & 0x0Fu];
    }
}

void TSVFormat::printPrologue() {
    for (auto iterator = m_answerTerms.begin(); iterator != m_answerTerms.end(); ++iterator) {
        if (iterator != m_answerTerms.begin())
            m_outputStream << '\t';
        m_outputStream << '?';
        write(to_pointer_cast<Variable>(*iterator)->getName());
    }
    m_outputStream << "\n";
}

void TSVFormat::printResult(const ResourceValueCache& resourceValueCache, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const size_t multiplicity) {
    if (m_isAskQuery) {
        if (m_firstResult)
            m_outputStream << "\n";
    }
    else {
        std::string lexicalForm;
        DatatypeID datatypeID;
        for (size_t index = 0; index < multiplicity; ++index) {
            for (auto iterator = argumentIndexes.begin(); iterator != argumentIndexes.end(); ++iterator) {
                if (iterator != argumentIndexes.begin())
                    m_outputStream << '\t';
                if (resourceValueCache.getResource(argumentsBuffer[*iterator], lexicalForm, datatypeID)) {
                    switch (datatypeID) {
                    case D_INVALID_DATATYPE_ID:
                        break;
                    case D_IRI_REFERENCE:
                        m_outputStream << "<";
                        writeURI(lexicalForm);
                        m_outputStream << ">";
                        break;
                    case D_BLANK_NODE:
                        m_outputStream << "_:";
                        write(lexicalForm);
                        break;
                    case D_XSD_STRING:
                        m_outputStream << '\"';
                        write(lexicalForm);
                        m_outputStream << '\"';
                        break;
                    case D_RDF_PLAIN_LITERAL:
                        {
                            const std::string::const_iterator atPosition = lexicalForm.begin() + lexicalForm.find_last_of('@');
                            m_outputStream << '\"';
                            write(lexicalForm.begin(), atPosition);
                            m_outputStream << '\"';
                            write(atPosition, lexicalForm.end());
                        }
                        break;
                    case D_XSD_INTEGER:
                    case D_XSD_BOOLEAN:
                        m_outputStream << lexicalForm;
                        break;
                    default:
                        {
                            const std::string& datatypeIRI = Dictionary::getDatatypeIRI(datatypeID);
                            m_outputStream << '\"';
                            write(lexicalForm);
                            m_outputStream << "\"^^<";
                            writeURI(datatypeIRI);
                            m_outputStream << '>';
                        }
                        break;
                    }
                }
                else {
                    std::ostringstream message;
                    message << "Resource ID " << argumentsBuffer[*iterator] << " cannot be resolved.";
                    throw RDF_STORE_EXCEPTION(message.str());
                }
            }
            m_outputStream << "\n";
        }
    }
    m_firstResult = true;
}

void TSVFormat::printEpilogue() {
    m_outputStream.flush();
}

// TurtleFormat

class TurtleFormat : public QueryAnswerFormat {

protected:

    const Prefixes& m_prefixes;
    const bool m_printPrefixes;
    ResourceValue m_resourceValue;
    std::string m_literalText;

public:

    TurtleFormat(OutputStream& outputStream, const bool isAskQuery, const std::vector<Term>& answerTerms, const Prefixes& prefixes, const bool printPrefixes) :
        QueryAnswerFormat(outputStream, isAskQuery, answerTerms),
        m_prefixes(prefixes),
        m_printPrefixes(printPrefixes),
        m_resourceValue(),
        m_literalText()
    {
    }

    virtual void printPrologue();

    virtual void printResult(const ResourceValueCache& resourceValueCache, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const size_t multiplicity);

    virtual void printEpilogue();

};

void TurtleFormat::printPrologue() {
    if (m_printPrefixes) {
        const std::map<std::string, std::string>& prefixIRIsByPrefixNames = m_prefixes.getPrefixIRIsByPrefixNames();
        for (auto prefix : prefixIRIsByPrefixNames)
            m_outputStream << "@prefix " << prefix.first << " <" << prefix.second << "> .\n";
        m_outputStream << "\n";
    }
}

void TurtleFormat::printResult(const ResourceValueCache& resourceValueCache, const std::vector<ResourceID>& argumentsBuffer, const std::vector<ArgumentIndex>& argumentIndexes, const size_t multiplicity) {
    for (auto iterator = argumentIndexes.begin(); iterator != argumentIndexes.end(); ++iterator) {
        const ResourceID resourceID = argumentsBuffer[*iterator];
        if (resourceID == INVALID_RESOURCE_ID)
            m_outputStream << "UNDEF";
        else if (resourceValueCache.getResource(resourceID, m_resourceValue)) {
            Dictionary::toTurtleLiteral(m_resourceValue, m_prefixes, m_literalText);
            m_outputStream << m_literalText;
        }
        else {
            std::ostringstream message;
            message << "Resource ID " << argumentsBuffer[*iterator] << " cannot be resolved.";
            throw RDF_STORE_EXCEPTION(message.str());
        }
        m_outputStream << ' ';
    }
    m_outputStream << '.';
    if (multiplicity > 1)
        m_outputStream << " # " << multiplicity;
    m_outputStream << '\n';
}

void TurtleFormat::printEpilogue() {
    m_outputStream.flush();
}

// QueryAnswerFormat

QueryAnswerFormat::QueryAnswerFormat(OutputStream& outputStream, const bool isAskQuery, const std::vector<Term>& answerTerms) :
    m_outputStream(outputStream),
    m_isAskQuery(isAskQuery),
    m_answerTerms(answerTerms)
{
}

QueryAnswerFormat::~QueryAnswerFormat() {
}

std::unique_ptr<QueryAnswerFormat> newQueryAnswerFormat(const std::string& queryAnswerFormatName, OutputStream& outputStream, const bool isAskQuery, const std::vector<Term>& answerTerms, const Prefixes& prefixes) {
    std::string queryAnswerFormatNameLowercase(queryAnswerFormatName);
    ::toLowerCase(queryAnswerFormatNameLowercase);
    if (queryAnswerFormatNameLowercase == "xml")
        return std::unique_ptr<QueryAnswerFormat>(new XMLFormat(outputStream, isAskQuery, answerTerms));
    else if (queryAnswerFormatNameLowercase == "json")
        return std::unique_ptr<QueryAnswerFormat>(new JSONFormat(outputStream, isAskQuery, answerTerms));
    else if (queryAnswerFormatNameLowercase == "csv")
        return std::unique_ptr<QueryAnswerFormat>(new CSVFormat(outputStream, isAskQuery, answerTerms));
    else if (queryAnswerFormatNameLowercase == "csvsimplified")
        return std::unique_ptr<QueryAnswerFormat>(new CSVSimplifiedFormat(outputStream, isAskQuery, answerTerms, prefixes));
    else if (queryAnswerFormatNameLowercase == "tsv")
        return std::unique_ptr<QueryAnswerFormat>(new TSVFormat(outputStream, isAskQuery, answerTerms));
    else if (queryAnswerFormatNameLowercase == "turtle")
        return std::unique_ptr<QueryAnswerFormat>(new TurtleFormat(outputStream, isAskQuery, answerTerms, prefixes, true));
    else if (queryAnswerFormatNameLowercase == "turtlenoprefixes")
        return std::unique_ptr<QueryAnswerFormat>(new TurtleFormat(outputStream, isAskQuery, answerTerms, prefixes, false));
    else {
        std::ostringstream message;
        message << "Query answer format '" << queryAnswerFormatName << "' is not supported.";
        throw RDF_STORE_EXCEPTION(message.str());
    }
}
