// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../../RDFStoreException.h"
#include "../../dictionary/Dictionary.h"
#include "../../equality/EqualityManager.h"
#include "../../storage/DataStore.h"
#include "../../storage/TupleTable.h"
#include "../../util/Prefixes.h"
#include "../../util/OutputStream.h"
#include "../../util/Vocabulary.h"
#include "../InputOutput.h"
#include "../InputConsumer.h"
#include "TurtleParser.h"

// TurtleFormatExporter

class TurtleFormatExporter : public InputConsumer {

protected:

    enum State { AT_BEGINNING, AFTER_PREFIX, IN_TRIPLE, AFTER_TRIPLE };

    Prefixes& m_prefixes;
    OutputStream& m_outputStream;
    State m_state;
    ResourceText m_currentSubject;
    ResourceText m_currentPredicate;
    size_t m_currentColumn;

    static void analyzeString(std::string::const_iterator start, std::string::const_iterator end, bool& containsSingleQuote, bool& containsDoubleQuote, bool& containsLineBreaks);

    bool canContinueLine();

    void print(const char c);

    void separateResources();

    void writeString(std::string::const_iterator start, std::string::const_iterator end);

    void printResource(const ResourceText& resourceText);

    void finishCurrentTriple();

public:

    TurtleFormatExporter(Prefixes& prefixes, OutputStream& outputStream);

    virtual ~TurtleFormatExporter();

    virtual void start();

    virtual void reportError(const size_t line, const size_t column, const char* const errorDescription);

    virtual void consumePrefixMapping(const std::string& prefixName, const std::string& prefixIRI);

    virtual void consumeTriple(const size_t line, const size_t column, const ResourceText& subject, const ResourceText& predicate, const ResourceText& object);

    virtual void consumeRule(const size_t line, const size_t column, const Rule& rule);

    virtual void finish();

};

always_inline void TurtleFormatExporter::analyzeString(std::string::const_iterator start, std::string::const_iterator end, bool& containsSingleQuote, bool& containsDoubleQuote, bool& containsLineBreaks) {
    containsSingleQuote = false;
    containsDoubleQuote = false;
    containsLineBreaks = false;
    while (start != end) {
        switch (*start) {
        case '\'':
            containsSingleQuote = true;
            break;
        case '\"':
            containsDoubleQuote = true;
            break;
        case '\r':
        case '\n':
            containsLineBreaks = true;
            break;
        }
        ++start;
    }
}

always_inline bool TurtleFormatExporter::canContinueLine() {
    return m_currentColumn < 1000;
}

always_inline void TurtleFormatExporter::print(const char c) {
    m_outputStream << c;
    ++m_currentColumn;
}

always_inline void TurtleFormatExporter::separateResources() {
    if (canContinueLine())
        print(' ');
    else {
        m_outputStream << "\n        ";
        m_currentColumn = 8;
    }
}

always_inline void TurtleFormatExporter::writeString(std::string::const_iterator start, std::string::const_iterator end) {
    // We do not need to parse the string as UTF-8 because all characters that require attention are ASCII;
    // hence, we can write all bytes of the string directly, without any problems.
    bool containsSingleQuote;
    bool containsDoubleQuote;
    bool containsLineBreaks;
    analyzeString(start, end, containsSingleQuote, containsDoubleQuote, containsLineBreaks);
    char quoteType = (containsDoubleQuote && !containsSingleQuote ? '\'' : '\"');
    print(quoteType);
    if (containsLineBreaks) {
        print(quoteType);
        print(quoteType);
    }
    while (start != end) {
        switch (*start) {
        case '\\':
            m_outputStream << "\\\\";
            m_currentColumn += 2;
            break;
        case '\t':
            m_outputStream << "\\t";
            m_currentColumn += 2;
            break;
        case '\b':
            m_outputStream << "\\b";
            m_currentColumn += 2;
            break;
        case '\r':
        case '\n':
            m_outputStream << *start;
            m_currentColumn = 1;
            break;
        case '\f':
            m_outputStream << "\\f";
            m_currentColumn += 2;
            break;
        case '\'':
        case '\"':
            if (*start == quoteType)
                print('\\');
            // deliberate fall-through!
        default:
            print(*start);
            break;
        }
        ++start;
    }
    print(quoteType);
    if (containsLineBreaks) {
        print(quoteType);
        print(quoteType);
    }
}

always_inline void TurtleFormatExporter::printResource(const ResourceText& resourceText) {
    switch (resourceText.m_resourceType) {
    case UNDEFINED_RESOURCE:
        m_outputStream << "UNDEF";
        m_currentColumn += 5;
        break;
    case IRI_REFERENCE:
        {
            std::string iriReference = m_prefixes.encodeIRI(resourceText.m_lexicalForm);
            m_outputStream << iriReference;
            m_currentColumn += iriReference.length();
        }
        break;
    case BLANK_NODE:
        m_outputStream << "_:" << resourceText.m_lexicalForm;
        m_currentColumn += 2 + resourceText.m_lexicalForm.length();
        break;
    case LITERAL:
        if (resourceText.m_datatypeIRI == XSD_STRING)
            writeString(resourceText.m_lexicalForm.begin(), resourceText.m_lexicalForm.end());
        else if (resourceText.m_datatypeIRI == RDF_PLAIN_LITERAL) {
            const size_t atPosition = resourceText.m_lexicalForm.find_last_of('@');
            if (atPosition == std::string::npos)
                writeString(resourceText.m_lexicalForm.begin(), resourceText.m_lexicalForm.end());
            else {
                std::string::const_iterator atIterator = resourceText.m_lexicalForm.begin() + atPosition;
                writeString(resourceText.m_lexicalForm.begin(), atIterator);
                while (atIterator != resourceText.m_lexicalForm.end()) {
                    m_outputStream << *atIterator;
                    ++atIterator;
                    ++m_currentColumn;
                }
            }
        }
        else if (resourceText.m_datatypeIRI == XSD_BOOLEAN) {
            if (resourceText.m_lexicalForm == "false" || resourceText.m_lexicalForm == "0") {
                m_outputStream << "false";
                m_currentColumn += 5;
            }
            else {
                m_outputStream << "true";
                m_currentColumn += 4;
            }
        }
        else if (resourceText.m_datatypeIRI == XSD_INTEGER) {
            m_outputStream << resourceText.m_lexicalForm;
            m_currentColumn += resourceText.m_lexicalForm.size();
        }
        else {
            writeString(resourceText.m_lexicalForm.begin(), resourceText.m_lexicalForm.end());
            std::string datatypeIRI = m_prefixes.encodeIRI(resourceText.m_datatypeIRI);
            m_outputStream << "^^" << datatypeIRI;
            m_currentColumn += 2 + datatypeIRI.length();
        }
        break;
    }
}

always_inline void TurtleFormatExporter::finishCurrentTriple() {
    if (m_state == IN_TRIPLE) {
        m_outputStream << " .\n";
        m_state = AFTER_TRIPLE;
        m_currentColumn = 1;
    }
}

TurtleFormatExporter::TurtleFormatExporter(Prefixes& prefixes, OutputStream& outputStream) : m_prefixes(prefixes), m_outputStream(outputStream), m_state(AT_BEGINNING), m_currentSubject(), m_currentPredicate(), m_currentColumn(0) {
}

TurtleFormatExporter::~TurtleFormatExporter() {
}

void TurtleFormatExporter::start() {
    const std::map<std::string, std::string>& prefixIRIsByPrefixNames = m_prefixes.getPrefixIRIsByPrefixNames();
    if (!prefixIRIsByPrefixNames.empty()) {
        for (std::map<std::string, std::string>::const_iterator iterator = prefixIRIsByPrefixNames.begin(); iterator != prefixIRIsByPrefixNames.end(); ++iterator)
            m_outputStream << "@prefix " << iterator->first << " <" << iterator->second << "> .\n";
        m_state = AFTER_PREFIX;
    }
    m_currentColumn = 1;
}

void TurtleFormatExporter::reportError(const size_t line, const size_t column, const char* const errorDescription) {
}

void TurtleFormatExporter::consumePrefixMapping(const std::string& prefixName, const std::string& prefixIRI) {
    finishCurrentTriple();
    if (m_state == AFTER_TRIPLE)
        m_outputStream << '\n';
    m_outputStream << "@prefix " << prefixName << " <" << prefixIRI << "> .\n";
    m_prefixes.declarePrefix(prefixName, prefixIRI);
    m_state = AFTER_PREFIX;
    m_currentColumn = 1;
}

void TurtleFormatExporter::consumeTriple(const size_t line, const size_t column, const ResourceText& subject, const ResourceText& predicate, const ResourceText& object) {
    if (m_state == IN_TRIPLE) {
        if (m_currentSubject == subject) {
            if (m_currentPredicate == predicate) {
                if (canContinueLine()) {
                    m_outputStream << " , ";
                    m_currentColumn += 3;
                }
                else {
                    m_outputStream << " ,\n    ";
                    m_currentColumn = 4;
                }
            }
            else {
                m_outputStream << " ;\n    ";
                m_currentColumn = 4;
                printResource(predicate);
                separateResources();
                m_currentPredicate = predicate;
            }
            printResource(object);
            return;
        }
        else
            finishCurrentTriple();
    }
    if (m_state == AFTER_PREFIX || m_state == AFTER_TRIPLE) {
        m_outputStream << '\n';
        m_currentColumn = 1;
    }
    printResource(subject);
    separateResources();
    printResource(predicate);
    separateResources();
    printResource(object);
    m_state = IN_TRIPLE;
    m_currentSubject = subject;
    m_currentPredicate = predicate;
}

void TurtleFormatExporter::consumeRule(const size_t line, const size_t column, const Rule& rule) {
}

void TurtleFormatExporter::finish() {
    finishCurrentTriple();
}

// NTriplesFormatExporter

class NTriplesFormatExporter : public InputConsumer {

protected:

    OutputStream& m_outputStream;

    void writeString(std::string::const_iterator start, std::string::const_iterator end);

    void printResource(const ResourceText& resourceText);

public:

    NTriplesFormatExporter(OutputStream& outputStream);

    virtual ~NTriplesFormatExporter();

    virtual void start();

    virtual void reportError(const size_t line, const size_t column, const char* const errorDescription);

    virtual void consumePrefixMapping(const std::string& prefixName, const std::string& prefixIRI);

    virtual void consumeTriple(const size_t line, const size_t column, const ResourceText& subject, const ResourceText& predicate, const ResourceText& object);

    virtual void consumeRule(const size_t line, const size_t column, const Rule& rule);
    
    virtual void finish();
    
};

void NTriplesFormatExporter::writeString(std::string::const_iterator start, std::string::const_iterator end) {
    // We do not need to parse the string as UTF-8 because all characters that require attention are ASCII;
    // hence, we can write all bytes of the string directly, without any problems.
    m_outputStream << '"';
    while (start != end) {
        switch (*start) {
        case '\\':
            m_outputStream << "\\\\";
            break;
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
        case '\f':
            m_outputStream << "\\f";
            break;
        case '\'':
        case '\"':
            m_outputStream << '\\';
            // deliberate fall-through!
        default:
            m_outputStream << *start;
            break;
        }
        ++start;
    }
    m_outputStream << '"';
}

void NTriplesFormatExporter::printResource(const ResourceText& resourceText) {
    switch (resourceText.m_resourceType) {
    case UNDEFINED_RESOURCE:
        m_outputStream << "UNDEF";
        break;
    case IRI_REFERENCE:
        m_outputStream << '<' << resourceText.m_lexicalForm << '>';
        break;
    case BLANK_NODE:
        m_outputStream << "_:" << resourceText.m_lexicalForm;
        break;
    case LITERAL:
        if (resourceText.m_datatypeIRI == XSD_STRING)
            writeString(resourceText.m_lexicalForm.begin(), resourceText.m_lexicalForm.end());
        else if (resourceText.m_datatypeIRI == RDF_PLAIN_LITERAL) {
            const size_t atPosition = resourceText.m_lexicalForm.find_last_of('@');
            if (atPosition == std::string::npos)
                writeString(resourceText.m_lexicalForm.begin(), resourceText.m_lexicalForm.end());
            else {
                std::string::const_iterator atIterator = resourceText.m_lexicalForm.begin() + atPosition;
                writeString(resourceText.m_lexicalForm.begin(), atIterator);
                while (atIterator != resourceText.m_lexicalForm.end()) {
                    m_outputStream << *atIterator;
                    ++atIterator;
                }
            }
        }
        else {
            writeString(resourceText.m_lexicalForm.begin(), resourceText.m_lexicalForm.end());
            m_outputStream << "^^<" << resourceText.m_datatypeIRI << '>';
        }
        break;
    }
}

NTriplesFormatExporter::NTriplesFormatExporter(OutputStream& outputStream) : m_outputStream(outputStream) {
}

NTriplesFormatExporter::~NTriplesFormatExporter() {
}

void NTriplesFormatExporter::start() {
}

void NTriplesFormatExporter::reportError(const size_t line, const size_t column, const char* const errorDescription) {
}

void NTriplesFormatExporter::consumePrefixMapping(const std::string& prefixName, const std::string& prefixIRI) {
}

void NTriplesFormatExporter::consumeTriple(const size_t line, const size_t column, const ResourceText& subject, const ResourceText& predicate, const ResourceText& object) {
    printResource(subject);
    m_outputStream << ' ';
    printResource(predicate);
    m_outputStream << ' ';
    printResource(object);
    m_outputStream << " .\n";
}

void NTriplesFormatExporter::consumeRule(const size_t line, const size_t column, const Rule& rule) {
}

void NTriplesFormatExporter::finish() {
}

// TurtleFormatHandler

class TurtleFormatHandler : public FormatHandler {

protected:

    static TurtleFormatHandler s_turtleFormatHandler;

    TurtleFormatHandler();

public:

    virtual bool storesFacts() const;

    virtual bool storesRules() const;

    virtual void load(InputSource& inputSource, Prefixes& prefixes, LogicFactory& logicFactory, InputConsumer& inputConsumer, std::string& formatName) const;

    virtual void save(DataStore& dataStore, Prefixes& prefixes, OutputStream& outputStream, const std::string& formatName) const;

    virtual std::unique_ptr<InputConsumer> newExporter(Prefixes& prefixes, OutputStream& outputStream, const std::string& formatName) const;

};

TurtleFormatHandler TurtleFormatHandler::s_turtleFormatHandler;

TurtleFormatHandler::TurtleFormatHandler() : FormatHandler(2, "Turtle handler", FormatNames{ "Turtle", "TriG", "N-Triples", "N-Quads" }) {
}

bool TurtleFormatHandler::storesFacts() const {
    return true;
}

bool TurtleFormatHandler::storesRules() const {
    return false;
}

void TurtleFormatHandler::load(InputSource& inputSource, Prefixes& prefixes, LogicFactory& logicFactory, InputConsumer& inputConsumer, std::string& formatName) const {
    TurtleParser turtleParser(prefixes);
    bool hasTurtle;
    bool hasTriG;
    bool hasQuads;
    turtleParser.parse(inputSource, inputConsumer, hasTurtle, hasTriG, hasQuads);
    if (hasTriG)
        formatName = "TriG";
    else if (hasQuads)
        formatName = "N-Quads";
    else if (hasTurtle)
        formatName = "Turtle";
    else
        formatName = "N-Triples";
}

std::unique_ptr<InputConsumer> TurtleFormatHandler::newExporter(Prefixes& prefixes, OutputStream& outputStream, const std::string& formatName) const {
    if (formatName == "Turtle")
        return std::unique_ptr<InputConsumer>(new TurtleFormatExporter(prefixes, outputStream));
    else if (formatName == "N-Triples")
        return std::unique_ptr<InputConsumer>(new NTriplesFormatExporter(outputStream));
    else {
        std::ostringstream message;
        message << "RDFox does not support quads and thus cannot save data in format '" << formatName << "'.";
        throw RDF_STORE_EXCEPTION(message.str());
    }
}

void TurtleFormatHandler::save(DataStore& dataStore, Prefixes& prefixes, OutputStream& outputStream, const std::string& formatName) const {
    if (formatName == "TriG" || formatName == "N-Quads") {
        std::ostringstream message;
        message << "RDFox does not support quads and thus cannot save data in format '" << formatName << "'.";
        throw RDF_STORE_EXCEPTION(message.str());
    }
    std::vector<ResourceID> argumentsBuffer(3);
    std::vector<ArgumentIndex> argumentIndexes;
    ArgumentIndexSet allInputArguments;
    argumentIndexes.push_back(0);
    argumentIndexes.push_back(1);
    argumentIndexes.push_back(2);
    const Dictionary& dictionary = dataStore.getDictionary();
    const EqualityManager& equalityManager = dataStore.getEqualityManager();
    const TupleTable& rdfTable = dataStore.getTupleTable("internal$rdf");
    const bool usesOptimizedEquality = (dataStore.getEqualityAxiomatizationType() != EQUALITY_AXIOMATIZATION_OFF);
    ResourceText subject;
    ResourceText predicate;
    ResourceText object;
    std::unique_ptr<InputConsumer> inputConsumer;

    // Load the prefixes first
    std::unique_ptr<TupleIterator> tupleIterator = rdfTable.createTupleIterator(argumentsBuffer, argumentIndexes, allInputArguments, allInputArguments);
    if (formatName == "Turtle") {
        prefixes.declareStandardPrefixes();
        ResourceValue resourceValue;
        size_t multiplicity = tupleIterator->open();
        while (multiplicity != 0) {
            for (size_t index = 0; index <= 2; ++index)
                if (dictionary.getResource(argumentsBuffer[index], resourceValue) && resourceValue.getDatatypeID() == D_IRI_REFERENCE)
                    prefixes.createAutomaticPrefix(resourceValue.getString(), 256);
            multiplicity = tupleIterator->advance();
        }
        inputConsumer.reset(new TurtleFormatExporter(prefixes, outputStream));
    }
    else
        inputConsumer.reset(new NTriplesFormatExporter(outputStream));

    // Now write the tuples grouped by subject
    inputConsumer->start();
    const ResourceID maxResourceID = dictionary.getMaxResourceID();
    allInputArguments.add(0);
    tupleIterator = rdfTable.createTupleIterator(argumentsBuffer, argumentIndexes, allInputArguments, allInputArguments);
    size_t multiplicity;
    for (ResourceID subjectID = 1; subjectID <= maxResourceID; ++subjectID) {
        argumentsBuffer[0] = subjectID;
        multiplicity = tupleIterator->open();
        if (multiplicity != 0) {
            dictionary.getResource(subjectID, subject);
            ResourceID currentPredicateID = argumentsBuffer[1];
            dictionary.getResource(currentPredicateID, predicate);
            while (multiplicity != 0) {
                if (argumentsBuffer[1] != currentPredicateID) {
                    currentPredicateID = argumentsBuffer[1];
                    dictionary.getResource(currentPredicateID, predicate);
                }
                dictionary.getResource(argumentsBuffer[2], object);
                inputConsumer->consumeTriple(1, 1, subject, predicate, object);
                if (usesOptimizedEquality && currentPredicateID == OWL_SAME_AS_ID) {
                    ResourceID resourceID = equalityManager.getNextEqual(subjectID);
                    while (resourceID != INVALID_RESOURCE_ID) {
                        dictionary.getResource(resourceID, object);
                        inputConsumer->consumeTriple(1, 1, subject, predicate, object);
                        resourceID = equalityManager.getNextEqual(resourceID);
                    }
                }
                multiplicity = tupleIterator->advance();
            }
        }
    }
    inputConsumer->finish();
}
