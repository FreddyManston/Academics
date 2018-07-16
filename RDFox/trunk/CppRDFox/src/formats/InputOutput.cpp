// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "InputConsumer.h"
#include "InputSource.h"
#include "InputOutput.h"

// format handler registry

static std::vector<FormatHandler*>& getRegisteredFormatHandlers() {
    static std::vector<FormatHandler*> registeredFormatHandlers;
    return registeredFormatHandlers;
}

static std::unordered_map<std::string, FormatHandler*>& getRegisteredFormatHandlersByFormatName() {
    static std::unordered_map<std::string, FormatHandler*> registeredFormatHandlersByFormatName;
    return registeredFormatHandlersByFormatName;
}

// FormatHandler

always_inline static bool priorityComparator(const FormatHandler* const formatHandler1, const FormatHandler* const formatHandler2) {
    return formatHandler1->getPriority() < formatHandler2->getPriority();
}

FormatHandler::FormatHandler(const size_t priority, const std::string& name, FormatNames&& handledFormatNames) : m_priority(priority), m_name(name), m_handledFormatNames(std::move(handledFormatNames)) {
    std::vector<FormatHandler*>& registeredFormatHandlers = getRegisteredFormatHandlers();
    std::vector<FormatHandler*>::iterator location = std::lower_bound(registeredFormatHandlers.begin(), registeredFormatHandlers.end(), this, priorityComparator);
    registeredFormatHandlers.insert(location, this);
    for (std::unordered_set<std::string>::const_iterator iterator = m_handledFormatNames.begin(); iterator != m_handledFormatNames.end(); ++iterator)
        getRegisteredFormatHandlersByFormatName()[*iterator] = this;
}

FormatHandler::FormatHandler(const size_t priority, const std::string& name, const FormatNames& handledFormatNames) : m_priority(priority), m_name(name), m_handledFormatNames(handledFormatNames) {
    std::vector<FormatHandler*>& registeredFormatHandlers = getRegisteredFormatHandlers();
    std::vector<FormatHandler*>::iterator location = std::lower_bound(registeredFormatHandlers.begin(), registeredFormatHandlers.end(), this, priorityComparator);
    registeredFormatHandlers.insert(location, this);
    for (std::unordered_set<std::string>::const_iterator iterator = m_handledFormatNames.begin(); iterator != m_handledFormatNames.end(); ++iterator)
        getRegisteredFormatHandlersByFormatName()[*iterator] = this;
}

FormatHandler::~FormatHandler() {
}

// InputConsumerForwarder

class InputConsumerForwarder : public InputConsumer {

protected:

    InputConsumer& m_inputConsumer;
    bool m_hasErrors;
    bool m_hasData;

public:

    struct NextFormatException {
        size_t m_line;
        size_t m_column;
        std::string m_errorDescription;

        NextFormatException(const size_t line, const size_t column,const char* const errorDescription) : m_line(line), m_column(column), m_errorDescription(errorDescription) {
        }

    };

    InputConsumerForwarder(InputConsumer& inputConsumer) : m_inputConsumer(inputConsumer) {
    }

    always_inline bool hasErrors() const {
        return m_hasErrors;
    }

    virtual void start();

    virtual void reportError(const size_t line, const size_t column, const char* const errorDescription);

    virtual void consumeTriple(const size_t line, const size_t column, const ResourceText& subject, const ResourceText& predicate, const ResourceText& object);

    virtual void consumeRule(const size_t line, const size_t column, const Rule& rule);

    virtual void finish();

};

void InputConsumerForwarder::start() {
    m_hasErrors = m_hasData = false;
    m_inputConsumer.start();
}

void InputConsumerForwarder::reportError(const size_t line, const size_t column, const char* const errorDescription) {
    m_hasErrors = true;
    if (m_hasData)
        m_inputConsumer.reportError(line, column, errorDescription);
    else
        throw NextFormatException(line, column, errorDescription);
}

void InputConsumerForwarder::consumeTriple(const size_t line, const size_t column, const ResourceText& subject, const ResourceText& predicate, const ResourceText& object) {
    m_hasData = true;
    m_inputConsumer.consumeTriple(line, column, subject, predicate, object);
}

void InputConsumerForwarder::consumeRule(const size_t line, const size_t column, const Rule& rule) {
    m_hasData = true;
    m_inputConsumer.consumeRule(line, column, rule);
}

void InputConsumerForwarder::finish() {
    m_inputConsumer.finish();
}

// I/O functions

const FormatHandler* getFormatHandlerFor(const std::string& formatName) {
    const std::unordered_map<std::string, FormatHandler*> registeredFormatHandlersByFormatName = getRegisteredFormatHandlersByFormatName();
    const std::unordered_map<std::string, FormatHandler*>::const_iterator iterator = registeredFormatHandlersByFormatName.find(formatName);
    if (iterator == registeredFormatHandlersByFormatName.end())
        return nullptr;
    else
        return iterator->second;
}

void load(InputSource& inputSource, Prefixes& prefixes, LogicFactory& logicFactory, InputConsumer& inputConsumer, std::string& formatName) {
    std::ostringstream cannotFindFormatMessage;
    InputConsumerForwarder inputConsumerForwarder(inputConsumer);
    cannotFindFormatMessage << "Could not find a format handler that can parse the input." << std::endl;
    const std::vector<FormatHandler*> registeredFormatHandlers = getRegisteredFormatHandlers();
    for (std::vector<FormatHandler*>::const_iterator iterator = registeredFormatHandlers.begin(); iterator != registeredFormatHandlers.end(); ++iterator) {
        inputSource.rewind();
        try {
            (*iterator)->load(inputSource, prefixes, logicFactory, inputConsumerForwarder, formatName);
            if (inputConsumerForwarder.hasErrors()) {
                std::ostringstream message;
                message << "Handler for format '" << (*iterator)->getName() << " reported errors.";
                throw RDF_STORE_EXCEPTION(message.str());
            }
            else
                return;
        }
        catch (const InputConsumerForwarder::NextFormatException& nextFormatException) {
            cannotFindFormatMessage << (*iterator)->getName() << ": line " << nextFormatException.m_line << ", column " << nextFormatException.m_column << ": " << nextFormatException.m_errorDescription << std::endl;
        }
    }
    throw RDF_STORE_EXCEPTION(cannotFindFormatMessage.str());
}

void save(DataStore& dataStore, Prefixes& prefixes, OutputStream& outputStream, const std::string& formatName) {
    const FormatHandler* formatHandler = getFormatHandlerFor(formatName);
    if (formatHandler)
        formatHandler->save(dataStore, prefixes, outputStream, formatName);
    else {
        std::ostringstream message;
        message << "Format with name '" << formatName << "' is unknown.";
        throw RDF_STORE_EXCEPTION(message.str());
    }
}

std::unique_ptr<InputConsumer> newExporter(Prefixes& prefixes, OutputStream& outputStream, const std::string& formatName) {
    const FormatHandler* formatHandler = getFormatHandlerFor(formatName);
    if (formatHandler)
        return formatHandler->newExporter(prefixes, outputStream, formatName);
    else {
        std::ostringstream message;
        message << "Format with name '" << formatName << "' is unknown.";
        throw RDF_STORE_EXCEPTION(message.str());
    }
}
