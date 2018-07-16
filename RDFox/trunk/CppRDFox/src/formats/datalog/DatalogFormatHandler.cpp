// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../../RDFStoreException.h"
#include "../../storage/DataStore.h"
#include "../../storage/RuleIterator.h"
#include "../../util/Prefixes.h"
#include "../../util/OutputStream.h"
#include "../../util/Vocabulary.h"
#include "../InputConsumer.h"
#include "../InputOutput.h"
#include "DatalogParser.h"

// DatalogFormatExporter

class DatalogFormatExporter : public InputConsumer {

protected:

    enum State { AT_BEGINNING, AFTER_PREFIX, AFTER_RULE };

    Prefixes& m_prefixes;
    OutputStream& m_outputStream;
    State m_state;

public:

    DatalogFormatExporter(Prefixes& prefixes, OutputStream& outputStream);

    virtual ~DatalogFormatExporter();

    virtual void start();

    virtual void reportError(const size_t line, const size_t column, const char* const errorDescription);

    virtual void consumePrefixMapping(const std::string& prefixName, const std::string& prefixIRI);

    virtual void consumeTriple(const size_t line, const size_t column, const ResourceText& subject, const ResourceText& predicate, const ResourceText& object);

    virtual void consumeRule(const size_t line, const size_t column, const Rule& rule);

    virtual void finish();

};

DatalogFormatExporter::DatalogFormatExporter(Prefixes& prefixes, OutputStream& outputStream) : m_prefixes(prefixes), m_outputStream(outputStream), m_state(AT_BEGINNING) {
}

DatalogFormatExporter::~DatalogFormatExporter() {
}

void DatalogFormatExporter::start() {
    const std::map<std::string, std::string>& prefixIRIsByPrefixNames = m_prefixes.getPrefixIRIsByPrefixNames();
    if (!prefixIRIsByPrefixNames.empty()) {
        for (std::map<std::string, std::string>::const_iterator iterator = prefixIRIsByPrefixNames.begin(); iterator != prefixIRIsByPrefixNames.end(); ++iterator)
            m_outputStream << "PREFIX " << iterator->first << " <" << iterator->second << ">\n";
        m_state = AFTER_PREFIX;
    }
}

void DatalogFormatExporter::reportError(const size_t line, const size_t column, const char* const errorDescription) {
}

void DatalogFormatExporter::consumePrefixMapping(const std::string& prefixName, const std::string& prefixIRI) {
    if (m_state == AFTER_RULE)
        m_outputStream << "\n";
    m_outputStream << "PREFIX " << prefixName << " <" << prefixIRI << ">\n";
    m_prefixes.declarePrefix(prefixName, prefixIRI);
    m_state = AFTER_PREFIX;
}

void DatalogFormatExporter::consumeTriple(const size_t line, const size_t column, const ResourceText& subject, const ResourceText& predicate, const ResourceText& object) {
}

void DatalogFormatExporter::consumeRule(const size_t line, const size_t column, const Rule& rule) {
    if (m_state == AFTER_PREFIX)
        m_outputStream << '\n';
    m_outputStream << rule->toString(m_prefixes) << '\n';
    m_state = AFTER_RULE;
}

void DatalogFormatExporter::finish() {
}

// DatalogFormatHandler

class DatalogFormatHandler : public FormatHandler {

protected:

    static DatalogFormatHandler s_datalogFormatHandler;

    DatalogFormatHandler();

public:

    virtual bool storesFacts() const;

    virtual bool storesRules() const;

    virtual void load(InputSource& inputSource, Prefixes& prefixes, LogicFactory& logicFactory, InputConsumer& inputConsumer, std::string& formatName) const;

    virtual void save(DataStore& dataStore, Prefixes& prefixes, OutputStream& outputStream, const std::string& formatName) const;

    virtual std::unique_ptr<InputConsumer> newExporter(Prefixes& prefixes, OutputStream& outputStream, const std::string& formatName) const;

};

DatalogFormatHandler DatalogFormatHandler::s_datalogFormatHandler;

DatalogFormatHandler::DatalogFormatHandler() : FormatHandler(1, "Datalog handler", FormatNames{ "Datalog" }) {
}

bool DatalogFormatHandler::storesFacts() const {
    return false;
}

bool DatalogFormatHandler::storesRules() const {
    return true;
}

void DatalogFormatHandler::load(InputSource& inputSource, Prefixes& prefixes, LogicFactory& logicFactory, InputConsumer& inputConsumer, std::string& formatName) const {
    DatalogParser datalogParser(prefixes);
    datalogParser.bind(inputSource);
    datalogParser.parse(logicFactory, inputConsumer);
    datalogParser.unbind();
    formatName = "Datalog";
}

void DatalogFormatHandler::save(DataStore& dataStore, Prefixes& prefixes, OutputStream& outputStream, const std::string& formatName) const {
    std::unique_ptr<RuleIterator> ruleIterator = dataStore.createRuleIterator();
    DatalogFormatExporter datalogFormatExporter(prefixes, outputStream);
    datalogFormatExporter.start();
    for (bool valid = ruleIterator->open(); valid; valid = ruleIterator->advance())
        datalogFormatExporter.InputConsumer::consumeRule(1, 1, ruleIterator->getRule());
    datalogFormatExporter.finish();
}

std::unique_ptr<InputConsumer> DatalogFormatHandler::newExporter(Prefixes& prefixes, OutputStream& outputStream, const std::string& formatName) const {
    return std::unique_ptr<InputConsumer>(new DatalogFormatExporter(prefixes, outputStream));
}
