// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../RDFStoreException.h"
#include "../logic/Logic.h"
#include "../storage/DataStore.h"
#include "../storage/Parameters.h"
#include "../querying/QueryIterator.h"
#include "../formats/turtle/SPARQLParser.h"
#include "../util/OutputStream.h"
#include "../query-answer-format/QueryAnswerFormat.h"
#include "SPARQLEndPoint.h"

// ostreamOutputStream

class ostreamOutputStream : public OutputStream {

protected:

    std::ostream& m_output;

public:

    ostreamOutputStream(std::ostream& output) : m_output(output) {
    }

    virtual void flush() {
        m_output.flush();
    }

    virtual size_t write(const void* const data, const size_t numberOfBytesToWrite) {
        m_output.write(reinterpret_cast<const char*>(data), static_cast<std::streamsize>(numberOfBytesToWrite));
        return numberOfBytesToWrite;
    }

};

// SPARQLEndPoint

SPARQLEndPoint::SPARQLEndPoint() {
}

always_inline static void setFaultMessage(const char* const message, std::ostringstream& output, dlib::outgoing_things& outgoing) {
    outgoing.headers.insert(std::pair<std::string, std::string>("Content-Type", "text/plain; charset=UTF-8"));
    outgoing.http_return = 400;
    outgoing.http_return_status = "Bad Request";
    output.str("");
    output.clear();
    output << message;
}

const std::string SPARQLEndPoint::on_request(const dlib::incoming_things& incoming, dlib::outgoing_things& outgoing) {
    std::ostringstream output;
    try {
        // TODO: Queries are stored as key_value_map, but protocol allows to have multiple default-graph-uri
        // and named-graph-uri. We should write our request url parser or extend/modify the one in dlib
        const std::string& queryString = incoming.queries["query"];
        Prefixes prefixes;
        SPARQLParser parser(prefixes);
        LogicFactory factory(::newLogicFactory());
        Query query = parser.parse(factory, queryString.c_str(), queryString.length());
        DataStore* dataStore = getDataStore();
        if (dataStore == nullptr)
            setFaultMessage("No data store is active at present.", output, outgoing);
        else {
            const std::string& queryAnswerFormatName = incoming.queries["output"];
            ostreamOutputStream outputStream(output);
            std::unique_ptr<QueryAnswerFormat> queryAnswerFormat = ::newQueryAnswerFormat(queryAnswerFormatName, outputStream, false, query->getAnswerTerms(), Prefixes::s_emptyPrefixes);
            std::unique_ptr<QueryIterator> queryIterator = dataStore->compileQuery(query, Parameters());
            const ResourceValueCache& resourceValueCache = queryIterator->getResourceValueCache();
            const std::vector<ResourceID>& argumentsBuffer = queryIterator->getArgumentsBuffer();
            const std::vector<ArgumentIndex>& argumentsIndexes = queryIterator->getArgumentIndexes();
            size_t multiplicity = queryIterator->open();
            queryAnswerFormat->printPrologue();
            while (multiplicity != 0) {
                queryAnswerFormat->printResult(resourceValueCache, argumentsBuffer, argumentsIndexes, multiplicity);
                multiplicity = queryIterator->advance();
            }
            queryAnswerFormat->printEpilogue();
        }
    }
    catch (const RDFStoreException& e) {
        setFaultMessage(e.what(), output, outgoing);
    }
    return output.str();
}
