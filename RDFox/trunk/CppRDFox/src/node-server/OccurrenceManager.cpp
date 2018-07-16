// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../RDFStoreException.h"
#include "../dictionary/Dictionary.h"
#include "../formats/InputConsumer.h"
#include "../formats/turtle/AbstractParserImpl.h"
#include "../storage/ArgumentIndexSet.h"
#include "../storage/DataStore.h"
#include "../util/ThreadContext.h"
#include "../util/MemoryManager.h"
#include "../util/LockFreeBitSet.h"
#include "../util/OutputStream.h"
#include "OccurrenceManagerImpl.h"
#include "ResourceIDMapperImpl.h"

// OccurrenceParser

class OccurrenceParser : protected AbstractParser<OccurrenceParser> {

    friend class AbstractParser<OccurrenceParser>;

protected:

    void doReportError(const size_t line, const size_t column, const char* const errorDescription);

    void prefixMappingParsed(const std::string& prefixName, const std::string& prefixIRI);

public:

    OccurrenceParser(Prefixes& prefixes);

    void parse(InputSource& inputSource, Dictionary& dictionary, ResourceIDMapper& resourceIDMapper, OccurrenceManager& occurrenceManager, const NodeID myNodeID);
    
};

always_inline void OccurrenceParser::doReportError(const size_t line, const size_t column, const char* const errorDescription) {
    std::ostringstream message;
    message << "Error  at line = " << line << ", column = " << column << ": " << errorDescription << std::endl;
    throw RDF_STORE_EXCEPTION(message.str());
}

always_inline void OccurrenceParser::prefixMappingParsed(const std::string& prefixName, const std::string& prefixIRI) {
}

OccurrenceParser::OccurrenceParser(Prefixes& prefixes) : AbstractParser<OccurrenceParser>(prefixes) {
}

void OccurrenceParser::parse(InputSource& inputSource, Dictionary& dictionary, ResourceIDMapper& resourceIDMapper, OccurrenceManager& occurrenceManager, const NodeID myNodeID) {
    m_tokenizer.initialize(inputSource);
    nextToken();
    ThreadContext& threadContext = ThreadContext::getCurrentThreadContext();
    ResourceText resourceText;
    std::unique_ptr<uint8_t[]> temporaryOccurrenceSet(new uint8_t[occurrenceManager.getResourceWidth()]);
    while (m_tokenizer.isGood()) {
        if (m_tokenizer.tokenEquals(TurtleTokenizer::LANGUAGE_TAG, "@prefix")) {
            nextToken();
            parsePrefixMapping();
            if (!m_tokenizer.nonSymbolTokenEquals('.'))
                reportError("Punctuation character '.' expected after '@prefix'.");
            nextToken();
        }
        else if (m_tokenizer.symbolLowerCaseTokenEquals("prefix"))
            parsePrefixMapping();
        else {
            if (!m_tokenizer.isNumber())
                reportError("Expecting a resource ID.");
            const ResourceID globalResourceID = static_cast<ResourceID>(atoll(m_tokenizer.getToken().c_str()));
            if (globalResourceID == INVALID_RESOURCE_ID)
                reportError("Global resource 0 should not occur in an occurrences file because it is owned by everyone.");
            m_tokenizer.nextToken();
            parseResource(resourceText);
            occurrenceManager.clearAll(temporaryOccurrenceSet.get());
            OccurrenceSet currentOccurrenceSet = temporaryOccurrenceSet.get();
            bool containsMyNodeID = false;
            while (m_tokenizer.isGood() && !m_tokenizer.nonSymbolTokenEquals('.')) {
                if (m_tokenizer.nonSymbolTokenEquals(';'))
                    currentOccurrenceSet += occurrenceManager.getSetWidth();
                else {
                    if (!m_tokenizer.isNumber())
                        reportError("Node ID expected.");
                    const NodeID nodeID = static_cast<NodeID>(atol(m_tokenizer.getToken(0).c_str()));
                    occurrenceManager.add(currentOccurrenceSet, nodeID);
                    if (nodeID == myNodeID)
                        containsMyNodeID = true;
                }
                m_tokenizer.nextToken();
            }
            if (!m_tokenizer.nonSymbolTokenEquals('.'))
                reportError("Each resource should be terminated with '.'");
            if (myNodeID == OccurrenceManager::LOAD_RESOURCES_FOR_ALL_NODES || containsMyNodeID) {
                const ResourceID localResourceID = dictionary.resolveResource(threadContext, nullptr, resourceText);
                occurrenceManager.copyAll(temporaryOccurrenceSet.get(),  occurrenceManager.getOccurrenceSetSafe(localResourceID, 0));
                resourceIDMapper.addMapping(threadContext, localResourceID, globalResourceID);
            }
            m_tokenizer.nextToken();
        }
    }
    m_tokenizer.deinitialize();
}

// OccurrenceManager

void OccurrenceManager::extendTo(const size_t resourceIDStart) {
    uint8_t* current = m_data.getEnd();
    if (!m_data.ensureEndAtLeast(resourceIDStart, m_resourceWidth))
        throw RDF_STORE_EXCEPTION("Cannot extend the occurrence set.");
    uint8_t* newEnd = m_data.getEnd();
    while (current != newEnd)
        *(current++) = 0xFF;
}

OccurrenceManager::OccurrenceManager(MemoryManager& memoryManager, const uint8_t numberOfNodes, const uint8_t arity) :
    m_data(memoryManager),
    m_numberOfNodes(numberOfNodes),
    m_arity(arity),
    m_setWidth((m_numberOfNodes - 1) / 8 + 1),
    m_resourceWidth(m_setWidth * m_arity),
    m_fullSets(new uint8_t[m_resourceWidth])
{
    for (size_t index = 0; index < m_resourceWidth; ++index)
        m_fullSets[index] = 0xFF;
}

void OccurrenceManager::initialize() {
    if (!m_data.initializeLarge())
        throw RDF_STORE_EXCEPTION("Cannot initialize OccuerenceManager.");
}

void OccurrenceManager::loadFromInputSource(Prefixes& prefixes, InputSource& inputSource, Dictionary& dictionary, ResourceIDMapper& resourceIDMapper, const NodeID myNodeID) {
    OccurrenceParser occurrenceParser(prefixes);
    occurrenceParser.parse(inputSource, dictionary, resourceIDMapper, *this, myNodeID);
}

void OccurrenceManager::save(Prefixes& prefixes, const Dictionary& dictionary, OutputStream& outputStream) const {
    // Write prefixes
    const std::map<std::string, std::string>& prefixIRIsByPrefixNames = prefixes.getPrefixIRIsByPrefixNames();
    if (!prefixIRIsByPrefixNames.empty()) {
        for (std::map<std::string, std::string>::const_iterator iterator = prefixIRIsByPrefixNames.begin(); iterator != prefixIRIsByPrefixNames.end(); ++iterator)
            outputStream << "@prefix " << iterator->first << " <" << iterator->second << "> .\n";
        outputStream << '\n';
    }
    // Write the resources
    const ResourceID afterLastResourceID = getAfterLastResourceID();
    ResourceID resourceID = 1;
    ResourceValue resourceValue;
    while (resourceID < afterLastResourceID) {
        if (dictionary.getResource(resourceID, resourceValue)) {
            outputStream << resourceID << ' ' << resourceValue.toString(prefixes);
            for (uint8_t position = 0; position < m_arity; ++position) {
                if (position != 0)
                    outputStream << ' ' << ';';
                ImmutableOccurrenceSet occurrenceSet = getOccurrenceSet(resourceID, position);
                iterator current = begin(occurrenceSet);
                while (!current.isAtEnd()) {
                    if (*current)
                        outputStream << ' ' << current.getCurrentNodeID();
                    ++current;
                }
            }
            outputStream << " .\n";
        }
        ++resourceID;
    }
}

void OccurrenceManager::save(const Dictionary& dictionary, OutputStream& outputStream) const {
    Prefixes prefixes;
    prefixes.declareStandardPrefixes();
    const ResourceID afterLastResourceID = getAfterLastResourceID();
    ResourceID resourceID = 1;
    ResourceValue resourceValue;
    while (resourceID < afterLastResourceID) {
        if (dictionary.getResource(resourceID, resourceValue) && resourceValue.getDatatypeID() == D_IRI_REFERENCE)
            prefixes.createAutomaticPrefix(resourceValue.getString(), 256);
        ++resourceID;
    }
    save(prefixes, dictionary, outputStream);
}
