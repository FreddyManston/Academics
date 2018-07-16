// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../Common.h"
#include "../RDFStoreException.h"
#include "../util/ComponentStatistics.h"
#include "../util/Vocabulary.h"
#include "../util/InputStream.h"
#include "../util/OutputStream.h"
#include "Datatype.h"

always_inline static bool parseBoolean(const std::string& lexicalForm) {
    if (lexicalForm == "false" || lexicalForm == "0")
        return false;
    else if (lexicalForm == "true" || lexicalForm == "1")
        return true;
    else {
        std::ostringstream message;
        message << "Lexical form '" << lexicalForm << "' is invalid for the xsd:boolean datatype.";
        throw RDF_STORE_EXCEPTION(message.str());
    }
}

// Declaration BooleanDatatype

class MemoryManager;

static const std::string s_xsdBoolean(XSD_BOOLEAN);

class BooleanDatatype : public Datatype {

protected:

    ResourceID m_falseResourceID;
    ResourceID m_trueResourceID;

public:

    BooleanDatatype(MemoryManager& memoryManager, ResourceID& nextResourceID, MemoryRegion<LexicalFormHandle>& lexicalFormHandlesByResourceID, MemoryRegion<DatatypeID>& datatypeIDsByResourceID, DataPool& dataPool);

    void initialize(const size_t initialResourceCapacity);

    void setNumberOfThreads(const size_t numberOfThreads);

    void getResource(const LexicalFormHandle lexicalFormHandle, const DatatypeID datatypeID, ResourceValue& resourceValue) const;

    void getResource(const LexicalFormHandle lexicalFormHandle, const DatatypeID datatypeID, std::string& lexicalForm, std::string& datatypeIRI) const;

    void getResource(const LexicalFormHandle lexicalFormHandle, const DatatypeID datatypeID, std::string& lexicalForm) const;

    ResourceID tryResolveResource(ThreadContext& threadContext, const bool value) const;

    ResourceID tryResolveResource(ThreadContext& threadContext, const ResourceValue& resourceValue) const;

    ResourceID tryResolveResource(ThreadContext& threadContext, const std::string& lexicalForm, const std::string& datatypeIRI) const;

    ResourceID tryResolveResource(ThreadContext& threadContext, const std::string& lexicalForm, const DatatypeID datatypeID) const;

    ResourceID resolveResource(ThreadContext& threadContext, const bool value);

    ResourceID resolveResource(ThreadContext& threadContext, DictionaryUsageContext* dictionaryUsageContext, const ResourceValue& resourceValue, const ResourceID preferredResourceID);

    ResourceID resolveResource(ThreadContext& threadContext, DictionaryUsageContext* dictionaryUsageContext, const std::string& lexicalForm, const std::string& datatypeIRI, const ResourceID preferredResourceID);

    ResourceID resolveResource(ThreadContext& threadContext, DictionaryUsageContext* dictionaryUsageContext, const std::string& lexicalForm, const DatatypeID datatypeID, const ResourceID preferredResourceID);

    void save(OutputStream& outputStream) const;

    void load(InputStream& inputStream);

    std::unique_ptr<ComponentStatistics> getComponentStatistics() const;

};

// Implementation BooleanDatatype

BooleanDatatype::BooleanDatatype(MemoryManager& memoryManager, ResourceID& nextResourceID, MemoryRegion<LexicalFormHandle>& lexicalFormHandlesByResourceID, MemoryRegion<DatatypeID>& datatypeIDsByResourceID, DataPool& dataPool) :
    Datatype(nextResourceID, lexicalFormHandlesByResourceID, datatypeIDsByResourceID, dataPool)
{
}

void BooleanDatatype::initialize(const size_t initialResourceCapacity) {
    m_falseResourceID = nextResourceID<true>(nullptr, XSD_FALSE_ID);
    m_trueResourceID = nextResourceID<true>(nullptr, XSD_TRUE_ID);
    ::atomicWrite(m_lexicalFormHandlesByResourceID[m_falseResourceID], 0);
    ::atomicWrite(m_datatypeIDsByResourceID[m_falseResourceID], D_XSD_BOOLEAN);
    ::atomicWrite(m_lexicalFormHandlesByResourceID[m_trueResourceID], 1);
    ::atomicWrite(m_datatypeIDsByResourceID[m_trueResourceID], D_XSD_BOOLEAN);
}

void BooleanDatatype::setNumberOfThreads(const size_t numberOfThreads) {
}

void BooleanDatatype::getResource(const LexicalFormHandle lexicalFormHandle, const DatatypeID datatypeID, ResourceValue& resourceValue) const {
    resourceValue.setBoolean(lexicalFormHandle != 0);
}

void BooleanDatatype::getResource(const LexicalFormHandle lexicalFormHandle, const DatatypeID datatypeID, std::string& lexicalForm, std::string& datatypeIRI) const {
    lexicalForm = lexicalFormHandle != 0 ? "true" : "false";
    datatypeIRI = s_xsdBoolean;
}

void BooleanDatatype::getResource(const LexicalFormHandle lexicalFormHandle, const DatatypeID datatypeID, std::string& lexicalForm) const {
    lexicalForm = lexicalFormHandle != 0 ? "true" : "false";
}

ResourceID BooleanDatatype::tryResolveResource(ThreadContext& threadContext, const bool value) const {
    return value ? m_trueResourceID : m_falseResourceID;
}

ResourceID BooleanDatatype::tryResolveResource(ThreadContext& threadContext, const ResourceValue& resourceValue) const {
    return tryResolveResource(threadContext, resourceValue.getBoolean());
}

ResourceID BooleanDatatype::tryResolveResource(ThreadContext& threadContext, const std::string& lexicalForm, const std::string& datatypeIRI) const {
    return tryResolveResource(threadContext, parseBoolean(lexicalForm));
}

ResourceID BooleanDatatype::tryResolveResource(ThreadContext& threadContext, const std::string& lexicalForm, const DatatypeID datatypeID) const {
    return tryResolveResource(threadContext, parseBoolean(lexicalForm));
}

ResourceID BooleanDatatype::resolveResource(ThreadContext& threadContext, const bool value) {
    return value ? m_trueResourceID : m_falseResourceID;
}

ResourceID BooleanDatatype::resolveResource(ThreadContext& threadContext, DictionaryUsageContext* dictionaryUsageContext, const ResourceValue& resourceValue, const ResourceID preferredResourceID) {
    return resolveResource(threadContext, resourceValue.getBoolean());
}

ResourceID BooleanDatatype::resolveResource(ThreadContext& threadContext, DictionaryUsageContext* dictionaryUsageContext, const std::string& lexicalForm, const std::string& datatypeIRI, const ResourceID preferredResourceID) {
    return resolveResource(threadContext, parseBoolean(lexicalForm));
}

ResourceID BooleanDatatype::resolveResource(ThreadContext& threadContext, DictionaryUsageContext* dictionaryUsageContext, const std::string& lexicalForm, const DatatypeID datatypeID, const ResourceID preferredResourceID) {
    return resolveResource(threadContext, parseBoolean(lexicalForm));
}

void BooleanDatatype::save(OutputStream& outputStream) const {
    outputStream.writeString("BooleanDatatype");
    outputStream.write(m_falseResourceID);
    outputStream.write(m_trueResourceID);
}

void BooleanDatatype::load(InputStream& inputStream) {
    if (!inputStream.checkNextString("BooleanDatatype"))
        throw RDF_STORE_EXCEPTION("Invalid input file: cannot load BooleanDatatype.");
    m_falseResourceID = inputStream.read<ResourceID>();
    m_trueResourceID = inputStream.read<ResourceID>();
}

std::unique_ptr<ComponentStatistics> BooleanDatatype::getComponentStatistics() const {
    std::unique_ptr<ComponentStatistics> result(new ComponentStatistics("BooleanDatatype"));
    result->addIntegerItem("Aggregate size", sizeof(ResourceID) * 2);
    return result;
}

// BooleanDatatypeFactory

DATATYPE_FACTORY(BooleanDatatypeFactory, BooleanDatatype, BooleanDatatype);

BooleanDatatypeFactory::BooleanDatatypeFactory() :
    m_datatypeIRIsByID(std::unordered_map<DatatypeID, std::string>{ { D_XSD_BOOLEAN, s_xsdBoolean } }),
    m_datatypeIDsByAllIRIs(std::unordered_map<std::string, DatatypeID>{ { s_xsdBoolean, D_XSD_BOOLEAN } })
{
}

void BooleanDatatypeFactory::parseResourceValue(ResourceValue& resourceValue, const std::string& lexicalForm, const std::string& datatypeIRI) const {
    resourceValue.setBoolean(parseBoolean(lexicalForm));
}

void BooleanDatatypeFactory::parseResourceValue(ResourceValue& resourceValue, const std::string& lexicalForm, const DatatypeID datatypeID) const {
    resourceValue.setBoolean(parseBoolean(lexicalForm));
}

void BooleanDatatypeFactory::toLexicalForm(const DatatypeID datatypeID, const uint8_t* const data, const size_t dataSize, std::string& lexicalForm) const {
    lexicalForm = *reinterpret_cast<const bool*>(data) ? "true" : "false";
}

void BooleanDatatypeFactory::toTurtleLiteral(const DatatypeID datatypeID, const uint8_t* const data, const size_t dataSize, const Prefixes& prefixes, std::string& literalText) const {
    literalText = *reinterpret_cast<const bool*>(data) ? "true" : "false";
}

EffectiveBooleanValue BooleanDatatypeFactory::getEffectiveBooleanValue(const ResourceValue& resourceValue) const {
    return resourceValue.getBoolean() ? EBV_TRUE : EBV_FALSE;
}
