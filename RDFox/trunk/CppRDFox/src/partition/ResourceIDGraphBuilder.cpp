// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../RDFStoreException.h"
#include "../dictionary/Dictionary.h"
#include "../util/Vocabulary.h"
#include "ResourceIDGraph.h"
#include "ResourceIDGraphBuilder.h"

ResourceIDGraphBuilder::ResourceIDGraphBuilder(Dictionary& dictionary, ResourceIDGraph& resourceIDGraph, const bool pruneClassesAndLiterals) :
    m_dictionary(dictionary),
    m_resourceIDGraph(resourceIDGraph),
    m_pruneClassesAndLiterals(pruneClassesAndLiterals)
{
}

void ResourceIDGraphBuilder::start() {
}

void ResourceIDGraphBuilder::reportError(const size_t line, const size_t column, const char* const errorDescription) {
    std::ostringstream message;
    message << "Error at line = " << line << ", column = " << column << ": " << errorDescription;
    throw RDF_STORE_EXCEPTION(message.str());
}

void ResourceIDGraphBuilder::consumeTriple(const size_t line, const size_t column, const ResourceText& subject, const ResourceText& predicate, const ResourceText& object) {
    const ResourceID subjectID = m_dictionary.resolveResource(subject);
    m_resourceIDGraph.addNode(subjectID, true);
    if (!m_pruneClassesAndLiterals || (object.m_resourceType != LITERAL && (predicate.m_resourceType != IRI_REFERENCE || predicate.m_lexicalForm != RDF_TYPE))) {
        const ResourceID objectID = m_dictionary.resolveResource(object);
        m_resourceIDGraph.addNode(objectID, false);
        m_resourceIDGraph.addEdge(subjectID, objectID);
    }
}

void ResourceIDGraphBuilder::consumeRule(const size_t line, const size_t column, const Rule& rule) {
}

void ResourceIDGraphBuilder::finish() {
}
