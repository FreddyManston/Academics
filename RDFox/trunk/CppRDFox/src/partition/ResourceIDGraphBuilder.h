// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef RESOURCEIDGRAPHBUILDER_H_
#define RESOURCEIDGRAPHBUILDER_H_

#include "../formats/InputConsumer.h"

class Dictionary;
class ResourceIDGraph;

class ResourceIDGraphBuilder : public InputConsumer {
    
protected:

    Dictionary& m_dictionary;
    ResourceIDGraph& m_resourceIDGraph;
    const bool m_pruneClassesAndLiterals;

public:
    
    ResourceIDGraphBuilder(Dictionary& dictionary, ResourceIDGraph& resourceIDGraph, const bool pruneClassesAndLiterals);
    
    virtual void start();
    
    virtual void reportError(const size_t line, const size_t column, const char* const errorDescription);
    
    virtual void consumeTriple(const size_t line, const size_t column, const ResourceText& subject, const ResourceText& predicate, const ResourceText& object);
    
    virtual void consumeRule(const size_t line, const size_t column, const Rule& rule);
    
    virtual void finish();
    
};


#endif /* RESOURCEIDGRAPHBUILDER_H_ */
