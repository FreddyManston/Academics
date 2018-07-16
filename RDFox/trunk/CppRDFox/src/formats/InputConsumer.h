// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef INPUTCONSUMER_H_
#define INPUTCONSUMER_H_

#include "../Common.h"
#include "../logic/Logic.h"

class InputConsumer {

public:

    virtual ~InputConsumer();

    virtual void start();

    virtual void reportError(const size_t line, const size_t column, const char* const errorDescription);

    virtual void consumePrefixMapping(const std::string& prefixName, const std::string& prefixIRI);

    virtual void consumeTriple(const size_t line, const size_t column, const ResourceText& subject, const ResourceText& predicate, const ResourceText& object);

    virtual void consumeRule(const size_t line, const size_t column, const Rule& rule);

    virtual void finish();

};

#endif // INPUTCONSUMER_H_
