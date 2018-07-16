// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "InputConsumer.h"

InputConsumer::~InputConsumer() {
}

void InputConsumer::start() {
}

void InputConsumer::reportError(const size_t line, const size_t column, const char* const errorDescription) {
}

void InputConsumer::consumePrefixMapping(const std::string& prefixName, const std::string& prefixIRI) {
}

void InputConsumer::consumeTriple(const size_t line, const size_t column, const ResourceText& subject, const ResourceText& predicate, const ResourceText& object) {
}

void InputConsumer::consumeRule(const size_t line, const size_t column, const Rule& rule) {
}

void InputConsumer::finish() {
}
