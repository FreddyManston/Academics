// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef HASHPARTITIONER_H_
#define HASHPARTITIONER_H_

#include "../all.h"

class Prefixes;
class Dictionary;
class OccurrenceManager;

extern void hashPartitioner(std::ostream& output, const std::vector<std::string>& fileNames, const std::string& outputFileNamePrefix, const std::string outputFileNameSuffix, const size_t numberOfPartitions, Prefixes& prefixes, Dictionary& dictionary, OccurrenceManager& occurrenceManager);

#endif /* HASHPARTITIONER_H_ */
