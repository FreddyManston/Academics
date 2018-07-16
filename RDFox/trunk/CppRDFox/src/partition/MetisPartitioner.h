// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef METISPARTITIONER_H_
#define METISPARTITIONER_H_

#include "../all.h"

class Prefixes;
class Dictionary;
class OccurrenceManager;

extern void metisPartitioner(std::ostream& output, const std::vector<std::string>& fileNames, const std::string& outputFileNamePrefix, const std::string outputFileNameSuffix, const bool pruneClassesLiterals, const size_t numberOfPartitions, Prefixes& prefixes, Dictionary& dictionary, OccurrenceManager& occurrenceManager, const bool useVertexWeights);

#endif /* METISPARTITIONER_H_ */
