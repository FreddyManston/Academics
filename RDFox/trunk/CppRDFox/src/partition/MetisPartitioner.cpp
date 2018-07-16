// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../all.h"
#include "../formats/InputOutput.h"
#include "../partition/ResourceIDGraph.h"
#include "../partition/ResourceIDGraphBuilder.h"
#include "../partition/PartitionBuilderImpl.h"
#include "../tasks/Tasks.h"
#include "../util/File.h"
#include "MetisPartitioner.h"

class OwnerwshipAssignmentByResourceIDPartition {

protected:

    const ResourceIDPartition& m_resourceIDPartition;

public:

    always_inline OwnerwshipAssignmentByResourceIDPartition(const ResourceIDPartition& resourceIDPartition) : m_resourceIDPartition(resourceIDPartition) {
    }

    always_inline NodeID getOwner(const ResourceID subjectID, const ResourceID predicateID, const ResourceID objectID) const {
        return m_resourceIDPartition[subjectID];
    }

};

void metisPartitioner(std::ostream& output, const std::vector<std::string>& fileNames, const std::string& outputFileNamePrefix, const std::string outputFileNameSuffix, const bool pruneClassesLiterals, const size_t numberOfPartitions, Prefixes& prefixes, Dictionary& dictionary, OccurrenceManager& occurrenceManager, const bool useVertexWeights) {
    // Read RDF file into adjacency graph
    if (fileNames.size() == 1)
        output << "Building the adjacency graph from file '" << fileNames.front() << "'." << std::endl;
    else {
        output << "Building the adjacency graph from the following files:";
        for (auto iterator = fileNames.begin(); iterator != fileNames.end(); ++iterator)
            output << std::endl << "    " << *iterator;
        output << std::endl;
    }
    TimePoint startTime = ::getTimePoint();
    ResourceIDGraph resourceIDGraph(useVertexWeights);
    ResourceIDGraphBuilder resourceIDGraphBuilder(dictionary, resourceIDGraph, pruneClassesLiterals);
    for (auto iterator = fileNames.begin(); iterator != fileNames.end(); ++iterator) {
        File input;
        input.open(*iterator, File::OPEN_EXISTING_FILE, true, false, true);
        ::parseFile(input, prefixes, resourceIDGraphBuilder);
        input.close();
    }
    Duration duration = ::getTimePoint() - startTime;
    output << "The adjacency graph was constructed in " << duration / 1000.0 << " s." << std::endl;
    prefixes.declareStandardPrefixes();

    // Create the partition
    output << "Partitioning the graph." << std::endl;
    startTime = ::getTimePoint();
    ResourceIDPartition resourceIDPartition = resourceIDGraph.partition(&output, numberOfPartitions);
    duration = ::getTimePoint() - startTime;
    output << "Partitioning was performed in " << duration / 1000.0 << " s." << std::endl;

    // Read RDF file and output the partitions
    output << "Writing the partition files." << std::endl;
    startTime = ::getTimePoint();
    unique_ptr_vector<File> filesOwner;
    unique_ptr_vector<File::OutputStreamType> streamsOwner;
    unique_ptr_vector<InputConsumer> partitionsOwner;
    std::vector<InputConsumer*> partitions;
    for (size_t partitionIndex = 0; partitionIndex < numberOfPartitions; ++partitionIndex) {
        std::ostringstream outputFileName;
        outputFileName << outputFileNamePrefix << ".p" << partitionIndex << outputFileNameSuffix;
        filesOwner.push_back(std::unique_ptr<File>(new File()));
        filesOwner.back()->open(outputFileName.str(), File::CREATE_NEW_OR_TRUNCATE_EXISTING_FILE, false, true, true);
        streamsOwner.push_back(std::unique_ptr<File::OutputStreamType>(new File::OutputStreamType(*filesOwner.back())));
        partitionsOwner.push_back(::newExporter(prefixes, *streamsOwner.back(), "Turtle"));
        partitions.push_back(partitionsOwner.back().get());
    }
    PartitionBuilder<OwnerwshipAssignmentByResourceIDPartition> partitionBuilder(dictionary, occurrenceManager, partitions, resourceIDPartition);
    for (auto iterator = fileNames.begin(); iterator != fileNames.end(); ++iterator) {
        File input;
        input.open(*iterator, File::OPEN_EXISTING_FILE, true, false, true);
        ::parseFile(input, prefixes, partitionBuilder);
        input.close();
    }
    for (auto iterator = filesOwner.begin(); iterator != filesOwner.end(); ++iterator)
        (*iterator)->close();
    duration = ::getTimePoint() - startTime;
    output << "Partition files were written in " << duration / 1000.0 << " s." << std::endl;
}
