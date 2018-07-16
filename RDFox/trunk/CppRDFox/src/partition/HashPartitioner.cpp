// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../all.h"
#include "../formats/InputOutput.h"
#include "../tasks/Tasks.h"
#include "../util/File.h"
#include "PartitionBuilderImpl.h"
#include "HashPartitioner.h"

class OwnerwshipAssignmentByHashing {

protected:

    const size_t m_numberOfPartitions;

public:

    always_inline OwnerwshipAssignmentByHashing(const size_t numberOfPartitions) : m_numberOfPartitions(numberOfPartitions) {
    }

    always_inline NodeID getOwner(const ResourceID subjectID, const ResourceID predicateID, const ResourceID objectID) const {
        return static_cast<NodeID>((subjectID * 2654435761) % m_numberOfPartitions);
    }
    
};

void hashPartitioner(std::ostream& output, const std::vector<std::string>& fileNames, const std::string& outputFileNamePrefix, const std::string outputFileNameSuffix, const size_t numberOfPartitions, Prefixes& prefixes, Dictionary& dictionary, OccurrenceManager& occurrenceManager) {
    // Read RDF file and output the partitions
    output << "Writing the partition files." << std::endl;
    TimePoint startTime = ::getTimePoint();
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
    PartitionBuilder<OwnerwshipAssignmentByHashing> partitionBuilder(dictionary, occurrenceManager, partitions, numberOfPartitions);
    for (auto iterator = fileNames.begin(); iterator != fileNames.end(); ++iterator) {
        File input;
        input.open(*iterator, File::OPEN_EXISTING_FILE, true, false, true);
        ::parseFile(input, prefixes, partitionBuilder);
        input.close();
    }
    for (auto iterator = filesOwner.begin(); iterator != filesOwner.end(); ++iterator)
        (*iterator)->close();
    Duration duration = ::getTimePoint() - startTime;
    output << "Partition files were written in " << duration / 1000.0 << " s." << std::endl;
}
