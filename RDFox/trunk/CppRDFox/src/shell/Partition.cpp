// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "../all.h"
#include "../dictionary/Dictionary.h"
#include "../partition/MetisPartitioner.h"
#include "../partition/HashPartitioner.h"
#include "../node-server/OccurrenceManager.h"
#include "../util/File.h"
#include "ShellCommand.h"

// Partition modes
enum PartitionMode { PARTITION_PRUNE, PARTITION_EDGE, PARTITION_VERTEX, PARTITION_HASH, };

class Partition : public ShellCommand {
    
public:
    
    Partition() : ShellCommand("partition") {
    }
    
    virtual std::string getOneLineHelp() const {
        return "partitions data into m sets";
    }
    
    virtual void printHelpPage(std::ostream& output) const {
        output
            << "partition [prune | edge | vertex | hash]? <number of partitions> <file name>*" << std::endl
            << "    Partitions an RDF graph into m subgraphs, with or without pruning classes and literals, or by hash partitioning." << std::endl
            << "    The default is graph partitioning with pruning." << std::endl;
    }

    void parseFileName(std::string& fileNamePrefix, std::string& extension, std::string& fileType) const {
        size_t prefixEnd = fileNamePrefix.rfind('.');
        if (prefixEnd != std::string::npos) {
            fileType = fileNamePrefix.substr(prefixEnd);
            if (File::isKnownFileType(fileType.c_str() + 1)) {
                const size_t fileTypeStart = prefixEnd;
                prefixEnd = fileNamePrefix.rfind('.', fileTypeStart - 1);
                if (prefixEnd != std::string::npos)
                    extension = fileNamePrefix.substr(prefixEnd, fileTypeStart - prefixEnd);
            }
            else {
                extension = fileType;
                fileType.clear();
            }
        }
        fileNamePrefix.erase(prefixEnd);
    }

    virtual void execute(Shell& shell, Shell::ArgumentsTokenizer& arguments) const {
        // Get partitioning mode.
        PartitionMode partitionMode = PARTITION_PRUNE;
        if (arguments.symbolLowerCaseTokenEquals("prune"))
            arguments.nextToken();
        else if (arguments.symbolLowerCaseTokenEquals("edge")) {
            partitionMode = PARTITION_EDGE;
            arguments.nextToken();
        }
        else if (arguments.symbolLowerCaseTokenEquals("vertex")) {
            partitionMode = PARTITION_VERTEX;
            arguments.nextToken();
        }
        else if (arguments.symbolLowerCaseTokenEquals("hash")) {
            partitionMode = PARTITION_HASH;
            arguments.nextToken();
        }

        // Get number of partitions
        size_t numberOfPartitions;
        if (!arguments.isNumber()) {
            shell.printLine("Expected an integer for specifying the number of partitions.");
            return;
        }
        numberOfPartitions = static_cast<size_t>(atoi(arguments.getToken(0).c_str()));
        arguments.nextToken();

        // Get the file names
        std::vector<std::string> fileNames;
        while (arguments.isSymbol() || arguments.isQuotedString() || arguments.isQuotedIRI()) {
            std::string inputFileName = arguments.getToken();
            arguments.nextToken();
            if (::fileExists(inputFileName.c_str()))
                fileNames.push_back(inputFileName);
            else if (isAbsoluteFileName(inputFileName)) {
                shell.printLine("File with name '", inputFileName, "' cannot be found.");
                return;
            }
            else {
                const std::string factsFileName = shell.expandRelativeFileNameEx(inputFileName, "dir.facts");
                if (::fileExists(factsFileName.c_str()))
                    fileNames.push_back(factsFileName);
                else {
                    shell.printLine("File with name '", inputFileName, "' cannot be found.");
                    return;
                }
            }
        }
        if (fileNames.empty()) {
            shell.printLine("There are no files to partition.");
            return;
        }

        // Detach the extension name from the file name
        std::string fileNamePrefix(fileNames.front());
        std::string extension;
        std::string fileType;
        parseFileName(fileNamePrefix, extension, fileType);

        // Initialize the output of partitioning
        Prefixes prefixes;
        MemoryManager memoryManager(static_cast<size_t>(::getTotalPhysicalMemorySize() * 0.9));
        Dictionary dictionary(memoryManager, false);
        dictionary.initialize();
        OccurrenceManager occurrenceManager(memoryManager, static_cast<uint32_t>(numberOfPartitions), 3);
        occurrenceManager.initialize();

        // Now do the partitioning
        switch (partitionMode) {
        case PARTITION_PRUNE:
            metisPartitioner(shell.getOutput(), fileNames, fileNamePrefix, extension + fileType, true, numberOfPartitions, prefixes, dictionary, occurrenceManager, true);
            break;
        case PARTITION_EDGE:
            metisPartitioner(shell.getOutput(), fileNames, fileNamePrefix, extension + fileType, false, numberOfPartitions, prefixes, dictionary, occurrenceManager, true);
            break;
        case PARTITION_VERTEX:
            metisPartitioner(shell.getOutput(), fileNames, fileNamePrefix, extension + fileType, false, numberOfPartitions, prefixes, dictionary, occurrenceManager, false);
            break;
        case PARTITION_HASH:
            hashPartitioner(shell.getOutput(), fileNames, fileNamePrefix, extension + fileType, numberOfPartitions, prefixes, dictionary, occurrenceManager);
            break;
        }
        

        // Write out occurrences file
        std::string occurrenceFileName = fileNamePrefix + ".ocr" + fileType;
        shell.printLine("Writing the occurrences file '", occurrenceFileName, "'.");
        TimePoint startTime = ::getTimePoint();
        File occurrenceFile;
        occurrenceFile.open(occurrenceFileName, File::CREATE_NEW_OR_TRUNCATE_EXISTING_FILE, false, true, true, false);
        File::OutputStreamType occurrenceFileOutputStream(occurrenceFile);
        occurrenceManager.save(prefixes, dictionary, occurrenceFileOutputStream);
        occurrenceFile.close();
        Duration duration = ::getTimePoint() - startTime;
        shell.printLine("The occurrences file was written in ", duration / 1000.0, " s.");
    }
};

static Partition s_partition;
