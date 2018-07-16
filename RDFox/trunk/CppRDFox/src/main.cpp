// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifdef WITH_TEST
#include <CppTest/main.h>
#endif

#include "../src/all.h"
#include "../src/RDFStoreException.h"
#include "../src/shell/Shell.h"

void waitForKey() {
    char c[256];
    std::cout << "Press something... ";
    std::cin >> c;
}

int runShell(int argc, char* argv[]) {
    Shell shell(std::cin, std::cout);
    if (argc >= 3) {
        shell.setRootDirectory(argv[2]);
        if (argc >= 4)
            shell.executeCommand(argv[3]);
    }
    shell.run();
    return 0;
}

int main(int argc, char* argv[]) {
    int exitCode;
#ifdef WITH_TEST
    CppTest::MainResult mainResult = CppTest::main(argc, argv);
    if (mainResult == CppTest::ALL_TESTS_PASSED)
        exitCode = 0;
    else if (mainResult == CppTest::AT_LEAST_ONE_TEST_FAILED)
        exitCode = 1;
    else
#endif
    if (argc >= 2 && ::strcmp(argv[1], "-shell") == 0)
        exitCode = runShell(argc, argv);
    else {
#ifdef WITH_TEST
        std::cout << "Usage: CppRDFox [-test | -testd] [test1 test2 ...] | -shell [<root directory> [<command>]]" << std::endl;
#else
        std::cout << "Usage: CppRDFox -shell [<root directory> [<command>]]" << std::endl;
#endif
        exitCode = 1;
    }
#ifdef WIN32
        waitForKey();
#endif
    return exitCode;
}
