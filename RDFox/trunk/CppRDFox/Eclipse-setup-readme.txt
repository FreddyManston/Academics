To compile this project in Eclipse, one needs to determine several platform-dependent options, which can be accomplished as follows.

* Go to "Window -> Preferences -> C/C++ -> Build -> Build Variables".
* Define variable SHARED_LIBRARY_SUFFIX as "dylib" on Mac OS X and "so" on Linux. The variable should be of type 'String'.
* Define variable TARGET_OS as "darwin" on Mac OS X and "linux" on Linux. The variable should be of type 'String'.
