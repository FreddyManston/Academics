The distribution of RDFox consists of the following files and directories:

- The 'examples' directory contains a demonstration program that shows how to call
  JRDFox as a library from Java and Pyhton. The Apache Ant script in the
  'examples/Java' can be used to compile and run the Java demonstration program.

- The 'javadoc' directory contains the Javadoc documentation for JDRFox.

- The 'lib' directory contains the libraries.

  * JRDFox.jar is the Java bridge to the C++ native RDFox engine.
  * owlapi-*.jar is the OWL API that JRDFox uses to load ontologies. For more information
    about the OWL API, please refer to http://owlapi.sourceforge.net.
  * PRDFox.py is the Python bridge to the C++ native RDFox engine.
  * CppRDFox (on Mac OS X and Linux) and CppRDFox.exe (on Windows) are stand-alone executables
    that can be used to run RDFox on the command line. The system provides a shell that
    can load an RDF file, materialize facts w.r.t. a set of rules, and answer SPARQL queries.
  * libCppRDFox.dylib (on Mac OS X), libCppRDFox.so (on Linux), and CppRDFox.dll (on Windows)
	are dynamic libraries that implement the native methods of JRDFox and PRDFox.

To use JRDFox in your project, simply add JRDFox.jar and owlapi-*.jar to your classpath,
and make sure that the path to the dynamic library is correctly specified  when starting
your program using the following JVM option:

    -Djava.library.path=<path> 

To set up an Eclipse project that uses JRDFox, you can specify the path to the dynamic libraries
by specifying the "Native library location" for the JRDFox.jar library in the "Java Build
Path -> Libraries" part of the project properties page.
