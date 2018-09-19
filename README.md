# Honours-Project

2018 Honours Project: Creating a Defeasible Datalog, using RDFox.

-----------------------------------
INITIAL PROCUREMENT OF SOURCE CODE:
-----------------------------------

All RDFox source code was downloaded from https://www.cs.ox.ac.uk/isg/tools/RDFox/ , between May and June of 2018.
This was while the source code was open source under a free academic licenceand before development, by Oxford Semantic Technologies, for commercial use had begun.

The DefeasibleDatalog wrapper was written in Python and thus makes use of the main RDFox reasoner (written in C++) and its Python bridge, PRDFox. 
Thus, the Java bridge, JRDFox has been removed from this repository's source code.



------------------
OPERATING SYSTEMS:
------------------

The Linux specific version of RDFox was made use of (as all development, for this project, was done on Linux systems).
Thus most of the essential code in this repo has only been tested to run on Linux systems.

Mac systems have also been catered for (by adding the Mac dependency library, libCppRDFox.dylib), however, not much testing has been done to show that the code will run flawlessly on all Mac systems.

Note that there has been no support for Windows systems, as yet.


---------------------
STUCTURE OF THE REPO:
---------------------

|-RDFox-linux
|  |-src
|  |  |-Python
|  |  |  |-Defeasibility Wrapper
|  |  |  |  |-data . . . . . . . . . . 'all the test cases used to test the correctness of the DefeasibleDatalog implementation'
|  |  |  |  |-DDLOG_Demo.py. . . . . . 'a demonstration of how to use the DefeasibleDatalog.py implementation'
|  |  |  |  |-PRDFox.py. . . . . . . . 'the python bridge of the RDFox system'
|  |  |  |  |-DefeasibleDatalog.py . . 'the DefeasibleDatalog implementation, i.e. the main purpose of this project'
|  |  |  |-main. . . . . . . . . . . . 'hosts a demonstration of how PRDFox.py is used'
|  |  |  |-feasibility_demo. . . . . . 'hosts the initial feasibility demonstration done for the project'
|  |  |  |-tests . . . . . . . . . . . 'hosts files used to test whether or not the RDFox system is working correctly'
|  |-lib
|  |  |-PRDFox.py .  . . . . . . . . . 'the python bridge of the RDFox system'
|  |  |-libCppRDFox.so . . . . . . . . 'the dynamic library used for Linux systems'
|  |  |-libCppRDFox.dylib. . . . . . . 'the dynamic library used for Mac OS systems'
|  |  |-CppRDFox . . . . . . . . . . . 'the main RDFox system'



-------------
FURTHER WORK:
-------------

This project was meant as a proof of concept, to prove that datalog could be extended to incorporate defeasible reasoning, and the implementation was developed as such.
In other words, minor optimisations have been incorporated and minimal test cases have been provided.
Development was undertaken without any intent of commercial use of the final product.
