# Honours-Project

2018 Honours Project: Creating a Defeasible Datalog, using RDFox.

-----------------
ABOUT THE PROJECT
-----------------

This project is meant to be a proof of concept. It shows that datalog can be extended to incorporate defeasible reasoning, using a modified version of the Rational Closure (RC) algorithm given by Britz et. al. [1]. Furthermore, this implementation was developed as such.

RDFox was solely made use of to perform the reasoning and materialisation tasks needed by this project. This was possible since the RC algorithm reduces defeasibile inference checks to a series of classical inference checks. However, in doing this, this project also extends RDFox with the capability to reason with defeasible information

[1] Katarina Britz, Thomas Meyer, and Ivan Varzinczak. 2011. Semantic Foundation for Preferential Description Logics. In Lecture Notes in Computer Science (including subseries Lecture Notes in Artificial Intelligence and Lecture Notes in Bioinformatics), Vol. 7106 LNAI. 491–500.


---------------------
STUCTURE OF THE REPO:
---------------------

|-DefRDFox
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


-------------
FURTHER WORK:
-------------

Since this project was mainly a proof of concept, minor optimisations have been implemented and minimal test cases have been provided. Also, development was undertaken without any intent of commercial use for the final product.

Thus, further work to be done could be increasing the efficiency and optimisation of the implementation, as well as purposing the defeasible reasoner for a more user-friendly environment.