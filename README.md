# Honours-Project

2018 Honours Project: Creating a Defeasible Datalog, using RDFox.


INITIAL PROCUREMENT OF SOURCE CODE:

All RDFoxc source code was downloaded from https://www.cs.ox.ac.uk/isg/tools/RDFox/ , between May and June of 2018.
This was while the source code was open source under a free academic licenceand before development, by Oxford Semantic Technologies, for commercial use had begun.

This software was written in Python and thus makes use of the main RDFox reasoner (written in C++) and its Python bridge, PRDFox. 
Thus, the Java bridge, JRDFox has been removed from this repository's source code.


OPERATING SYSTEM:

The Linux specific version of RDFox was made use of (as all development, for this project, was done on Linux systems).
Thus most of the essential code in this repo will only run on Linux systems.

The code could be run on a Mac system by changing all instances of libCppRDFox.so to libCppRDFox.dylib.
However, this has not been tested yet.


STUCTURE OF THE REPO:

The main code sources that were produced by this project can be found in RDFox-linux/src/Python/Defeasibility\ Wrapper and RDFox-linux/src/Python/feasibility_demo. 
These files contain the Defeasible Datalog implementation (which was the main purpose of this project) as well as a demo of the workings of RDFox, respectively.


FURTHER WORK:

This project was meant as a proof of concept, to prove that datalog could be extended to incorporate defeasible reasoning, and the implementation was developed as such.
In other words, minor optimisations have been incorporated and minimal test cases have been provided.
Development was undertaken without any intent of commercial use of the final product.
