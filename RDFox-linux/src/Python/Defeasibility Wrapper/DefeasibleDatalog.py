# Author:		Joshua J. Abraham
# Date:			July 2018
#Description:	The implementation of the Rational Closure algorithm (Introducing Defeasibility into OWL Ontologies, G. Casini et. al.) as a Python Wrapper for RDFox (https://www.cs.ox.ac.uk/isg/tools/RDFox/).
# Licence: RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.
# Note: This program makes use of datalog (.dlog files) for the TBox, as well as 
#		the Terse RDF Triple Language (Turtle, .ttl files) for the ABox.

import shutil, os, sys, re, io
from PRDFox import DataStore, DataStoreType, TupleIterator, Datatype, ResourceType, UpdateType, Prefixes

DD_DATALOG_FILE = "PRDFoxDW_RankedRules.dlog"
DD_TURTLE_FILE = "PRDFoxDW_MatTest.ttl"


### FUNCTIONS FOR PRDFox ###

def getString(lexicalForm, datatype):
    if (datatype == Datatype.INVALID):
        return "UNDEF"
    if (datatype == Datatype.IRI_REFERENCE):
        return "<" + lexicalForm + ">"
    if (datatype == Datatype.BLANK_NODE):
        return "_:" + lexicalForm
    if (datatype == Datatype.XSD_INTEGER):
        return lexicalForm
    if (datatype == Datatype.XSD_STRING):
        return '\"' + lexicalForm + '\"'
    if (datatype == Datatype.RDF_PLAIN_LITERAL):
        atPosition = lexicalForm.rfind('@');
        return '\"' + lexicalForm[0, atPosition - 1] + '\"' + lexicalForm[atPosition:];
    return '\"' + lexicalForm + "\"^^<" + datatype.IRI + ">";

def getQueryResult(tupleIterator):
	multiplicity = tupleIterator.open()
	result = []
	while multiplicity > 0:
		result.append('    ' + ' '.join([getString(lexicalForm, datatype) for (lexicalForm, datatype) in tupleIterator.getResources()]) + " ." + (" # * " + str(multiplicity) if multiplicity > 1 else ""))
		multiplicity = tupleIterator.getNext()
	return(result)

# Performs materialisation using RDFox and returns the results as a string
def performRDFoxMaterialisation(turtle_file, datalog_file):
	#print("\nDOING MATERIALISATION....")
	DataStore.loadLibrary('../../../lib/libCppRDFox.so')
	with DataStore(storeType = DataStoreType.PAR_COMPLEX_NN, parameters = {"equality" : "off"}) as dataStore:
		dataStore.importFile(turtle_file)
		dataStore.importFile(datalog_file)
		dataStore.applyRules();
		with TupleIterator(dataStore, 'select ?x ?y ?z where { ?x ?y ?z }', {'query.domain' : 'IDB'}) as allTupleIterator:
			return(getQueryResult(allTupleIterator))


### FUNCTIONS FOR THE DEFEASIBLE DATALOG WRAPPER ###

# Imports the rules from the datalog file and initialises DD_DATALOG_FILE and DD_TURTLE_FILE
# with the PREFIXES so that they can be used for testing
def initialise(PATH):
	try:
		#os.makedirs("DefeasibleDatalog_TestData")
		C_TBOX = []
		D_TBOX = []

		with open(PATH, "r") as C_TBOX_FILE, open(DD_TURTLE_FILE, "w+") as TURTLE_TEST_FILE, open(DD_DATALOG_FILE, "w+") as DLOG_RANK_FILE:
			CLASSICAL_TBOX = C_TBOX_FILE.readlines()

			# IMPORTING THE DATALOG RULES (i.e. the .dlog file)
			section = ""
			for line in CLASSICAL_TBOX:
				# If line contains a PREFIX...
				if ("PREFIX" in line):
					TURTLE_TEST_FILE.write(line)
					DLOG_RANK_FILE.write(line)
				# If line is a CLASSICAL/DEFEASIBLE header...
				elif ("CLASSICAL STATEMENTS" in line):
					section = "classical"
				elif ("DEFEASIBLE STATEMENTS" in line):
					section = "defeasible"
				# If line is a comment or is empty...
				elif (line[0] == '#' or line == '\n'):
					continue
				# If line is a datalog rule...
				else:
					if (section == "classical"):
						C_TBOX.append(line.strip("\n"))
					elif (section == "defeasible"):
						D_TBOX.append(line.strip("\n"))
					else:
						print("Invalid datalog syntax/structure.")

		return([C_TBOX, D_TBOX])

	except:
		print("Error when importing the datalog rules (.dlog file).")

# Checks if a given antecedent can be entailed from a given knowledge base
def doesEntail(knowledge_base, query):
	ENTAILS = True

	with open(DD_DATALOG_FILE, "r") as DLOG_RANK_FILE, open(DD_TURTLE_FILE, "r") as TURTLE_TEST_FILE:
		DLOG_RANKS = DLOG_RANK_FILE.readlines()
		TURTLE_TEST = TURTLE_TEST_FILE.readlines()

	#print("\nPRINTING THE FILES:")
	#print DLOG_RANKS
	#print TURTLE_TEST
	#print("\nKNOWLEDGE BASE:")
	#print knowledge_base
	#print("\nQUERY:")
	#print query

	for rule in knowledge_base:
		DLOG_RANKS.append(rule)
	#print("\nDLOG RANKS ARRAY")
	#print DLOG_RANKS
	TURTLE_TEST.append("<http://ddlog.test.example> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " + query + " .")
	#print("\nTURTLE TEST ARRAY")
	#print TURTLE_TEST

	# ADDING RULES AND TEST TRIPLE TO THE FILES
	with open(DD_DATALOG_FILE, "w+") as DLOG_RANK_FILE, open(DD_TURTLE_FILE, "w+") as TURTLE_TEST_FILE:
		for line in DLOG_RANKS:
			DLOG_RANK_FILE.write(line)
			if "\n" not in line:
				DLOG_RANK_FILE.write("\n")
		for line in TURTLE_TEST:
			TURTLE_TEST_FILE.write(line)
			if "\n" not in line:
				TURTLE_TEST_FILE.write("\n")

	# CHECKING FOR ENTAILMENT
	MATERIALISATION = performRDFoxMaterialisation(DD_TURTLE_FILE, DD_DATALOG_FILE)
	for triple in MATERIALISATION:
		if "<http://ddlog.test.example> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://defeasibledatalog.org/hons/negation#False> ." in triple:
			ENTAILS = False

	# CLEANING THE FILES AFTER USE:
	DLOG_RANKS = [line for line in DLOG_RANKS if "PREFIX" in line]
	TURTLE_TEST = [line for line in TURTLE_TEST if "PREFIX" in line]
	with open(DD_DATALOG_FILE, "w+") as DLOG_RANK_FILE, open(DD_TURTLE_FILE, "w+") as TURTLE_TEST_FILE:
		for line in DLOG_RANKS:
			DLOG_RANK_FILE.write(line)
			if "\n" not in line:
				DLOG_RANK_FILE.write("\n")
		for line in TURTLE_TEST:
			TURTLE_TEST_FILE.write(line)
			if "\n" not in line:
				TURTLE_TEST_FILE.write("\n")

	#print("Does it entail: " + str(ENTAILS))
	return ENTAILS

# Acquires the antecedent, given a single datalog rule
def getAntecedent(dlog_rule):
	# Text cleaning...
	dlog_rule = re.sub("(\(\?.\))", "", dlog_rule)	# Getting rid of all variables (e.g. (?X))
	dlog_rule = re.sub("[\s\.]*", "", dlog_rule)	# Getting rid of all white space and dots.
	antecedent = dlog_rule.split(":-")[1]			# Getting rid of the consequent
	antecedent = antecedent.split(",")[0]			# For rules such as: 'neg:False(?X) :- animal:Penguin(?X), ability:Fly(?X) .'

	return antecedent

# Flags all exceptionalities, given a set of classical and defeasible rules.
# Returns all defeasible rules that are exceptional
def checkExceptionality(C_TBOX, D_TBOX):
	EXCEPTIONS = []
	FULL_TBOX = C_TBOX + D_TBOX

	for rule in D_TBOX:
		antecedent = getAntecedent(rule)

		if not doesEntail(FULL_TBOX, antecedent):
			EXCEPTIONS.append(rule)

	#print("\nTHE EXCEPTIONS ARE.... ")
	#print EXCEPTIONS
	return EXCEPTIONS

# Ranks all the defeasible rules according to exceptionality,
# using the Ranking algorithm found in Introducing Defeasibility into OWL Ontologies, G. Casini et. al.
def rankRules(C_TBOX, D_TBOX):
	RANKS = []

	# INITIAL EXCEPTIONALITY CHECK
	E0 = D_TBOX
	E1 = checkExceptionality(C_TBOX, E0)
	#print("\nE0 and E1, respectively:")
	#print E0
	#print E1
	if(len(E1) == 0):								# i.e. no contradictions found
		for rule in E0:
			C_TBOX.append(rule)
		return C_TBOX
	elif(set(E1) == set(E0)):						# i.e. all defeasible rules give a contradiction
		RANKS.append(E1)
		RANKS.append(C_TBOX)
		return RANKS
	#print("\nCURRENT RANKS:")
	#print RANKS

	# FOLLOWING EXCEPTIONALITY CHECKS
	while(set(E1) != set(E0) and len(E1) != 0):
		#print("\nE0 and E1, respectively:")
		#print E0
		#print E1
		RANKS.append(list(set(E0) - set(E1)))
		#print("\nCURRENT RANKS:")
		#print RANKS
		E0 = E1
		E1 = checkExceptionality(C_TBOX, E0)
		
	# ADDING THE LAST EXCEPTIONAL RANK
	if(len(E1) == 0):								# i.e. no contradictions found
		for rule in E0:
			C_TBOX.append(rule)
	elif(set(E1) == set(E0)):						# i.e. all defeasible rules give a contradiction
		RANKS.append(E1)
	else:
		print("Unexpected exit in rankRules().")
		sys.exit()

	# ADDING THE CLASSICAL RULES, i.e. the infinite level
	RANKS.append(C_TBOX)

	return RANKS

# Deletes all files and/or folders that were created by this program
def cleanUp():
	os.remove(DD_DATALOG_FILE) 
	os.remove(DD_TURTLE_FILE) 
	#shutil.rmtree("DefeasibleDatalog_TestData")

### START OF MAIN ###

CLASSICAL_TBOX_PATH = "data/test_rules2_classical.dlog"
DEFEASIBLE_TBOX_PATH = "data/test_rules2_defeasible.dlog"

print("\nSTARTING PROGRAMMING...")
print("\nIMPORTING THE TBox (i.e. the datalog/.dlog file)...")

K = initialise(CLASSICAL_TBOX_PATH)
C_TBOX = K[0]
D_TBOX = K[1]
print("\nTHE IMPORTED CLASSICAL DATALOG RULES ARE:")
for rule in C_TBOX:
	print("\t" + rule)
print("THE IMPORTED DEFEASIBLE DATALOG RULES ARE:")
for rule in D_TBOX:
	print("\t" + rule)

print("\nRANKING THE RULES...")
RANKED_RULES = rankRules(C_TBOX, D_TBOX)

print("\nRULES HAVE BEEN RANKED AS FOLLOWS:")
RANKED_RULES.reverse()
print("Level " + u"\u221E" + ":")						# Infinite/Classical level
for rule in RANKED_RULES[0]:
	print("\t" + rule)
if(len(RANKED_RULES) > 1):
	for level in range(len(RANKED_RULES) - 1):			# Exceptional/Defeasible levels
		print("Level " + str(len(RANKED_RULES) - (level + 2)) + ":")
		for rule in RANKED_RULES[level + 1]:
			print("\t" + rule)

cleanUp()