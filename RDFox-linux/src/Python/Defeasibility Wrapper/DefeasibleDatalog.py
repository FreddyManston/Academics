'''
# Author:		Joshua J. Abraham
# Date:			July 2018
#Description:	The implementation of the Rational Closure 
#				  algorithm (Introducing Defeasibility into OWL Ontologies, G. Casini et. al.) 
#				  as a Python Wrapper for RDFox (https://www.cs.ox.ac.uk/isg/tools/RDFox/).
# Licence:		RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.
# Note:			This program makes use of datalog (.dlog files) for the TBox, as well as 
#				  the Terse RDF Triple Language (Turtle, .ttl files) for the ABox.
'''

import shutil, os, sys, platform, io, errno, re
from PRDFox import DataStore, DataStoreType, TupleIterator, Datatype, ResourceType, UpdateType, Prefixes

class DefeasibleDatalog:

	PREFIXES = []
	DD_DATALOG_FILE = "PRDFoxDW_RankedRules.dlog"
	DD_TURTLE_FILE = "PRDFoxDW_MatTest.ttl"


### FUNCTIONS FOR PRDFox ###

	def getString(self, lexicalForm, datatype):
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

	def getQueryResult(self, tupleIterator):
		multiplicity = tupleIterator.open()
		result = []
		while multiplicity > 0:
			result.append('    ' + ' '.join([self.getString(lexicalForm, datatype) for (lexicalForm, datatype) in tupleIterator.getResources()]) + " ." + (" # * " + str(multiplicity) if multiplicity > 1 else ""))
			multiplicity = tupleIterator.getNext()
		return(result)

	'''
	Performs materialisation using RDFox and returns the results as a string
	'''
	def performRDFoxMaterialisation(self, turtle_file, datalog_file):
		try:
			library_path = ''
			if(platform.system() == 'Linux'):
				library_path = '../../../lib/libCppRDFox.so'
			elif(platform.system() == 'Darwin'):	# i.e. Mac
				library_path = '../../../lib/libCppRDFox.dylib'
			else:
				raise Exception("OS mismatch error. The materialisations have not been catered to perform on your operating system as yet. Sorry. Please use either a Linux (preferable) or Mac machine.")

			if os.path.isfile(library_path):
				DataStore.loadLibrary(library_path)
			else:
				raise IOError(errno.ENOENT, os.strerror(errno.ENOENT), library_path)

			with DataStore(storeType = DataStoreType.PAR_COMPLEX_NN, parameters = {"equality" : "off"}) as dataStore:
				dataStore.importFile(turtle_file)
				dataStore.importFile(datalog_file)
				dataStore.applyRules();
				with TupleIterator(dataStore, 'select ?x ?y ?z where { ?x ?y ?z }', {'query.domain' : 'IDB'}) as allTupleIterator:
					return(self.getQueryResult(allTupleIterator))

		except Exception as exception:
			print("ERROR WHEN PERFORMING RDFOX MATERIALISATION.")
			print("ERROR: " + str(exception))
			sys.exit(0)


### FUNCTIONS FOR THE DEFEASIBLE DATALOG WRAPPER ###

	'''
	Imports the rules from the datalog file and initialises DD_DATALOG_FILE and DD_TURTLE_FILE
	with the PREFIXES so that they can be used for testing
	'''
	def initialise(self, PATH):
		try:
			#os.makedirs("DefeasibleDatalog_TestData")
			C_TBOX = []
			D_TBOX = []

			if not os.path.isfile(PATH):
				raise Exception(PATH + " does not exist.")

			with open(PATH, "r") as C_TBOX_FILE, open(self.DD_TURTLE_FILE, "w+") as TURTLE_TEST_FILE, open(self.DD_DATALOG_FILE, "w+") as DLOG_RANK_FILE:
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

		except Exception as exception:
			print("ERROR WHEN IMPORTING THE DATALOG RULES (.DLOG FILE).")
			print("ERROR: " + str(exception))
			sys.exit(0)

	'''
	If no consequent is given then,
			checks if the given antecedent can be entailed from the given knowledge base
	If a consequent is given then,
	 		checks if the consequent can be entailed from the the given antecedent
	'''
	def doesEntail(self, knowledge_base, antecedent, consequent=None):
		ENTAILS = True

		with open(self.DD_DATALOG_FILE, "r") as DLOG_RANK_FILE, open(self.DD_TURTLE_FILE, "r") as TURTLE_TEST_FILE:
			DLOG_RANKS = DLOG_RANK_FILE.readlines()
			TURTLE_TEST = TURTLE_TEST_FILE.readlines()

		for rule in knowledge_base:
			DLOG_RANKS.append(rule)

		TURTLE_TEST.append("<http://ddlog.test.example> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " + antecedent + " .")

		# ADDING RULES AND TEST TRIPLE TO THE FILES
		with open(self.DD_DATALOG_FILE, "w+") as DLOG_RANK_FILE, open(self.DD_TURTLE_FILE, "w+") as TURTLE_TEST_FILE:
			for line in DLOG_RANKS:
				DLOG_RANK_FILE.write(line)
				if "\n" not in line:
					DLOG_RANK_FILE.write("\n")
			for line in TURTLE_TEST:
				TURTLE_TEST_FILE.write(line)
				if "\n" not in line:
					TURTLE_TEST_FILE.write("\n")

		# CHECKING FOR ENTAILMENT
		MATERIALISATION = self.performRDFoxMaterialisation(self.DD_TURTLE_FILE, self.DD_DATALOG_FILE)
		if consequent is None:
			for triple in MATERIALISATION:
				if "<http://ddlog.test.example> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://defeasibledatalog.org/hons/negation#False> ." in triple:
					ENTAILS = False
					break
		else:
			ENTAILS = False
			for triple in MATERIALISATION:
				if "<http://ddlog.test.example> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> "+ consequent + " ." in triple:
					ENTAILS = True
					break

		# CLEANING THE FILES AFTER USE:
		DLOG_RANKS = [line for line in DLOG_RANKS if "PREFIX" in line]
		TURTLE_TEST = [line for line in TURTLE_TEST if "PREFIX" in line]
		with open(self.DD_DATALOG_FILE, "w+") as DLOG_RANK_FILE, open(self.DD_TURTLE_FILE, "w+") as TURTLE_TEST_FILE:
			for line in DLOG_RANKS:
				DLOG_RANK_FILE.write(line)
				if "\n" not in line:
					DLOG_RANK_FILE.write("\n")
			for line in TURTLE_TEST:
				TURTLE_TEST_FILE.write(line)
				if "\n" not in line:
					TURTLE_TEST_FILE.write("\n")

		return ENTAILS

	'''
	Acquires the antecedent, given a single datalog rule
	'''
	def getAntecedent(self, dlog_rule):
		# Text cleaning...
		dlog_rule = re.sub("(\(\?.\))", "", dlog_rule)	# Getting rid of all variables (e.g. (?X))
		dlog_rule = re.sub(" \.", "", dlog_rule)		# Getting rid of the last dot, i.e. " ."
		dlog_rule = re.sub("\s", "", dlog_rule)			# Getting rid of all remainingwhite spaces
		#dlog_rule = re.sub("[\s\.]*", "", dlog_rule)	# Getting rid of all white space and dots.
		antecedent = dlog_rule.split(":-")[1]			# Getting rid of the consequent
		antecedent = antecedent.split(",")[0]			# For rules such as: 'neg:False(?X) :- animal:Penguin(?X), ability:Fly(?X) .'

		return antecedent

	'''
	Acquires the consequent, given a single datalog rule
	'''
	def getConsequent(self, dlog_rule):
		# Text cleaning...
		dlog_rule = re.sub("(\(\?.\))", "", dlog_rule)	# Getting rid of all variables (e.g. (?X))
		dlog_rule = re.sub(" \.", "", dlog_rule)		# Getting rid of the last dot, i.e. " ."
		dlog_rule = re.sub("\s", "", dlog_rule)			# Getting rid of all remainingwhite spaces
		#dlog_rule = re.sub("[\s\.]*", "", dlog_rule)	# Getting rid of all white space and dots.
		consequent = dlog_rule.split(":-")[0]			# Getting rid of the antecedent

		return consequent

	'''
	Flags all exceptionalities, given a set of classical and defeasible rules.
	Returns all defeasible rules that are exceptional
	'''
	def checkExceptionality(self, C_TBOX, D_TBOX):
		EXCEPTIONS = []
		FULL_TBOX = C_TBOX + D_TBOX

		for rule in D_TBOX:
			antecedent = self.getAntecedent(rule)
			# Check if not the antecedent holds, i.e. a clash w.r.t. the antecedent can be found
			if not self.doesEntail(FULL_TBOX, antecedent):
				EXCEPTIONS.append(rule)

		return EXCEPTIONS

	'''
	Ranks all the defeasible rules according to exceptionality,
	using the Ranking algorithm found in Introducing Defeasibility into OWL Ontologies, G. Casini et. al.
	'''
	def rankRules(self, C_TBOX, D_TBOX):
		RANKS = []

		# INITIAL EXCEPTIONALITY CHECK
		E0 = D_TBOX
		E1 = self.checkExceptionality(C_TBOX, E0)

		if(len(E1) == 0):								# i.e. no contradictions found
			if(len(E0) > 0):
				RANKS.append(E0)
			RANKS.append(C_TBOX)
			return RANKS

		elif(set(E1) == set(E0)):						# i.e. all defeasible rules give a contradiction
			for rule in E1:
				C_TBOX.append(rule)
			RANKS.append(C_TBOX)
			return RANKS

		# FOLLOWING EXCEPTIONALITY CHECKS
		while(set(E1) != set(E0) and len(E1) != 0):
			RANKS.append(list(set(E0) - set(E1)))
			E0 = E1
			E1 = self.checkExceptionality(C_TBOX, E0)
			
		# ADDING THE LAST EXCEPTIONAL RANK
		if(len(E1) == 0):								# i.e. no contradictions found
			RANKS.append(E0)
		elif(set(E1) == set(E0)):						# i.e. all defeasible rules give a contradiction
			for rule in E1:
				C_TBOX.append(rule)
		else:
			print("Unexpected exit in rankRules().")
			sys.exit()

		# ADDING THE CLASSICAL RULES, i.e. the infinite level
		RANKS.append(C_TBOX)
		RANKS.reverse()								# puts infinite rank in first position, then rank n-1, rank n-2 ... rank 0

		return RANKS

	'''
	Answers a query (classical or defeasible) to a defeasible knowledge base
	Knowledge base is a datalog file (.dlog) and query is a datalog rule
	'''
	def rationalClosure(self, ranked_rules, query):
		antecedent = self.getAntecedent(query)
		consequent = self.getConsequent(query)
		i = len(ranked_rules) - 1
		ENTAILS = None

		# Stop loop when only the infinite rank (i.e. classical rules) remains
		while(i >= 1):
			knowledge_base = []
			for rank in ranked_rules:
				for rule in rank:
					knowledge_base.append(rule)

			# FINDING THE CORRECT RANK
			if not (self.doesEntail(knowledge_base, antecedent)):
				del ranked_rules[i]

			# CHECKING THE ENTAILMENT, ONCE THE CORRECT RANK HAS BEEN FOUND
			else:
				ENTAILS = self.doesEntail(knowledge_base, antecedent, consequent)
			i -= 1

		# i.e. all ranks have been eliminated, except for the infinite rank
		if(i == 0):
			ENTAILS = self.doesEntail(ranked_rules[0], antecedent, consequent)

		return ENTAILS

	'''
	Deletes all files and/or folders that were created by this program
	'''
	def cleanUp(self):
		print("\nCOMMENCING CLEAN UP...")
		os.remove(self.DD_DATALOG_FILE) 
		os.remove(self.DD_TURTLE_FILE) 
		#shutil.rmtree("DefeasibleDatalog_TestData")
		print("CLEAN UP COMPLETED")

### START OF MAIN ###

if __name__ == "__main__":
	TBOX_PATH = "data/test_rules1.dlog"
	DDLOG = DefeasibleDatalog()

	print("\nSTARTING PROGRAMMING...")
	print("\nIMPORTING THE TBox (i.e. the datalog/.dlog file)...")

	K = DDLOG.initialise(TBOX_PATH)	# The Knowledge Base
	C_TBOX = K[0]					# Classical TBox
	D_TBOX = K[1]					# Defeasible TBox
	print("\nTHE IMPORTED CLASSICAL DATALOG RULES ARE:")
	for rule in C_TBOX:
		print("\t" + rule)
	print("THE IMPORTED DEFEASIBLE DATALOG RULES ARE:")
	for rule in D_TBOX:
		print("\t" + rule)

	print("\nRANKING THE RULES...")
	RANKED_RULES = DDLOG.rankRules(C_TBOX, D_TBOX)

	print("\nRULES HAVE BEEN RANKED AS FOLLOWS:")
	print("Level " + u"\u221E" + ":")						# Infinite/Classical level
	for rule in RANKED_RULES[0]:
		print("\t" + rule)
	if (len(RANKED_RULES) > 1):
		for level in range(len(RANKED_RULES) - 1):			# Exceptional/Defeasible levels
			print("Level " + str(len(RANKED_RULES) - (level + 2)) + ":")
			for rule in RANKED_RULES[level + 1]:
				print("\t" + rule)

	print("\nDOING QUERY...")
	QUERY = "<http://animals.test.example/hons/ability#Fly>(?X) :- <http://animals.test.example/hons/animal#Penguin>(?X)"
	#QUERY = "<http://defeasibledatalog.org/hons/negation#False> :- <http://animals.test.example/hons/animal#Penguin>(?X), <http://animals.test.example/hons/ability#Fly>(?X)"
	#QUERY = "<http://disease.test.example/hons/disease#Men>(?X) :- dis:VirMen(?X)"
	#QUERY = "<http://persons.test.example/hons/tax#isTaxed>(?X) :- <http://persons.test.example/hons/person#Student>(?X)"

	print("\nDOES '" + QUERY + "' ENTAIL FROM THE KNOWLEDGE BASE?")
	ANSWER = DDLOG.rationalClosure(RANKED_RULES, QUERY)

	if ANSWER:
		print("YES")
	elif not ANSWER:
		print("NO")
	else:
		print("ERROR: Unexpected exit from rationalClosure() function.")

	DDLOG.cleanUp()