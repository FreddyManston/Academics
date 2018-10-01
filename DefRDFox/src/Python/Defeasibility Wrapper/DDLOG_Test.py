'''
# Author:		Joshua J. Abraham
# Start Date:	July 2018
# Last Edit:	Sept. 2018
# Description:	A test for the DefeasibleDatalog reasoner, to show its correctness.
#
#				Loops through all provided test case and tests each one sequentially. Also uses the given queries, each one
#				associated with their specific test case, when testing query resolution
#				
#				Test rules should be found in the 'data/' folder and should have the '.dlog' extension
#				Test queries, for each test file, should be stored in one file which has the same name as the test file, 
#				but the extension '.qr' replaced with '.dlog'.
#				
#				Also, queries should be in the form of a datalog rule and contain full URNs as PREFIXES are not yet supported
#				for queries,
#					e.g. test.example/animal#Bird(?X) :- test.example/animal#Penguin(?X)
#					to test whether a Penguin is a Bird in the provided Ranked Knowledge Base.
#
# Licence:		RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.
# Note:			This program makes use of datalog (.dlog files) for the TBox, as well as 
#				  the Terse RDF Triple Language (Turtle, .ttl files) for the ABox.
'''

import os, glob, time
from DefeasibleDatalog import DefeasibleDatalog

if __name__ == "__main__":
	print("\nBEGINNING THE TESTS....")
	PATH = 'data/'				# the path to the data file

	for DLOG_FILE_PATH in glob.glob(os.path.join(PATH, '*.dlog')):
		print("\n\n-------------------------------------------------------")
		print("\nTESTING FILE: " + DLOG_FILE_PATH)
		TBOX_PATH = DLOG_FILE_PATH
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
		START = time.time()
		RANKED_RULES = DDLOG.rankRules(C_TBOX, D_TBOX)
		print("\nRANKING TOOK %s seconds" % round((time.time() - START), 4))
		print("RULES HAVE BEEN RANKED AS FOLLOWS:")
		print("Level " + u"\u221E" + ":")						# Infinite/Classical level
		for rule in RANKED_RULES[0]:
			print("\t" + rule)
		if (len(RANKED_RULES) > 1):
			for level in range(len(RANKED_RULES) - 1):			# Exceptional/Defeasible levels
				print("Level " + str(len(RANKED_RULES) - (level + 2)) + ":")
				for rule in RANKED_RULES[level + 1]:
					print("\t" + rule)

		print("\n\nPERFORMING QUERIES...")

		QUERY_FILE_PATH = DLOG_FILE_PATH[:-4] + "qr"	# changing the extension from .dlog to .qr
		with open(QUERY_FILE_PATH, "r") as QUERY_FILE:

			QFILE = QUERY_FILE.readlines()

			for QUERY in QFILE:
				print("\nDOES '" + QUERY.replace("\n", "") + "' ENTAIL FROM THE KNOWLEDGE BASE?")
				ANSWER = DDLOG.rationalClosure(RANKED_RULES, QUERY)

				if ANSWER:
					print("YES")
				elif not ANSWER:
					print("NO")
				else:
					print("ERROR: Unexpected exit from rationalClosure() function.")

		DDLOG.cleanUp()


print("\n\n-------------------------------------------------------")
print("\nALL TESTS COMPLETED WITHOUT ISSUE.")