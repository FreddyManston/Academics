from DefeasibleDatalog import DefeasibleDatalog

if __name__ == "__main__":
	TBOX_PATH = "data/test_rules6.dlog"
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
	#QUERY = "<http://animals.test.example/hons/ability#Fly>(?X) :- <http://animals.test.example/hons/animal#Penguin>(?X)"
	#QUERY = "<http://defeasibledatalog.org/hons/negation#False> :- <http://animals.test.example/hons/animal#Penguin>(?X), <http://animals.test.example/hons/ability#Fly>(?X)"
	#QUERY = "<http://disease.test.example/hons/disease#BactMenStrain0>(?X) :- <http://disease.test.example/hons/mortality#Fatal>(?X)"
	QUERY = "<http://persons.test.example/hons/tax#isTaxed>(?X) :- <http://persons.test.example/hons/person#Student>(?X)"

	print("\nDOES '" + QUERY + "' ENTAIL FROM THE KNOWLEDGE BASE?")
	ANSWER = DDLOG.rationalClosure(RANKED_RULES, QUERY)

	if ANSWER:
		print("YES")
	elif not ANSWER:
		print("NO")
	else:
		print("ERROR: Unexpected exit from rationalClosure() function.")

	DDLOG.cleanUp()