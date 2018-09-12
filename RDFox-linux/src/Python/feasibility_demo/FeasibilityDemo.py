# Author:		Joshua J. Abraham
# Date:			July 2018
#Description:	An exploration of the basic functions provided by the Python RDFox bridge - main concept in mind is to extend RDFox with the capability of defeasible reasoning.
# Licence: RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.


### LINKS TO PAPERS: ###

## Defeasible Logics in Propositional Logic ##
## /home/fmanston/Desktop/Varsity/ComputerScience/Honours/Hons. Project/T.Meyer_Projects/DDLOG/Papers/The Journal of Logic Programming Volume 42 issue 1 2000 [doi 10.1016%2Fs0743-1066%2899%2900060-6] G. Antoniou; M.J. Maher; D. Billington -- Defeasible l.pdf

##


from PRDFox import DataStore, DataStoreType, TupleIterator, Datatype, ResourceType, UpdateType, Prefixes

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

def printQueryResult(tupleIterator):
    multiplicity = tupleIterator.open()
    while multiplicity > 0:
        print('    ' + ' '.join([getString(lexicalForm, datatype) for (lexicalForm, datatype) in tupleIterator.getResources()]) + " ." + (" # * " + str(multiplicity) if multiplicity > 1 else ""))
        multiplicity = tupleIterator.getNext()

def getQueryResult(tupleIterator):
    multiplicity = tupleIterator.open()
    result = ''
    while multiplicity > 0:
        result += ('    ' + ' '.join([getString(lexicalForm, datatype) for (lexicalForm, datatype) in tupleIterator.getResources()]) + " ." + (" # * " + str(multiplicity) if multiplicity > 1 else ""))
        multiplicity = tupleIterator.getNext()
    return(result)

# checkIfBird() is a test wrapper for a specific turtle data set.
def checkIfBird(queryResult):
	if ("<has> <Wings>" in queryResult and "<can> <Fly>" in queryResult):
		return("a 'Bird'.")
	elif ("<has> <Wings>" in queryResult and "<cant> <Fly>" in queryResult):
		return("a 'Bird'.")
	else:
		return("not a 'Bird'.")

#turtle_file = "data/lubm1.ttl"
#turtle_file = "data/lubm1-new.ttl"
#program_file = "data/LUBM_L.dlog"
#turtle_file = "data/demo.ttl"
#program_file = "data/demo.dlog"
turtle_file = "data/test_case1.ttl"
program_file = "data/test_rules1_classical.dlog"

print("\nSTARTING PROGRAMMING....\n")
DataStore.loadLibrary('../../../lib/libCppRDFox.so')

prefixes = Prefixes.DEFAULT

with DataStore(storeType = DataStoreType.PAR_COMPLEX_NN, parameters = {"equality" : "off"}) as dataStore:
	print("The number of triples before insertion: " + str(dataStore.getTriplesCount()))
	'''
	print("Adding triples to the store programmatically")
	dictionary = dataStore.getDictionary()
	dataStore.addTripleByResourceValues(["Bird", Datatype.IRI_REFERENCE], ["can", Datatype.IRI_REFERENCE], ["Fly", Datatype.IRI_REFERENCE])
	dataStore.addTripleByResourceValues(["Bird", Datatype.IRI_REFERENCE], ["has", Datatype.IRI_REFERENCE], ["Wings", Datatype.IRI_REFERENCE])
	dataStore.addTripleByResourceValues(["Robin", Datatype.IRI_REFERENCE], ["can", Datatype.IRI_REFERENCE], ["Fly", Datatype.IRI_REFERENCE])
	dataStore.addTripleByResourceValues(["Robin", Datatype.IRI_REFERENCE], ["has", Datatype.IRI_REFERENCE], ["Wings", Datatype.IRI_REFERENCE])
	dataStore.addTripleByResourceValues(["Penguin", Datatype.IRI_REFERENCE], ["cant", Datatype.IRI_REFERENCE], ["Fly", Datatype.IRI_REFERENCE])
	dataStore.addTripleByResourceValues(["Penguin", Datatype.IRI_REFERENCE], ["has", Datatype.IRI_REFERENCE], ["Wings", Datatype.IRI_REFERENCE])
	dataStore.addTripleByResourceValues(["Dolphin", Datatype.IRI_REFERENCE], ["cant", Datatype.IRI_REFERENCE], ["Fly", Datatype.IRI_REFERENCE])
	dataStore.addTripleByResourceValues(["Dolphin", Datatype.IRI_REFERENCE], ["hasnt", Datatype.IRI_REFERENCE], ["Wings", Datatype.IRI_REFERENCE])

	print("The number of triples after insertion: " + str(dataStore.getTriplesCount()))
	'''
	print("\nIMPORTING TURTLE FILE....")
	dataStore.importFile(turtle_file)
	print("The number of triples after import from '" + turtle_file + "': " + str(dataStore.getTriplesCount()))

	print("\nPRINTING TRIPLES...")
	with TupleIterator(dataStore, 'select ?x ?y ?z where { ?x ?y ?z }', {'query.domain' : 'IDB'}) as allTupleIterator:
		printQueryResult(allTupleIterator)

	print("\nAPPLYING THE RULES (i.e. doing materialisation)....")
	#dataStore.importFile(turtle_file)
	dataStore.importFile(program_file)
	dataStore.applyRules();
	print("The number of triples after materialisation: " + str(dataStore.getTriplesCount()))
	
	print("\nPRINTING TRIPLES...")
	with TupleIterator(dataStore, 'select ?x ?y ?z where { ?x ?y ?z }', {'query.domain' : 'IDB'}) as allTupleIterator:
		printQueryResult(allTupleIterator)

	with TupleIterator(dataStore, "select distinct ?y where { ?x ?y ?z }") as predicateTupleIterator:
		with TupleIterator(dataStore, "select distinct ?z where { ?x rdf:type ?z }", prefixes = prefixes) as conceptTupleIterator:
			print("\nPRINTING THE LIST OF PREDIACTES...")
			printQueryResult(predicateTupleIterator)
			print("\nPRINTING THE LIST OF CONCEPTS...")
			printQueryResult(conceptTupleIterator)
			with open (program_file, "r") as programFile:
				program = programFile.read()
			print("\ADDING RULES TO THE STORE")
			dataStore.importText(program)
			print("Setting the number of threads")
			dataStore.setNumberOfThreads(2)
			print("APPLYING REASONING")
			dataStore.applyRules()
			print("\nThe number of triples after reasoning: " + str(dataStore.getTriplesCount()))
			print("\nList of predicates")
			printQueryResult(predicateTupleIterator)
			print("\nList of concepts")
			printQueryResult(conceptTupleIterator)
			print("Done")

	
	with TupleIterator(dataStore, "select distinct ?y where { ?x ?y ?z }") as predicateTupleIterator:
		with TupleIterator(dataStore, "select distinct ?z where { ?x rdf:type ?z }", prefixes = prefixes) as conceptTupleIterator:
			print("\nPrinting the list of predicates")
			printQueryResult(predicateTupleIterator)
			print("Done")

	with TupleIterator(dataStore, "select ?y ?z where { <Bird> ?y ?z }") as mySubjectIterator:
		print("\nPrinting data for subject \"<Bird>\"")
		printQueryResult(mySubjectIterator)
		print("A 'Bird' is " + checkIfBird(getQueryResult(mySubjectIterator)))
		print("Done")
	with TupleIterator(dataStore, "select ?y ?z where { <Robin> ?y ?z }") as mySubjectIterator:
		print("\nPrinting data for subject \"<Robin>\"")
		printQueryResult(mySubjectIterator)
		print("A 'Robin' is " + checkIfBird(getQueryResult(mySubjectIterator)))
		print("Done")
	with TupleIterator(dataStore, "select ?y ?z where { <Penguin> ?y ?z }") as mySubjectIterator:
		print("\nPrinting data for subject \"<Penguin>\"")
		printQueryResult(mySubjectIterator)
		print("A 'Penguin' is " + checkIfBird(getQueryResult(mySubjectIterator)))
		print("Done")
	with TupleIterator(dataStore, "select ?y ?z where { <Dolphin> ?y ?z }") as mySubjectIterator:
		print("\nPrinting data for subject \"<Dolphin>\"")
		printQueryResult(mySubjectIterator)
		print("A 'Dolphin' is " + checkIfBird(getQueryResult(mySubjectIterator)))
		print("Done")