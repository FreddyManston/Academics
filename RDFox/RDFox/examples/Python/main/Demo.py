# RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

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


turtleFileName = "data/lubm1.ttl"
additionalFileName = "data/lubm1-new.ttl"
programFileName = "data/LUBM_L.dlog"

DataStore.loadLibrary('../../../lib/libCppRDFox.dylib')

prefixes = Prefixes.DEFAULT

with DataStore(storeType = DataStoreType.PAR_COMPLEX_NN, parameters = {"equality" : "off"}) as dataStore:
    print("Adding triples to the store programmatically")
    dictionary = dataStore.getDictionary()
    dataStore.addTripleByResourceValues(["mysubject", Datatype.IRI_REFERENCE], ["mypredicate", Datatype.IRI_REFERENCE], ["myobject1", Datatype.IRI_REFERENCE])
    dataStore.addTripleByResourceValues(["mysubject", Datatype.IRI_REFERENCE], ["mypredicate", Datatype.IRI_REFERENCE], [b"r\xc3\xa9p\xc3\xa8te1", Datatype.IRI_REFERENCE])
    dataStore.addTripleByResourceValues(["mysubject", Datatype.IRI_REFERENCE], ["mypredicate", Datatype.IRI_REFERENCE], [u"r\u00E9p\u00E8te2", Datatype.IRI_REFERENCE])
    dataStore.addTripleByResourceValues(["mysubject", Datatype.IRI_REFERENCE], ["mypredicate", Datatype.IRI_REFERENCE], [b"B\xc3\xa4ume1", Datatype.IRI_REFERENCE])
    dataStore.addTripleByResourceValues(["mysubject", Datatype.IRI_REFERENCE], ["mypredicate", Datatype.IRI_REFERENCE], [u"B\u00E4ume2", Datatype.IRI_REFERENCE])
    dataStore.addTripleByResourceValues(["mysubject", Datatype.IRI_REFERENCE], ["mypredicate", Datatype.IRI_REFERENCE], [b"\xd0\xbf\xd1\x80\xd1\x8a\xd1\x81\xd1\x821", Datatype.IRI_REFERENCE])
    dataStore.addTripleByResourceValues(["mysubject", Datatype.IRI_REFERENCE], ["mypredicate", Datatype.IRI_REFERENCE], [u"\u043F\u0440\u044A\u0441\u04422", Datatype.IRI_REFERENCE])
    dataStore.addTripleByResourceValues(["mysubject", Datatype.IRI_REFERENCE], ["mypredicate", Datatype.IRI_REFERENCE], ["4", Datatype.XSD_DOUBLE])
    dataStore.addTripleByResourceValues(["mysubject", Datatype.IRI_REFERENCE], ["mypredicate", Datatype.IRI_REFERENCE], ["4", Datatype.XSD_INTEGER])
    dataStore.addTripleByResourceValues(["mysubject", Datatype.IRI_REFERENCE], ["mypredicate", Datatype.IRI_REFERENCE], ["04", Datatype.XSD_INTEGER])
    dataStore.addTripleByResources([ResourceType.IRI_REFERENCE, "mysubject", ""], [ResourceType.IRI_REFERENCE, "mypredicate", ""], [ResourceType.LITERAL, "5", "http://www.w3.org/2001/XMLSchema#double"])
    dataStore.addTriplesByResourceIDs([dictionary.resolveResourceValues("mysubject", Datatype.IRI_REFERENCE), dictionary.resolveResourceValues("mypredicate", Datatype.IRI_REFERENCE), dictionary.resolveResourceValues("6", Datatype.XSD_INTEGER)])
    dataStore.addTriplesByResourceIDs([dictionary.resolveResources(ResourceType.IRI_REFERENCE, "mysubject", ""), dictionary.resolveResources(ResourceType.IRI_REFERENCE, "mypredicate", ""), dictionary.resolveResources(ResourceType.LITERAL, "6", "http://www.w3.org/2001/XMLSchema#float")])
    dataStore.addTriplesByResourceIDs(dictionary.resolveResourceValues(["mysubject", "mypredicate", "7"], [Datatype.IRI_REFERENCE, Datatype.IRI_REFERENCE, Datatype.XSD_INTEGER]))
    dataStore.addTriplesByResourceIDs(dictionary.resolveResources([ResourceType.IRI_REFERENCE, ResourceType.IRI_REFERENCE, ResourceType.LITERAL], ["mysubject", "mypredicate", "7"], ["", "", "http://www.w3.org/2001/XMLSchema#float"]))
    dataStore.importText('<mySubject> rdf:type <myClass1>, <myClass2>; <mypredicate> <myobject2>, <myobject3> .', prefixes = prefixes)
    
    print("The number of triples after insertion: " + str(dataStore.getTriplesCount()))
    print("Printing triples:")
    with TupleIterator(dataStore, 'select ?x ?y ?z where { ?x ?y ?z }', {'query.domain' : 'IDB'}) as allTupleIterator:
        printQueryResult(allTupleIterator)
    print("Importing turtle file")
    dataStore.importFile(turtleFileName)
    print("The number of triples after import from file: " + str(dataStore.getTriplesCount()))
    with TupleIterator(dataStore, "select distinct ?y where { ?x ?y ?z }") as predicateTupleIterator:
        with TupleIterator(dataStore, "select distinct ?z where { ?x rdf:type ?z }", prefixes = prefixes) as conceptTupleIterator:
            print("Printing the list of predicates")
            printQueryResult(predicateTupleIterator)
            print("Printing the list of concepts")
            printQueryResult(conceptTupleIterator)
            with open (programFileName, "r") as programFile:
                program = programFile.read()
            print("Adding rules to the store")
            dataStore.importText(program)
            print("Setting the number of threads")
            dataStore.setNumberOfThreads(2)
            print("Applying reasoning")
            dataStore.applyRules()
            print("The number of triples after reasoning: " + str(dataStore.getTriplesCount()))
            print("List of predicates")
            printQueryResult(predicateTupleIterator)
            print("List of concepts")
            printQueryResult(conceptTupleIterator)
            print("Done")
    with TupleIterator(dataStore, "select ?y ?z where { <mysubject> ?y ?z }") as mySubjectIterator:
        print("Printing data for subject \"<mysubject>\"")
        printQueryResult(mySubjectIterator)
        print("Done")
    print("Scheduling tuples for addition")
    dataStore.importFile(additionalFileName, UpdateType.SCHEDULE_FOR_ADDITION);
    print("The number of triples after scheduling: " + str(dataStore.getTriplesCount()))
    print("Reasoning incrementally")
    dataStore.applyRules(True)
    print("The number of triples after incremental reasoning: " + str(dataStore.getTriplesCount()))
    print("Scheduling tuples for deletion")
    dataStore.importFile(additionalFileName, UpdateType.SCHEDULE_FOR_DELETION);
    print("The number of triples after scheduling: " + str(dataStore.getTriplesCount()))
    print("Reasoning incrementally")
    dataStore.applyRules(True)
    print("The number of triples after incremental reasoning: " + str(dataStore.getTriplesCount()))
    print("Saving store")
    dataStore.save("mystore.fmt")
print("Loading saved store")
with DataStore(fileName="mystore.fmt") as loadedDataStore:
    with TupleIterator(loadedDataStore, "select ?y ?z where { <mysubject> ?y ?z }") as mySubjectIterator:
        print("Printing data for subject \"<mysubject>\" from the loaded store")
        printQueryResult(mySubjectIterator)
        print("Done")

