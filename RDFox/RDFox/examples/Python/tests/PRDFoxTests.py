import unittest
import sys
import os
sys.path.append(os.path.abspath('../main/'))
from PRDFox import DataStore, DataStoreType, TupleIterator, Datatype, ResourceType, QueryDomain, EqualityType, Prefixes, UpdateType

DataStore.loadLibrary('../../../lib/libCppRDFox.dylib')

class PRDFoxTests(unittest.TestCase):
    
    def test_basic(self):
        with DataStore(storeType = DataStoreType.PAR_COMPLEX_NN, parameters = {'equality' : EqualityType.NO_UNA}) as store:
            self.assertEquals(0, store.getTriplesCount())
            text = ''
            loopLength = 100
            for index in range(0, loopLength):
                text = ''.join([text, "<a", str(index), "> <R> <a", str((index + 1) % loopLength), "> ."]);
            store.importText(text);
            self.assertEquals(loopLength, store.getTriplesCount());
            store.importText("<R>(?x, ?z) :- <R>(?x, ?y), <R>(?y, ?z) .");
#           perform materialisation
            store.applyRules();
#           the IDBs should be loopLength * loopLength (for facts of the form <ai> <R> <aj>.) plus loopLength (for facts of the form <ai> owl:sameAs <ai>) plus 2 (for the facts <R> owl:sameAs <R>. and owl:sameAs owl:sameAs owl:sameAs.)
            self.assertEquals(loopLength * loopLength + loopLength + 2, store.getTriplesCount(QueryDomain.IDB));
            self.assertEquals(loopLength * loopLength + loopLength + 2, store.getTriplesCount(QueryDomain.IDB_REP));
            self.assertEquals(loopLength, store.getTriplesCount(QueryDomain.EDB));
            self.assertEquals(loopLength * loopLength + 2, store.getTriplesCount(QueryDomain.IDB_REP_NO_EDB));
#           incrementally add a rule
            store.importText("[?y1, owl:sameAs, ?y2] :- <R>(?x, ?y1) , <R>(?x, ?y2) . ", prefixes = Prefixes.DEFAULT, updateType = UpdateType.SCHEDULE_FOR_ADDITION);
            store.applyRules(True);
#           the idbs should be  loopLength * loopLength (for facts of the form <ai> <R> <aj>.) plus loopLength * loopLength (for facts of the form <ai> owl:sameAs <aj>) plus 2 (for the facts <R> owl:sameAs <R>. and owl:sameAs owl:sameAs owl:sameAs.)
            self.assertEquals(loopLength * loopLength + loopLength * loopLength + 2, store.getTriplesCount(QueryDomain.IDB));
#           the representative idbs should be 4: <ai> <R> <ai> (for one of the indexes i) and 3 equality statements for <ai> <R> and owl:sameAs
            self.assertEquals(4, store.getTriplesCount(QueryDomain.IDB_REP));
            self.assertEquals(loopLength, store.getTriplesCount(QueryDomain.EDB));
            self.assertEquals(4, store.getTriplesCount(QueryDomain.IDB_REP_NO_EDB));

    def test_addition(self):
        with DataStore(storeType = DataStoreType.PAR_COMPLEX_NN, parameters = {'equality' : EqualityType.OFF}) as store:
            self.assertEquals(0, store.getTriplesCount())
            # importing a triples file
            store.importFile('../main/data/lubm1.ttl')
            self.assertEquals(100545, store.getTriplesCount())
            # importing the datalog program
            store.importFile('../main/data/LUBM_L.dlog')
            store.applyRules();
            self.assertEquals(137933, store.getTriplesCount())
            chainLength = 1000;
            # adding using text
            store.initialize()
            self.assertEquals(0, store.getTriplesCount())
            text = ''
            for index in range(0, chainLength):
                text = ''.join([text, "<a", str(index), "> <a", str(index), "> \"a", str(index), "\"^^xsd:string ."])
                text = ''.join([text, "<a", str(index), "> <a", str(index), "> \"", str(index), "\"^^xsd:int ."])
                text = ''.join([text, "<a", str(index), "> <a", str(index), "> _:", str(index), " ."])
            store.importText(text, prefixes = Prefixes.DEFAULT);
            self.assertEquals(3 * chainLength, store.getTriplesCount())
            # add using resourceIDs from dictionary
            store.initialize()
            self.assertEquals(0, store.getTriplesCount())
            resources = []
            for index in range(0, chainLength):
                resources.append(["a" + str(index), Datatype.IRI_REFERENCE])
                resources.append(["a" + str(index), Datatype.XSD_STRING])
                resources.append([str(index), Datatype.XSD_INTEGER])
                resources.append([str(index), Datatype.BLANK_NODE])
            lexicalForms, datatypes = zip(*resources)                
            dictionary = store.getDictionary()
            resourceIDs = dictionary.resolveResourceValues(list(lexicalForms), list(datatypes))
            triples = []
            for index in range(0, chainLength):
                for index2 in range(0, 3):
                    triples.append(resourceIDs[4 * index])
                    triples.append(resourceIDs[4 * index])
                    triples.append(resourceIDs[4 * index + index2 + 1])
            store.addTriplesByResourceIDs(triples)
            self.assertEquals(3 * chainLength, store.getTriplesCount())
            store.importText(text, prefixes = Prefixes.DEFAULT)
            self.assertEquals(3 * chainLength, store.getTriplesCount())
            # add using ResourceValues
            store.initialize()
            self.assertEquals(0, store.getTriplesCount())
            resources = []
            for index in range(0, chainLength):
                subjectPredicateResource = ["a" + str(index), Datatype.IRI_REFERENCE];
                objectsResources = [["a" + str(index), Datatype.XSD_STRING], [str(index), Datatype.XSD_INTEGER], [str(index), Datatype.BLANK_NODE]]
                for objectResource in objectsResources:
                    resources.append(subjectPredicateResource)
                    resources.append(subjectPredicateResource)
                    resources.append(objectResource)
            lexicalForms, datatypes = zip(*resources)
            store.addTriplesByResourceValues(list(lexicalForms), list(datatypes))
            self.assertEquals(3 * chainLength, store.getTriplesCount())
            store.importText(text, prefixes = Prefixes.DEFAULT);
            self.assertEquals(3 * chainLength, store.getTriplesCount());
            # add using Resources
            store.initialize()
            self.assertEquals(0, store.getTriplesCount())
            resources = []
            for index in range(0, chainLength):
                subjectPredicateResource = [ResourceType.IRI_REFERENCE, "a" + str(index), ""]
                objectResources = [[ResourceType.LITERAL, "a" + str(index), "http://www.w3.org/2001/XMLSchema#string"], [ResourceType.LITERAL, str(index), "http://www.w3.org/2001/XMLSchema#integer"], [ResourceType.BLANK_NODE, str(index), ""]]
                for objectResource in objectResources:
                    resources.append(subjectPredicateResource)
                    resources.append(subjectPredicateResource)
                    resources.append(objectResource)
            resourceTypes, lexicalForms, datatypeIRIs = zip(*resources)
            store.addTriplesByResources(list(resourceTypes), list(lexicalForms), list(datatypeIRIs))
            self.assertEquals(3 * chainLength, store.getTriplesCount())
            store.importText(text, prefixes = Prefixes.DEFAULT)
            self.assertEquals(3 * chainLength, store.getTriplesCount())

    def test_query(self):
        numbersOfResources = [1, 5, 10]
        for numberOfResources in numbersOfResources:
            with DataStore(storeType = DataStoreType.PAR_COMPLEX_NN, parameters = {'equality' : EqualityType.OFF}) as store:
                resources = []
                for index in range(0, numberOfResources):
                    resources.append(["a" + str(index), Datatype.IRI_REFERENCE])
                lexicalForms, datatypes = zip(*resources)                
                dictionary = store.getDictionary()
                resourceIDs = dictionary.resolveResourceValues(list(lexicalForms), list(datatypes))
                triples = []
                for sindex in range(0, numberOfResources):
                    for pindex in range(0, numberOfResources):
                        for oindex in range(0, numberOfResources):
                            triples.append(resourceIDs[sindex])
                            triples.append(resourceIDs[pindex])
                            triples.append(resourceIDs[oindex])
                store.addTriplesByResourceIDs(triples)
                with TupleIterator(store, "select ?x ?v where { ?x ?y ?z . ?z ?u ?v }", prefixes = Prefixes.EMPTY) as tupleIterator:
                    result = {}
                    multiplicity = tupleIterator.open()
                    while multiplicity > 0:
                        xlf, xdt = tupleIterator.getResource(0)
                        vlf, vdt = tupleIterator.getResource(1)
                        key = xlf + vlf
                        result[key] = 1 + result.get(key, 0)
                        multiplicity = tupleIterator.getNext()
                for index1 in range(0, numberOfResources):
                    for index2 in range(0, numberOfResources):
                        self.assertEquals(numberOfResources * numberOfResources * numberOfResources, result.get("a" + str(index1) + "a" + str(index2)))

if __name__ == '__main__':
    unittest.main()