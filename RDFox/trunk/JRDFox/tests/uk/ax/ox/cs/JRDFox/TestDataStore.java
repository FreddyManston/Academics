package uk.ax.ox.cs.JRDFox;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashMap;

import org.junit.Test;
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;
import org.semanticweb.owlapi.model.OWLOntologyManager;

import uk.ac.ox.cs.JRDFox.JRDFoxDemo;
import uk.ac.ox.cs.JRDFox.JRDFoxException;
import uk.ac.ox.cs.JRDFox.Prefixes;
import uk.ac.ox.cs.JRDFox.model.BlankNode;
import uk.ac.ox.cs.JRDFox.model.Datatype;
import uk.ac.ox.cs.JRDFox.model.GroundTerm;
import uk.ac.ox.cs.JRDFox.model.Individual;
import uk.ac.ox.cs.JRDFox.model.Literal;
import uk.ac.ox.cs.JRDFox.store.DataStore;
import uk.ac.ox.cs.JRDFox.store.DataStore.QueryDomain;
import uk.ac.ox.cs.JRDFox.store.DataStore.StoreType;
import uk.ac.ox.cs.JRDFox.store.DataStore.UpdateType;
import uk.ac.ox.cs.JRDFox.store.Dictionary;
import uk.ac.ox.cs.JRDFox.store.Resource;
import uk.ac.ox.cs.JRDFox.store.TupleIterator;

public class TestDataStore {

	@Test 
	public void testBasic()  throws JRDFoxException, URISyntaxException {
		HashMap<String, String> parameters = new HashMap<String, String>();
		parameters.put("equality", "noUNA");
		DataStore store = new DataStore(StoreType.ParallelComplexNN, parameters);
		try {
			assertEquals(0, store.getTriplesCount());
			// insert triples encoding a cycle
			StringBuilder sb = new StringBuilder();
			int loopLength = 100;
			for (int i = 0; i < loopLength; i++)
				sb.append("<a").append(i).append("> <R> <a").append((i+1) % loopLength).append("> .");
			store.importText(sb.toString());
			assertEquals(loopLength, store.getTriplesCount());
			// import transitive closure
			store.importText("<R>(?x, ?z) :- <R>(?x, ?y), <R>(?y, ?z) .");
			// perform materialisation
			store.applyReasoning();
			// the IDBs should be loopLength * loopLength (for facts of the form <ai> <R> <aj>.) plus loopLength (for facts of the form <ai> owl:sameAs <ai>) plus 2 (for the facts <R> owl:sameAs <R>. and owl:sameAs owl:sameAs owl:sameAs.) 
			assertEquals(loopLength * loopLength + loopLength + 2, store.getTriplesCount(QueryDomain.IDB));
			assertEquals(loopLength * loopLength + loopLength + 2, store.getTriplesCount(QueryDomain.IDBrep));
			assertEquals(loopLength, store.getTriplesCount(QueryDomain.EDB));
			assertEquals(loopLength * loopLength + 2, store.getTriplesCount(QueryDomain.IDBrepNoEDB));
			// incrementally add a rule
			store.importText("[?y1, owl:sameAs, ?y2] :- <R>(?x, ?y1) , <R>(?x, ?y2) . ", Prefixes.DEFAULT_IMMUTABLE_INSTANCE, UpdateType.ScheduleForAddition);
			store.applyReasoning(true);
			// the idbs should be  loopLength * loopLength (for facts of the form <ai> <R> <aj>.) plus loopLength * loopLength (for facts of the form <ai> owl:sameAs <aj>) plus 2 (for the facts <R> owl:sameAs <R>. and owl:sameAs owl:sameAs owl:sameAs.)
			assertEquals(loopLength * loopLength + loopLength * loopLength + 2, store.getTriplesCount(QueryDomain.IDB));
			// the representative idbs should be 4: <ai> <R> <ai> (for one of the indexes i) and 3 equality statements for <ai> <R> and owl:sameAs 
			assertEquals(4, store.getTriplesCount(QueryDomain.IDBrep));
			assertEquals(4, store.getTriplesCount(QueryDomain.IDBrepNoEDB));
			assertEquals(loopLength, store.getTriplesCount(QueryDomain.EDB));
		}
		finally {
			store.dispose();
		}
	}
	
	@Test
	public void testAddition() throws JRDFoxException, URISyntaxException, OWLOntologyCreationException {
		DataStore store = new DataStore(StoreType.ParallelComplexNN);
		try {
			assertEquals(0, store.getTriplesCount());
			// importing a triples file
			store.importFiles(new File[] {new File(JRDFoxDemo.class.getResource("data/lubm1.ttl").toURI())});
			assertEquals(100545, store.getTriplesCount());
			// importing an ontology
			OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
			OWLOntology ontology = manager.loadOntologyFromOntologyDocument(IRI.create(JRDFoxDemo.class.getResource("data/univ-bench.owl")));
			store.importOntology(ontology, UpdateType.Add, true, true, true, true);
			store.applyReasoning();
			assertEquals(138008, store.getTriplesCount());
			int chainLength = 1000;
			// adding using text
			store.initialize();
			assertEquals(0, store.getTriplesCount());			
			StringBuilder sb = new StringBuilder();
			for (int index = 0; index < chainLength; index++) {
				sb.append("<a").append(index).append("> <a").append(index).append("> \"a").append(index).append("\"^^xsd:string .");
				sb.append("<a").append(index).append("> <a").append(index).append("> \"").append(index).append("\"^^xsd:int .");
				sb.append("<a").append(index).append("> <a").append(index).append("> _:").append(index).append(" .");
			}
			store.importText(sb.toString(), Prefixes.DEFAULT_IMMUTABLE_INSTANCE);
			assertEquals(3 * chainLength, store.getTriplesCount());
			// add using resourceIDs from dictionary
			store.initialize();
			assertEquals(0, store.getTriplesCount());
			String[] lexicalForms = new String[4 * chainLength];
			Datatype[] datatypes = new Datatype[4 * chainLength];
			for (int index = 0; index < chainLength; index++) {
				lexicalForms[4 * index] = "a" + index;
				lexicalForms[4 * index + 1] = "a" + index;				
				lexicalForms[4 * index + 2] = Integer.toString(index);
				lexicalForms[4 * index + 3] = Integer.toString(index);
				datatypes[4 * index] = Datatype.IRI_REFERENCE;
				datatypes[4 * index + 1] = Datatype.XSD_STRING;
				datatypes[4 * index + 2] = Datatype.XSD_INT;
				datatypes[4 * index + 3] = Datatype.BLANK_NODE;
			}
			Dictionary dictionary = store.getDictionary();
			long[] resourceIDs = dictionary.resolveResources(lexicalForms, datatypes);
			long[] triples = new long[9 * chainLength];
			for (int index = 0; index < chainLength; index ++) {
				for (int index2 = 0; index2 < 3; index2++) {
					triples[9 * index + 3 * index2] = resourceIDs[4 * index];
					triples[9 * index + 3 * index2 + 1] = resourceIDs[4 * index];
					triples[9 * index + 3 * index2 + 2] = resourceIDs[4 * index + index2 + 1];
				}
			}
			store.addTriplesByResourceIDs(triples);
			assertEquals(3 * chainLength, store.getTriplesCount());
			store.importText(sb.toString(), Prefixes.DEFAULT_IMMUTABLE_INSTANCE);
			assertEquals(3 * chainLength, store.getTriplesCount());
			// add using Resources
			store.initialize();
			assertEquals(0, store.getTriplesCount());
			Resource[] resources = new Resource[9 * chainLength];
			for (int index = 0; index < chainLength; index ++) {
				Resource subjectPredicateResource = new Resource("a" + index, Datatype.IRI_REFERENCE);				
				Resource[] objectsResources = new Resource[] { 
						new Resource("a" + index, Datatype.XSD_STRING), 
						new Resource(Integer.toString(index), Datatype.XSD_INT), 
						new Resource(Integer.toString(index), Datatype.BLANK_NODE)
						};
				for (int index2 = 0; index2 < 3; index2++) {
					resources[9 * index + 3 * index2] = subjectPredicateResource;
					resources[9 * index + 3 * index2 + 1] = subjectPredicateResource;
					resources[9 * index + 3 * index2 + 2] = objectsResources[index2];
				}
			}
			store.addTriples(resources);
			assertEquals(3 * chainLength, store.getTriplesCount());
			store.importText(sb.toString(), Prefixes.DEFAULT_IMMUTABLE_INSTANCE);
			assertEquals(3 * chainLength, store.getTriplesCount());
			// add using GroundTerms
			store.initialize();
			assertEquals(0, store.getTriplesCount());
			GroundTerm[] terms = new GroundTerm[9 * chainLength];
			for (int index = 0; index < chainLength; index ++) {
				GroundTerm subjectPredicateTerm = Individual.create("a" + index);				
				GroundTerm[] objectsTerms = new GroundTerm[] { 
						Literal.create("a" + index, Datatype.XSD_STRING), 
						Literal.create(Integer.toString(index), Datatype.XSD_INT), 
						BlankNode.create(Integer.toString(index))
						};
				for (int index2 = 0; index2 < 3; index2++) {
					terms[9 * index + 3 * index2] = subjectPredicateTerm;
					terms[9 * index + 3 * index2 + 1] = subjectPredicateTerm;
					terms[9 * index + 3 * index2 + 2] = objectsTerms[index2];
				}
			}
			store.addTriples(terms);
			assertEquals(3 * chainLength, store.getTriplesCount());
			store.importText(sb.toString(), Prefixes.DEFAULT_IMMUTABLE_INSTANCE);
			assertEquals(3 * chainLength, store.getTriplesCount());
			// add using Strings
			store.initialize();
			assertEquals(0, store.getTriplesCount());
			String[] strings = new String[9 * chainLength];
			int[] datatypeIDs = new int[9 * chainLength];
			for (int index = 0; index < chainLength; index ++) {
				String subjectPredicateString = "a" + index;
				int[] objectsDatatypeIDs = new int[] { Datatype.XSD_STRING.getDatatypeID(), Datatype.XSD_INT.getDatatypeID(), Datatype.BLANK_NODE.getDatatypeID() };
				String[] objectsStrings = new String[] { 
						"a" + index, 
						Integer.toString(index), 
						Integer.toString(index)
						};
				for (int index2 = 0; index2 < 3; index2++) {
					strings[9 * index + 3 * index2] = subjectPredicateString;
					strings[9 * index + 3 * index2 + 1] = subjectPredicateString;
					strings[9 * index + 3 * index2 + 2] = objectsStrings[index2];
					datatypeIDs[9 * index + 3 * index2] = Datatype.IRI_REFERENCE.getDatatypeID();
					datatypeIDs[9 * index + 3 * index2 + 1] = Datatype.IRI_REFERENCE.getDatatypeID();
					datatypeIDs[9 * index + 3 * index2 + 2] = objectsDatatypeIDs[index2];
				}
			}			
			store.addTriples(strings, datatypeIDs);
			assertEquals(3 * chainLength, store.getTriplesCount());
			store.importText(sb.toString(), Prefixes.DEFAULT_IMMUTABLE_INSTANCE);
			assertEquals(3 * chainLength, store.getTriplesCount());
		}
		finally {
			store.dispose();
		}
	}
	
	public void testQuery(DataStore store, int numberOfResources, int windowSize, boolean useResources) throws JRDFoxException {
		TupleIterator iterator = store.compileQuery("select ?x ?v where { ?x ?y ?z . ?z ?u ?v }", Prefixes.EMPTY_IMMUTABLE_INSTANCE, Collections.emptyMap(), windowSize);
		try {
			HashMap<String, Integer> result = new HashMap<String, Integer>(); 
			for (iterator.open(); iterator.getMultiplicity() > 0; iterator.advance()) {
				String value = useResources ? 
						iterator.getGroundTerm(0).toString().concat((iterator.getGroundTerm(1).toString())) :
							iterator.getResource(0).toString().concat((iterator.getResource(1).toString()));
				result.put(value, 1 + result.getOrDefault(value, 0));
			}
			for (int index1 = 0; index1 < numberOfResources; index1++)
				for (int index2 = 0; index2 < numberOfResources; index2++)
					assertEquals(numberOfResources * numberOfResources * numberOfResources, result.get("<a" + index1 + "><a" + index2 + ">").intValue());
		}
		finally {
			iterator.dispose();
		}
	}
	
	public void testQuery(int numberOfResources, boolean useCache) throws JRDFoxException {
		DataStore store = new DataStore(StoreType.ParallelComplexNN, Collections.emptyMap(), useCache, useCache);
		try {
			String[] lexicalForms = new String[numberOfResources];
			Datatype[] datatypes = new Datatype[numberOfResources];
			for (int index = 0; index < numberOfResources; index++) {
				lexicalForms[index] = "a" + index;
				datatypes[index] = Datatype.IRI_REFERENCE;
			}
			long[] resourceIDs = store.getDictionary().resolveResources(lexicalForms, datatypes);
			long[] triples = new long[3 * numberOfResources * numberOfResources * numberOfResources];
			for (int sindex = 0; sindex < numberOfResources; sindex++) {
				for (int pindex = 0; pindex < numberOfResources; pindex++) {
					for (int oindex = 0; oindex < numberOfResources; oindex++) {
						int index = 3 * (sindex * numberOfResources * numberOfResources + pindex * numberOfResources + oindex); 
						triples[index] = resourceIDs[sindex];
						triples[index + 1] = resourceIDs[pindex];
						triples[index + 2] = resourceIDs[oindex];						
					}
				}				
			}
			store.addTriplesByResourceIDs(triples);
			int[] windowSizes = new int[] {1, 10, 100, 1000};
			for (int windowSize : windowSizes) {
				testQuery(store, numberOfResources, windowSize, true);
				testQuery(store, numberOfResources, windowSize, false);
			}
		}
		finally {
			store.dispose();
		}
	}
	
	@Test
	public void testQuery() throws JRDFoxException {
		int[] numbersOfResources = new int[] {1, 5, 10};
		for (int numberOfResources : numbersOfResources) {
			testQuery(numberOfResources + 1, true);
			testQuery(numberOfResources + 1, false);
		}
	}
}
