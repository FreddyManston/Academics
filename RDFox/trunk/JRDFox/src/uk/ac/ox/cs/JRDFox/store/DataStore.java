// JRDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

package uk.ac.ox.cs.JRDFox.store;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.semanticweb.owlapi.model.OWLOntology;

import uk.ac.ox.cs.JRDFox.JRDFoxException;
import uk.ac.ox.cs.JRDFox.Prefixes;
import uk.ac.ox.cs.JRDFox.model.Atom;
import uk.ac.ox.cs.JRDFox.model.BlankNode;
import uk.ac.ox.cs.JRDFox.model.Datatype;
import uk.ac.ox.cs.JRDFox.model.GroundTerm;
import uk.ac.ox.cs.JRDFox.model.Individual;
import uk.ac.ox.cs.JRDFox.model.Literal;
import uk.ac.ox.cs.JRDFox.model.Rule;
import uk.ac.ox.cs.JRDFox.owl2rl.OWL2RLTranslator;
import uk.ac.ox.cs.JRDFox.owl2rl.RuleListener;

public class DataStore {

	public enum UpdateType {
		Add,
		ScheduleForAddition,
		ScheduleForDeletion
	}
	
	public enum Format {
		Turtle("Turtle"),
		NTriples("N-Triples"),
		Datalog("Datalog");		
		
		protected final String m_name;
	    
	    private Format(String name) {
	    	m_name = name;
	    }
	    
	    public String getName() {
	    	return m_name;
	    }
	}

	/**
	 * The different store types supported by RDFox.
	 * 
	 * The SequentialHead store uses the RDFox default data layout. The SequentialHead store is 
	 * sequential, which means that it should be used from one thread only. In particular, 
	 * reasoning should be done with one thread, and also only one query at a time should be 
	 * issued. It can store up to 2^48 triples.
	 *
	 * The SequentialTail store uses an experimental data layout which in certain cases achieves
	 * faster reasoning. The SequentialTail store is sequential, which means that it should be 
	 * used from one thread only. In particular, reasoning should be done with one thread, and 
	 * also only one query at a time should be issued. It can store up to 2^48 triples.
	 * 
	 * The NarrowParallelHead store uses the RDFox default data layout. The NarrowParallelHead store is 
	 * parallel, which means that it supports parallel operation, both for reasoning and for query 
	 * answering. It can store up to 2^32 triples.
	 * 
	 * The WideParallelHead store uses the RDFox default data layout. The WideParallelHead store is 
	 * parallel, which means that it supports parallel operation, both for reasoning and for query 
	 * answering. It can store up to 2^64 triples at the expense of higher memory consumption per triple.
	 *  
	 */
	public enum StoreType {
		/**
		 * The Sequential store uses the RDFox default data layout. The store is sequential, which 
		 * means that it should be used from one thread only. In particular, reasoning should be 
		 * done with one thread, and also only one query at a time should be issued. It can store 
		 * up to 2^48 triples.
		 */
		Sequential("seq"),
		
		/**
		 * The ParallelSimpleNN store uses the RDFox default data layout. The store is parallel, which
		 * means that it supports parallel operation, both for reasoning and for query answering. 
		 * It employs a simple indexing scheme designed for highly-efficient updates. It can store up 
		 * to 2^32 resources and up to 2^32 triples.  
		 */
		ParallelSimpleNN("par-simple-nn"),
		
		/**
         * The ParallelSimpleNN store uses the RDFox default data layout. The store is parallel, which
         * means that it supports parallel operation, both for reasoning and for query answering. 
         * It employs a simple indexing scheme designed for highly-efficient updates. It can store up to 
         * 2^32 resources and up to 2^64 triples. 
         */
		ParallelSimpleNW("par-simple-nw"),
		
		/**
         * The ParallelSimpleNN store uses the RDFox default data layout. The store is parallel, which
         * means that it supports parallel operation, both for reasoning and for query answering. 
         * It employs a simple indexing scheme designed for highly-efficient updates. It can store up to 
         * 2^64 resources and up to 2^64 triples. 
         */
        ParallelSimpleWW("par-simple-ww"),
        
        /**
         * The ParallelSimpleNN store uses the RDFox default data layout. The store is parallel, which
         * means that it supports parallel operation, both for reasoning and for query answering. 
         * It employs a complex indexing scheme designed for highly-efficient data access. It can store up 
         * to 2^32 resources and up to 2^32 triples.  
         */
        ParallelComplexNN("par-complex-nn"),
        
        /**
         * The ParallelSimpleNN store uses the RDFox default data layout. The store is parallel, which
         * means that it supports parallel operation, both for reasoning and for query answering. 
         * It employs a complex indexing scheme designed for highly-efficient data access. It can store up 
         * to 2^32 resources and up to 2^64 triples. 
         */
        ParallelComplexNW("par-complex-nw"),
        
        /**
         * The ParallelSimpleNN store uses the RDFox default data layout. The store is parallel, which
         * means that it supports parallel operation, both for reasoning and for query answering. 
         * It employs a complex indexing scheme designed for highly-efficient data access. It can store up 
         * to 2^64 resources and up to 2^64 triples. 
         */
        ParallelComplexWW("par-complex-ww");

		private final String m_type;

		private StoreType(String type) {
			m_type = type;
		}

		public String getType() {
			return m_type;
		}

	}

	public enum QueryDomain {
		EDB("EDB"),
		IDB("IDB"),
		IDBrep("IDBrep"),
		IDBrepNoEDB("IDBrepNoEDB");
		
		protected static String s_DomainKey = "domain";
		
		protected String m_name;
		
		QueryDomain(String name) {
			m_name = name;
		}
		
		@Override
		public String toString() {
			return m_name;
		}
	}
	
	static {
		try {
			String logAPI = (System.getProperty("log.RDFox") != null && !System.getProperty("log.RDFox").equalsIgnoreCase("false")) ? "-logAPI" : "";
			String libName = "CppRDFox" + logAPI;
			String libJarSource = "/uk/ac/ox/cs/JRDFox/dll/libCppRDFox" + logAPI;
			Field loadedLibraryNames = ClassLoader.class.getDeclaredField("loadedLibraryNames");
			loadedLibraryNames.setAccessible(true);
			@SuppressWarnings("unchecked")
			List<String> loadedLibraries = (List<String>)loadedLibraryNames.get(null);
			boolean loaded = false;
			for (String library : loadedLibraries)
				if (library.contains("CppRDFox"))
					loaded = true;
			if (!loaded)
				if (!loadRDFoxFromJar(libJarSource))  
					System.loadLibrary(libName);
		}
		catch (IOException e) {
			throw new RuntimeException(e);
		}
		catch (NoSuchFieldException e) {
			throw new RuntimeException(e);
		}
		catch (IllegalAccessException e) {
			throw new RuntimeException(e);
		}
	}

	protected static boolean loadRDFoxFromJar(String path) throws IOException {
		InputStream inputStream = DataStore.class.getResourceAsStream(path);
		if (inputStream == null)
			return false;
		try {
			File temp = File.createTempFile("CppRDFox", ".dylib");
			if (!temp.exists())
				return false;
			temp.deleteOnExit();
			byte[] buffer = new byte[1024];
			int readBytes;
			OutputStream outputStream = new FileOutputStream(temp);
			try {
				while ((readBytes = inputStream.read(buffer)) != -1)
					outputStream.write(buffer, 0, readBytes);
			}
			finally {
				outputStream.close();
			}
			System.load(temp.getAbsolutePath());
			return true;
		}
		finally {
			inputStream.close();
		}
	}

	protected static final int DEFAULT_WINDOW_SIZE = 1000;
	
	protected volatile long m_storePtr;
	protected final Dictionary m_dictionary;
	protected volatile long m_generation;

	protected native static void nCreate(String dataStoreType, String[] parameters, long[] pointers) throws JRDFoxException;

	protected native static void nInitialize(long dataStorePtr) throws JRDFoxException;

	protected native static void nSetNumberOfThreads(long dataStorePtr, int numberOfThreads) throws JRDFoxException; 

	protected native static void nAddTriples(long dataStorePtr, String[] lexicalForms, int[] datatypeIDs, int numberOfResources, int updateType) throws JRDFoxException;

	protected native static void nAddTriplesByResourceIDs(long dataStorePtr, long[] resourceIDs, int numberOfResources, int updateType) throws JRDFoxException;

	protected native static void nImportFiles(long dataStorePtr, String[] fileNames, int updateType, boolean decomposeRules, boolean renameBlankNodes) throws JRDFoxException;

	protected native static void nImportText(long dataStorePtr, String text, int updateType, boolean decomposeRules, boolean renameBlankNodes, String[] prefixes) throws JRDFoxException;
	
	protected native static void nExport(long dataStorePtr, String fileName, String formatName) throws JRDFoxException;

	protected native static void nApplyRules(long dataStorePtr, boolean incrementally) throws JRDFoxException;

	protected native static void nUpdateStatistics(long dataStorePtr) throws JRDFoxException;
	
	protected native static void nMakeFactsExplicit(long dataStorePtr) throws JRDFoxException;

	protected native static void nClearRulesAndMakeFactsExplicit(long dataStorePtr) throws JRDFoxException;

	protected native static void nSave(long datastorePtr, String fileName) throws JRDFoxException;

	protected native static void nLoad(String fileName, long[] pointers) throws JRDFoxException;

	protected native static void nDispose(long dataStorePtr);

	protected void ensureValidState() throws JRDFoxException {
		if (m_storePtr == 0)
			throw new JRDFoxException("This store has already been disposed and thus cannot be accessed any more.");
	}

	public DataStore(StoreType dataStoreType) throws JRDFoxException {
		this(dataStoreType, Collections.emptyMap(), true, true);
	}

	public DataStore(StoreType dataStoreType, Map<String, String> parameters) throws JRDFoxException {
		this(dataStoreType, parameters, true, true);
	}

	public DataStore(StoreType dataStoreType, Map<String, String> parameters, boolean cacheResources, boolean cacheGroundTerms) throws JRDFoxException {
		long[] pointers = new long[2];
		nCreate(dataStoreType.getType(), mapToStringArray(parameters), pointers);
		m_storePtr = pointers[0];
		m_dictionary = new Dictionary(this, pointers[1], cacheResources, cacheGroundTerms);
		m_generation = 1;
		initialize();
	}

	public DataStore(File file) throws JRDFoxException {
		this(file, false, true);
	}
	
	public DataStore(File file, boolean cacheResources, boolean cacheGroundTerms) throws JRDFoxException {
		long[] pointers = new long[2];        
		nLoad(file.getAbsolutePath(), pointers);
		m_storePtr = pointers[0];
		m_dictionary = new Dictionary(this, pointers[1], cacheResources, cacheGroundTerms);
		m_generation = 1;
		m_dictionary.initialize();		
	}

	/**
	 * Initializes an empty store.
	 * 
	 * @throws JRDFoxException   thrown if an error occurs
	 */
	public synchronized void initialize() throws JRDFoxException {
		ensureValidState();
		m_generation++;
		m_dictionary.initialize();
		nInitialize(m_storePtr);
	}

	/**
	 * Sets the number of threads used during reasoning and parallel import of data,
	 * 
	 * @param numberOfThreads       the new number of threads to use
	 * @throws JRDFoxException   	thrown if an error occurs
	 */
	public void setNumberOfThreads(int numberOfThreads) throws JRDFoxException {
		ensureValidState();
		nSetNumberOfThreads(m_storePtr, numberOfThreads);
	}

	/**
	 * Adds the specified triples to the store without performing reasoning.
	 * 
	 * @param triples               the triples to be added to the store
	 * @throws JRDFoxException   	thrown if an error occurs
	 */
	public void addTriples(Resource[] triples) throws JRDFoxException {
		addTriples(triples, triples.length, UpdateType.Add);
	}

	/**
	 * Adds the specified triples to the store without performing reasoning.
	 * 
	 * @param triples               the triples to be added to the store
	 * @param updateType            determines what action should be performed on the triples 
	 * @throws JRDFoxException   	thrown if an error occurs
	 */
	public void addTriples(Resource[] triples, UpdateType updateType) throws JRDFoxException { 
		addTriples(triples, triples.length, updateType);
	}

	/**
	 * Adds the specified triples to the store without performing reasoning.
	 * 
	 * @param triples               the triples to be added to the store
	 * @param numberOfResources		the number of entries in the <code>triples</code> array that should be added
	 * @param updateType            determines what action should be performed on the triples 
	 * @throws JRDFoxException   	thrown if an error occurs
	 */
	public void addTriples(Resource[] triples, int numberOfResources, UpdateType updateType) throws JRDFoxException { 
		ensureValidState();
		if (numberOfResources % 3 != 0)
			throw new JRDFoxException("Invalid input: the number of resources should be a multiple of 3.");
		String[] lexicalForms = new String[numberOfResources];
		int[] datatypeIDs = new int[numberOfResources];                
		for (int index = 0; index < numberOfResources; index ++) {
			if (triples[index].m_lexicalForm == null)
				throw new NullPointerException("Resources should have non-null lexical forms");
			lexicalForms[index] = triples[index].m_lexicalForm;
			datatypeIDs[index] = triples[index].m_datatype.getDatatypeID();
		}
		nAddTriples(m_storePtr, lexicalForms, datatypeIDs, numberOfResources, updateType.ordinal());
	}

	/**
	 * Adds the specified triples to the store without performing reasoning.
	 * 
	 * @param triples               the triples to be added to the store
	 * @throws JRDFoxException   	thrown if an error occurs
	 */
	public void addTriples(GroundTerm[] triples) throws JRDFoxException {
		addTriples(triples, triples.length, UpdateType.Add);
	}

	/**
	 * Adds the specified triples to the store without performing reasoning.
	 * 
	 * @param triples               the triples to be added to the store
	 * @param updateType            determines what action should be performed on the triples 
	 * @throws JRDFoxException   	thrown if an error occurs
	 */
	public void addTriples(GroundTerm[] triples, UpdateType updateType) throws JRDFoxException {
		addTriples(triples, triples.length, updateType);
	}

	/**
	 * Adds the specified triples to the store without performing reasoning.
	 * 
	 * @param triples               the triples to be added to the store
	 * @param numberOfResources		the number of entries in the <code>triples</code> array that should be added
	 * @param updateType            determines what action should be performed on the triples 
	 * @throws JRDFoxException   	thrown if an error occurs
	 */
	public void addTriples(GroundTerm[] triples, int numberOfResources, UpdateType updateType) throws JRDFoxException { 
		ensureValidState();
		if (numberOfResources % 3 != 0)
			throw new JRDFoxException("Invalid input: the number of resources should be a multiple of 3.");
		String[] lexicalForms = new String[numberOfResources];
		int[] datatypeIDs = new int[numberOfResources];                
		for (int index = 0; index < numberOfResources; index ++) {
			GroundTerm term = triples[index];
			if (term instanceof Individual) {
				lexicalForms[index] = ((Individual)term).getIRI();
				datatypeIDs[index] = Datatype.IRI_REFERENCE.getDatatypeID();
			}
			else if (term instanceof BlankNode) {
				lexicalForms[index] = ((BlankNode)term).getID();
				datatypeIDs[index] = Datatype.BLANK_NODE.getDatatypeID();
			}
			else if (term instanceof Literal) {
				lexicalForms[index] = ((Literal)term).getLexicalForm();
				String datatypeIRI = ((Literal)term).getDatatype().getIRI();
				Datatype datatype = Datatype.value(datatypeIRI);
				if (datatype != null)
					datatypeIDs[index] = datatype.getDatatypeID();
				else
					throw new JRDFoxException("Unknown datatype IRI:" + datatypeIRI);
			}
			if (lexicalForms[index] == null)
				throw new NullPointerException("Lexcial forms should not be null.");
		}
		nAddTriples(m_storePtr, lexicalForms, datatypeIDs, numberOfResources, updateType.ordinal());
	}

	/**
	 * Adds the specified triples to the store without performing reasoning.
	 * 
	 * @param lexicalForms			the lexical forms of the triple resources
	 * @param datatypeIDs			the datatype IDs of the triple resources
	 * @throws JRDFoxException   	thrown if an error occurs
	 */
	public void addTriples(String[] lexicalForms, int[] datatypeIDs) throws JRDFoxException {
		addTriples(lexicalForms, datatypeIDs, lexicalForms.length, UpdateType.Add);
	}

	/**
	 * Adds the specified triples to the store without performing reasoning.
	 * 
	 * @param lexicalForms			the lexical forms of the triple resources
	 * @param datatypeIDs			the datatype IDs of the triple resources
	 * @param updateType            determines what action should be performed on the triples 
	 * @throws JRDFoxException   	thrown if an error occurs
	 */
	public void addTriples(String[] lexicalForms, int[] datatypeIDs, UpdateType updateType) throws JRDFoxException {
		addTriples(lexicalForms, datatypeIDs, lexicalForms.length, updateType);
	}

	/**
	 * Adds the specified triples to the store without performing reasoning.
	 * 
	 * @param lexicalForms			the lexical forms of the triple resources
	 * @param datatypeIDs			the datatype IDs of the triple resources
	 * @param numberOfResources		the number of entries in the <code>triples</code> array that should be added
	 * @param updateType            determines what action should be performed on the triples 
	 * @throws JRDFoxException   	thrown if an error occurs
	 */
	public void addTriples(String[] lexicalForms, int[] datatypeIDs, int numberOfResources, UpdateType updateType) throws JRDFoxException { 
		ensureValidState();
		if (numberOfResources % 3 != 0)
			throw new JRDFoxException("Invalid input: the number of resources should be a multiple of 3.");
		if (numberOfResources > lexicalForms.length)
			throw new JRDFoxException("Invalid input: the number of resources is larger than the number of lexical forms.");
		if (numberOfResources > datatypeIDs.length)
			throw new JRDFoxException("Invalid input: the number of resources is larger than the number of datatype IDs.");
		for (int index = 0; index < numberOfResources; ++index)
			if (lexicalForms[index] == null)
				throw new NullPointerException("Lexical forms should not be null.");
		nAddTriples(m_storePtr, lexicalForms, datatypeIDs, numberOfResources, updateType.ordinal());
	}
	/**
	 * Adds the specified triples to the store without performing reasoning.
	 * 
	 * @param triples               the triples to be added to the store
	 * @throws JRDFoxException   	thrown if an error occurs
	 */
	public void addTriplesByResourceIDs(long[] triples) throws JRDFoxException {
		addTriplesByResourceIDs(triples, triples.length, UpdateType.Add);
	}

	/**
	 * Adds the specified triples to the store without performing reasoning.
	 * 
	 * @param triples               the triples to be added to the store
	 * @param updateType            determines what action should be performed on the triples
	 * @throws JRDFoxException   	thrown if an error occurs
	 */
	public void addTriplesByResourceIDs(long[] triples, UpdateType updateType) throws JRDFoxException {
		addTriplesByResourceIDs(triples, triples.length, updateType);
	}

	/**
	 * Adds the specified triples to the store without performing reasoning.
	 * 
	 * @param triples               the triples to be added to the store
	 * @param numberOfResources		the number of entries in the <code>triples</code> array that should be added
	 * @param updateType            determines what action should be performed on the triples
	 * @throws JRDFoxException   	thrown if an error occurs
	 */
	public void addTriplesByResourceIDs(long[] triples, int numberOfResources, UpdateType updateType) throws JRDFoxException { 
		ensureValidState();       
		if (numberOfResources % 3 != 0)
			throw new JRDFoxException("Invalid input: the number of resources should be a multiple of 3.");
		nAddTriplesByResourceIDs(m_storePtr, triples, numberOfResources, updateType.ordinal());
	}

	/**
	 * Imports the specified datalog or data files; the importation is done in parallel if the store supports it.
	 * 
	 * @param files                 the files to be imported
	 * @param updateType            determines what action should be performed on the triples
	 * @param decomposeRules  		determines whether rules should be optimized using tree decomposition
	 * @param renameBlankNodes		determines whether blank nodes in the files should be renamed apart during import
	 * @throws JRDFoxException   	thrown if an error occurs
	 */
	public void importFiles(File[] files, UpdateType updateType, boolean decomposeRules, boolean renameBlankNodes) throws JRDFoxException {
		ensureValidState();
		String[] fileNames = new String[files.length];
		for (int index = 0; index < files.length; index++)
			fileNames[index] = files[index].getAbsolutePath();
		nImportFiles(m_storePtr, fileNames, updateType.ordinal(), decomposeRules, renameBlankNodes);
	}

	/**
	 * Imports the specified datalog or data files; the importation is done in parallel if the store supports it.
	 * 
	 * @param files                 the files to be imported
	 * @param updateType            determines what action should be performed on the data
	 * @throws JRDFoxException   	thrown if an error occurs
	 */
	public void importFiles(File[] files, UpdateType updateType) throws JRDFoxException {
		importFiles(files, updateType, false, false);
	}
	
	/**
	 * Imports the specified datalog or data files; the importation is done in parallel if the store supports it.
	 * 
	 * @param files                 the files to be imported
	 * @throws JRDFoxException   	thrown if an error occurs
	 */
	public void importFiles(File[] files) throws JRDFoxException {
		importFiles(files, UpdateType.Add);
	}
	
	/**
	 * Imports the specified datalog or data files; the importation is done in parallel if the store supports it.
	 * 
	 * @param text                  the textual representation of the imported data
	 * @param prefixes              the prefixes to be populated
	 * @param updateType            determines what action should be performed on the triples
	 * @param decomposeRules  		determines whether rules should be optimized using tree decomposition
	 * @param renameBlankNodes		determines whether blank nodes in the files should be renamed apart during import
	 * @throws JRDFoxException   	thrown if an error occurs
	 */
	public void importText(String text, Prefixes prefixes, UpdateType updateType, boolean decomposeRules, boolean renameBlankNodes) throws JRDFoxException {
		ensureValidState();
		if (text == null)
			throw new NullPointerException("Imported text should be non-empty");
		nImportText(m_storePtr, text, updateType.ordinal(), decomposeRules, renameBlankNodes, mapToStringArray(prefixes.getPrefixIRIsByPrefixName()));
	}

	/**
	 * Imports the specified datalog or data files; the importation is done in parallel if the store supports it.
	 * 
	 * @param text                  the textual representation of the imported data
	 * @param prefixes              the prefixes to be populated
	 * @param updateType            determines what action should be performed on the data
	 * @throws JRDFoxException   	thrown if an error occurs
	 */
	public void importText(String text, Prefixes prefixes, UpdateType updateType) throws JRDFoxException {
		importText(text, prefixes, updateType, false, false);
	}
	
	/**
	 * Imports the specified datalog or data files; the importation is done in parallel if the store supports it.
	 * 
	 * @param text                  the textual representation of the imported data
	 * @param prefixes              the prefixes to be populated
	 * @throws JRDFoxException   	thrown if an error occurs
	 */
	public void importText(String text, Prefixes prefixes) throws JRDFoxException {
		importText(text, prefixes, UpdateType.Add);
	}

	/**
	 * Imports the specified datalog or data files; the importation is done in parallel if the store supports it.
	 * 
	 * @param text                  the textual representation of the imported data
	 * @throws JRDFoxException   	thrown if an error occurs
	 */
	public void importText(String text) throws JRDFoxException {		
		importText(text, new Prefixes());
	}

	/**
	 * Imports the datalog fragment of the ontology into the store.
	 * 
	 * @param ontology          	the ontology to be transformed
	 * @throws JRDFoxException   	thrown if an error occurs
	 */
	public void importOntology(OWLOntology ontology) throws JRDFoxException {
		importOntology(ontology, UpdateType.Add);
	}

	/**
	 * Imports the datalog fragment of the ontology into the store.
	 * 
	 * @param ontology          	the ontology to be transformed
	 * @param updateType            determines what action should be performed on the triples
	 * @throws JRDFoxException   	thrown if an error occurs
	 */
	public void importOntology(OWLOntology ontology, UpdateType updateType) throws JRDFoxException {
		importOntology(ontology, updateType, true, false);
	}

	/**
	 * Imports the datalog fragment of the ontology into the store.
	 * 
	 * @param ontology          	the ontology to be transformed
	 * @param updateType            determines what action should be performed on the triples
	 * @param processTBoxAxioms		specifies whether the TBox axioms in the ontology should be processed
	 * @param processABoxAxioms		specifies whether the ABox axioms in the ontology should be processed
	 * @throws JRDFoxException   	thrown if an error occurs
	 */
	public void importOntology(OWLOntology ontology, UpdateType updateType, boolean processTBoxAxioms, boolean processABoxAxioms) throws JRDFoxException {
		importOntology(ontology, updateType, processTBoxAxioms, processABoxAxioms, false, false);
	}
	
	/**
	 * Imports the triples and the datalog fragment of the ontology into the store.
	 *  
	 * @param ontology          			the ontology to be transformed
	 * @param updateType            		determines what action should be performed on the triples
	 * @param processTBoxAxioms				specifies whether the TBox axioms in the ontology should be processed
	 * @param processABoxAxioms				specifies whether the ABox axioms in the ontology should be processed
	 * @param processAnnotationTBoxAxioms	specifies whether the annotation TBox axioms in the ontology should be processed
	 * @param processAnnotationABoxAxioms	specifies whether the annotation ABox axioms in the ontology should be processed
	 * @throws JRDFoxException   		thrown if an error occurs
	 */
	public void importOntology(OWLOntology ontology, UpdateType updateType, boolean processTBoxAxioms, boolean processABoxAxioms, boolean processAnnotationTBoxAxioms, boolean processAnnotationABoxAxioms) throws JRDFoxException {
		importOntology(ontology, updateType, processTBoxAxioms, processABoxAxioms, processAnnotationTBoxAxioms, processAnnotationABoxAxioms, false, false);
	}

	/**
	 * Imports the triples and the datalog fragment of the ontology into the store.
	 *   
	 * @param ontology          			the ontology to be transformed
	 * @param updateType            		determines what action should be performed on the triples
	 * @param processTBoxAxioms				specifies whether the TBox axioms in the ontology should be processed
	 * @param processABoxAxioms				specifies whether the ABox axioms in the ontology should be processed
	 * @param processAnnotationTBoxAxioms	specifies whether the annotation TBox axioms in the ontology should be processed
	 * @param processAnnotationABoxAxioms	specifies whether the annotation ABox axioms in the ontology should be processed
	 * @param decomposeRules  				determines whether rules should be optimized using tree decomposition
	 * @param renameBlankNodes				determines whether blank nodes in the files should be renamed apart during import
	 * @throws JRDFoxException			thrown if an error occurs
	 */

	public void importOntology(OWLOntology ontology, UpdateType updateType, boolean processTBoxAxioms, boolean processABoxAxioms, boolean processAnnotationTBoxAxioms, boolean processAnnotationABoxAxioms, boolean decomposeRules, boolean renameBlankNodes) throws JRDFoxException {
		ensureValidState();
		final String CRLF = System.getProperty("line.separator");
		final int importWindowSize = 3 * 10;
		final List<Resource> resources = new ArrayList<Resource>(importWindowSize);
		List<JRDFoxException> errors = new ArrayList<JRDFoxException>();
		final StringBuilder buffer = new StringBuilder();
		new OWL2RLTranslator(new RuleListener() {
			public void addRule(Rule rule) {
				rule.toString(buffer, Prefixes.DEFAULT_IMMUTABLE_INSTANCE);
				buffer.append(CRLF);
			}
			public void addAtom(Atom atom) {
				if (!atom.isGround())
					return;
				GroundTerm subject = (GroundTerm)atom.getArgument(0); 
				if (subject instanceof BlankNode)
					resources.add(new Resource(((BlankNode)subject).getID(), Datatype.BLANK_NODE));
				else
					resources.add(new Resource(((Individual)subject).getIRI(), Datatype.IRI_REFERENCE));            	
				if (atom.getNumberOfArguments() == 2) {
					resources.add(new Resource(atom.getPredicate().getIRI(), Datatype.IRI_REFERENCE));            		
					GroundTerm object = (GroundTerm)atom.getArgument(1);
					if (object instanceof BlankNode)
						resources.add(new Resource(((BlankNode)object).getID(), object.getDatatype()));
					else if (object instanceof Individual)
						resources.add(new Resource(((Individual)object).getIRI(), object.getDatatype()));
					else if (object instanceof Literal)
						resources.add(new Resource(((Literal)object).getLexicalForm(), object.getDatatype()));
					else 
						throw new Error("Unknown ground term type.");
				}
				else if (atom.getNumberOfArguments() == 1) {
					resources.add(new Resource("http://www.w3.org/1999/02/22-rdf-syntax-ns#type", Datatype.IRI_REFERENCE));
					resources.add(new Resource(atom.getPredicate().getIRI(), Datatype.IRI_REFERENCE));
				}
				else
					throw new Error("Unsupported arity: " + atom.getNumberOfArguments());				
				if (resources.size() == importWindowSize) {
					try {
						addTriples(resources.toArray(new Resource[resources.size()]));						
					}
					catch (JRDFoxException e) {
					}
					finally {
						resources.clear();
					}
				}
			}
		}, errors, processTBoxAxioms, processABoxAxioms, processAnnotationTBoxAxioms, processAnnotationABoxAxioms).translate(ontology, true);
		if (resources.size() > 0)
			addTriples(resources.toArray(new Resource[resources.size()]));
		importText(new String(buffer.toString()), Prefixes.DEFAULT_IMMUTABLE_INSTANCE, updateType, decomposeRules, renameBlankNodes);
	}
	

	/**
	 * Exports the triples of the current store into a file
	 * @param outputFile			the output file
	 * @param format				the format
	 * @throws JRDFoxException   	thrown if an error occurs
	 */
	public void export(File outputFile, Format format) throws JRDFoxException {
		ensureValidState();
		nExport(m_storePtr, outputFile.getAbsolutePath(), format.getName());
	}

	/**
	 * Performs reasoning with the rules and data currently loaded into the store.
	 * 
	 * @param incrementally         determines whether to reason incrementally
	 * @throws JRDFoxException   	thrown if an error occurs
	 */
	public void applyReasoning(boolean incrementally) throws JRDFoxException {
		ensureValidState();
		nApplyRules(m_storePtr, incrementally);
	}

	/**
	 * Performs reasoning with the rules and data currently loaded into the store.
	 * 
	 * @throws JRDFoxException   thrown if an error occurs
	 */
	public void applyReasoning() throws JRDFoxException {
		applyReasoning(false);
	}

	/**
	 * Updates the statistical information about the data in the store. This information is used to optimize query and rule processing.
	 * 
	 * @throws JRDFoxException   thrown if an error occurs
	 */
	public void updateStatistics() throws JRDFoxException {
		ensureValidState();
		nUpdateStatistics(m_storePtr);
	}

	/**
	 * Makes all facts explicit.
	 * 
	 * @throws JRDFoxException	thrown if an error occurs
	 */
	public void makeFactsExplicit() throws JRDFoxException {
		ensureValidState();
		nMakeFactsExplicit(m_storePtr);
	}

	/**
	 * Removes all rules and makes the current facts EDBs.
	 * 
	 * @throws JRDFoxException   thrown if an error occurs
	 */
	public void clearRulesAndMakeFactsExplicit() throws JRDFoxException {
		ensureValidState();
		nClearRulesAndMakeFactsExplicit(m_storePtr);
	}

	/**
	 * Saves the current store into a file.
	 * 
	 * @param file                  the file where the store is saved.
	 * @throws JRDFoxException   	thrown if an error occurs
	 */
	public void save(File file) throws JRDFoxException {
		ensureValidState();
		nSave(m_storePtr, file.getAbsolutePath());
	}

	/**
	 * Compiles a query.
	 * 
	 * @param query                 the query
	 * @param prefixes              the prefixes used in the query
	 * @param parameters            the parameters used during query compilation
	 * @param windowSize            the size of the window for query answer retrieval
	 * @return                      a tuple iterator
	 * @throws JRDFoxException   	thrown if an error occurs
	 */
	public TupleIterator compileQuery(String query, Prefixes prefixes, Map<String, String> parameters, int windowSize) throws JRDFoxException {
		ensureValidState();		
		return new TupleIterator(this, query, prefixes, parameters, windowSize);
	}

	/**
	 * Compiles a query.
	 * 
	 * @param query                 the query
	 * @param prefixes              the prefixes used in the query
	 * @param parameters            the parameters used during query compilation
	 * @return                      a tuple iterator
	 * @throws JRDFoxException   	thrown if an error occurs
	 */
	public TupleIterator compileQuery(String query, Prefixes prefixes, Map<String, String> parameters) throws JRDFoxException { 
		return compileQuery(query, prefixes, parameters, DEFAULT_WINDOW_SIZE);
	}

	/**
	 * Compiles a query.
	 * 
	 * @param query                 the query
	 * @param prefixes              the prefixes used in the query
	 * @return                      a tuple iterator
	 * @throws JRDFoxException   	thrown if an error occurs
	 */
	public TupleIterator compileQuery(String query, Prefixes prefixes) throws JRDFoxException { 
		return compileQuery(query, prefixes, Collections.emptyMap(), DEFAULT_WINDOW_SIZE);
	}

	/**
	 * Compiles a query.
	 * 
	 * @param query                 the query
	 * @return                      a tuple iterator
	 * @throws JRDFoxException   	thrown if an error occurs
	 */
	public TupleIterator compileQuery(String query) throws JRDFoxException {
		return compileQuery(query, Prefixes.EMPTY_IMMUTABLE_INSTANCE, Collections.emptyMap(), DEFAULT_WINDOW_SIZE);
	}
	
	/***
	 * Returns the number of (IDB) triples in the store
	 * @return						the number of triples
	 * @throws JRDFoxException		thrown if an error occurs
	 */
	public long getTriplesCount() throws JRDFoxException {
		return getTriplesCount(QueryDomain.IDBrep);
	}
	
	/***
	 * Returns the number of triples in the store for the specified query domain
	 * @param queryDomain determines which triples to be counted
	 * @return						the number of triples
	 * @throws JRDFoxException		thrown if an error occurs
	 */
	public long getTriplesCount(QueryDomain queryDomain) throws JRDFoxException {
		HashMap<String, String> parameters = new HashMap<String, String>();
		parameters.put(QueryDomain.s_DomainKey, queryDomain.m_name);
		TupleIterator tupleIterator = compileQuery("select distinct where { ?x ?y ?z }", Prefixes.EMPTY_IMMUTABLE_INSTANCE, parameters);
		try {
			return tupleIterator.open();
		}
		finally {
			tupleIterator.dispose();
		}
	}
	
	/**
	 * Retrieves the dictionary object for the current store
	 * @return Returns the dictionary used by the current store
	 * @throws JRDFoxException	thrown if an error occurs
	 */
	public Dictionary getDictionary() throws JRDFoxException {
		return m_dictionary;
	}

	/**
	 * Disposes the store. Make sure you always call this method once the store is no longer needed!
	 */
	public synchronized void dispose() {
		if (m_storePtr != 0) {
			nDispose(m_storePtr);
			m_storePtr = 0;
			m_dictionary.m_dictionaryPtr = 0;
		}
	}

	protected void finalize() {
		dispose();
	}

	static protected String[] mapToStringArray(Map<String, String> map) {
		String[] stringArray = new String[2 * map.size()];
		int currentIndex = 0;
		for (Map.Entry<String, String> entry : map.entrySet()) {
			if (entry.getValue() == null)
				throw new NullPointerException();
			stringArray[currentIndex++] = entry.getKey();
			stringArray[currentIndex++] = entry.getValue();
		}
		return stringArray;
	}
}
