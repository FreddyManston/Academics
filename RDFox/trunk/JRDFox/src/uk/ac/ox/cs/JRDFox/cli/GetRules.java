// JRDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

package uk.ac.ox.cs.JRDFox.cli;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyManager;
import org.semanticweb.owlapi.util.AutoIRIMapper;
import org.semanticweb.owlapi.util.SimpleIRIMapper;

import uk.ac.ox.cs.JRDFox.JRDFoxException;
import uk.ac.ox.cs.JRDFox.Prefixes;
import uk.ac.ox.cs.JRDFox.model.Atom;
import uk.ac.ox.cs.JRDFox.model.Rule;
import uk.ac.ox.cs.JRDFox.owl2rl.OWL2RLTranslator;
import uk.ac.ox.cs.JRDFox.owl2rl.RuleListener;

public class GetRules {
	public static void main(String[] args) throws Exception {
		try {
			boolean processLogicalTBoxAxioms = true;
			boolean processLogicABoxAxioms = false;
			boolean processAnnotationTBoxAxioms = false;
			boolean processAnnotationABoxAxioms = false;
			OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
			int argumentIndex = 0;
			while (argumentIndex < args.length) {
				if ("-dir".equalsIgnoreCase(args[argumentIndex])) {
					if (argumentIndex + 1 >= args.length)
						throw new Usage();
					String directory = args[argumentIndex + 1];
					System.out.print("Directory '");
					System.out.print(directory);
					System.out.println("' will be used to look up ontologies.");
					manager.addIRIMapper(new AutoIRIMapper(new File(directory), true));
					argumentIndex += 2;
				}
				else if ("-map".equalsIgnoreCase(args[argumentIndex])) {
					if (argumentIndex + 1 >= args.length)
						throw new Usage();
					File mapFile = new File(args[argumentIndex + 1]);
					Properties map = new Properties();
					map.loadFromXML(new FileInputStream(mapFile));
					File mapFileParent = mapFile.getParentFile();
					for (Map.Entry<Object, Object> entry : map.entrySet()) {
						String ontologyIRI = entry.getKey().toString();
						String ontologyDocumentFileName = entry.getValue().toString();
						File ontologyDocumentFile = new File(mapFileParent, ontologyDocumentFileName);
						manager.addIRIMapper(new SimpleIRIMapper(IRI.create(ontologyIRI), IRI.create(ontologyDocumentFile)));
						System.out.print("Ontology '");
						System.out.print(ontologyIRI);
						System.out.print("' will be loaded from file '");
						System.out.print(ontologyDocumentFile);
						System.out.println("'.");
					}
					argumentIndex += 2;
				}
				else if ("-TBox".equalsIgnoreCase(args[argumentIndex])) {
					argumentIndex++;
					if (argumentIndex >= args.length)
						throw new Usage();
					processLogicalTBoxAxioms = "yes".equalsIgnoreCase(args[argumentIndex]) || "true".equalsIgnoreCase(args[argumentIndex]) || "on".equalsIgnoreCase(args[argumentIndex]);
					argumentIndex++;
				}	
				else if ("-ABox".equalsIgnoreCase(args[argumentIndex])) {
					argumentIndex++;
					if (argumentIndex >= args.length)
						throw new Usage();
					processLogicABoxAxioms = "yes".equalsIgnoreCase(args[argumentIndex]) || "true".equalsIgnoreCase(args[argumentIndex]) || "on".equalsIgnoreCase(args[argumentIndex]);
					argumentIndex++;
				}
				else if ("-AnnotationTBox".equalsIgnoreCase(args[argumentIndex])) {
					argumentIndex++;
					if (argumentIndex >= args.length)
						throw new Usage();
					processAnnotationTBoxAxioms = "yes".equalsIgnoreCase(args[argumentIndex]) || "true".equalsIgnoreCase(args[argumentIndex]) || "on".equalsIgnoreCase(args[argumentIndex]);
					argumentIndex++;
				}	
				else if ("-AnnotationABox".equalsIgnoreCase(args[argumentIndex])) {
					argumentIndex++;
					if (argumentIndex >= args.length)
						throw new Usage();
					processAnnotationABoxAxioms = "yes".equalsIgnoreCase(args[argumentIndex]) || "true".equalsIgnoreCase(args[argumentIndex]) || "on".equalsIgnoreCase(args[argumentIndex]);
					argumentIndex++;
				}
				else
					break;
			}
			if (argumentIndex + 2 != args.length)
				throw new Usage();
			File inputFile = new File(args[argumentIndex]);
			File outputFile = new File(args[argumentIndex + 1]);
			System.out.print("Loading the ontology from file '");
			System.out.print(inputFile);
			System.out.print("'...");
			System.out.flush();
			OWLOntology rootOntology = manager.loadOntologyFromOntologyDocument(inputFile);
			System.out.println(" DONE!");
			System.out.print("Translating the ontology into rules...");
			System.out.flush();
			final Prefixes prefixes = new Prefixes();
			final PrintWriter output = new PrintWriter(new OutputStreamWriter(new FileOutputStream(outputFile), "UTF-8"));
			try {
				List<JRDFoxException> errors = new ArrayList<JRDFoxException>();
				new OWL2RLTranslator(new RuleListener() {
					public void addRule(Rule rule) {
						rule.print(output, prefixes);
						output.println();
					}
					public void addAtom(Atom atom) {
						atom.print(output, prefixes);
						output.println(" .");
					}
					
				}, errors, processLogicalTBoxAxioms, processLogicABoxAxioms, processAnnotationTBoxAxioms, processAnnotationABoxAxioms).translate(rootOntology, true);
				System.out.println(" DONE!");
				if (!errors.isEmpty()) {
					System.out.println("The following errors were reported during translation:");
					System.out.println("------------------------------------------------------------------------");
					boolean first = true;
					for (JRDFoxException exception : errors) {
						if (first)
							first = false;
						else
							System.out.println();
						System.out.println(exception.getMessage());
					}
					System.out.println("------------------------------------------------------------------------");
				}
			}
			finally {
				output.close();
			}
			System.out.println(" DONE!");
		}
		catch (Usage e) {
			System.out.println("Usage: GetRules (-TBox (true | false) | -ABox (true | false) | -AnnotationTBox (true | false) | -AnnotationABox (true | false) | -dir <directory for ontologies> | -map <ontology mapping file>)* <input> <output>");
		}
	}
	
	@SuppressWarnings("serial")
	protected static class Usage extends Exception {
	}
}
