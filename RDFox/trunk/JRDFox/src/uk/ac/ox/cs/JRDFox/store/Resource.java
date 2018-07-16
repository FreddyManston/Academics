// JRDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

package uk.ac.ox.cs.JRDFox.store;

import java.io.PrintWriter;

import uk.ac.ox.cs.JRDFox.Prefixes;
import uk.ac.ox.cs.JRDFox.model.Datatype;

public class Resource {
	public String m_lexicalForm;
	public Datatype m_datatype;
	
	public Resource(String lexicalForm, Datatype datatype) {
		m_lexicalForm = lexicalForm;
		m_datatype = datatype;
	}

	public void print(PrintWriter output, Prefixes prefixes) {
		print(output, prefixes, m_lexicalForm, m_datatype);
	}

	public String toString() {
		StringBuilder builder = new StringBuilder();
		toString(builder, Prefixes.EMPTY_IMMUTABLE_INSTANCE, m_lexicalForm, m_datatype);
		return builder.toString();
	}
	
	public String toString(Prefixes prefixes) {
		StringBuilder builder = new StringBuilder();
		toString(builder, prefixes, m_lexicalForm, m_datatype);
		return builder.toString();
	}

	public void toString(StringBuilder builder, Prefixes prefixes) {
		toString(builder, prefixes, m_lexicalForm, m_datatype);
	}

	protected static void printString(PrintWriter output, String string, int toPosition) {
		output.print('"');
		for (int index = 0; index < toPosition; ++index) {
			char c = string.charAt(index);
			switch (c) {
			case '\n':
				output.print("\\n");
				break;
			case '\r':
				output.print("\\r");
				break;
			case '"':
			case '\\':
				output.print('\\');
				// deliberate fall-through!
			default:
				output.print(c);
				break;
			}
		}
		output.print('"');
	}
	
	public static void print(PrintWriter output, Prefixes prefixes, String lexicalForm, Datatype datatype) {
		switch (datatype) {
		case INVALID_DATATYPE:
			output.print("UNDEF");
			break;
		case IRI_REFERENCE:
			output.print(prefixes.abbreviateIRI(lexicalForm));
			break;
		case BLANK_NODE:
			output.print("_:");
			output.print(lexicalForm);
			break;
		case XSD_STRING:
			printString(output, lexicalForm, lexicalForm.length());
			break;
		case RDF_PLAIN_LITERAL:
			{
				int atPosition = lexicalForm.lastIndexOf('@');
				int lexicalFormLength = lexicalForm.length();
				if (atPosition == -1)
					printString(output, lexicalForm, lexicalFormLength);
				else {
					printString(output, lexicalForm, atPosition);
					if (atPosition + 1 < lexicalFormLength)
						for (int index = atPosition; index < lexicalFormLength; ++index)
							output.print(lexicalForm.charAt(index));
				}
			}
		 	break;
		default:
			printString(output, lexicalForm, lexicalForm.length());
			output.print("^^");
			output.print(prefixes.abbreviateIRI(datatype.getIRI()));
			break;
		}
	}

	protected static void appendString(StringBuilder builder, String string, int toPosition) {
		builder.append('"');
		for (int index = 0; index < toPosition; ++index) {
			char c = string.charAt(index);
			switch (c) {
			case '\n':
				builder.append("\\n");
				break;
			case '\r':
				builder.append("\\r");
				break;
			case '"':
			case '\\':
				builder.append('\\');
				// deliberate fall-through!
			default:
				builder.append(c);
				break;
			}
		}
		builder.append('"');
	}
	
	public static void toString(StringBuilder builder, Prefixes prefixes, String lexicalForm, Datatype datatype) {
		switch (datatype) {
		case INVALID_DATATYPE:
			builder.append("UNDEF");
			break;
		case IRI_REFERENCE:
			prefixes.appendAbbreviatedIRI(builder, lexicalForm);
			break;
		case BLANK_NODE:
			builder.append("_:");
			builder.append(lexicalForm);
			break;
		case XSD_STRING:
			appendString(builder, lexicalForm, lexicalForm.length());
			break;
		case RDF_PLAIN_LITERAL:
			{
				int atPosition = lexicalForm.lastIndexOf('@');
				int lexicalFormLength = lexicalForm.length();
				if (atPosition == -1)
					appendString(builder, lexicalForm, lexicalFormLength);
				else {
					appendString(builder, lexicalForm, atPosition);
					if (atPosition + 1 < lexicalFormLength)
						for (int index = atPosition; index < lexicalFormLength; ++index)
							builder.append(lexicalForm.charAt(index));
				}
			}
		 	break;
		default:
			appendString(builder, lexicalForm, lexicalForm.length());
			builder.append("^^");
			prefixes.appendAbbreviatedIRI(builder, datatype.getIRI());
			break;
		}
	}
}
