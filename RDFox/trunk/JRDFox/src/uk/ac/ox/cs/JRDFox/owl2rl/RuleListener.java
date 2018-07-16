// JRDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

package uk.ac.ox.cs.JRDFox.owl2rl;

import uk.ac.ox.cs.JRDFox.model.Atom;
import uk.ac.ox.cs.JRDFox.model.Rule;

public interface RuleListener {
	
	void addRule(Rule rule);

	void addAtom(Atom atom);
	
}
