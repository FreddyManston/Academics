// JRDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

package uk.ac.ox.cs.JRDFox.owl2rl;

import uk.ac.ox.cs.JRDFox.model.Variable;

public class VariableProducer {
	protected int m_nextVariableID;

	public VariableProducer() {
		m_nextVariableID = 0;
	}

	public Variable nextFreshVariable() {
		Variable result;
		if (m_nextVariableID == 0)
			result = Variable.create("X");
		else
			result = Variable.create("X" + m_nextVariableID);
		m_nextVariableID++;
		return result;
	}
}
