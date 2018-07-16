// JRDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

package uk.ac.ox.cs.JRDFox.owl2rl;

import java.util.ArrayList;
import java.util.List;

import uk.ac.ox.cs.JRDFox.model.Atom;
import uk.ac.ox.cs.JRDFox.model.Rule;

public abstract class BodyExpression {
    public static final TerminalBodyExpression TERMINAL = new TerminalBodyExpression();

    public BodyExpression prepend(Atom atom) {
        return new ConjunctionBodyExpression(atom, this);
    }
    public void getRules(RuleListener ruleListener, Atom head) {
        List<Atom> bodyAtoms = new ArrayList<Atom>();
        getRules(ruleListener, head, bodyAtoms);
    }
    protected abstract void getRules(RuleListener ruleListener, Atom head, List<Atom> bodyAtoms);

    public static class TerminalBodyExpression extends BodyExpression {
        protected TerminalBodyExpression() {
        }
        protected void getRules(RuleListener ruleListener, Atom head, List<Atom> bodyAtoms) {
            Rule rule = Rule.create(head, bodyAtoms.toArray(new Atom[bodyAtoms.size()]));
            ruleListener.addRule(rule);
        }
    }

    protected static class ConjunctionBodyExpression extends BodyExpression {
        protected final Atom m_atom;
        protected final BodyExpression m_successor;

        public ConjunctionBodyExpression(Atom atom, BodyExpression successor) {
            m_atom = atom;
            m_successor = successor;
        }
        protected void getRules(RuleListener ruleListener, Atom head, List<Atom> bodyAtoms) {
            // This method could have been implemented recursively in the obvious way.
            // We eliminate tail recursion as a slight optimization.
            BodyExpression current = this;
            while (current instanceof ConjunctionBodyExpression) {
                ConjunctionBodyExpression atomBodyExpression = (ConjunctionBodyExpression)current;
                bodyAtoms.add(atomBodyExpression.m_atom);
                current = atomBodyExpression.m_successor;
            }
            current.getRules(ruleListener, head, bodyAtoms);
        }
    }

    public static class DisjunctiveBodyExpression extends BodyExpression {
        protected final BodyExpression[] m_disjuncts;

        public DisjunctiveBodyExpression(BodyExpression[] disjuncts) {
            m_disjuncts = disjuncts;
        }
        protected void getRules(RuleListener ruleListener, Atom head, List<Atom> bodyAtoms) {
            int lastAtomIndex = bodyAtoms.size() - 1;
            for (BodyExpression disjunct : m_disjuncts) {
                disjunct.getRules(ruleListener, head, bodyAtoms);
                int toDeleteIndex = bodyAtoms.size() - 1;
                while (toDeleteIndex > lastAtomIndex) {
                    bodyAtoms.remove(toDeleteIndex);
                    toDeleteIndex--;
                }
            }
        }
    }
}
