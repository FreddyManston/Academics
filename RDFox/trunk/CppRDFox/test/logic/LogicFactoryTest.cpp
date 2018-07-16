// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifdef WITH_TEST

#define SUITE_NAME        LogicFactoryTest
#define FIXTURE_NAME    AbstractLogicTest

#include <CppTest/AutoTest.h>

#include "AbstractLogicTest.h"

TEST(testVariable) {
    Variable X = V("X");
    Variable Xprime = V("X");
    ASSERT_TRUE(X.get() == Xprime.get());
    ASSERT_TRUE(X == Xprime);
    Variable Y = V("Y");
    ASSERT_FALSE(X == Y);
    ASSERT_TRUE(X != Y);
    ASSERT_MEMBERS(X, X->getFreeVariables());
    ASSERT_MEMBERS(Y, Y->getFreeVariables());

    ResourceByID r2 = m_factory->getResourceByID(2);
    ASSERT_TRUE(Y == X->apply(S() << M(X, Y)));
    ASSERT_TRUE(r2 == X ->apply(S() << M(X, r2)));

    LogicFactory logicFactory(::newLogicFactory());
    Variable Xc = X->clone(logicFactory);
    ASSERT_TRUE(X != Xc);
    ASSERT_EQUAL(X->getName(), Xc->getName());
}

TEST(testResourceByID) {
    ResourceByID r2 = m_factory->getResourceByID(2);
    ResourceByID r2prime = m_factory->getResourceByID(2);
    ASSERT_TRUE(r2.get() == r2prime.get());
    ASSERT_TRUE(r2 == r2prime);
    ResourceByID r3 = m_factory->getResourceByID(3);
    ASSERT_FALSE(r2 == r3);
    ASSERT_TRUE(r2 != r3);
    ASSERT_EMPTY(r2->getFreeVariables());
    ASSERT_EMPTY(r3->getFreeVariables());
    ASSERT_TRUE(r2 == r2->apply(S() << M(V("X"), r3)));

    LogicFactory logicFactory(::newLogicFactory());
    ResourceByID r2c = r2->clone(logicFactory);
    ASSERT_TRUE(r2 != r2c);
    ASSERT_EQUAL(r2->getResourceID(), r2c->getResourceID());
}

TEST(testResourceByName) {
    ResourceByName u1 = I("u1");
    ResourceByName u1prime = I("u1");
    ASSERT_TRUE(u1.get() == u1prime.get());
    ASSERT_TRUE(u1 == u1prime);
    ASSERT_EMPTY(u1->getFreeVariables());
    ASSERT_TRUE(u1 == u1->apply(S() << M(V("X"), V("Y"))));

    ResourceByName b1 = B("u1");
    ASSERT_TRUE(u1.get() != b1.get());
    ASSERT_TRUE(u1 != b1);
    ASSERT_EMPTY(b1->getFreeVariables());
    ASSERT_TRUE(b1 == b1->apply(S() << M(V("X"), V("Y"))));

    ResourceByName l1 = L("u1", "d1");
    ASSERT_TRUE(u1.get() != l1.get());
    ASSERT_TRUE(u1 != l1);
    ASSERT_TRUE(b1.get() != l1.get());
    ASSERT_TRUE(b1 != l1);
    ASSERT_EMPTY(l1->getFreeVariables());
    ASSERT_TRUE(l1 == l1->apply(S() << M(V("X"), V("Y"))));

    ResourceByName l2 = L("u1", "d2");
    ASSERT_TRUE(l1 != l2);

    ResourceByName l2prime = L("u1", "d2");
    ASSERT_TRUE(l2.get() == l2prime.get());
    ASSERT_TRUE(l2 == l2prime);

    LogicFactory logicFactory(::newLogicFactory());
    ResourceByName u1c = u1->clone(logicFactory);
    ASSERT_TRUE(u1 != u1c);
    ASSERT_EQUAL(u1->getResourceText(), u1c->getResourceText());
}

TEST(testPredicate) {
    Predicate p1 = P("p1");
    Predicate p1prime = P("p1");
    ASSERT_TRUE(p1.get() == p1prime.get());
    ASSERT_TRUE(p1 == p1prime);
    Predicate p2 = P("p2");
    ASSERT_FALSE(p1 == p2);
    ASSERT_TRUE(p1 != p2);

    LogicFactory logicFactory(::newLogicFactory());
    Predicate p1c = p1->clone(logicFactory);
    ASSERT_TRUE(p1 != p1c);
    ASSERT_EQUAL(p1->getName(), p1c->getName());
}

TEST(testAtom) {
    Predicate p1 = m_factory->getPredicate("p1");
    Term ar1 = I("iri1");
    Variable ar2 = V("X");
    ResourceByName ar3 = L("abc", "datatype-iri1");
    Variable ar4 = V("Y");
    std::vector<Term> args1;
    args1.push_back(ar1);
    args1.push_back(ar2);
    args1.push_back(ar3);
    args1.push_back(ar4);

    Atom a1 = A(p1, args1);
    Atom a1prime = A(p1, args1);
    ASSERT_TRUE(a1.get() == a1prime.get());
    ASSERT_TRUE(a1 == a1prime);
    ASSERT_MEMBERS(ar2 << ar4, a1->getFreeVariables());
    ASSERT_MEMBERS(ar4 << ar2, a1->getFreeVariables()); // this is more a test of the test framework!

    std::vector<Term> args1apply;
    args1apply.push_back(ar1);
    args1apply.push_back(I("s"));
    args1apply.push_back(ar3);
    args1apply.push_back(ar4);
    ASSERT_TRUE(A(p1, args1apply) == a1->apply(S() << M(V("X"), I("s"))));

    std::vector<Term> args2;
    args2.push_back(ar1);
    args2.push_back(ar2);
    args2.push_back(ar4);
    args2.push_back(ar3);
    Atom a2 = A(p1, args2);
    ASSERT_TRUE(a1.get() != a2.get());
    ASSERT_TRUE(a1 != a2);
    ASSERT_MEMBERS(ar2 << ar4, a2->getFreeVariables());

    std::vector<Term> args3;
    args3.push_back(ar1);
    args3.push_back(ar2);
    Atom a3 = A(p1, args3);
    ASSERT_TRUE(a1.get() != a2.get());
    ASSERT_TRUE(a1 != a2);
    ASSERT_MEMBERS(ar2, a3->getFreeVariables());

    Predicate p4 = m_factory->getPredicate("p4");
    Atom a4 = A(p4, args1);
    ASSERT_TRUE(a1.get() != a4.get());
    ASSERT_TRUE(a1 != a4);

    Predicate p5 = m_factory->getRDFPredicate();
    std::vector<Term> args5;
    args5.push_back(ar1);
    args5.push_back(ar2);
    args5.push_back(ar3);
    Atom a5 = A(p5, args5);
    Atom a5prime = A(ar1, ar2, ar3);
    ASSERT_TRUE(a5.get() == a5prime.get());
    ASSERT_TRUE(a5 == a5prime);

    LogicFactory logicFactory(::newLogicFactory());
    Atom a1c = a1->clone(logicFactory);
    ASSERT_TRUE(a1 != a1c);
}

TEST(testNegation) {
    Atom a1 = A(I("a"), V("X"), V("Y"));
    Negation na1 = NOT(a1);
    Negation na1prime = NOT(a1);
    ASSERT_TRUE(na1.get() == na1prime.get());
    ASSERT_TRUE(na1 == na1prime);
    ASSERT_TRUE(na1->getAtomicFormula(0) == a1);

    Atom a2 = A(I("b"), V("X"), V("Y"));
    Negation na2 = NOT(a2);
    ASSERT_TRUE(na1.get() != na2.get());
    ASSERT_TRUE(na1 != na2);

    Atom a1apply = A(I("a"), I("s"), V("Y"));
    Negation na1apply = NOT(a1apply);
    ASSERT_TRUE(na1apply == na1->apply(S() << M(V("X"), I("s"))));

    Atom a3 = A(I("b"), V("X"), V("Y"));
    Negation na3 = NOT(VA() << V("X"), AFA() << a3);
    ASSERT_TRUE(na3.get() != na2.get());
    ASSERT_TRUE(na3 != na2);

    Atom a3apply = A(I("b"), V("X"), I("t"));
    Negation na3apply = NOT(VA() << V("X"), AFA() << a3apply);
    ASSERT_TRUE(na3apply == na3->apply(S() << M(V("X"),I("s")) << M(V("Y"),I("t"))));

    Negation na13 = NOT(VA() << V("Y"), AFA() << a1 << a3);
    ASSERT_EQUAL(1, na13->getNumberOfExistentialVariables());
    ASSERT_TRUE(na13->getExistentialVariable(0) == V("Y"));
    ASSERT_EQUAL(2, na13->getNumberOfAtomicFormulas());
    ASSERT_TRUE(na13->getAtomicFormula(0) == a1);
    ASSERT_TRUE(na13->getAtomicFormula(1) == a3);
    Negation na13apply = NOT(VA() << V("Y"), AFA() << A(I("a"), I("s"), V("Y")) << A(I("b"), I("s"), V("Y")));
    ASSERT_TRUE(na13apply == na13->apply(S() << M(V("X"),I("s")) << M(V("Y"),I("t"))));

    LogicFactory logicFactory(::newLogicFactory());
    Negation na1c = na1->clone(logicFactory);
    ASSERT_TRUE(na1 != na1c);
}

TEST(testAggregateBind) {
    AggregateBind ab1 = AGGBND("min", true, BEA() << BBFC("+", V("X"), V("Y")) , V("Z"));
    AggregateBind ab1prime = AGGBND("min", true, BEA() << BBFC("+", V("X"), V("Y")) , V("Z"));
    ASSERT_TRUE(ab1.get() == ab1prime.get());
    ASSERT_TRUE(ab1 == ab1prime);

    AggregateBind ab1apply = AGGBND("min", true, BEA() << BBFC("+", V("W"), V("Y")) , I("a"));
    ASSERT_TRUE(ab1apply == ab1->apply(S() << M(V("X"), V("W")) << M(V("Z"), I("a"))));

    LogicFactory logicFactory(::newLogicFactory());
    AggregateBind aec = ab1->clone(logicFactory);
    ASSERT_TRUE(ab1 != aec);
}

TEST(testAggregate) {
    Atom at1 = A(I("a"), V("X"), V("Y"));

    Aggregate agg1 = AGG(AFA() << at1, false,
        VA() << V("X"),
        AGGBNDA() << AGGBND("count", true, BEA() << V("Y"), V("Z"))
    );
    Aggregate agg1prime = AGG(AFA() << at1, false,
        VA() << V("X"),
        AGGBNDA() << AGGBND("count", true, BEA() << V("Y"), V("Z"))
    );
    ASSERT_TRUE(agg1.get() == agg1prime.get());
    ASSERT_TRUE(agg1 == agg1prime);
    ASSERT_TRUE(agg1->getAtomicFormula(0) == at1);
    ASSERT_MEMBERS(V("X") << V("Z"), agg1->getFreeVariables());
    ASSERT_MEMBERS(V("X") << V("Z"), agg1->getArguments());

    Aggregate agg2 = AGG(AFA() << at1, false,
        VA(),
        AGGBNDA() << AGGBND("count", true, BEA() << V("Y"), V("Z"))
    );
    ASSERT_TRUE(agg1.get() != agg2.get());
    ASSERT_TRUE(agg1 != agg2);

    Atom at1apply = A(I("a"), I("s"), V("Y"));
    Aggregate agg1apply = AGG(AFA() << at1apply, false,
        VA(),
        AGGBNDA() << AGGBND("count", true, BEA() << V("Y"), I("t"))
    );
    ASSERT_TRUE(agg1apply == agg1->apply(S() << M(V("X"), I("s")) << M(V("Z"), I("t"))));

    LogicFactory logicFactory(::newLogicFactory());
    Aggregate agg1c = agg1->clone(logicFactory);
    ASSERT_TRUE(agg1 != agg1c);
}

TEST(testConjunction) {
    Atom a1 = A(I("a"), V("X"), V("Y"));
    Atom a2 = A(V("X"), I("a"), V("Z"));
    Conjunction c1 = AND(a1, a2);
    std::vector<Formula> conjuncts;
    conjuncts.push_back(a1);
    conjuncts.push_back(a2);
    Conjunction c1prime = AND(conjuncts);
    ASSERT_TRUE(c1.get() == c1prime.get());
    ASSERT_TRUE(c1 == c1prime);
    ASSERT_MEMBERS(V("X") << V("Y") << V("Z"), c1->getFreeVariables());

    Conjunction c2 = AND(a2, a1);
    ASSERT_TRUE(c1.get() != c2.get());
    ASSERT_TRUE(c1 != c2);

    Atom a1apply = A(I("a"), I("s"), V("Y"));
    Atom a2apply = A(I("s"), I("a"), V("Z"));
    std::vector<Formula> conjunctsApply;
    conjunctsApply.push_back(a1apply);
    conjunctsApply.push_back(a2apply);
    Conjunction c1apply = AND(conjunctsApply);
    ASSERT_TRUE(c1apply == c1->apply(S() << M(V("X"), I("s"))));

    LogicFactory logicFactory(::newLogicFactory());
    Conjunction c1c = c1->clone(logicFactory);
    ASSERT_TRUE(c1 != c1c);
}

TEST(testDisjunction) {
    Atom a1 = A(I("a"), V("X"), V("Y"));
    Atom a2 = A(V("X"), I("a"), V("Z"));
    Disjunction d1 = OR(a1, a2);
    std::vector<Formula> disjuncts;
    disjuncts.push_back(a1);
    disjuncts.push_back(a2);
    Disjunction d1prime = OR(disjuncts);
    ASSERT_TRUE(d1.get() == d1prime.get());
    ASSERT_TRUE(d1 == d1prime);
    ASSERT_MEMBERS(V("X") << V("Y") << V("Z"), d1->getFreeVariables());

    Disjunction d2 = OR(a2, a1);
    ASSERT_TRUE(d1.get() != d2.get());
    ASSERT_TRUE(d1 != d2);

    Atom a1apply = A(I("a"), I("s"), V("Y"));
    Atom a2apply = A(I("s"), I("a"), V("Z"));
    std::vector<Formula> disjunctsAapply;
    disjunctsAapply.push_back(a1apply);
    disjunctsAapply.push_back(a2apply);
    Disjunction d1apply = OR(disjunctsAapply);
    ASSERT_TRUE(d1apply == d1->apply(S() << M(V("X"), I("s"))));

    LogicFactory logicFactory(::newLogicFactory());
    Disjunction d1c = d1->clone(logicFactory);
    ASSERT_TRUE(d1 != d1c);
}

TEST(testOptional) {
    Atom a1 = A(I("a"), V("X"), V("Y"));
    Atom a2 = A(V("X"), I("a"), V("Z"));
    Optional o1 = OPT(a1, a2);
    std::vector<Formula> optionals;
    optionals.push_back(a2);
    Optional o1prime = OPT(a1, optionals);
    ASSERT_TRUE(o1.get() == o1prime.get());
    ASSERT_TRUE(o1 == o1prime);
    ASSERT_MEMBERS(V("X") << V("Y") << V("Z"), o1->getFreeVariables());

    Optional o2 = OPT(a2, a1);
    ASSERT_TRUE(o1.get() != o2.get());
    ASSERT_TRUE(o1 != o2);

    Atom a1apply = A(I("a"), I("s"), V("Y"));
    Atom a2apply = A(I("s"), I("a"), V("Z"));
    std::vector<Formula> optionalsApply;
    optionalsApply.push_back(a2apply);
    Optional o1apply = OPT(a1apply, optionalsApply);
    ASSERT_TRUE(o1apply == o1->apply(S() << M(V("X"), I("s"))));

    LogicFactory logicFactory(::newLogicFactory());
    Optional o1c = o1->clone(logicFactory);
    ASSERT_TRUE(o1 != o1c);
}

TEST(testMinus) {
    Atom a1 = A(I("a"), V("X"), V("Y"));
    Atom a2 = A(V("X"), I("a"), V("Z"));
    Minus m1 = MINUS(a1, a2);
    std::vector<Formula> subtrahends;
    subtrahends.push_back(a2);
    Minus m1prime = MINUS(a1, subtrahends);
    ASSERT_TRUE(m1.get() == m1prime.get());
    ASSERT_TRUE(m1 == m1prime);
    ASSERT_MEMBERS(V("X") << V("Y"), m1->getFreeVariables());

    Minus m2 = MINUS(a2, a1);
    ASSERT_TRUE(m1.get() != m2.get());
    ASSERT_TRUE(m1 != m2);

    Atom a1apply = A(I("a"), I("s"), V("Y"));
    Atom a2apply = A(I("s"), I("a"), V("Z"));
    std::vector<Formula> subtrahendsApply;
    subtrahendsApply.push_back(a2apply);
    Minus m1apply = MINUS(a1apply, subtrahendsApply);
    ASSERT_TRUE(m1apply == m1->apply(S() << M(V("X"), I("s")) << M(V("Z"), I("u"))));

    LogicFactory logicFactory(::newLogicFactory());
    Minus m1c = m1->clone(logicFactory);
    ASSERT_TRUE(m1 != m1c);
}

TEST(testValues) {
    Variable X = V("X");
    Variable Y = V("Y");
    Variable Z = V("Z");

    ResourceByName a = I("a");
    ResourceByName b = I("b");
    ResourceByName c = I("c");
    ResourceByName d = I("d");
    ResourceByName e = I("e");

    std::vector<std::vector<GroundTerm> > data;
    data.push_back(GTA() << a << b);
    data.push_back(GTA() << UNDEF() << c);
    data.push_back(GTA() << d << e);

    Values v1 = VALUES(VA() << X << Y, data);
    Values v1prime = VALUES(VA() << X << Y, data);
    ASSERT_TRUE(v1.get() == v1prime.get());
    ASSERT_TRUE(v1 == v1prime);
    ASSERT_MEMBERS(V("X") << V("Y"), v1->getFreeVariables());

    Values v2 = VALUES(VA() << Y << X, data);
    ASSERT_TRUE(v1.get() != v2.get());
    ASSERT_TRUE(v1 != v2);

    Values v1apply = VALUES(VA() << X << Z, data);
    ASSERT_TRUE(v1apply == v1->apply(S() << M(Y, Z)));

    LogicFactory logicFactory(::newLogicFactory());
    Values v1c = v1->clone(logicFactory);
    ASSERT_TRUE(v1 != v1c);
}

TEST(testQuery) {
    Variable X = V("X");
    Variable Y = V("Y");
    Variable Z = V("Z");
    Atom a1 = A(X, Y, Z);
    std::vector<Term> terms1;
    terms1.push_back(X);
    terms1.push_back(Y);
    Query q1 = Q(true, terms1, a1);
    Query q1prime = Q(true, terms1, a1);
    ASSERT_TRUE(q1.get() == q1prime.get());
    ASSERT_TRUE(q1 == q1prime);
    ASSERT_TRUE(q1->isDistinct());
    ASSERT_MEMBERS(X << Y, q1->getFreeVariables());

    Atom a1apply = A(X, I("u"), Z);
    std::vector<Term> terms1apply;
    terms1apply.push_back(X);
    terms1apply.push_back(I("u"));
    Query q1apply = Q(true, terms1apply, a1apply);
    ASSERT_TRUE(q1apply == q1->apply(S() << M(V("Y"), I("u")) << M(V("Z"), I("s"))));

    std::vector<Term> terms2;
    terms2.push_back(Y);
    terms2.push_back(X);
    Query q2 = Q(true, terms2, a1);
    ASSERT_TRUE(q1.get() != q2.get());
    ASSERT_TRUE(q1 != q2);
    ASSERT_MEMBERS(X << Y, q2->getFreeVariables());

    std::vector<Formula> conjs;
    conjs.push_back(a1);
    Query q3 = Q(true, terms2, a1);
    ASSERT_TRUE(q1.get() != q3.get());
    ASSERT_TRUE(q1 != q3);
    ASSERT_MEMBERS(X << Y, q3->getFreeVariables());

    LogicFactory logicFactory(::newLogicFactory());
    Query q1c = q1->clone(logicFactory);
    ASSERT_TRUE(q1 != q1c);
}

TEST(testRule1) {
    Variable X = V("X");
    Variable Y = V("Y");
    Variable Z = V("Z");
    ResourceByName res1 = I("res1");
    ResourceByName res2 = I("res2");
    Atom h = A(Y, res2, X);
    std::vector<Literal> body;
    body.push_back(A(X, Y, Z));
    body.push_back(A(res1, Y, res2));
    Rule r = R(h, body);
    Rule rprime = R(h, body);
    ASSERT_TRUE(r.get() == rprime.get());
    ASSERT_TRUE(r == rprime);

    ASSERT_EMPTY(r->getFreeVariables());
    ASSERT_TRUE(r == r->apply(S() << M(V("Y"), I("u")) << M(V("Z"), I("s"))));
}

TEST(testRule2) {
    Variable X = V("X");
    Variable Y = V("Y");
    Variable Z = V("Z");
    ResourceByName T = I("T");
    ResourceByName U = I("U");
    std::vector<Atom> head;
    head.push_back(A(X, T, Y));
    head.push_back(A(Y, T, Z));
    std::vector<Literal> body;
    body.push_back(A(X, U, Y));
    body.push_back(A(Y, U, Z));
    Rule r = R(head, body);
    Rule rprime = R(head, body);
    ASSERT_TRUE(r.get() == rprime.get());
    ASSERT_TRUE(r == rprime);

    ASSERT_EMPTY(r->getFreeVariables());
    ASSERT_TRUE(r == r->apply(S() << M(V("Y"), I("u")) << M(V("Z"), I("s"))));
}

TEST(testRenameVariablesInSubquery) {
    size_t formulaWithImplicitExistentialVariablesCounter;
    formulaWithImplicitExistentialVariablesCounter = 0;
    Query q1 = Q("SELECT ?X WHERE { ?X rdf:type <A> . { SELECT ?X ?Z WHERE { ?X <R> ?Y . ?Y <S> ?Z } } }");
    Query q2 = q1->applyEx(S() << M(V("X"), V("Y")), true, formulaWithImplicitExistentialVariablesCounter);
    Query q3 = Q("SELECT ?Y WHERE { ?Y rdf:type <A> . { SELECT ?Y ?__SQ1__Z WHERE { ?Y <R> ?__SQ2__Y . ?__SQ2__Y <S> ?__SQ1__Z } } }");
    ASSERT_TRUE(q2 == q3);
}

TEST(testRenameVariablesInMinus) {
    size_t formulaWithImplicitExistentialVariablesCounter;
    formulaWithImplicitExistentialVariablesCounter = 0;
    Formula f1 = Q("SELECT ?X WHERE { ?X <R> ?Y . { ?X rdf:type <A> MINUS { ?X <S> ?Y } MINUS { ?X rdf:type <B> } MINUS { ?X <T> ?Y } } }")->getQueryFormula();
    Formula f2 = f1->applyEx(S() << M(V("X"), V("Z")), true, formulaWithImplicitExistentialVariablesCounter);
    Formula f3 = Q("SELECT ?X WHERE { ?Z <R> ?Y . { ?Z rdf:type <A> MINUS { ?Z <S> ?__SM1__Y } MINUS { ?Z rdf:type <B> } MINUS { ?Z <T> ?__SM2__Y } } }")->getQueryFormula();
    ASSERT_TRUE(f2 == f3);
}

#endif
