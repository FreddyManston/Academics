// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifdef WITH_TEST

#define SUITE_NAME        TermArrayTest

#include <CppTest/AutoTest.h>

#include "../../src/util/MemoryManager.h"
#include "../../src/dictionary/Dictionary.h"
#include "../../src/logic/Logic.h"
#include "../../src/querying/TermArray.h"

TEST(testTermArray) {
    LogicFactory logicFactory = newLogicFactory();
    Variable X = logicFactory->getVariable("X");
    Variable Y = logicFactory->getVariable("Y");
    Variable Z = logicFactory->getVariable("Z");
    ResourceByID r1 = logicFactory->getResourceByID(1);
    ResourceByName r2 = logicFactory->getIRIReference("abc");
    ResourceByName r3 = logicFactory->getBlankNode("abc");
    ResourceByName r4 = logicFactory->getIRIReference("def");

    Atom a1 = logicFactory->getRDFAtom(X, r1, r2);
    Atom a2 = logicFactory->getRDFAtom(Y, X, r3);
    Atom a3 = logicFactory->getRDFAtom(r2, Y, r3);
    Conjunction c1 = logicFactory->getConjunction(a1, a2);
    Conjunction c2 = logicFactory->getConjunction(c1, a3);
    std::vector<Term> arguments;
    arguments.push_back(r4);
    arguments.push_back(X);
    arguments.push_back(Z);
    Query q = logicFactory->getQuery(false, arguments, c2);

    TermArray termArray(q);
    ASSERT_EQUAL(0, termArray.getPosition(r4));
    ASSERT_EQUAL(1, termArray.getPosition(X));
    ASSERT_EQUAL(2, termArray.getPosition(Z));
    ASSERT_EQUAL(3, termArray.getPosition(r1));
    ASSERT_EQUAL(4, termArray.getPosition(r2));
    ASSERT_EQUAL(5, termArray.getPosition(Y));
    ASSERT_EQUAL(6, termArray.getPosition(r3));
}

TEST(testTermArrayWithVectorConstructor) {
    LogicFactory logicFactory = newLogicFactory();
    Variable X = logicFactory->getVariable("X");
    Variable Y = logicFactory->getVariable("Y");
    ResourceByID r1 = logicFactory->getResourceByID(1);
    ResourceByName r2 = logicFactory->getIRIReference("abc");
    ResourceByName r3 = logicFactory->getBlankNode("abc");

    Atom a1 = logicFactory->getRDFAtom(X, r1, r2);
    Atom a2 = logicFactory->getRDFAtom(Y, X, r3);
    Atom a3 = logicFactory->getRDFAtom(r2, Y, r3);
    Conjunction c1 = logicFactory->getConjunction(a1, a2);
    Conjunction c2 = logicFactory->getConjunction(c1, a3);
    std::vector<Formula> formulas;
    formulas.push_back(c1);
    formulas.push_back(c2);

    TermArray termArray(formulas);
    ASSERT_EQUAL(0, termArray.getPosition(X));
    ASSERT_EQUAL(1, termArray.getPosition(r1));
    ASSERT_EQUAL(2, termArray.getPosition(r2));
    ASSERT_EQUAL(3, termArray.getPosition(Y));
    ASSERT_EQUAL(4, termArray.getPosition(r3));
}

#endif
