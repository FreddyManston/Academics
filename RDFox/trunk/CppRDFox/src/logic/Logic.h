// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef LOGIC_H_
#define LOGIC_H_

#include "../util/SmartPointer.h"

// Forward declarations

class LogicObjectVisitor;

class _LogicFactory;
typedef SmartPointer<_LogicFactory> LogicFactory;

class _LogicObject;
typedef SmartPointer<const _LogicObject> LogicObject;

class _BuiltinExpression;
typedef SmartPointer<const _BuiltinExpression> BuiltinExpression;

class _BuiltinFunctionCall;
typedef SmartPointer<const _BuiltinFunctionCall> BuiltinFunctionCall;

class _ExistenceExpression;
typedef SmartPointer<const _ExistenceExpression> ExistenceExpression;

class _Term;
typedef SmartPointer<const _Term> Term;

class _Variable;
typedef SmartPointer<const _Variable> Variable;

class _GroundTerm;
typedef SmartPointer<const _GroundTerm> GroundTerm;

class _ResourceByID;
typedef SmartPointer<const _ResourceByID> ResourceByID;

class _ResourceByName;
typedef SmartPointer<const _ResourceByName> ResourceByName;

class _Predicate;
typedef SmartPointer<const _Predicate> Predicate;

class _Formula;
typedef SmartPointer<const _Formula> Formula;

class _Literal;
typedef SmartPointer<const _Literal> Literal;

class _AtomicFormula;
typedef SmartPointer<const _AtomicFormula> AtomicFormula;

class _Atom;
typedef SmartPointer<const _Atom> Atom;

class _Bind;
typedef SmartPointer<const _Bind> Bind;

class _Filter;
typedef SmartPointer<const _Filter> Filter;

class _Negation;
typedef SmartPointer<const _Negation> Negation;

class _AggregateBind;
typedef SmartPointer<const _AggregateBind> AggregateBind;

class _Aggregate;
typedef SmartPointer<const _Aggregate> Aggregate;

class _Conjunction;
typedef SmartPointer<const _Conjunction> Conjunction;

class _Disjunction;
typedef SmartPointer<const _Disjunction> Disjunction;

class _Optional;
typedef SmartPointer<const _Optional> Optional;

class _Minus;
typedef SmartPointer<const _Minus> Minus;

class _Values;
typedef SmartPointer<const _Values> Values;

class _Query;
typedef SmartPointer<const _Query> Query;

class _Rule;
typedef SmartPointer<const _Rule> Rule;

// Useful definitions

typedef std::vector<Rule> DatalogProgram;
typedef std::unordered_map<Variable, Term> Substitution;

// The relevant includes

#include "LogicFactory.h"
#include "LogicObject.h"
#include "BuiltinExpression.h"
#include "BuiltinFunctionCall.h"
#include "ExistenceExpression.h"
#include "Term.h"
#include "Variable.h"
#include "GroundTerm.h"
#include "ResourceByID.h"
#include "ResourceByName.h"
#include "Predicate.h"
#include "Formula.h"
#include "Literal.h"
#include "AtomicFormula.h"
#include "Atom.h"
#include "Bind.h"
#include "Filter.h"
#include "Negation.h"
#include "AggregateBind.h"
#include "Aggregate.h"
#include "Conjunction.h"
#include "Disjunction.h"
#include "Optional.h"
#include "Minus.h"
#include "Values.h"
#include "Query.h"
#include "Rule.h"
#include "LogicObjectVisitor.h"

#endif // LOGIC_H_
