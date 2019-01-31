'''
Module to assist building syntactically correct StepFunctions State Machines 
using Python's type system.

Comparison Operator classes are generated dynamically from resp. 
Base classes and added to the module's __dict__.

Author: Mike Gilson
Date: 10/13/17
'''

from abc import ABC, abstractmethod
import numbers
import sys


class Buildable(ABC):
    '''
    Abstract Base Class for building StateMachine
    '''
    @abstractmethod
    def build(self): pass


class StateMachine(Buildable):
    def __init__(self, name, start, states):
        self.name = name
        self.states = states
        self.start = start

    def build(self):
        return {
            'Comment': 'An example of the Amazon States Language using a choice state.',
            'StartAt': self.start,
            'States': self.states.build()
        }


class States(Buildable):

    def __init__(self, *states):
        self.states = states

    def build(self):
        return {state.name: state.build() for state in self.states}


#
# States must be named in the enclosing scope
#

class Named(Buildable):

    def __init__(self, name):
        self.name = name

    def build(self):
        '''Subclasses must implement'''
        pass


class State(Named):
    '''States are Buildable and Named'''
    pass


class Succeed(State):
    '''
    This is a terminal state with no properties, next, or end.
    '''
    def __init__(self, name):
        super().__init__(name)

    def build(self):
        return {
            'Type': 'Succeed'
        }


class Fail(State):
    '''
    This is a terminal state with no properties, next, or end.

    E.g.
    'FailState': {
      'Type': 'Fail',
      'Cause': 'Invalid response.',
      'Error': 'ErrorA'
    }
    '''
    def __init__(self, name, cause=None, error=None):
        super().__init__(name)
        self.cause = cause
        self.error = error

    def build(self):
        base = {
            'Type': 'Fail'
        }
        if self.cause:
            base['Cause'] = self.cause
        if self.error:
            base['Error'] = self.error
        return base


class Wait(State):
    def __init__(self, name, wait_time, next):
        super().__init__(name)
        self.wait_time = wait_time
        self.next = next

    def build(self):
        return {
            'Type': 'Wait',
            'Seconds': self.wait_time,
            'Next': self.next
        }


class Task(State):
    def __init__(self, name, resource, next=None, end=False, input_path=None, output_path=None, result_path=None):
        super().__init__(name)
        self.resource = resource
        self.next = next
        self.end = end
        self.input_path = input_path
        self.output_path = output_path
        self.result_path = result_path

    def build(self):
        if (not self.next) and (not self.end):
            raise ValueError('Must set either next or end')
        base = {
            'Type': 'Task',
            'Resource': self.resource,
        }
        if self.input_path:
            base['InputPath'] = self.input_path
        if self.output_path:
            base['OutputPath'] = self.output_path
        if self.result_path:
            base['ResultPath'] = self.result_path
        if self.end:
            base['End'] = True
            return base
        if self.next:
            base['Next'] = self.next
            return base


class Choice(State):

    def __init__(self, name, default, *choices, end=False):
        super().__init__(name)
        self.default = default
        self.choices = choices
        self.end = end

    def build(self):
        return {
            'Type': 'Choice',
            'Choices': [choice.build() for choice in self.choices],
            'Default': self.default
        }


class Rule(Buildable):

    def __init__(self, rule, next):
        self.rule = rule
        self.next = next

    def build(self):
        rule_build = self.rule.build()
        rule_build['Next'] = self.next
        return rule_build

#
# Base classes for unary and n-ary (variadic) boolean clauses
#


class UnaryBooleanClause(Buildable):

    def __init__(self, op, clause):
        self.op = op; self.clause = clause

    def build(self):
        return {self.op : self.clause.build()}


class VariadicBooleanClause(Buildable):

    def __init__(self, op, *clauses):
        self.op = op; self.clauses = clauses

    def build(self):
        return {self.op : [clause.build() for clause in self.clauses]}

#
# Implementations of BooleanClauses: Not, And, Or
#


class Not(UnaryBooleanClause):
    def __init__(self, clause):
        super().__init__('Not', clause)


class And(VariadicBooleanClause):
    def __init__(self, *clauses):
        super().__init__('And', *clauses)


class Or(VariadicBooleanClause):
    def __init__(self, *clauses):
        super().__init__('Or', *clauses)

#
# Base class for comparison predicates
#


class Predicate(Buildable):
    def __init__(self, op, var, val):
        self.op = op; self.var = var; self.val = val

    def build(self):
        return {
            'Variable': self.var,
            self.op: self.val
        }

#
# TODO: Predicate subclasses _should_ have their value types verified at some point, given the use case of this module.
#
# The flow is normally: Construct StateMachine from Types here -> .build into a Python dict -> json.dumps the dict
# To avoid explicit type checks, (see: https://www.oreilly.com/library/view/python-cookbook/0596001673/ch05s11.html)
# would have to know which methods are called by json.dumps on the dict.
#


class StringPredicate(Predicate):
    def __init__(self, op, var, val):
        if not isinstance(val, str):
            raise ValueError('StringPredicates requires value of type str')
        super().__init__(op, var, val)


class NumericPredicate(Predicate):
    def __init__(self, op, var, val):
        if not isinstance(val, numbers.Number):
            raise ValueError('NumericPredicates requires value of type Number')
        super().__init__(op, var, val)


class TimeStampPredicate(Predicate):
    def __init__(self, op, var, val):
        # TODO: how to handle RFC3339 profile ISO 8601 validation
        # See: https://stackoverflow.com/questions/8556398/generate-rfc-3339-timestamp-in-python/39418771#39418771
        super().__init__(op, var, val)


def predicate_subclass_factory(base_class, class_name):
    return type(
        class_name,
        (base_class,),
        {'__init__': lambda self, var, val, class_name=class_name: base_class.__init__(self, class_name, var, val)}
    )

string_predicates = (
    'StringEquals',
    'StringGreaterThan',
    'StringGreaterThanEquals',
    'StringLessThan',
    'StringLessThanEquals'
)

numeric_predicates = (
    'NumericEquals',
    'NumericGreaterThan',
    'NumericGreaterThanEquals',
    'NumericLessThan',
    'NumericLessThanEquals'
)

timestamp_predicates = (
    'TimestampEquals',
    'TimestampGreaterThan',
    'TimestampGreaterThanEquals',
    'TimestampLessThan',
    'TimestampLessThanEquals',
)

predicate_classes = [
    predicate_subclass_factory(base, predicate)
    for base, predicate in
    list(zip((StringPredicate,)*len(string_predicates), string_predicates)) +
    list(zip((NumericPredicate,)*len(numeric_predicates), numeric_predicates)) +
    list(zip((TimeStampPredicate,)*len(timestamp_predicates), timestamp_predicates))
]

current_module = sys.modules[__name__]
for predicate_subclass in predicate_classes:
    setattr(current_module, predicate_subclass.__name__, predicate_subclass)

