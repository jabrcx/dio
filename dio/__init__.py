# Copyright (c) 2014, John A. Brunelle
# All rights reserved.

"""lazy dict-based i/o processing pipelines"""


import sys, types, functools


#--- setup logging

import logging
#(logging.NullHandler was introduced in 2.7, and this code is 2.6 compatible)
class NullHandler(logging.Handler):
    def emit(self, record):
        pass
logging.getLogger('dio').addHandler(NullHandler())


#--- decorators for the basic roles

def processor(f):
	"""Turn a processing function into a pipline participator.

	This serves two purposes:

	1) This assigns out and err, which all processors must expose as keyword
	   arguments, to the global default instances if no other value has been
	   given.  This accomplishes later binding than if they were ordinary
	   defaults in the original function definitions.

	2) Most processors are coroutines (extended generators) that get sent
	   input.  If f is a coroutine, this primes the coroutine so that it runs
	   until its first yield and is ready to get sent real input; this returns
	   the coroutine (generator object) for further use.

	   This is irrelevant for sources -- plain functions that don't get sent
	   any input.  In that case, calling the decorated function calls the
	   original function, which fully executes it, and there is nothing left to
	   use (this returns None).
	"""
	@functools.wraps(f)  #(for __doc__)
	def primed_f(*args,**kwargs):
		#set out and err to default values if not specified; default_out/err
		#won't be available for the standard sink processors, since they are
		#used to set default_out/err
		if 'default_out' in globals() and 'default_err' in globals():
			for k, v in (('out',default_out), ('err',default_err)):
				if k not in kwargs or kwargs[k] is None:
					kwargs[k] = v

		f2 = f(*args,**kwargs)

		#coroutine priming
		if isinstance(f2, types.GeneratorType):
			f2.send(None)
			return f2

	return primed_f

def restart_on_error(f):
	"""Decorate a processing function so that it's restarted upon any errors.

	Ideally, it would *continue* on error, but you can't resume a generator
	after it's raised an exception.  Therefore this restarts a new instance of
	the generator.

	This @restart_on_error decorator should be applied *before* (i.e. on a
	lower line) than the @processor decorator.
	"""
	def restarter(*args, **kwargs):
		while True:
			try:
				f2 = f(*args, **kwargs)
				f2.next()
				while True:
					d = yield
					f2.send(d)
			except Exception, e:
				#(GeneratorExit is not an Exception, just BaseException)
				kwargs['err'].send(e2d(e))
	return restarter


#--- exception handling

def e2d(e):
	"""Convert an Exception to a LazyDict.

	#FIXME this needs a lot of work -- it's really just a placeholder for now.
	"""
	return {'message': e.message}


#--- sources

@processor
def source(iterable, out=None, err=None):
	"""Turn any iterable into a source to start a processing pipeline."""
	for d in iterable:
		out.send(d)


#--- standard i/o

#though these have the form of standard processors, out and err should be
#file-like objects, not other processors.

#repr/eval (file-like)
import ast
@processor
def repr_in(inn=None, out=None, err=None):
	for line in inn:
		out.send(ast.literal_eval(line))
@processor
def repr_out(out=None, err=None):
	while True:
		d = yield
		out.write(repr(d)+'\n')

#pickle (file-like)
import cPickle
@processor
def pickle_in(inn=None, out=None, err=None):
	while True:
		try:
			out.send(cPickle.load(inn))
		except EOFError:
			break
@processor
def pickle_out(out=None, err=None):
	"""like other sinks, out and err should be file-like objects"""
	while True:
		d = yield
		cPickle.dump(d, out)

#json (file-like)
import json
@processor
def json_in(inn=sys.stdin, out=None, err=None):
	for line in inn:
		try:
			out.send(json.loads(line))
		except ValueError:
			if line.strip()!='':
				raise
@processor
def json_out(out=None, err=None):
	"""like other sinks, out and err should be file-like objects"""
	while True:
		d = yield
		json.dump(d, out)
		out.write('\n')

#buffers (iterable/appendable)
@processor
def buffer_in(inn=None, out=None, err=None):
	global buffer
	for d in inn:
		out.send(d)
@processor
def buffer_out(out=None, err=None):
	global buffer
	while True:
		d = yield
		out.append(d)

#--- default i/o source and sinks
#these are intended to be changed, if desired, at the beginning of a pipeline
default_in = json_in
default_out = json_out(out=sys.stdout)
default_err = json_out(out=sys.stderr)


#--- cli

def cli(p):
	"""Run any processor as a standalone cli instance in a shell pipeline.

	This works for both sources and pipeline participators.
	"""
	out=p()
	#if p is a generator, it needs input; get it from the default source
	if isinstance(out, types.GeneratorType):
		default_in(out=out)


#--- common constructs

@processor
def identity(out=None, err=None):
	"""Output all input dicts."""
	while True:
		d = yield
		out.send(d)


@processor
def filter(f, out=None, err=None):
	"""Output input dicts iff f(d) is True."""
	while True:
		d = yield
		try:
			if f(d): out.send(d)
		except Exception, e:
			err.send({'error':str(e)})


@processor
def apply(f, out=None, err=None):
	"""For each sent d, send all f(d).

	:param f: a callable that accepts a single input dict and yields zero or
		more output dicts.

	.. note::
		this overwrites the built-in `apply', but that's been deprecated since
		2.3.
	"""
	while True:
		d = yield
		try:
			for d2 in f(d):
				out.send(d2)
		except Exception, e:
			err.send({'error':str(e)})


#--- common reducers

def math(op):
	@functools.wraps(op)  #(for __doc__)
	def p(out=None, err=None):
		result = {}
		try:
			while True:
				d = yield
				for k, v in d.iteritems():
					try:
						result[k] = op(result.get(k), v)
					except KeyError:
						result[k] = 1
		except GeneratorExit:
			for i in result.items():
				out.send(dict((i,)))

	return p

@processor
@math
def count(x, v):
	"""Count appearences of each key."""
	return x + 1 if x is not None else 1

@processor
@math
def sum_(x, v):
	"""Sum values for each key."""
	return x + v if x is not None else v


#--- coreutils

@processor
def sort(out=None, err=None):
	"""Sort by value.

	Assumes each input is a one-item dict.
	"""
	result = []
	try:
		while True:
			d = yield
			result.append(d)
	except GeneratorExit:
		result.sort(key=lambda d: d.itervalues().next())
		for d in result:
			out.send(d)

@processor
def uniq(out=None, err=None):
	"""The dio equivalent of coreutils' uniq."""
	prev = None
	while True:
		d = yield
		if d!=prev:  #(always True the first time, when prev is None)
			out.send(d)
		prev = d

@processor
def wc(out=None, err=None):
	"""The dio processor analogous to coreutils' wc."""
	count = 0
	try:
		while True:
			d = yield
			count += 1
	except GeneratorExit:
		out.send({"count":count})
