# Copyright (c) 2014, John A. Brunelle
# All rights reserved.

"""lazy dict-based i/o processing pipelines"""


import sys, operator, functools, cPickle


#--- decorators for the basic roles

def processor(f):
	"""Decorate a processing function.

	This is for extended generator functions that get sent input and send
	output.  This sets up its i/o and primes it such that it's ready to be sent
	input.
	"""
	@functools.wraps(f)  #(for __doc__)
	def primed_f(*args,**kwargs):
		#set out and err to default values if not specified; this accomplishes
		#later binding than if they were simple defaults in the original
		#function definition; default_out/err won't be available for the
		#standard sink processors, since they are used to set default_out/err
		if 'default_out' in globals() and 'default_err' in globals():
			for k, v in (('out',default_out), ('err',default_err)):
				if k not in kwargs or kwargs[k] is None:
					kwargs[k] = v

		f2 = f(*args,**kwargs)

		#coroutine priming
		f2.next()

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

#Sources are not normal processors (i.e. are not coroutines) since they don't
#get sent data.  (Faking it by including an unreachable yield won't work since
#the standard @processor priming will fully run them, raising StopIteration.)

def source(iterable, out=None, err=None):
	"""Turn any iterable into a source to start a processing pipeline."""

	#set default i/o, since this is not taken care of by a decorator
	global default_out
	if out is None: out = default_out
	global default_err
	if err is None: err = default_err

	#send each
	for x in iterable:
		out.send(x)


#--- standard sinks

#though these have the form of standard processors, out and err should be
#file-like objects, not other processors.

#print
@processor
def out_printer(out=sys.stdout, err=None):
	while True:
		d = yield
		out.write(str(d))
		out.write('\n')
@processor
def err_printer(out=None, err=sys.stderr):
	while True:
		d = yield
		err.write(str(d))
		err.write('\n')

#accumulate
accumulated_out = []
@processor
def out_accumulator(out=None, err=None):
	global accumulated_out
	while True:
		d = yield
		accumulated_out.append(d)
accumulated_err = []
@processor
def err_accumulator(out=None, err=None):
	global accumulated_err
	while True:
		d = yield
		accumulated_err.append(d)


#--- serialization

#to/from stdout/stdin w/ pickle
def in_pickle(inn=sys.stdin, out=None, err=None):
	#set default i/o, since this is not taken care of by a decorator
	global default_out
	if out is None: out = default_out
	global default_err
	if err is None: err = default_err

	#send each
	while True:
		try:
			out.send(cPickle.load(inn))
		except EOFError:
			break
@processor
def out_pickle(out=None, err=None):
	"""like other sinks, out and err should be file-like objects"""
	while True:
		d = yield
		cPickle.dump(d, out)

#to/from stdout/stdin w/ json
import json
def in_json(inn=sys.stdin, out=None, err=None):
	#set default i/o, since this is not taken care of by a decorator
	global default_out
	if out is None: out = default_out
	global default_err
	if err is None: err = default_err

	#send each
	for line in inn.readlines():
		try:
			out.send(json.loads(line))
		except ValueError:
			if line.strip()!='':
				raise
@processor
def out_json(out=sys.stdout, err=None):
	"""like other sinks, out and err should be file-like objects"""
	while True:
		d = yield
		json.dump(d, out)
		out.write('\n')


#--- default i/o

#the default output and error sinks
#these are intended to be changed, if desired, at the beginning of a pipeline
default_in = in_json
default_out = out_json(out=sys.stdout)
default_err = out_json(out=sys.stderr)


#--- cli

def cli(p):
	"""Run any processor as a standalone cli instance in a shell pipeline."""
	default_in(out=p())


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
