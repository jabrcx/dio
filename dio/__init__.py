# -*- coding: utf-8 -*-

# Copyright (c) 2014, John A. Brunelle
# All rights reserved.


"""lazy dict-based i/o processing pipelines"""


import sys, types, functools, errno, bisect
import errors


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
	def g(*args,**kwargs):
		#set out and err to default values if not specified; default_out/err
		#won't be available for the standard sink processors, since they are
		#used to set default_out/err
		if 'default_out' in globals() and 'default_err' in globals():
			for k, v in (('out',default_out), ('err',default_err)):
				if k not in kwargs or kwargs[k] is None:
					kwargs[k] = v

		try:
			f2 = f(*args,**kwargs)
		except StopIteration:
			#f was a source, which ran the pipeline, and StopIteration has
			#bubbled up, which just means something down the pipeline has
			#stopped it prematurely; that's fine.
			pass
		else:
			#coroutine priming
			if isinstance(f2, types.GeneratorType):
				f2.send(None)
				return f2

	return g

def restart_on_error(f):
	"""Decorate a processing function so that it's restarted upon any errors.

	Ideally, it would *continue* on error, but you can't resume a generator
	after it's raised an exception.  Therefore this restarts a new instance of
	the generator.

	This @restart_on_error decorator should be applied *before* (i.e. on a
	lower line) than the @processor decorator.
	"""
	def g(*args, **kwargs):
		while True:
			try:
				f2 = f(*args, **kwargs)
				f2.next()
				while True:
					d = yield
					f2.send(d)
			except (GeneratorExit, StopIteration):
				break
			except Exception, e:
				kwargs['err'].send(errors.e2d(e))
	return g

def suppress_epipe(f):
	def g(*args, **kwargs):
		try:
			f2 = f(*args, **kwargs)
			f2.next()
			while True:
				d = yield
				f2.send(d)
		except IOError, e:
			if e.errno != errno.EPIPE:
				raise
	return g


#--- sources

@processor
def source(iterable, out=None, err=None):
	"""Turn any iterable into a source to start a processing pipeline."""
	for d in iterable:
		out.send(d)


#--- serialization

def pre_serialize(d):
	if type(d) != dict:
		d['__class__'] = '.'.join((d.__class__.__module__, d.__class__.__name__))
	return d

def post_deserialize(d):
	#LBYL since optimizing for built-in dicts
	if d.has_key('__class__'):
		m, c = d['__class__'].rsplit('.',1)  #assuming all subclasses are in modules
		m = __import__(m, fromlist=[None])  #any non-empty fromlist allows importing from a package hierarchy
		d = getattr(m, c)(d)
	return d


#--- standard i/o

#though these have the form of standard processors, out and err should be
#file-like objects, not other processors.

#repr/eval (file-like)
import ast
@processor
def repr_in(inn=None, out=None, err=None):
	for line in inn:
		out.send(post_deserialize(ast.literal_eval(line)))
@processor
@suppress_epipe
def repr_out(out=None, err=None):
	while True:
		d = yield
		out.write(repr(pre_serialize(d))+'\n')

#pickle (file-like)
import cPickle
@processor
def pickle_in(inn=None, out=None, err=None):
	while True:
		try:
			out.send(post_deserialize(cPickle.load(inn)))
		except EOFError:
			break
@processor
@suppress_epipe
def pickle_out(out=None, err=None):
	"""like other sinks, out and err should be file-like objects"""
	while True:
		d = yield
		cPickle.dump(pre_serialize(d), out)

#json (file-like)
import json
@processor
def json_in(inn=sys.stdin, out=None, err=None):
	for line in inn:
		try:
			out.send(post_deserialize(json.loads(line)))
		except ValueError:
			if line.strip()!='':
				raise
@processor
@suppress_epipe
def json_out(out=None, err=None):
	"""like other sinks, out and err should be file-like objects"""
	while True:
		d = yield
		json.dump(pre_serialize(d), out)
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


#--- common constructs

@processor
@restart_on_error
def identity(out=None, err=None):
	"""Output all input dicts."""
	while True:
		d = yield
		out.send(d)

@processor
@restart_on_error
def filter(f, out=None, err=None):
	"""Output input dicts iff f(d) is True.

	:param f: a callable that accepts a single input dict and returns the
		boolean of whether or not to send the given dict.
	"""
	while True:
		d = yield
		if f(d): out.send(d)

@processor
@restart_on_error
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
		for d2 in f(d):
			out.send(d2)

@processor
@restart_on_error
def tidy(keys, out=None, err=None):
	"""Get all the given keys and drop all other keys.

	:param: keys: the keys of interest
	:type: keys: an iterable

	The given keys are used, thus triggering any extensions needed to compute
	them.  Thus, this is not simply called `strip` -- it may add data to the
	dicts, too.

	Having all the given keys is not required.
	"""
	keys = set(keys)
	while True:
		d = yield
		for k in keys:
			#EAFP since assuming caller expects these keys
			try:
				d[k]  #trigger any extensions needed to compute it
			except KeyError:
				pass
		for k in set(d.keys()) - keys:
			d.pop(k, None)  #(the key will always be there, but it not being there is not an error per se)
		out.send(d)

@processor
@restart_on_error
def strip(keys, out=None, err=None):
	"""Drop the given keys.

	:param: keys: the keys to drop
	:type: keys: an iterable

	The given keys are used, thus triggering any extensions needed to compute
	them.  Thus, this is not simply called `strip` -- it may add data to the
	dicts, too.

	Having all the given keys is not required.
	"""
	keys = set(keys)
	while True:
		d = yield
		for k in keys:
			d.pop(k, None)
		out.send(d)


#--- common reducers

@processor
def count(out=None, err=None):
	"""Count appearances of each key."""
	result = {}
	try:
		while True:
			d = yield
			for k, v in d.iteritems():
				result[k] = result.get(k,0) + 1
	except GeneratorExit:
		for i in result.iteritems():
			out.send(dict((i,)))

@processor
def sum_(out=None, err=None):
	"""Sum values for each key."""
	result = {}
	try:
		while True:
			d = yield
			for k, v in d.iteritems():
				result[k] = result.get(k,0) + v
	except GeneratorExit:
		for i in result.iteritems():
			out.send(dict((i,)))

@processor
def average(out=None, err=None):
	"""Sum values for each key."""
	counts = {}  #for each key, Σ1 for all values v for that key
	sums = {}  #for each key, Σv for all values v for that key
	sums_sqs = {}  #for each key, Σ(v²) for all values v for that key
	try:
		while True:
			d = yield
			for k, v in d.iteritems():
				counts[k] = counts.get(k,0) + 1
				sums[k] = sums.get(k,0) + v
				sums_sqs[k] = sums_sqs.get(k,0) + v**2
	except GeneratorExit:
		count = len(sums)
		##2.6 doesn't have dict comprehensions
		#averages = { k: float(v)/counts[k] for k, v in sums.iteritems() }
		averages = dict((k,float(v)/counts[k]) for k, v in sums.iteritems())
		for i in averages.iteritems():
			out.send(dict((i,)))

@processor
def min_(n, key, out=None, err=None):
	"""Emit the n dicts with the max values for key.

	The results are currently in sorted order, but that may change back to
	input order in the future.
	"""
	#FIXME replace this by an efficient algorithm / data structure, like a
	#binary tree.  The insert/pop operations below are O(n) on result set size
	#(but at least result set size does not scale with input size).

	#NOTE -- keep this code in sync with max_!!! (probably should be combined)

	results_d = []  #list of result d
	results_v = []  #list of d[key] for the results

	bubble_value = None  #the current threshold value (inside the bubble)

	try:
		while True:
			d = yield
			v = d[key]

			if v < bubble_value or bubble_value is None:
				#insert the new value
				pos = bisect.bisect_left(results_v, v)
				results_d.insert(pos, d)
				results_v.insert(pos, v)

				#remove the old bubble value (if necessary)
				if len(results_v) > n:
					pos = bisect.bisect_right(results_v, bubble_value)
					results_d.pop()
					results_v.pop()

					#set the new bubble value
					bubble_value = results_v[-1]
	except GeneratorExit:
		for d in results_d:
			out.send(d)

@processor
def max_(n, key, out=None, err=None):
	"""Emit the n dicts with the max values for key.

	The results are currently in sorted order, but that may change back to
	input order in the future.
	"""

	#FIXME replace this by an efficient algorithm / data structure, like a
	#binary tree.  The insert/pop operations below are O(n) on result set size
	#(but at least result set size does not scale with input size).

	#NOTE -- keep this code in sync with min_!!! (probably should be combined)

	results_d = []  #list of result d
	results_v = []  #list of d[key] for the results

	bubble_value = None  #the current threshold value (inside the bubble)

	try:
		while True:
			d = yield
			v = d[key]

			if v > bubble_value or bubble_value is None:
				#insert the new value
				pos = bisect.bisect_right(results_v, v)
				results_d.insert(pos, d)
				results_v.insert(pos, v)

				#remove the old bubble value (if necessary)
				if len(results_v) > n:
					pos = bisect.bisect_left(results_v, bubble_value)
					results_d.pop(0)
					results_v.pop(0)

					#set the new bubble value
					bubble_value = results_v[0]
	except GeneratorExit:
		for d in results_d:
			out.send(d)
