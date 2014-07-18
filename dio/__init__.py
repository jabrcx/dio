# Copyright (c) 2014, John A. Brunelle
# All rights reserved.

"""lazy dict-based i/o processing pipelines"""


import sys


#--- decorators for the basic roles

def processor(f):
	"""Decorate a processing function.

	This is for functions that get sent input and send output.  This sets up 
	its i/o and primes it such that it's ready to be sent input.
	"""
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


#--- standard sinks

#print
@processor
def out_printer(out=None, err=None):
	while True:
		d = yield
		sys.stdout.write(str(d))
		sys.stdout.write('\n')
@processor
def err_printer(out=None, err=None):
	while True:
		d = yield
		sys.stderr.write(str(d))
		sys.stderr.write('\n')

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

#the default output and error sinks
#these are intended to be changed, if desired, at the beginning of a pipeline
default_out = out_printer()
default_err = err_printer()


#--- sources

#Sources are not normal processors (i.e. are not coroutines) since they don't 
#get sent data.  (Faking it by including an unreachable yield won't work since 
#the standard @processor priming will fully run them, raising StopIteration.)

def source(iterable, out=None, err=None):
	"""Turn any iterable into a source to start a processing pipeline."""
	for x in iterable:
		out.send(x)


#--- common constructs

@processor
def filter(f, out=None, err=None):
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


#--- coreutils

@processor
def uniq(out=None, err=None):
	"""The dio equivalent of coreutils' uniq."""
	prev = None
	while True:
		d = yield
		if d!=prev:  #(always True the first time, when prev is None)
			out.send(d)
		prev = d
