# Copyright (c) 2014, John A. Brunelle
# All rights reserved.

"""pipelining using coroutines, and some standard processors"""


import sys


def processor(f):
	"""Prime a coroutine function by calling it and its next()."""
	def primed_f(*args,**kwargs):
		f2 = f(*args,**kwargs)
		f2.next()
		return f2
	return primed_f

@processor
def _STDERR():
	while True:
		d = yield
		sys.stderr.write(str(d))
		sys.stderr.write('\n')
STDERR = _STDERR()

@processor
def _STDOUT():
	while True:
		d = yield
		sys.stdout.write(str(d))
		sys.stdout.write('\n')
STDOUT = _STDOUT()
