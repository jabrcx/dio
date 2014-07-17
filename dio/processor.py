# Copyright (c) 2014, John A. Brunelle
# All rights reserved.

"""pipelining using coroutines, and some standard processors"""


import sys


def processor(f):
	"""Prime a coroutine function by calling its next()."""
	def primed_f(*args,**kwargs):
		f2 = f(*args,**kwargs)
		f2.next()
		return f2
	return primed_f

@processor
def stdout_printer():
	while True:
		d = yield
		sys.stdout.write(str(d))
		sys.stdout.write('\n')
STDOUT = stdout_printer()

@processor
def stderr_printer():
	while True:
		d = yield
		sys.stderr.write(str(d))
		sys.stderr.write('\n')
STDERR = stderr_printer()
