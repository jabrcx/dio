# -*- coding: utf-8 -*-

# Copyright (c) 2014, John A. Brunelle
# All rights reserved.


"""error handling

This could be more powerful by holding onto the exception and traceback 
objects, and extending text data from those, but for now I'm keeping this 
simple, with JSON-serializable data.  Still, an Error is a LazyDict, since I'm 
expecting at least a bit more functionality.
"""


import sys, traceback
import lazydict


#--- the Error lazy dict

class x_error_to_exception_type(lazydict.Extension):
	source = ('error',)
	target = ('exception_type',)
	def __call__(cls, error):
		return error.split(':',1)[0],

class x_error_to_exception_message(lazydict.Extension):
	source = ('error',)
	target = ('exception_message',)
	def __call__(cls, error):
		return error.split(':',1)[1].strip(),

class Error(lazydict.LazyDict):
	extensions = [
		x_error_to_exception_type(),
		x_error_to_exception_message(),
	]


#--- convenience functions

def e2d(e):
	"""Convert an exception to a dio dict.

	:param e: an exception object
	:type e: Exception

	:returns: an dio dict
	:rtype: Error

	Note that this must be called during the handling of the given exception, 
	since it grabs further information from the current stack frame and assumes
	it pertains to the given exception.
	"""

	return Error({
		'error': '%s: %s' % (e.__class__.__name__, str(e)),
		'traceback': ''.join(traceback.format_exception(*sys.exc_info())).strip(),
	})
	return Error({
		'exception': e
	})
