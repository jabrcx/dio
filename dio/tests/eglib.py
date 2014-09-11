# -*- coding: utf-8 -*-

# Copyright (c) 2014, John A. Brunelle
# All rights reserved.

"""an example LazyDict with Extensions"""


import time, itertools

from dio import lazydict, processor


#--- an example LazyDict

class x_age(lazydict.Extension):
	"""A very typical, one-to-one extension."""
	source= ('birthdate',)
	target = ('age',)
	def __call__(cls, birthdate):
		return time.time() - birthdate,

class x_math(lazydict.Extension):
	"""A very typical, many-to-many extension."""
	source= ('x', 'y',)
	target = ('sum', 'diff',)
	def __call__(cls, x, y):
		return x+y, x-y

class x_indirect1of2(lazydict.Extension):
	source= ('a',)
	target = ('b',)
	def __call__(cls, a):
		return a+1,
class x_indirect2of2(lazydict.Extension):
	source= ('b',)
	target = ('c',)
	def __call__(cls, b):
		return b+1,

class x_identity(lazydict.Extension):
	"""A simple identity/rename extension.

	Note that there is no implementation -- this is for testing the default
	behavior.
	"""
	source= ('name',)
	target = ('name_copy',)

class x_never(lazydict.Extension):
	"""An extension where the target never actually gets computed."""
	source= ('name',)
	target = ('never',)
	def __call__(self, name):
		return None,

class ExampleLazyDict(lazydict.LazyDict):
	_keys = [
		#--- primary

		'name',
		#a string

		'birthdate',
		#a float, seconds since the epoch

		'x',
		#an int

		'y',
		#an int

		'a',
		#a number


		#--- derived

		'age',
		#an float, seconds since birthdate

		'sum',
		#x+y

		'diff',
		#x-y

		'name_copy',
		#just a copy of the name

		'b',
		#a+1

		'c',
		#b+1
	]

	extensions = [
		x_age(),
		x_math(),
		x_identity(),
		x_indirect1of2(),
		x_indirect2of2(),
	]


#--- some processors

@processor
def send_one(out=None, err=None):
	out.send(ExampleLazyDict(x=3, y=5))

@processor
def send_infinite(out=None, err=None):
	for d in itertools.repeat(ExampleLazyDict(x=3, y=5)):
		out.send(d)

@processor
def x_gt_10(out=None, err=None):
	while True:
		d = yield
		if d['x'] > 10:
			out.send(d)

@processor
def y_gt_10(out=None, err=None):
	while True:
		d = yield
		if d['y'] > 10:
			out.send(d)
