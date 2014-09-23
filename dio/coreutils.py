# -*- coding: utf-8 -*-

# Copyright (c) 2014, John A. Brunelle
# All rights reserved.

"""processors analogous to GNU coreutils"""


from dio import processor


@processor
def sort(keys=[], reverse=False, out=None, err=None):
	"""The dio analogue to coreutils' sort.

	Sorts output by value of the given key.  If key is None, assumes all dicts
	are one-item, and sorts by the value regardless of key.
	"""
	if len(keys) == 0:
		key = None
	elif len(keys) == 1:
		key = keys[0]
	else:
		raise NotImplementedError(
			"sorting by more than one key is not yet implemented"
		)

	if key is not None:
		keyf = lambda d: d[key]
	else:
		keyf = lambda d: d.itervalues().next()

	results = []
	try:
		while True:
			d = yield
			results.append(d)
	except GeneratorExit:
		results.sort(key=keyf, reverse=reverse)
		for d in results:
			out.send(d)

@processor
def uniq(out=None, err=None):
	"""The dio analogue to coreutils' uniq."""
	prev = None
	while True:
		d = yield
		if d!=prev:  #(always True the first time, when prev is None)
			out.send(d)
		prev = d

@processor
def wc(out=None, err=None):
	"""The dio analogue to coreutils' wc."""
	count = 0
	try:
		while True:
			d = yield
			count += 1
	except GeneratorExit:
		out.send({"count":count})

@processor
def head(n, out=None, err=None):
	i = 0
	while True:
		d = yield
		i += 1
		out.send(d)
		if i==n:
			break
