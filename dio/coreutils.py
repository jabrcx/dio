# -*- coding: utf-8 -*-

# Copyright (c) 2014, John A. Brunelle
# All rights reserved.

"""processors analogous to GNU coreutils"""


from dio import processor


@processor
def sort(out=None, err=None):
	"""The dio analogue to coreutils' sort.

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
