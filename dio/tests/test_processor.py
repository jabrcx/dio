# Copyright (c) 2014, John A. Brunelle
# All rights reserved.

"""unit tests"""

import sys, time, string, unittest
import mock

from dio.processor import processor, STDOUT, STDERR


#--- examples

def source(stdout=STDOUT, stderr=STDERR):
	import string
	for c in string.ascii_letters:
		stdout.send(dict(letter=c))

@processor
def filter1(stdout=STDOUT, stderr=STDERR):
	while True:
		d = yield
		if d['letter'] in 'xyz':
			stdout.send(d)
		if d['letter'] in 'e':
			stderr.send({'error':'test error message for e'})

@processor
def filter2(stdout=STDOUT, stderr=STDERR):
	while True:
		d = yield
		if d['letter'] in 'xy':
			stdout.send(d)
		if d['letter'] in 'z':
			stderr.send({'error':'test error message for z'})

@processor
def verifier(stdout=STDOUT, stderr=STDERR):
	while True:
		d = yield
		assert d['letter'] in 'xy', "the filter did not produce the expected results"


#--- tests

class ProcessorTestCase(unittest.TestCase):
	def test_basics(self):
		source(filter1(filter2(verifier())))  #this prints to stderr


if __name__=='__main__':
	unittest.main()
