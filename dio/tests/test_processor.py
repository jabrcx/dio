# Copyright (c) 2014, John A. Brunelle
# All rights reserved.

"""unit tests"""

import sys, time, string, unittest
import mock

import dio.processor
from dio.processor import processor


#--- i/o redirection

stdout_l = []
@processor
def stdout_accumulator():
	global stdout_l
	while True:
		d = yield
		stdout_l.append(d)

stderr_l = []
@processor
def stderr_accumulator():
	global stderr_l
	while True:
		d = yield
		stderr_l.append(d)
dio.processor.STDOUT = stdout_accumulator()
dio.processor.STDERR = stderr_accumulator()


#--- examples

def source(stdout=dio.processor.STDOUT, stderr=dio.processor.STDERR):
	import string
	for c in string.ascii_letters:
		stdout.send({'letter':c})

@processor
def filter1(stdout=dio.processor.STDOUT, stderr=dio.processor.STDERR):
	while True:
		d = yield
		if d['letter'] in 'xyz':
			stdout.send(d)
		if d['letter'] in 'e':
			stderr.send({'error':'test error message for e'})

@processor
def filter2(stdout=dio.processor.STDOUT, stderr=dio.processor.STDERR):
	while True:
		d = yield
		if d['letter'] in 'xy':
			stdout.send(d)
		if d['letter'] in 'z':
			stderr.send({'error':'test error message for z'})

@processor
def verifier(stdout=dio.processor.STDOUT, stderr=dio.processor.STDERR):
	while True:
		d = yield
		assert d['letter'] in 'xy', "the filter did not produce the expected results"
		stdout.send(d)


#--- tests

class ProcessorTestCase(unittest.TestCase):
	def test_basics(self):
		source(filter1(filter2(verifier(dio.processor.STDOUT))))

		self.assertEqual(
			stdout_l,
			[
				{'letter': 'x'},
				{'letter': 'y'},
			]
		)
		self.assertEqual(
			stderr_l,
			[
				{'error': 'test error message for e'},
				{'error': 'test error message for z'},
			]
		)


if __name__=='__main__':
	unittest.main()
